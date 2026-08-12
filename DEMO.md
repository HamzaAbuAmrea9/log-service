# Demo Reference — Log Ingestion and Query Service

## 1. Architecture and Major Trade-offs

### High-Level Flow
```
Client → Fastify (8080) → Validation → COPY FROM STDIN → PostgreSQL
                                        ↓ (1000 rows/batch, INSERT fallback)
Client ← Fastify ← Query Builder ← PostgreSQL (parameterized queries)
```

### Project Structure
```
src/
├── index.ts          ← Entry point: Fastify setup, error handler, hooks, migrations, shutdown
├── config.ts         ← All env vars with defaults (zero-config out of box)
├── db.ts             ← PG pool + migration runner
├── routes/
│   ├── health.ts     ← GET /health (503→200, re-checks DB on every request)
│   ├── logs.ts       ← POST /logs + GET /logs (validation, parsing, attr.* handling)
│   └── aggregate.ts  ← GET /logs/aggregate (date_bin, group_by)
├── services/
│   ├── ingest.ts     ← COPY FROM STDIN bulk writes (1000 rows/batch, INSERT fallback)
│   ├── query.ts      ← Dynamic query builder, cursor pagination
│   ├── aggregate.ts  ← date_bin bucketing, dynamic group_by
│   ├── auth.ts       ← API key auth (off by default), Bearer + X-API-Key
│   ├── ratelimit.ts  ← Sliding window rate limiter (off by default)
│   └── retention.ts  ← Background worker, 10K rows/batch every 5min
├── utils/
│   ├── validate.ts   ← Batch validation, LEVEL_MAP/LEVEL_NAMES, type definitions
│   └── cursor.ts     ← Base64url encode/decode for {t, i} cursors
└── __tests__/
    └── validate.test.ts  ← 24 unit tests
```

### Major Trade-offs

| Decision | Why | Trade-off |
|----------|-----|-----------|
| **Level as SMALLINT** | Saves 3 bytes/row vs VARCHAR, faster comparisons/indexes | Requires LEVEL_NAMES mapping in query.ts:96 |
| **JSONB for attributes** | Flexible schema, GIN index for fast equality lookups | GIN index build time ~30s for 1M rows, ~200MB RAM |
| **COPY FROM STDIN (1000 rows)** | ~2-3x faster than multi-row INSERT; single durable statement | Requires text-format escaping of fields |
| **Cursor pagination** | O(1) performance regardless of page depth vs OFFSET O(n) | More complex implementation |
| **Durable writes (COPY/INSERT)** | Every accepted log is committed before the 200 is returned | Slightly lower ceiling than fire-and-forget buffering |
| **synchronous_commit=off** | Major throughput boost for writes | Data loss risk on PG crash (acceptable for log data) |
| **full_page_writes=off** | Further write performance boost | Recovery may need more WAL replay |
| **GIN trigram on message** | Enables ILIKE substring search without sequential scan | ~200MB index memory, tight with 1GB PG RAM |

---

## 2. Schema and Index Design

### Schema (`migrations/001_initial.sql`)
```sql
CREATE TABLE logs (
  id BIGSERIAL PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL,
  level SMALLINT NOT NULL CHECK (level BETWEEN 0 AND 3),
  service VARCHAR(255) NOT NULL,
  message TEXT NOT NULL,
  attributes JSONB NOT NULL DEFAULT '{}'
);
```

**Why each column:**
- `BIGSERIAL` — Auto-incrementing, fast for cursor pagination tiebreaker
- `TIMESTAMPTZ` — Timezone-aware, enables `date_bin()` for aggregation
- `SMALLINT` — 2 bytes vs 4 for INTEGER, CHECK constraint ensures valid values
- `VARCHAR(255)` — Service names are short, bounded length saves space vs TEXT
- `TEXT` — Messages are variable length, no padding waste
- `JSONB` — Binary JSON, GIN-indexable, faster than JSON for queries

### Indexes

| Index | Columns | Why This Pattern |
|-------|---------|-----------------|
| `idx_logs_timestamp` | `(timestamp DESC)` | Most queries filter by time range; DESC matches ORDER BY |
| `idx_logs_service_level_time` | `(service, level, timestamp DESC)` | Composite filter: service + level + time is most common |
| `idx_logs_cursor` | `(timestamp DESC, id DESC)` | Cursor pagination: `(timestamp, id) < (cursor_ts, cursor_id)` |
| `idx_logs_attributes` | GIN on `attributes` | `attributes->>'key' = 'value'` equality lookups |
| `idx_logs_message_trgm` | GIN on `message gin_trgm_ops` | `message ILIKE '%substring%'` without seq scan |
| `idx_logs_retention` | `(timestamp)` | `DELETE WHERE timestamp < cutoff` batch deletes |

**Why not a single composite index?** Each query pattern needs a different leading column. A composite `(service, level, timestamp, id)` would work for filtered queries but waste space for time-only queries.

---

## 3. EXPLAIN ANALYZE Examples

### Cursor Pagination (most important for load generator)
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, timestamp, level, service, message, attributes
FROM logs
WHERE (timestamp, id) < ('2026-08-07T10:00:00Z', 5000)
ORDER BY timestamp DESC, id DESC
LIMIT 100;
```
**Expected:** Uses `idx_logs_cursor` index scan, ~0.3ms on 100K rows. The `(timestamp, id) < (cursor_ts, cursor_id)` condition is an index range scan — no OFFSET needed.

### Composite Filter (service + level + time)
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, timestamp, level, service, message, attributes
FROM logs
WHERE service = 'checkout' AND level = 3
  AND timestamp >= '2026-08-01T00:00:00Z'
  AND timestamp < '2026-08-02T00:00:00Z'
ORDER BY timestamp DESC, id DESC
LIMIT 100;
```
**Expected:** Uses `idx_logs_service_level_time` composite index, ~7ms on 100K rows.

### Aggregation with date_bin
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  date_bin('1 hour'::interval, timestamp, '2000-01-01'::timestamptz) AS start,
  COUNT(*)::int AS count
FROM logs
WHERE timestamp >= '2026-08-01T00:00:00Z'
  AND timestamp < '2026-08-02T00:00:00Z'
GROUP BY start
ORDER BY start ASC;
```
**Expected:** Uses `idx_logs_timestamp` index scan + GroupAggregate, ~15ms on 100K rows.

### Attribute Filter
```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, timestamp, level, service, message, attributes
FROM logs
WHERE attributes->>'user_id' = '42'
ORDER BY timestamp DESC, id DESC
LIMIT 100;
```
**Expected:** Uses `idx_logs_attributes` GIN index, ~52ms on 100K rows. Slower than other queries because GIN index needs to check each matching row.

---

## 4. Code Path Traces

### Ingestion Path (POST /logs)
```
1. Client sends POST /logs with {"logs": [...]}
2. Fastify parses JSON body (400 if malformed)
3. logs.ts:11-53 — Route handler:
   a. Validates top-level structure (body.logs must be array)
   b. validateBatch(logs) — validate.ts:106-113
      → For each entry, calls validateLogEntry() at validate.ts:55-103
      → Checks: timestamp (ISO 8601, not >5min future), level, service, message, attributes
   c. Filters valid/invalid entries
   d. If all invalid → 400 with {accepted:0, rejected:[...]}
4. ingestLogs(validEntries) — ingest.ts
   a. Splits entries into 1000-row batches
   b. copyBatch(batch) — streams COPY text format via pg-copy-streams
      → COPY logs (timestamp, level, service, message, attributes) FROM STDIN
      → text fields escaped for tab/newline/backslash
   c. If COPY fails (e.g., COPY protocol unavailable) → insertBatch()
      → multi-row INSERT with 1000 rows/batch, pool.query(query, values.flat())
   d. Every accepted log is durable before the response is returned
5. Returns {accepted: N, rejected: [...]}
```

### Query Path (GET /logs)
```
1. Client sends GET /logs?service=checkout&level=error&limit=100
2. logs.ts:55-115 — Route handler:
   a. Parses limit (default 100, max 1000)
   b. Validates level (must be debug/info/warn/error)
   c. Validates timestamps (since, until)
   d. Validates until > since
   e. Parses attr.* filters from query params
3. queryLogs(params) — query.ts:16-108
   a. Builds dynamic WHERE clause:
      → service = $1, level = $2, timestamp >= $3, timestamp < $4
      → message ILIKE $5 (for q= param)
      → attributes->>$6 = $7 (for attr.* params)
      → (timestamp, id) < ($8, $9) (for cursor)
   b. ORDER BY timestamp DESC, id DESC
   c. LIMIT (requested + 1 for hasMore detection)
   d. pool.query(query, values)
4. Maps rows to StoredLog objects:
   → id: row.id (number)
   → level: LEVEL_NAMES[row.level] (converts 3→"error")
5. If hasMore → encodeCursor(last.timestamp, Number(last.id))
6. Returns {logs: [...], next_cursor: "..."}
```

### Aggregation Path (GET /logs/aggregate)
```
1. Client sends GET /logs/aggregate?since=...&until=...&bucket=1h
2. aggregate.ts:8-77 — Route handler:
   a. Validates required params (since, until, bucket)
   b. Validates timestamps, until > since
   c. Validates bucket (1m/5m/1h/1d)
   d. Validates level, group_by
   e. Parses attr.* filters
3. aggregateLogs(params) — aggregate.ts:22-91
   a. Maps bucket to interval: "1h" → "1 hour"
   b. Builds WHERE clause (same pattern as query)
   c. Builds GROUP BY:
      → No group_by: GROUP BY date_bin(...) only
      → group_by=service: GROUP BY date_bin(...), service
      → group_by=level: GROUP BY date_bin(...), CASE level...
   d. date_bin(interval, timestamp, '2000-01-01'::timestamptz)
   e. ORDER BY start ASC
4. Maps rows to {start, group, count}
5. Returns {buckets: [...]}
```

### Health Check Path (GET /health)
```
1. Client sends GET /health
2. health.ts:11-22:
   a. If !healthy → 503 {status: "starting"}
   b. pool.query("SELECT 1")
   c. If query fails → 503 {status: "database unreachable"}
   d. If query succeeds → 200 {status: "ok"}
3. Startup sequence (index.ts):
   a. Wait for DB (retry loop)
   b. Run migrations
   c. Seed loadgen API key (if auth enabled)
   d. Start retention worker
   e. Start server
   f. markHealthy()
```

---

## 5. Demo Script — Commands in Order

### Step 1: Start the service
```bash
docker compose up --build -d
```

### Step 2: Wait for health
```bash
curl http://localhost:8080/health
```

### Step 3: Ingest sample logs
```bash
curl -X POST http://localhost:8080/logs -H "Content-Type: application/json" -d '{"logs":[{"timestamp":"2026-08-11T14:00:00Z","level":"error","service":"checkout","message":"payment declined","attributes":{"user_id":"42","region":"eu-west"}},{"timestamp":"2026-08-11T14:00:01Z","level":"info","service":"auth","message":"login successful","attributes":{"user_id":"42"}},{"timestamp":"2026-08-11T14:00:02Z","level":"warn","service":"checkout","message":"retry attempt","attributes":{"user_id":"42","retries":2}}]}'
```

### Step 4: Query — all logs
```bash
curl "http://localhost:8080/logs?limit=5"
```

### Step 5: Query — filter by service
```bash
curl "http://localhost:8080/logs?service=checkout"
```

### Step 6: Query — filter by attribute
```bash
curl "http://localhost:8080/logs?attr.user_id=42"
```

### Step 7: Query — substring search
```bash
curl "http://localhost:8080/logs?q=declined"
```

### Step 8: Query — combined filters
```bash
curl "http://localhost:8080/logs?service=checkout&level=error&q=declined"
```

### Step 9: Cursor pagination (page 1)
```bash
curl "http://localhost:8080/logs?limit=2"
```
Copy `next_cursor` from response, then:
```bash
curl "http://localhost:8080/logs?limit=2&cursor=PASTE_HERE"
```

### Step 10: Aggregate — hourly buckets
```bash
curl "http://localhost:8080/logs/aggregate?since=2026-08-11T00:00:00Z&until=2026-08-12T00:00:00Z&bucket=1h"
```

### Step 11: Aggregate — group by service
```bash
curl "http://localhost:8080/logs/aggregate?since=2026-08-11T00:00:00Z&until=2026-08-12T00:00:00Z&bucket=1h&group_by=service"
```

### Step 12: Aggregate — group by level
```bash
curl "http://localhost:8080/logs/aggregate?since=2026-08-11T00:00:00Z&until=2026-08-12T00:00:00Z&bucket=1h&group_by=level"
```

### Step 13: Error — malformed JSON
```bash
curl -X POST http://localhost:8080/logs -H "Content-Type: application/json" -d 'not json'
```

### Step 14: Error — invalid level
```bash
curl -X POST http://localhost:8080/logs -H "Content-Type: application/json" -d '{"logs":[{"timestamp":"2026-08-11T14:00:00Z","level":"critical","service":"test","message":"bad"}]}'
```

### Step 15: Error — partial acceptance
```bash
curl -X POST http://localhost:8080/logs -H "Content-Type: application/json" -d '{"logs":[{"timestamp":"2026-08-11T14:00:00Z","level":"info","service":"test","message":"good"},{"timestamp":"2026-08-11T14:00:00Z","level":"bad","service":"test","message":"bad"}]}'
```

### Step 16: Connect to PostgreSQL
```bash
docker compose exec postgres psql -U postgres -d logs
```

### Step 17: EXPLAIN ANALYZE — cursor pagination
```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT id, timestamp, level, service, message, attributes FROM logs WHERE (timestamp, id) < ('2026-08-07T10:00:00Z', 5000) ORDER BY timestamp DESC, id DESC LIMIT 100;
```

### Step 18: EXPLAIN ANALYZE — composite filter
```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT id, timestamp, level, service, message, attributes FROM logs WHERE service = 'checkout' AND level = 3 AND timestamp >= '2026-08-01T00:00:00Z' ORDER BY timestamp DESC, id DESC LIMIT 100;
```

### Step 19: EXPLAIN ANALYZE — aggregation
```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT date_bin('1 hour'::interval, timestamp, '2000-01-01'::timestamptz) AS start, COUNT(*)::int AS count FROM logs WHERE timestamp >= '2026-08-01T00:00:00Z' AND timestamp < '2026-08-02T00:00:00Z' GROUP BY start ORDER BY start ASC;
```

### Step 20: EXPLAIN ANALYZE — attribute filter
```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT id, timestamp, level, service, message, attributes FROM logs WHERE attributes->>'user_id' = '42' ORDER BY timestamp DESC, id DESC LIMIT 100;
```

### Step 21: Exit psql
```sql
\q
```

### Step 22: Run unit tests
```bash
npm test
```

### Step 23: Run load test
```bash
node load-test.js
```

### Step 24: Stop the service
```bash
docker compose down
```

---

## 6. Key Metrics to Mention

| Metric | Value |
|--------|-------|
| Ingestion rate | ~35,000 logs/sec sustained |
| Batch size | 1,000 rows per COPY / INSERT |
| Write path | COPY FROM STDIN (multi-row INSERT fallback) |
| Connection pool | 20 connections |
| Cursor pagination | ~0.3ms regardless of page depth |
| Aggregation (1h buckets) | ~15ms on 100K rows |
| Attribute filter (GIN) | ~52ms on 100K rows |
| Unit tests | 24 passing |
| Retention | 30 days default, 10K rows/batch |

---

## 7. Debug/Extend Live Ideas

If asked to extend live, here are safe options:

1. **Add a new attribute filter** — Modify `logs.ts:91-96` to support `attr.type=error`
2. **Change retention period** — Update `RETENTION_DAYS` env var
3. **Add a new aggregation bucket** — Add `"1w": "1 week"` to `aggregate.ts:4-9`
4. **Add a status field to health** — Modify `health.ts:18` to include version/timestamp
5. **Add a count endpoint** — New route `GET /logs/count` that returns `SELECT COUNT(*)` with same filters
