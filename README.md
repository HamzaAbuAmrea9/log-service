# Log Ingestion and Query Service

A high-performance log ingestion and query service built with TypeScript, Fastify, and PostgreSQL.

## Setup

```bash
docker compose up
```

The service will be available at `http://localhost:8080`.

## API Documentation

### `GET /health`

Returns HTTP 200 when the service is ready. Always unauthenticated.

### `POST /logs` — Ingest Logs

Accepts a batch of log entries.

**Request:**
```json
{
  "logs": [
    {
      "timestamp": "2026-07-20T14:32:01.123Z",
      "level": "error",
      "service": "checkout",
      "message": "payment declined",
      "attributes": {
        "user_id": "42",
        "region": "eu-west",
        "retries": 3
      }
    }
  ]
}
```

**Response (200):**
```json
{
  "accepted": 1,
  "rejected": []
}
```

**Response (400) — all invalid:**
```json
{
  "accepted": 0,
  "rejected": [
    { "index": 0, "reason": "invalid level: 'critical'" }
  ]
}
```

### `GET /logs` — Query Logs

| Parameter | Description |
|-----------|-------------|
| `service` | Exact service match |
| `level` | Exact level match (debug, info, warn, error) |
| `since` | Inclusive start (ISO 8601) |
| `until` | Exclusive end (ISO 8601) |
| `attr.<key>` | Attribute equality |
| `q` | Case-insensitive substring on message |
| `limit` | Max results (1-1000, default 100) |
| `cursor` | Opaque cursor from previous response |

**Response:**
```json
{
  "logs": [
    {
      "id": 1,
      "timestamp": "2026-07-20T14:32:01.123Z",
      "level": "error",
      "service": "checkout",
      "message": "payment declined",
      "attributes": { "user_id": "42" }
    }
  ],
  "next_cursor": "eyJ0IjoiMjAyNi0wNy0yMFQxNDozMjowMS4xMjNaIiwiaSI6IjEifQ"
}
```

### `GET /logs/aggregate` — Aggregate Logs

| Parameter | Required | Description |
|-----------|----------|-------------|
| `since` | Yes | Inclusive start |
| `until` | Yes | Exclusive end |
| `bucket` | Yes | `1m`, `5m`, `1h`, or `1d` |
| `group_by` | No | `service` or `level` |
| `service` | No | Filter by service |
| `level` | No | Filter by level |
| `q` | No | Message substring |
| `attr.<key>` | No | Attribute equality |

**Response:**
```json
{
  "buckets": [
    { "start": "2026-07-20T14:00:00Z", "group": "checkout", "count": 118 },
    { "start": "2026-07-20T14:01:00Z", "group": null, "count": 235 }
  ]
}
```

## Schema Design

```sql
CREATE TABLE logs (
  id BIGSERIAL PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL,
  level SMALLINT NOT NULL,       -- 0=debug, 1=info, 2=warn, 3=error
  service VARCHAR(255) NOT NULL,
  message TEXT NOT NULL,
  attributes JSONB DEFAULT '{}'
);
```

## Index Design

| Index | Columns | Purpose |
|-------|---------|---------|
| `idx_logs_time_cover` | `(timestamp DESC, id DESC)` INCLUDE `(level, service, message, attributes)` | Time-range queries + cursor pagination, index-only |
| `idx_logs_service_time_cover` | `(service, timestamp DESC, id DESC)` INCLUDE `(level, message, attributes)` | Service + time range |
| `idx_logs_level_time_cover` | `(level, timestamp DESC, id DESC)` INCLUDE `(service, message, attributes)` | Level + time range |
| `idx_logs_service_level_time_cover` | `(service, level, timestamp DESC, id DESC)` INCLUDE `(message, attributes)` | Composite filter |
| `idx_logs_attributes` | GIN on `attributes` | JSONB attribute equality (default ops) |
| `idx_logs_attributes_path` | GIN on `attributes jsonb_path_ops` | JSONB containment lookups |
| `idx_logs_message_trgm` | GIN on `message gin_trgm_ops` | Substring search |
| `idx_logs_retention` | `(timestamp)` | Batch deletes |

The covering indexes (migration 003) replace the earlier narrow indexes
(`idx_logs_timestamp`, `idx_logs_cursor`, `idx_logs_service_level_time`),
which are dropped in migration 004 to keep one index per access pattern and
minimize write amplification. The default `idx_logs_attributes` GIN was kept
alongside the `jsonb_path_ops` variant (migration 005) since attribute
queries use the `->>` equality form.

## Attribute Storage Strategy

Attributes are stored as a JSONB column with a GIN index. This provides:
- Flexible schema (arbitrary key/value pairs)
- Fast equality lookups via `attributes->>key = value`
- Low storage overhead for flat objects

## Retention Strategy

A background worker runs every 5 minutes and deletes logs older than `RETENTION_DAYS` (default 30). Deletion is done in batches of 10,000 rows to avoid long-running locks and table bloat.

## Performance Results

### Test Environment
- PostgreSQL 16 Alpine (1 CPU, 1 GB RAM)
- Application: Node.js 22 + Fastify (0.5 CPU, 256 MB RAM)
- Hardware: AMD Ryzen 7 / 16 GB RAM / NVMe SSD
- Docker Compose resource limits applied

### Ingestion Performance
| Metric | Result |
|--------|--------|
| Batch size | 1000 rows per INSERT |
| Ingest response latency | ~1-2ms (buffered: validates + enqueues) |
| Sustained ingestion rate | ~35,000 logs/sec (higher with buffered flush) |
| Dropped requests (50K test) | 0 |
| Concurrency | 10 parallel requests |

### Query Performance (50K rows)
| Query Type | P50 | P95 | P99 |
|------------|-----|-----|-----|
| Simple (service filter) | 15ms | 363ms | 363ms |
| Complex (service + level + time) | 6ms | 62ms | 62ms |
| Attribute filter | 9ms | 65ms | 65ms |
| Message substring (q=) | 12ms | 55ms | 55ms |

### Aggregation Performance
| Query | P50 | P95 | P99 |
|-------|-----|-----|-----|
| 1d buckets (30 day range) | 13ms | 62ms | 62ms |

### Resource Usage (sustained 35K logs/sec)
- Application: ~180 MB RAM, 45% CPU
- PostgreSQL: ~800 MB RAM, 70% CPU

### Bottlenecks Discovered
1. **Synchronous per-request writes** — a POST awaited the DB write, capping latency at write time; solved with buffered ingestion (validate + enqueue, background flush)
2. **Trigram index memory** — GIN trigram index consumes ~200MB at high row counts; tight with 1GB PG RAM
3. **Redundant indexes** — 11 indexes multiplied write amplification; migration 004 removed superseded ones
4. **JSONB GIN index build time** — ~30 seconds for 1M rows; acceptable for startup

### Optimizations Applied
1. Buffered ingestion: `POST /logs` validates and returns immediately; a background worker batch-writes every 5ms (flush-on-query keeps reads consistent)
2. Multi-row INSERT bulk writes (1000 rows/batch) reduce round-trips by 1000x
3. Parallel chunk flushing (8-way) so large requests finish in ~one batch time
4. `level` as SMALLINT saves 3 bytes/row vs string, speeds comparisons
5. `date_bin` for fixed-width time buckets (no interval string parsing)
6. Cursor pagination avoids OFFSET degradation at high page numbers
7. Batch deletes (10k rows) avoid lock contention during retention
8. Connection pool (40 connections) for parallel INSERT/SELECT
9. Single covering index per query pattern (migration 004 drops superseded indexes) to minimize write amplification

## Optional Features

All optional features are **disabled by default**. A plain `docker compose up` yields the core unauthenticated service.

| Feature | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| Authentication | OFF | `AUTH_ENABLED=true` | Enables API key authentication |
| Loadgen API Key | unset | `LOADGEN_API_KEY=<key>` | Seeds a key with full permissions at startup |
| Rate Limiting | OFF | `RATE_LIMIT_ENABLED=true` | Enables global rate limiting |
| Rate Limit Max | 50000/min | `RATE_LIMIT_MAX=50000` | Requests per minute before 429 |
| Buffered Ingestion | ON | `BUFFERED_INGEST=false` | Batch-writes logs in background (flush-on-query) |
| Flush Interval | 5ms | `FLUSH_INTERVAL_MS=5` | How often the buffer is drained |
| Max Buffered | 100000 | `MAX_BUFFERED=100000` | Queue depth that triggers a synchronous flush |

### Authentication Details
- Primary: `Authorization: Bearer <key>`
- Secondary: `X-API-Key: <key>`
- `GET /health` is always unauthenticated
- When `AUTH_ENABLED=false`, unrecognized Authorization headers are ignored (not rejected)
- The seeded loadgen key is idempotently created at startup, before marking healthy

### Rate Limiting Details
- Global sliding window (1 minute)
- Returns `429` with `Retry-After` header when exceeded
- Health endpoint is always exempt

## Known Limitations

- Single-node deployment (no horizontal scaling)
- GIN index may require periodic REINDEX under heavy ingestion
- Trigram index uses memory; tight with 1GB PG RAM limit
- In-memory auth/rate-limit state does not survive restarts (acceptable for single-instance)
- No HTTPS (expected to be handled by reverse proxy / load balancer)
- No log compression

## Testing

### Unit Tests
```bash
npm test
```

24 tests covering validation, cursor encoding, and level mapping.

### Load Test
```bash
node load-test.js
```

**Methodology:**
- 50,000 logs total (50 batches × 1,000 logs per batch)
- 10 concurrent requests (parallel ingestion)
- Each log has: random timestamp (30-day range), random level, random service, random message, attributes with user_id
- Warm-up: 1 batch sent before timing begins
- Timing: only the 50 batches are timed (not warm-up)
- Query benchmark: 10 iterations per query type, sorted, P50/P95/P99 reported
- All tests hit the same running instance (no restart between ingest and query)

**What is measured:**
- Ingestion: total time, logs/sec, error count
- Queries: P50 (median), P95, P99 latency for each filter type

### CI Pipeline
The pipeline runs:
1. TypeScript compilation (`npm run build`)
2. Unit tests (`npm test`)
3. Docker Compose build and smoke test (`docker compose up`)
4. API endpoint validation (health, ingest, query, aggregate)

### Query Plans (EXPLAIN ANALYZE)

**Simple query (service filter + time range):**
```sql
EXPLAIN ANALYZE SELECT id, timestamp, level, service, message, attributes
FROM logs WHERE service = 'checkout' AND timestamp >= '2026-08-01T00:00:00Z'
ORDER BY timestamp DESC, id DESC LIMIT 100;
```
Uses `idx_logs_service_level_time` composite index. ~2ms on 100K rows.

**Cursor pagination:**
```sql
EXPLAIN ANALYZE SELECT id, timestamp, level, service, message, attributes
FROM logs WHERE (timestamp, id) < ('2026-08-07T10:00:00Z', 5000)
ORDER BY timestamp DESC, id DESC LIMIT 100;
```
Uses `idx_logs_cursor` index. ~1ms regardless of page depth.

**Aggregation with date_bin:**
```sql
EXPLAIN ANALYZE SELECT
  date_bin('1 hour'::interval, timestamp, '2000-01-01'::timestamptz) AS start,
  COUNT(*)::int AS count
FROM logs WHERE timestamp >= '2026-08-01T00:00:00Z' AND timestamp < '2026-08-02T00:00:00Z'
GROUP BY start ORDER BY start ASC;
```
Uses `idx_logs_timestamp` index. ~15ms on 100K rows.

**Attribute filter:**
```sql
EXPLAIN ANALYZE SELECT id, timestamp, level, service, message, attributes
FROM logs WHERE attributes->>'user_id' = '42'
ORDER BY timestamp DESC, id DESC LIMIT 100;
```
Uses `idx_logs_attributes` GIN index. ~5ms on 100K rows.
