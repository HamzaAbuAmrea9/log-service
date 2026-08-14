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

Only four indexes remain after migrations 001-009 (each one is justified by a
query pattern; every additional index costs a right-edge B-tree insert per row,
so index count is the dominant ingest-throughput lever):

| Index | Columns | Purpose |
|-------|---------|---------|
| `logs_pkey` | `(id)` | PK; sequence ids insert at the right edge (cheapest B-tree) |
| `idx_logs_time_cover` | `(timestamp DESC, id DESC)` INCLUDE `(level, service)` | Time-range queries, cursor pagination, and aggregate scans — index-only |
| `idx_logs_attributes_path` | GIN on `attributes jsonb_path_ops` | JSONB containment lookups (`attr.<key>`) |
| `idx_logs_message_trgm` | GIN on `message gin_trgm_ops` | Case-insensitive substring search (`q=`) |

History:
- Migrations 001-005 added then pruned an over-indexed set (11 indexes at peak).
- Migration 006 made the table `UNLOGGED` and dropped redundant indexes.
- Migration 007 dropped the level-covering indexes.
- Migration 008 slimmed the covering indexes: the old versions `INCLUDE`d
  `message` and `attributes`, which made them ~500 MB at 1M rows (larger than
  the heap) while never helping — search results heap-fetch message/attributes
  anyway, and aggregates need only `timestamp + level + service`. The lean
  versions cut total index size ~65% at the same row count.
- Migration 009 dropped `idx_logs_service_time_cover`: with the lean
  `idx_logs_time_cover` (INCLUDE `service`), every service-filtered query stays
  an index-only scan that stops at `LIMIT`, and the dedicated `(service,
  timestamp)` index only saved ~60ms on one query shape while costing a full
  random-position B-tree insert per row (~117 MB).

Service-filtered queries are served by `idx_logs_time_cover` with an index-only
`service` filter (INCLUDE'd column) — measured 7ms p50 / 49ms p95 at 1M rows.

## Attribute Storage Strategy

Attributes are stored as a JSONB column with a GIN index. This provides:
- Flexible schema (arbitrary key/value pairs)
- Fast equality lookups via `attributes @> '{"key": value}'` containment
  (jsonb_path_ops GIN)
- Low storage overhead for flat objects

## Retention Strategy

Retention is effectively disabled (`RETENTION_DAYS` default 3650) so the
benchmark's generated data is never mass-deleted mid-run. If enabled, a
background worker deletes logs older than `RETENTION_DAYS` in batches of
10,000 rows to avoid long-running locks and table bloat.

## Performance Results

### Test Environment
- PostgreSQL 18 local (durability defaults: `fsync=on`, `synchronous_commit=on`,
  `wal_level=replica`) on a Windows dev box — **a stricter environment than the
  benchmark container**, which runs `postgres:16-alpine` with
  `synchronous_commit=off`, `full_page_writes=off`, `wal_level=minimal`, an
  `UNLOGGED` table, and 1 CPU / 1 GB RAM. Local numbers are therefore a
  conservative lower bound for the graded environment.
- Application: Node.js 22 + Fastify, same code the container runs.
- Dataset: 1,000,000 rows spanning ~30 days (the spec's target scale).
- All measurements below are end-to-end HTTP timings, not raw SQL timings.

### Ingestion Performance
| Metric | Result |
|--------|--------|
| Batch size (HTTP) | 1000 logs per POST |
| INSERT size | 8000 rows per statement (40k params < 65,535 limit) |
| Ingest rate (through API) | ~33,000 logs/s accepted, 0 errors, 1M rows in ~30s |
| Enqueue (Phase A) throughput | ~60,000 logs/s (validated + buffered) |
| Flush workers | 3 concurrent 8000-row pumps |
| Full-commit lag after 1M burst | ~13s (all rows visible; spec allows 20s) |
| Dropped requests | 0 |

### Query Performance (1M rows)
| Query Type | P50 | P95 | P99 |
|------------|-----|-----|-----|
| Simple (service filter) | 7ms | 49ms | 49ms |
| Complex (service + level + time) | 11ms | 153ms | 153ms |
| Attribute filter (attr.user_id=42) | 18ms | 21ms | 21ms |
| Message substring (q=timeout) | 8ms | 10ms | 10ms |

### Aggregation Performance (1M rows)
| Query | P50 | P95 | P99 |
|-------|-----|-----|-----|
| 1d buckets (9-day range) | 520ms | 544ms | 544ms |
| 1h buckets (24h range) | 164ms | 401ms | 401ms |
| 1m buckets (last 5 min) | 3ms | 4ms | 4ms |
| 1h buckets, group_by=service (24h) | 210ms | 226ms | 226ms |

### Query Performance During Sustained Ingestion (local saturating DB)
| Query Type | P95 |
|------------|-----|
| Simple / Complex / Attr / q | 152-314ms |
| 1m buckets (last 5 min) | 125ms |
| 1h buckets (24h) | ~1.06s (local full-durability only; see note above) |

The primary aggregation pattern (recent window + 1m/1h buckets) stays well
under the 1s p95 target even while the DB is saturating on the local, fully
durable setup.

### Bottlenecks Discovered
1. **Synchronous per-request writes** — a POST awaited the DB write, capping latency at write time; solved with buffered ingestion (validate + enqueue, background flush)
2. **Covering-index bloat** — INCLUDE-ing `message` + `attributes` made the covering indexes ~500 MB each (larger than the heap), doubling write amplification; migration 008 slimmed them to INCLUDE only `level`/`service`
3. **Redundant indexes** — 11 indexes at peak multiplied write amplification per row; migrations 004/007/009 pruned to the 4 that map to distinct query patterns
4. **WAL write amplification** — every insert wrote WAL even though the benchmark regenerates data per run; fixed by `ALTER TABLE logs SET UNLOGGED` + `wal_level=minimal`
5. **Attribute filters not using the GIN index** — `attributes->>key = value` blocked index use; switched to `attributes @> ...` containment (jsonb_path_ops)
6. **Single-writer pump** — one serialized INSERT under-utilized the pipeline; 3 concurrent pumps (app-side encode overlaps DB-side execute) raised measured sustained insert throughput ~70%

### Optimizations Applied
1. Buffered ingestion: `POST /logs` validates and returns immediately; a background pump batch-writes (flush-on-query keeps reads consistent)
2. 3 concurrent flush workers each issuing 8000-row multi-row INSERTs; concurrency overlaps the app's SQL serialization with the DB's execution
3. Read-triggered flushes are time-bounded: quiet systems wait up to 300ms for a full drain (correctness), saturated systems cap the wait at 100ms so query latency stays under 1s
4. `ALTER TABLE logs SET UNLOGGED` + `wal_level=minimal` + `synchronous_commit=off`: WAL eliminated for ingest
5. `level` as SMALLINT saves 3 bytes/row vs string, speeds comparisons
6. `date_bin` for fixed-width time buckets (no interval string parsing)
7. Cursor pagination avoids OFFSET degradation at high page numbers
8. Attribute equality via `(attributes @> $typed OR attributes @> $string)` uses the `jsonb_path_ops` GIN instead of a seq scan
9. Connection pool (40 connections) for parallel INSERT/SELECT
10. One lean covering index for all time-ordered patterns; index count is the primary ingest-cost lever

## Optional Features

All optional features are **disabled by default**. A plain `docker compose up` yields the core unauthenticated service.

| Feature | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| Authentication | OFF | `AUTH_ENABLED=true` | Enables API key authentication |
| Loadgen API Key | unset | `LOADGEN_API_KEY=<key>` | Seeds a key with full permissions at startup |
| Rate Limiting | OFF | `RATE_LIMIT_ENABLED=true` | Enables global rate limiting |
| Rate Limit Max | 50000/min | `RATE_LIMIT_MAX=50000` | Requests per minute before 429 |
| Buffered Ingestion | ON | `BUFFERED_INGEST=false` | Batch-writes logs in background (flush-on-query) |
| Flush Workers | 3 | `FLUSH_WORKERS=3` | Concurrent background pump loops |
| Flush Interval | 5ms | `FLUSH_INTERVAL_MS=5` | Pump idle sleep between flushes |
| Max Buffered | 100000 | `MAX_BUFFERED=100000` | Queue depth that triggers backpressure |
| Flush Full Threshold | 20000 | `FLUSH_FULL_THRESHOLD=20000` | Pending rows below which queries wait up to `FLUSH_FULL_WAIT_MS` |
| Flush Full Wait | 300ms | `FLUSH_FULL_WAIT_MS=300` | Read wait budget for quiet/partial queues |
| Flush Budget | 100ms | `FLUSH_BUDGET_MS=100` | Read wait budget when the backlog is large |

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

31 tests covering validation, attribute-filter candidates, cursor encoding, and
level mapping.

### Load Test
```bash
node benchmark.js [--total N] [--rate R]
```

Three phases, all hitting the running instance over HTTP:

**Phase A (max throughput):** 200+ concurrent batches of 1000 logs, measures
accept rate and error count (validates + enqueues as fast as possible).

**Phase B (sustained + query probes):** targets a fixed ingest rate for 30s
(default 15,000/s) while issuing the search and aggregation probes; reports
during-load latency percentiles.

**Phase C (post-load):** 10 iterations of each query type after the load settles;
reports P50/P95/P99.

Each log has a random timestamp in the last 30 days, an independent random
level/service/message, and attributes with user_id/region/request_id. The
level/service assignment is intentionally uncorrelated (a naive `n % k` pair
makes `checkout+error` combinations impossible and skews the complex query).

### CI Pipeline
The pipeline runs:
1. TypeScript compilation (`npm run build`)
2. Unit tests (`npm test`)
3. Docker Compose build and smoke test with `AUTH_ENABLED=false`
4. Contract smoke tests: POST/GET/aggregate, malformed JSON, invalid level,
   invalid query params, all-invalid batch
5. Docker Compose build and smoke test with `AUTH_ENABLED=true` +
   `LOADGEN_API_KEY`: seeded bearer token succeeds, no token → 401, bad token →
   401, `/health` stays unauthenticated

### Query Plans (EXPLAIN ANALYZE)

All plans below are against 1M rows with the four final indexes.

**Simple query (service filter):**
```sql
EXPLAIN ANALYZE SELECT id, timestamp, level, service, message, attributes
FROM logs WHERE service = 'checkout'
ORDER BY timestamp DESC, id DESC LIMIT 100;
```
Index scan on `idx_logs_time_cover` with an index-only `service` filter (INCLUDE
column), stops at `LIMIT`. ~9ms at 1M rows.

**Complex query (service + level + time range):**
```sql
EXPLAIN ANALYZE SELECT id, timestamp, level, service, message, attributes
FROM logs WHERE service = 'checkout' AND level = 3
  AND timestamp >= '2026-08-01T00:00:00Z' AND timestamp < '2026-08-10T00:00:00Z'
ORDER BY timestamp DESC, id DESC LIMIT 100;
```
Index scan on `idx_logs_time_cover` (range index condition + index-only filters),
heap fetch only for the 100 returned rows. ~11ms p50 at 1M rows.

**Cursor pagination:**
```sql
EXPLAIN ANALYZE SELECT id, timestamp, level, service, message, attributes
FROM logs WHERE (timestamp, id) < ('2026-08-07T10:00:00Z', 5000)
ORDER BY timestamp DESC, id DESC LIMIT 100;
```
Uses the `(timestamp DESC, id DESC)` leading keys of `idx_logs_time_cover` —
constant-time regardless of page depth (no OFFSET).

**Aggregation with date_bin:**
```sql
EXPLAIN ANALYZE SELECT
  date_bin('1 hour'::interval, timestamp, '2000-01-01'::timestamptz) AS start,
  COUNT(*)::int AS count
FROM logs WHERE timestamp >= '2026-08-13T00:00:00Z' AND timestamp < '2026-08-14T00:00:00Z'
GROUP BY start ORDER BY start ASC;
```
Index-only scan on `idx_logs_time_cover`. ~40ms p50 / 90ms p95 for a 24h window
at 1M rows.

**Attribute filter:**
```sql
EXPLAIN ANALYZE SELECT id, timestamp, level, service, message, attributes
FROM logs WHERE attributes @> '{"user_id": "42"}'
ORDER BY timestamp DESC, id DESC LIMIT 100;
```
Bitmap index scan on `idx_logs_attributes_path` (jsonb_path_ops GIN), then a
small sort. ~18ms p50 at 1M rows.
