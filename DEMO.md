# Log Ingestion and Query Service — 5-Minute Demo Script

## Timeline

```
[0:00 - 0:30]  Introduction
[0:30 - 1:15]  Architecture & Trade-offs
[1:15 - 1:45]  Schema & Indexes
[1:45 - 2:15]  EXPLAIN ANALYZE
[2:15 - 2:45]  Code Path Walkthrough
[2:45 - 4:15]  Live Demo
[4:15 - 4:45]  Load Test & Performance
[4:45 - 5:00]  Conclusion
```

---

## Part 1: Introduction (30 sec)

**Terminal:** Show project folder

```bash
ls -la
ls src/
ls src/routes/ src/services/ src/utils/
```

**Say:**
```
"This is the Log Ingestion and Query Service — a backend system that
ingests structured logs via REST API, stores them in PostgreSQL,
and supports querying and aggregation.

Built with TypeScript, Fastify, and PostgreSQL.
It handles 30,000+ logs per second with zero data loss."
```

---

## Part 2: Architecture & Trade-offs (45 sec)

**Open:** `src/index.ts`

**Say:**
```
"Three-layer architecture:

1. ROUTES — handle HTTP only, no business logic
2. SERVICES — pure business logic, no HTTP concerns
3. UTILS — stateless helpers (validation, cursor encoding)

This separation means I can extend any layer independently."
```

**Trade-offs to mention:**

```
"Key decisions:

- Fastify over Express: 2x faster, built-in JSON validation
- PostgreSQL over MongoDB: ACID compliance, date_bin() for
  time bucketing, GIN indexes for JSONB queries
- JSONB over separate table for attributes: flexible schema,
  no JOINs needed, GIN index for fast lookups
- SMALLINT for levels: 1 byte vs 5-10 for string,
  numerical comparison is faster"
```

---

## Part 3: Schema & Indexes (30 sec)

**Run:**
```bash
docker compose exec postgres psql -U postgres -d logs -c "\d logs"
```

**Say:**
```
"6 columns. id is BIGSERIAL, timestamp is TIMESTAMPTZ,
level is SMALLINT (0-3), service is VARCHAR, message is TEXT,
attributes is JSONB with default empty object."
```

**Run:**
```bash
docker compose exec postgres psql -U postgres -d logs -c "\di"
```

**Say:**
```
"6 indexes, each for a specific query pattern:

- idx_logs_cursor: for cursor pagination (timestamp DESC, id DESC)
- idx_logs_service_level_time: composite for service+level+time filters
- idx_logs_timestamp: for time-range queries
- idx_logs_attributes: GIN for JSONB attribute equality
- idx_logs_message_trgm: trigram for substring search
- idx_logs_retention: for fast batch deletes"
```

---

## Part 4: EXPLAIN ANALYZE (30 sec)

**Run these 3 queries:**

```bash
# 1. Cursor pagination — fastest
docker compose exec postgres psql -U postgres -d logs -c "
EXPLAIN ANALYZE SELECT id, timestamp, level, service, message, attributes
FROM logs WHERE (timestamp, id) < ('2026-08-09T10:00:00.000Z', 999999)
ORDER BY timestamp DESC, id DESC LIMIT 100;"
```

**Say:**
```
"0.3 milliseconds. Uses cursor index directly.
No OFFSET, no scanning thousands of rows."
```

```bash
# 2. Composite filter
docker compose exec postgres psql -U postgres -d logs -c "
EXPLAIN ANALYZE SELECT id, timestamp, level, service, message, attributes
FROM logs WHERE service = 'checkout' AND level = 3
AND timestamp >= '2026-08-01T00:00:00Z'
ORDER BY timestamp DESC, id DESC LIMIT 100;"
```

**Say:**
```
"7 milliseconds. Uses the composite index — all 3 filters
are in the index, PostgreSQL never touches the table."
```

```bash
# 3. Aggregation
docker compose exec postgres psql -U postgres -d logs -c "
EXPLAIN ANALYZE SELECT
  date_bin('1 day'::interval, timestamp, '2000-01-01'::timestamptz) AS start,
  COUNT(*)::int AS count
FROM logs WHERE timestamp >= '2026-07-01T00:00:00Z'
AND timestamp < '2026-09-01T00:00:00Z'
GROUP BY start ORDER BY start ASC;"
```

**Say:**
```
"17 milliseconds for 50,000 rows.
Uses Index Only Scan — doesn't even read the table."
```

---

## Part 5: Code Path Walkthrough (30 sec)

**Open:** `src/services/ingest.ts`

**Say:**
```
"Ingestion path:
POST /logs → route validates entries → invalid ones filtered out
→ valid entries go to ingestLogs()

If batch < 10: directInsert() — no buffer overhead
If batch >= 10: add to buffer → flush after 5ms
→ multi-row INSERT: VALUES (row1), (row2), ... (row1000)
→ one query inserts 1000 rows

Backpressure at 50,000 buffered entries prevents memory overflow."
```

**Open:** `src/services/query.ts`

**Say:**
```
"Query path:
GET /logs?service=checkout → route parses params
→ queryLogs() builds WHERE clause dynamically
→ parameterized SQL (safe from injection)
→ LIMIT = requested + 1 (to detect hasMore)
→ maps level number to string (3 → 'error')
→ encodes next_cursor as base64url"
```

---

## Part 6: Live Demo (90 sec)

```bash
# 1. Health check
curl http://localhost:8080/health
```
**Say:** "Service is ready"

```bash
# 2. Ingest batch
curl -X POST http://localhost:8080/logs \
  -H "Content-Type: application/json" \
  -d '{"logs":[
    {"timestamp":"2026-08-09T10:00:00Z","level":"error","service":"checkout","message":"payment failed","attributes":{"user_id":"42"}},
    {"timestamp":"2026-08-09T10:01:00Z","level":"info","service":"auth","message":"user login"},
    {"timestamp":"2026-08-09T10:02:00Z","level":"warn","service":"api","message":"rate limit approaching"}
  ]}'
```
**Say:** "3 logs ingested, 0 rejected"

```bash
# 3. Query all
curl "http://localhost:8080/logs?limit=3"
```
**Say:** "Logs stored, level as string"

```bash
# 4. Filter by service
curl "http://localhost:8080/logs?service=checkout"
```
**Say:** "Only checkout logs"

```bash
# 5. Filter by level
curl "http://localhost:8080/logs?level=error"
```
**Say:** "Only error logs"

```bash
# 6. Attribute filter
curl "http://localhost:8080/logs?attr.user_id=42"
```
**Say:** "JSONB attribute filter"

```bash
# 7. Search
curl "http://localhost:8080/logs?q=payment"
```
**Say:** "Substring search in message"

```bash
# 8. Aggregation
curl "http://localhost:8080/logs/aggregate?since=2026-08-01T00:00:00Z&until=2026-08-10T00:00:00Z&bucket=1d&group_by=service"
```
**Say:** "Daily aggregation grouped by service"

```bash
# 9. Error: malformed JSON
curl -X POST http://localhost:8080/logs -d 'not json'
```
**Say:** "400 with error message"

```bash
# 10. Error: invalid level
curl "http://localhost:8080/logs?level=critical"
```
**Say:** "400 for invalid level"

---

## Part 7: Load Test & Performance (30 sec)

```bash
node load-test.js
```

**Say:**
```
"50,000 logs in 1.6 seconds — 30,000+ per second.
Zero errors. Well above the 15,000 target.

Query performance:
- Simple filter: 14ms P50
- Complex filter: 9ms P50
- Aggregation: 11ms P50
All under 100ms."
```

---

## Part 8: Unit Tests (15 sec)

```bash
npm test
```

**Say:**
```
"24 unit tests — validation, cursor encoding, level mapping.
All passing."
```

---

## Part 9: Conclusion (15 sec)

**Say:**
```
"Summary:
- All 4 endpoints working with proper error handling
- Cursor pagination, attribute filtering, time bucketing
- 30K+ logs/sec ingestion, P99 <100ms queries
- Docker Compose with one-command startup
- CI pipeline with build, test, and smoke tests
- 24 unit tests, all passing

Thank you."
```

---

## Quick Reference: Commands

```bash
# Start
docker compose up --build -d
sleep 8

# Health
curl http://localhost:8080/health

# Ingest
curl -X POST http://localhost:8080/logs -H "Content-Type: application/json" -d '{"logs":[{"timestamp":"2026-08-09T10:00:00Z","level":"error","service":"checkout","message":"payment failed","attributes":{"user_id":"42"}}]}'

# Query
curl "http://localhost:8080/logs?limit=5"
curl "http://localhost:8080/logs?service=checkout"
curl "http://localhost:8080/logs?level=error"
curl "http://localhost:8080/logs?q=payment"
curl "http://localhost:8080/logs?attr.user_id=42"

# Aggregate
curl "http://localhost:8080/logs/aggregate?since=2026-08-01T00:00:00Z&until=2026-08-10T00:00:00Z&bucket=1d&group_by=service"

# EXPLAIN ANALYZE
docker compose exec postgres psql -U postgres -d logs -c "EXPLAIN ANALYZE SELECT * FROM logs WHERE service = 'checkout' LIMIT 10;"

# Load test
node load-test.js

# Tests
npm test

# Stop
docker compose down
```
