-- Migration 008: slim down the covering indexes.
--
-- The old covering indexes INCLUDE'd message (text) and attributes (jsonb),
-- making them ~500 MB at 1M rows -- larger than the heap. That doubles write
-- amplification (the biggest ingest cost) and never helps: search results
-- must heap-fetch message/attributes anyway, and aggregates need only
-- timestamp + level + service. Lean versions keep index-only scans for
-- filters and aggregates while cutting index size ~80%.

DROP INDEX IF EXISTS idx_logs_time_cover;
DROP INDEX IF EXISTS idx_logs_service_time_cover;

CREATE INDEX idx_logs_time_cover ON logs ("timestamp" DESC, id DESC) INCLUDE (level, service);
CREATE INDEX idx_logs_service_time_cover ON logs (service, "timestamp" DESC, id DESC) INCLUDE (level);
