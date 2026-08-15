-- Migration 011: hourly ingestion rollup.
--
-- Per-hour COUNT by (service, level), maintained transactionally by the
-- ingest pump so aggregate-1h queries over long windows (24h+) stop scanning
-- ~1M+ index rows on a single CPU. The aggregate route serves whole middle
-- buckets from this table and only live-counts the (at most two) partial edge
-- buckets, keeping aggregate-1h-24h ~instant at any table size.
--
-- Rows inserted before this migration are backfilled here so the rollup
-- matches the existing heap.

CREATE TABLE IF NOT EXISTS log_rollup (
  bucket_start timestamptz NOT NULL,
  service VARCHAR(255) NOT NULL,
  level SMALLINT NOT NULL,
  count BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_start, service, level)
);

INSERT INTO log_rollup (bucket_start, service, level, count)
SELECT
  date_bin('1 hour', "timestamp", '2000-01-01'::timestamptz),
  service,
  level,
  COUNT(*)
FROM logs
GROUP BY 1, 2, 3
ON CONFLICT (bucket_start, service, level) DO NOTHING;
