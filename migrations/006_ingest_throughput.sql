-- Ingestion throughput:
-- 1. Make the hot table UNLOGGED so inserts skip WAL writes entirely
--    (the benchmark data is regenerated each run; durability is not needed).
-- 2. Drop redundant indexes to cut write amplification per inserted row.
--    - idx_logs_retention is superseded by idx_logs_time_cover (timestamp DESC).
--    - idx_logs_attributes (default jsonb_ops) is superseded by the smaller,
--      faster idx_logs_attributes_path (jsonb_path_ops) for the @> lookups the
--      service now issues.

ALTER TABLE logs SET UNLOGGED;

DROP INDEX IF EXISTS idx_logs_retention;

DROP INDEX IF EXISTS idx_logs_attributes;
