-- Reduce index count for higher sustained ingest throughput.
--
-- idx_logs_level_time_cover and idx_logs_service_level_time_cover are
-- dropped: their query patterns fall back to the remaining covering indexes
-- with negligible cost:
--   - level-only queries  -> idx_logs_time_cover (level is INCLUDE'd, index-only)
--   - service + level     -> idx_logs_service_time_cover (level is INCLUDE'd)
--
-- Every covering index costs ~1 right-edge B-tree insert per row, so dropping
-- two of them meaningfully raises the sustained logs/sec the 1-CPU database
-- can absorb.
DROP INDEX IF EXISTS idx_logs_level_time_cover;
DROP INDEX IF EXISTS idx_logs_service_level_time_cover;
