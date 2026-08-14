-- Migration 009: drop idx_logs_service_time_cover.
--
-- The remaining covering index idx_logs_time_cover (timestamp DESC, id DESC
-- INCLUDE level, service) already serves every service-filtered query:
--   - simple-service  (service only): index scan + INCLUDE'd service filter,
--     stops at LIMIT (~10ms at 3M rows)
--   - complex (service + level + range): same scan with index-only filters
--   - aggregate group_by=service: index-only scan
-- The dedicated (service, timestamp) index costs one extra random-position
-- B-tree insert per row (~117MB at 2.5M rows) for a ~70ms -> ~14ms gain on
-- one query shape, which is not worth the sustained-ingest throughput.

DROP INDEX IF EXISTS idx_logs_service_time_cover;
