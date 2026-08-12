-- Drop indexes superseded by the covering indexes added in 003.
-- Every remaining access pattern keeps exactly one index, reducing
-- write amplification under sustained high-rate ingestion.

-- Superseded by idx_logs_time_cover (timestamp DESC, id DESC) INCLUDE (...)
DROP INDEX IF EXISTS idx_logs_timestamp;

-- Superseded by idx_logs_time_cover
DROP INDEX IF EXISTS idx_logs_cursor;

-- Superseded by idx_logs_service_level_time_cover
DROP INDEX IF EXISTS idx_logs_service_level_time;

-- Superseded by idx_logs_attributes_path (jsonb_path_ops is smaller and
-- faster for the equality-only lookups this service issues)
DROP INDEX IF EXISTS idx_logs_attributes;
