-- Replace cursor index with covering index for index-only scans
-- Includes all columns needed for SELECT to avoid heap lookups
DROP INDEX IF EXISTS idx_logs_cursor;
CREATE INDEX idx_logs_query_cover ON logs (timestamp DESC, id DESC) INCLUDE (level, service, message, attributes);
