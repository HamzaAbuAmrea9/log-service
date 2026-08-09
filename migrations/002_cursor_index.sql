-- Optimized index for cursor-based pagination (timestamp DESC, id DESC)
-- Supports the ORDER BY timestamp DESC, id DESC + (timestamp, id) < (cursor_ts, cursor_id) pattern
CREATE INDEX idx_logs_cursor ON logs (timestamp DESC, id DESC);
