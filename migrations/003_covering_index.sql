-- Benchmark-oriented covering indexes for the hot query patterns.
-- These keep the common time-ordered reads index-only and reduce heap fetches.
CREATE INDEX IF NOT EXISTS idx_logs_time_cover
  ON logs (timestamp DESC, id DESC)
  INCLUDE (level, service, message, attributes);

CREATE INDEX IF NOT EXISTS idx_logs_service_time_cover
  ON logs (service, timestamp DESC, id DESC)
  INCLUDE (level, message, attributes);

CREATE INDEX IF NOT EXISTS idx_logs_level_time_cover
  ON logs (level, timestamp DESC, id DESC)
  INCLUDE (service, message, attributes);

CREATE INDEX IF NOT EXISTS idx_logs_service_level_time_cover
  ON logs (service, level, timestamp DESC, id DESC)
  INCLUDE (message, attributes);

-- JSONB path ops give better performance for repeated key/value equality checks.
CREATE INDEX IF NOT EXISTS idx_logs_attributes_path
  ON logs USING GIN (attributes jsonb_path_ops);
