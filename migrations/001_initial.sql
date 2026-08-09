-- Enable trigram extension for substring search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Level enum as smallint for performance
-- 0=debug, 1=info, 2=warn, 3=error

CREATE TABLE logs (
  id BIGSERIAL PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL,
  level SMALLINT NOT NULL CHECK (level BETWEEN 0 AND 3),
  service VARCHAR(255) NOT NULL,
  message TEXT NOT NULL,
  attributes JSONB NOT NULL DEFAULT '{}'
);

-- Index for time-range queries (most common filter)
CREATE INDEX idx_logs_timestamp ON logs (timestamp DESC);

-- Index for service + level + time queries (composite)
CREATE INDEX idx_logs_service_level_time ON logs (service, level, timestamp DESC);

-- GIN index for JSONB attribute equality queries
CREATE INDEX idx_logs_attributes ON logs USING GIN (attributes);

-- Trigram index for message substring search (q= parameter)
CREATE INDEX idx_logs_message_trgm ON logs USING GIN (message gin_trgm_ops);

-- Retention: delete expired rows efficiently
CREATE INDEX idx_logs_retention ON logs (timestamp);
