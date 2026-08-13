-- Restore the default jsonb_ops GIN index that was dropped in 004.
-- Attribute lookups use the `->>` equality form, so we keep both operator
-- classes to give the planner a usable index regardless of rewrite behavior.
CREATE INDEX IF NOT EXISTS idx_logs_attributes ON logs USING GIN (attributes);
