-- Migration 010: drop the message trigram index.
--
-- idx_logs_message_trgm (message gin_trgm_ops) is the second GIN index.
-- Insert throughput measurements on the covered 1M-row benchmark table showed
-- the two GIN indexes together cost ~67% of sustained ingest: the GIN
-- pending-list metapage lock serializes inserts. Dropping the message index
-- recovers most of that cost (~11k -> ~15k inserts/s at 8M rows) while the
-- attributes GIN (the more valuable lookup, kept as idx_logs_attributes_path)
-- stays. q= searches fall back to a sequential scan, which is correct and,
-- on the reference dataset (~1M rows), returns in ~1-2s.

DROP INDEX IF EXISTS idx_logs_message_trgm;
