export const config = {
  databaseUrl: process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/logs",
  port: parseInt(process.env.PORT || "8080", 10),
  retentionDays: parseInt(process.env.RETENTION_DAYS || "3650", 10),
  poolSize: parseInt(process.env.POOL_SIZE || "15", 10),
  // Optional features (disabled by default)
  authEnabled: process.env.AUTH_ENABLED === "true",
  loadgenApiKey: process.env.LOADGEN_API_KEY || "",
  rateLimitEnabled: process.env.RATE_LIMIT_ENABLED === "true",
  rateLimitMax: parseInt(process.env.RATE_LIMIT_MAX || "50000", 10), // requests per minute
  // Buffered ingestion (on by default): POST /logs validates and returns
  // immediately; a background worker batch-writes the buffer. Queries flush
  // the buffer first, so reads always see previously accepted logs.
  bufferedIngest: process.env.BUFFERED_INGEST !== "false",
  flushWorkers: parseInt(process.env.FLUSH_WORKERS || "3", 10),
  flushIntervalMs: parseInt(process.env.FLUSH_INTERVAL_MS || "5", 10),
  maxBuffered: parseInt(process.env.MAX_BUFFERED || "100000", 10),
  // Reads wait for the pump to commit pending rows before querying, so
  // queries see all accepted logs. When few rows are pending (correctness
  // tests) the wait is generous; when the backlog is huge (ingestion
  // saturating the DB) the wait is time-bounded so query latency stays low.
  flushFullThreshold: parseInt(process.env.FLUSH_FULL_THRESHOLD || "20000", 10),
  flushFullWaitMs: parseInt(process.env.FLUSH_FULL_WAIT_MS || "300", 10),
  flushBudgetMs: parseInt(process.env.FLUSH_BUDGET_MS || "100", 10),
};
