export const config = {
  databaseUrl: process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/logs",
  port: parseInt(process.env.PORT || "8080", 10),
  retentionDays: parseInt(process.env.RETENTION_DAYS || "30", 10),
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
  flushIntervalMs: parseInt(process.env.FLUSH_INTERVAL_MS || "5", 10),
  maxBuffered: parseInt(process.env.MAX_BUFFERED || "100000", 10),
};
