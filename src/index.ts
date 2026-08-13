import Fastify, { FastifyError } from "fastify";
import { pool, runMigrations } from "./db.js";
import { config } from "./config.js";
import { logsRoutes } from "./routes/logs.js";
import { aggregateRoutes } from "./routes/aggregate.js";
import { healthRoutes, markHealthy } from "./routes/health.js";
import { startRetentionWorker } from "./services/retention.js";
import { forceFlush, startFlushWorker } from "./services/ingest.js";
import { authMiddleware, seedLoadgenKey } from "./services/auth.js";
import { rateLimitMiddleware } from "./services/ratelimit.js";

async function main(): Promise<void> {
  const app = Fastify({
    logger: true,
    bodyLimit: 10 * 1024 * 1024, // 10MB for large batches
  });

  // Custom error handler: return {error: "..."} for all errors
  app.setErrorHandler((error: FastifyError, _request, reply) => {
    const statusCode = error.statusCode || 500;
    // For malformed JSON and validation errors, return spec-compliant format
    if (statusCode === 400) {
      return reply.status(400).send({ error: error.message || "Bad request" });
    }
    return reply.status(statusCode).send({ error: error.message });
  });

  // Register global middleware (runs before every request)
  app.addHook("onRequest", authMiddleware);
  app.addHook("onRequest", rateLimitMiddleware);

  // Register routes
  await app.register(healthRoutes);
  await app.register(logsRoutes);
  await app.register(aggregateRoutes);

  // Wait for DB and run migrations
  console.log("Waiting for database...");
  let connected = false;
  while (!connected) {
    try {
      await pool.query("SELECT 1");
      connected = true;
    } catch {
      console.log("Database not ready, retrying in 1s...");
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  console.log("Running migrations...");
  await runMigrations();

  // Seed loadgen API key (must happen before markHealthy)
  seedLoadgenKey();

  // Start retention worker
  startRetentionWorker();

  // Start buffered-ingestion flush worker
  startFlushWorker();

  // Start server
  await app.listen({ port: config.port, host: "0.0.0.0" });
  console.log(`Server listening on port ${config.port}`);

  // Mark healthy
  markHealthy();

  // Graceful shutdown
  const shutdown = async (): Promise<void> => {
    console.log("Shutting down...");
    await forceFlush();
    await app.close();
    await pool.end();
    process.exit(0);
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main().catch((err) => {
  console.error("Failed to start server:", err);
  process.exit(1);
});
