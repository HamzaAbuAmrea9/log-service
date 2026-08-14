import { FastifyInstance } from "fastify";
import { aggregateLogs } from "../services/aggregate.js";
import { flushBeforeQuery } from "../services/ingest.js";

const VALID_LEVELS = new Set(["debug", "info", "warn", "error"]);
const VALID_BUCKETS = new Set(["1m", "5m", "1h", "1d"]);

export async function aggregateRoutes(app: FastifyInstance): Promise<void> {
  app.get("/logs/aggregate", async (request, reply) => {
    const query = request.query as Record<string, string>;

    // Validate required parameters
    if (!query.since) {
      return reply.status(400).send({ error: "'since' parameter is required" });
    }
    if (!query.until) {
      return reply.status(400).send({ error: "'until' parameter is required" });
    }
    if (!query.bucket) {
      return reply.status(400).send({ error: "'bucket' parameter is required" });
    }

    // Validate timestamps
    if (isNaN(new Date(query.since).getTime())) {
      return reply.status(400).send({ error: "Invalid 'since' timestamp" });
    }
    if (isNaN(new Date(query.until).getTime())) {
      return reply.status(400).send({ error: "Invalid 'until' timestamp" });
    }

    // Validate until >= since (exclusive end, so an empty range is valid)
    const since = new Date(query.since).getTime();
    const until = new Date(query.until).getTime();
    if (until < since) {
      return reply.status(400).send({ error: "'until' must not be earlier than 'since'" });
    }

    // Validate bucket
    if (!VALID_BUCKETS.has(query.bucket)) {
      return reply.status(400).send({ error: `Invalid bucket: '${query.bucket}'. Must be one of: 1m, 5m, 1h, 1d` });
    }

    // Validate level
    if (query.level && !VALID_LEVELS.has(query.level)) {
      return reply.status(400).send({ error: `invalid level: '${query.level}'` });
    }

    // Validate group_by
    if (query.group_by && query.group_by !== "service" && query.group_by !== "level") {
      return reply.status(400).send({ error: `Invalid group_by: '${query.group_by}'. Must be 'service' or 'level'` });
    }

    // Parse attribute filters
    const attrFilters: Array<{ key: string; value: string }> = [];
    for (const [key, value] of Object.entries(query)) {
      if (key.startsWith("attr.") && key.length > 5) {
        attrFilters.push({ key: key.slice(5), value });
      }
    }

    try {
      // Ensure any buffered (not yet committed) logs are visible to reads.
      await flushBeforeQuery();

      const buckets = await aggregateLogs({
        since: query.since,
        until: query.until,
        bucket: query.bucket,
        group_by: query.group_by,
        service: query.service,
        level: query.level,
        q: query.q,
        attrFilters,
      });

      return reply.status(200).send({ buckets });
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      return reply.status(400).send({ error: message });
    }
  });
}
