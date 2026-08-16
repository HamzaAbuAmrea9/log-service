import { FastifyInstance } from "fastify";
import { aggregateLogs } from "../services/aggregate.js";
import { flushBeforeQuery } from "../services/ingest.js";
import { memCoversSince } from "../services/memrollup.js";

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
      const bucket = query.bucket as string;
      const bucketMs = bucket === "1m" ? 60000 : bucket === "5m" ? 300000 : bucket === "1h" ? 3600000 : 86400000;

      // When the memory mirror covers the entire window (including boundary
      // slivers), the aggregate can be served without touching the database at
      // all — skip the flush to avoid stalling on the pump queue.
      const nextMinute = Math.ceil(since / 60000) * 60000;
      const hasTextFilters = !!query.q || attrFilters.length > 0;
      if (!hasTextFilters && memCoversSince(nextMinute)) {
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
      }

      // Fall back to flushing before query for non-memory-covered or
      // text-filtered aggregates.
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
