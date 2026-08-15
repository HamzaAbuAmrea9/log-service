import { FastifyInstance } from "fastify";
import { ingestLogs, flushBeforeQuery } from "../services/ingest.js";
import { queryLogs } from "../services/query.js";
import { LogEntry, validateBatch, LEVEL_MAP } from "../utils/validate.js";

const VALID_LEVELS = new Set(["debug", "info", "warn", "error"]);
const MAX_LIMIT = 1000;
const DEFAULT_LIMIT = 100;

export async function logsRoutes(app: FastifyInstance): Promise<void> {
  app.post<{ Body: { logs: unknown[] } }>("/logs", async (request, reply) => {
    const body = request.body as Record<string, unknown> | undefined;

    // Validate top-level structure
    if (!body || !Array.isArray(body.logs)) {
      return reply.status(400).send({ error: "Request body must contain a 'logs' array" });
    }

    const logs: unknown[] = body.logs;

    if (logs.length === 0) {
      return reply.status(400).send({ error: "logs array must not be empty" });
    }

    // Validate all entries
    const errors = validateBatch(logs);

    // Filter valid entries
    const errorIndices = new Set(errors.map((e) => e.index));
    const validEntries: LogEntry[] = [];

    for (let i = 0; i < logs.length; i++) {
      if (!errorIndices.has(i)) {
        validEntries.push(logs[i] as LogEntry);
      }
    }

    // If all entries are rejected, return 400
    if (validEntries.length === 0) {
      return reply.status(400).send({
        accepted: 0,
        rejected: errors,
      });
    }

    // Ingest valid entries
    const result = await ingestLogs(validEntries);

    if (result.backpressured) {
      return reply
        .status(503)
        .header("Retry-After", "1")
        .send({ error: "service is at capacity; retry shortly" });
    }

    return reply.status(200).send({
      accepted: result.accepted,
      rejected: errors,
    });
  });

  app.get("/logs", async (request, reply) => {
    const query = request.query as Record<string, string>;

    // Parse and validate limit
    let limit = DEFAULT_LIMIT;
    if (query.limit) {
      if (!/^\d+$/.test(query.limit)) {
        return reply.status(400).send({ error: `limit must be an integer between 1 and ${MAX_LIMIT}` });
      }
      const parsed = parseInt(query.limit, 10);
      if (parsed < 1 || parsed > MAX_LIMIT) {
        return reply.status(400).send({ error: `limit must be between 1 and ${MAX_LIMIT}` });
      }
      limit = parsed;
    }

    // Validate level
    if (query.level && !VALID_LEVELS.has(query.level)) {
      return reply.status(400).send({ error: `invalid level: '${query.level}'` });
    }

    // Validate timestamps
    if (query.since && isNaN(new Date(query.since).getTime())) {
      return reply.status(400).send({ error: "Invalid 'since' timestamp" });
    }
    if (query.until && isNaN(new Date(query.until).getTime())) {
      return reply.status(400).send({ error: "Invalid 'until' timestamp" });
    }

    // Validate until >= since (exclusive end, so an empty range is valid)
    if (query.since && query.until) {
      const since = new Date(query.since).getTime();
      const until = new Date(query.until).getTime();
      if (until < since) {
        return reply.status(400).send({ error: "'until' must not be earlier than 'since'" });
      }
    }

    // Parse attribute filters (attr.<key>=<value>)
    const attrFilters: Array<{ key: string; value: string }> = [];
    for (const [key, value] of Object.entries(query)) {
      if (key.startsWith("attr.") && key.length > 5) {
        attrFilters.push({ key: key.slice(5), value });
      }
    }

    try {
      // Ensure any buffered (not yet committed) logs are visible to reads.
      await flushBeforeQuery();

      const result = await queryLogs({
        service: query.service,
        level: query.level,
        since: query.since,
        until: query.until,
        q: query.q,
        limit,
        cursor: query.cursor,
        attrFilters,
      });

      return reply.status(200).send(result);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      return reply.status(400).send({ error: message });
    }
  });
}
