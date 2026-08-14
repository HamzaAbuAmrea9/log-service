import { pool } from "../db.js";
import { StoredLog, QueryResult, LEVEL_MAP, LEVEL_NAMES, jsonbEqualityCandidates } from "../utils/validate.js";
import { encodeCursor, decodeCursor } from "../utils/cursor.js";

interface QueryParams {
  service?: string;
  level?: string;
  since?: string;
  until?: string;
  q?: string;
  limit?: number;
  cursor?: string;
  attrFilters?: Array<{ key: string; value: string }>;
}

export async function queryLogs(params: QueryParams): Promise<QueryResult> {
  const conditions: string[] = [];
  const values: unknown[] = [];
  let paramIdx = 1;

  // Filter: service
  if (params.service) {
    conditions.push(`service = $${paramIdx++}`);
    values.push(params.service);
  }

  // Filter: level
  if (params.level) {
    const levelNum = LEVEL_MAP[params.level];
    if (levelNum === undefined) {
      throw new Error(`Invalid level: ${params.level}`);
    }
    conditions.push(`level = $${paramIdx++}`);
    values.push(levelNum);
  }

  // Filter: since (inclusive)
  if (params.since) {
    const sinceDate = new Date(params.since);
    if (isNaN(sinceDate.getTime())) {
      throw new Error("Invalid 'since' timestamp");
    }
    conditions.push(`timestamp >= $${paramIdx++}`);
    values.push(params.since);
  }

  // Filter: until (exclusive)
  if (params.until) {
    const untilDate = new Date(params.until);
    if (isNaN(untilDate.getTime())) {
      throw new Error("Invalid 'until' timestamp");
    }
    conditions.push(`timestamp < $${paramIdx++}`);
    values.push(params.until);
  }

  // Filter: message substring (case-insensitive)
  if (params.q) {
    conditions.push(`message ILIKE $${paramIdx++}`);
    values.push(`%${params.q}%`);
  }

  // Filter: attributes (GIN-indexed containment; matches strings, numbers,
  // and booleans so it is equivalent to `attributes->>key = value`)
  if (params.attrFilters) {
    for (const attr of params.attrFilters) {
      const [numObj, strObj] = jsonbEqualityCandidates(attr.key, attr.value);
      conditions.push(
        `(attributes @> $${paramIdx}::jsonb OR attributes @> $${paramIdx + 1}::jsonb)`,
      );
      values.push(numObj, strObj);
      paramIdx += 2;
    }
  }

  // Cursor pagination
  if (params.cursor) {
    const decoded = decodeCursor(params.cursor);
    conditions.push(`(timestamp, id) < ($${paramIdx++}, $${paramIdx++})`);
    values.push(decoded.timestamp, decoded.id);
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const limit = params.limit || 100;

  const query = `
    SELECT id, timestamp, level, service, message, attributes
    FROM logs
    ${where}
    ORDER BY timestamp DESC, id DESC
    LIMIT $${paramIdx}
  `;
  values.push(limit + 1); // Fetch one extra to detect if there's a next page

  const { rows } = await pool.query(query, values);

  const hasMore = rows.length > limit;
  const logs: StoredLog[] = rows.slice(0, limit).map((row) => ({
    id: row.id,
    timestamp: row.timestamp,
    level: LEVEL_NAMES[row.level] || "unknown",
    service: row.service,
    message: row.message,
    attributes: row.attributes,
  }));

  let nextCursor: string | null = null;
  if (hasMore && logs.length > 0) {
    const last = logs[logs.length - 1];
    nextCursor = encodeCursor(last.timestamp, Number(last.id));
  }

  return { logs, next_cursor: nextCursor };
}
