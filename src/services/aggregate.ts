import { pool } from "../db.js";
import { AggregateBucket, LEVEL_MAP, jsonbEqualityCandidates } from "../utils/validate.js";

const BUCKET_INTERVALS: Record<string, string> = {
  "1m": "1 minute",
  "5m": "5 minutes",
  "1h": "1 hour",
  "1d": "1 day",
};

interface AggregateParams {
  since: string;
  until: string;
  bucket: string;
  group_by?: string;
  service?: string;
  level?: string;
  q?: string;
  attrFilters?: Array<{ key: string; value: string }>;
}

export async function aggregateLogs(params: AggregateParams): Promise<AggregateBucket[]> {
  const interval = BUCKET_INTERVALS[params.bucket];
  if (!interval) {
    throw new Error(`Invalid bucket: '${params.bucket}'. Must be one of: 1m, 5m, 1h, 1d`);
  }

  const conditions: string[] = ["timestamp >= $1", "timestamp < $2"];
  const values: unknown[] = [params.since, params.until];
  let paramIdx = 3;

  if (params.service) {
    conditions.push(`service = $${paramIdx++}`);
    values.push(params.service);
  }

  if (params.level) {
    const levelNum = LEVEL_MAP[params.level];
    if (levelNum === undefined) {
      throw new Error(`Invalid level: ${params.level}`);
    }
    conditions.push(`level = $${paramIdx++}`);
    values.push(levelNum);
  }

  if (params.q) {
    conditions.push(`message ILIKE $${paramIdx++}`);
    values.push(`%${params.q}%`);
  }

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

  const where = conditions.join(" AND ");
  const groupByClause = params.group_by === "service"
    ? ", service"
    : params.group_by === "level"
      ? ", level"
      : "";

  const groupSelect = params.group_by === "service"
    ? "service AS group_name"
    : params.group_by === "level"
      ? `CASE level WHEN 0 THEN 'debug' WHEN 1 THEN 'info' WHEN 2 THEN 'warn' WHEN 3 THEN 'error' END AS group_name`
      : "NULL AS group_name";

  // Use date_bin for fixed-width buckets (works with any interval)
  const query = `
    SELECT
      date_bin($${paramIdx}, timestamp, '2000-01-01'::timestamptz) AS start,
      ${groupSelect},
      COUNT(*)::int AS count
    FROM logs
    WHERE ${where}
    GROUP BY date_bin($${paramIdx}, timestamp, '2000-01-01'::timestamptz)${groupByClause}
    ORDER BY start ASC
  `;
  values.push(interval);

  const { rows } = await pool.query(query, values);

  return rows.map((row) => ({
    start: row.start,
    group: row.group_name,
    count: row.count,
  }));
}
