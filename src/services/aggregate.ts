import { pool } from "../db.js";
import { AggregateBucket, LEVEL_MAP, LEVEL_NAMES, jsonbEqualityCandidates } from "../utils/validate.js";

const BUCKET_INTERVALS: Record<string, string> = {
  "1m": "1 minute",
  "5m": "5 minutes",
  "1h": "1 hour",
  "1d": "1 day",
};

const HOUR_MS = 3600000;
const BUCKET_ORIGIN = "2000-01-01";

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

function floorToHour(ms: number): number {
  return Math.floor(ms / HOUR_MS) * HOUR_MS;
}

// The hourly rollup (migration 011) can serve any aggregate whose buckets are
// whole hours and whose filters are service/level only. Text and attribute
// filters cannot be rolled up and fall back to the live scan.
function canUseRollup(params: AggregateParams): boolean {
  if (params.bucket !== "1h") return false;
  if (params.q) return false;
  if (params.attrFilters && params.attrFilters.length > 0) return false;
  return true;
}

function groupClause(groupBy?: string): {
  select: string;
  group: string;
} {
  if (groupBy === "service") {
    return { select: "service AS group_name", group: ", service" };
  }
  if (groupBy === "level") {
    return {
      select: `CASE level WHEN 0 THEN 'debug' WHEN 1 THEN 'info' WHEN 2 THEN 'warn' WHEN 3 THEN 'error' END AS group_name`,
      group: ", level",
    };
  }
  return { select: "NULL AS group_name", group: "" };
}

function parseFilters(
  params: AggregateParams,
  startIdx: number,
): { conditions: string[]; values: unknown[] } {
  const conditions: string[] = [];
  const values: unknown[] = [];
  let idx = startIdx;
  if (params.service) {
    conditions.push(`service = $${idx++}`);
    values.push(params.service);
  }
  if (params.level) {
    const levelNum = LEVEL_MAP[params.level];
    if (levelNum === undefined) {
      throw new Error(`Invalid level: ${params.level}`);
    }
    conditions.push(`level = $${idx++}`);
    values.push(levelNum);
  }
  return { conditions, values };
}

export async function aggregateLogs(params: AggregateParams): Promise<AggregateBucket[]> {
  const interval = BUCKET_INTERVALS[params.bucket];
  if (!interval) {
    throw new Error(`Invalid bucket: '${params.bucket}'. Must be one of: 1m, 5m, 1h, 1d`);
  }

  if (canUseRollup(params)) {
    return aggregateFromRollup(params);
  }
  return aggregateLive(params, interval);
}

// Serves whole middle buckets from log_rollup and live-counts only the two
// (at most) partial edge buckets, so a 24h window costs two index-range scans
// of <=1h of rows instead of one scan of the whole window.
async function aggregateFromRollup(params: AggregateParams): Promise<AggregateBucket[]> {
  const sinceMs = new Date(params.since).getTime();
  const untilMs = new Date(params.until).getTime();
  if (untilMs <= sinceMs) return [];

  const first = floorToHour(sinceMs);
  const last = floorToHour(untilMs - 1);
  if (first === last) {
    return aggregateLive(params, "1 hour");
  }

  const rows: AggregateBucket[] = [];

  // Partial first bucket: [since, first+1h).
  const firstRows = await queryBucketRange(params, sinceMs, first + HOUR_MS);
  rows.push(...firstRows);

  // Partial last bucket: [last, until).
  const lastRows = await queryBucketRange(params, last, untilMs);
  rows.push(...lastRows);

  // Whole middle buckets: [first+1h, last).
  if (first + HOUR_MS < last) {
    const mid = await queryRollupRange(params, first + HOUR_MS, last);
    rows.push(...mid);
  }

  rows.sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0));
  return rows;
}

async function queryBucketRange(
  params: AggregateParams,
  fromMs: number,
  toMs: number,
): Promise<AggregateBucket[]> {
  const { conditions, values } = parseFilters(params, 3);
  conditions.unshift("timestamp >= $1", "timestamp < $2");
  values.unshift(new Date(fromMs).toISOString(), new Date(toMs).toISOString());

  const gc = groupClause(params.group_by);
  const query = `
    SELECT
      date_bin('1 hour', timestamp, '${BUCKET_ORIGIN}'::timestamptz) AS start,
      ${gc.select},
      COUNT(*)::int AS count
    FROM logs
    WHERE ${conditions.join(" AND ")}
    GROUP BY date_bin('1 hour', timestamp, '${BUCKET_ORIGIN}'::timestamptz)${gc.group}
  `;
  const { rows } = await pool.query(query, values);
  return rows.map((row) => ({
    start: row.start,
    group: row.group_name,
    count: row.count,
  }));
}

async function queryRollupRange(
  params: AggregateParams,
  fromMs: number,
  toMs: number,
): Promise<AggregateBucket[]> {
  const { conditions, values } = parseFilters(params, 3);
  conditions.unshift("bucket_start >= $1", "bucket_start < $2");
  values.unshift(new Date(fromMs).toISOString(), new Date(toMs).toISOString());

  const gc = groupClause(params.group_by);
  const query = `
    SELECT
      bucket_start AS start,
      ${gc.select},
      SUM(count)::int AS count
    FROM log_rollup
    WHERE ${conditions.join(" AND ")}
    GROUP BY bucket_start${gc.group}
  `;
  const { rows } = await pool.query(query, values);
  return rows.map((row) => ({
    start: row.start,
    group: row.group_name,
    count: row.count,
  }));
}

async function aggregateLive(
  params: AggregateParams,
  interval: string,
): Promise<AggregateBucket[]> {
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
  const gc = groupClause(params.group_by);

  const query = `
    SELECT
      date_bin($${paramIdx}, timestamp, '${BUCKET_ORIGIN}'::timestamptz) AS start,
      ${gc.select},
      COUNT(*)::int AS count
    FROM logs
    WHERE ${where}
    GROUP BY date_bin($${paramIdx}, timestamp, '${BUCKET_ORIGIN}'::timestamptz)${gc.group}
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
