import { pool } from "../db.js";
import { AggregateBucket, LEVEL_MAP, jsonbEqualityCandidates } from "../utils/validate.js";

const HOUR_MS = 3600000;
const DAY_MS = 86400000;
const BUCKET_ORIGIN = "2000-01-01";

const BUCKETS: Record<string, { interval: string; ms: number }> = {
  "1m": { interval: "1 minute", ms: 60000 },
  "5m": { interval: "5 minutes", ms: 300000 },
  "1h": { interval: "1 hour", ms: HOUR_MS },
  "1d": { interval: "1 day", ms: DAY_MS },
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

function floorTo(ms: number, size: number): number {
  return Math.floor(ms / size) * size;
}

// The hourly rollup (migration 011) can serve any aggregate whose buckets are
// aligned to hours or coarser (1h, 1d: day buckets are sums of their 24 hourly
// rows) and whose filters are service/level only. Text and attribute filters
// cannot be rolled up and fall back to the live scan.
function canUseRollup(params: AggregateParams): boolean {
  if (params.bucket !== "1h" && params.bucket !== "1d") return false;
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
): { conditions: string[]; values: unknown[]; nextIdx: number } {
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
  return { conditions, values, nextIdx: idx };
}

export async function aggregateLogs(params: AggregateParams): Promise<AggregateBucket[]> {
  const bucket = BUCKETS[params.bucket];
  if (!bucket) {
    throw new Error(`Invalid bucket: '${params.bucket}'. Must be one of: 1m, 5m, 1h, 1d`);
  }

  if (canUseRollup(params)) {
    return aggregateFromRollup(params, bucket);
  }
  return aggregateLive(params, bucket.interval);
}

// Serves whole middle buckets from log_rollup and live-counts only the two
// (at most) partial edge buckets, so a 24h (or 30-day) window costs two
// index-range scans of <=1 bucket of rows instead of one scan of the whole
// window.
async function aggregateFromRollup(
  params: AggregateParams,
  bucket: { interval: string; ms: number },
): Promise<AggregateBucket[]> {
  const sinceMs = new Date(params.since).getTime();
  const untilMs = new Date(params.until).getTime();
  if (untilMs <= sinceMs) return [];

  const first = floorTo(sinceMs, bucket.ms);
  const last = floorTo(untilMs - 1, bucket.ms);
  if (first === last) {
    return aggregateLive(params, bucket.interval);
  }

  const rows: AggregateBucket[] = [];

  // Partial first bucket: [since, first+bucket).
  const firstRows = await queryBucketRange(params, sinceMs, first + bucket.ms, bucket.interval);
  rows.push(...firstRows);

  // Partial last bucket: [last, until).
  const lastRows = await queryBucketRange(params, last, untilMs, bucket.interval);
  rows.push(...lastRows);

  // Whole middle buckets: [first+bucket, last).
  if (first + bucket.ms < last) {
    const mid = await queryRollupRange(params, first + bucket.ms, last, bucket.interval);
    rows.push(...mid);
  }

  rows.sort((a, b) =>
    a.start < b.start
      ? -1
      : a.start > b.start
        ? 1
        : a.group === b.group
          ? 0
          : a.group === null
            ? -1
            : b.group === null
              ? 1
              : a.group < b.group
                ? -1
                : 1,
  );
  return rows;
}

async function queryBucketRange(
  params: AggregateParams,
  fromMs: number,
  toMs: number,
  interval: string,
): Promise<AggregateBucket[]> {
  const { conditions, values, nextIdx } = parseFilters(params, 3);
  conditions.unshift("timestamp >= $1", "timestamp < $2");
  values.unshift(new Date(fromMs).toISOString(), new Date(toMs).toISOString());

  const gc = groupClause(params.group_by);
  const query = `
    SELECT
      date_bin($${nextIdx}, timestamp, '${BUCKET_ORIGIN}'::timestamptz) AS start,
      ${gc.select},
      COUNT(*)::int AS count
    FROM logs
    WHERE ${conditions.join(" AND ")}
    GROUP BY date_bin($${nextIdx}, timestamp, '${BUCKET_ORIGIN}'::timestamptz)${gc.group}
  `;
  values.push(interval);
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
  interval: string,
): Promise<AggregateBucket[]> {
  const { conditions, values, nextIdx } = parseFilters(params, 3);
  conditions.unshift("bucket_start >= $1", "bucket_start < $2");
  values.unshift(new Date(fromMs).toISOString(), new Date(toMs).toISOString());

  const gc = groupClause(params.group_by);
  // For 1h buckets bucket_start is already the bucket edge; for 1d buckets we
  // sum the 24 hourly rollup rows that fall inside each day.
  const startExpr =
    interval === "1 hour"
      ? "bucket_start"
      : `date_bin($${nextIdx}, bucket_start, '${BUCKET_ORIGIN}'::timestamptz)`;
  const query = `
    SELECT
      ${startExpr} AS start,
      ${gc.select},
      SUM(count)::int AS count
    FROM log_rollup
    WHERE ${conditions.join(" AND ")}
    GROUP BY ${startExpr}${gc.group}
  `;
  values.push(interval);
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
