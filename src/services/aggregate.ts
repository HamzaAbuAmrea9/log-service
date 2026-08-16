import { pool } from "../db.js";
import { AggregateBucket, LEVEL_MAP, LEVEL_NAMES, jsonbEqualityCandidates } from "../utils/validate.js";
import { memCoversSince, sumBuckets, sumSliverBuckets, ServiceCounts } from "./memrollup.js";

const HOUR_MS = 3600000;
const DAY_MS = 86400000;
const MINUTE_MS = 60000;
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

export async function aggregateLogs(params: AggregateParams): Promise<AggregateBucket[]> {
  const bucket = BUCKETS[params.bucket];
  if (!bucket) {
    throw new Error(`Invalid bucket: '${params.bucket}'. Must be one of: 1m, 5m, 1h, 1d`);
  }

  // Text and attribute filters cannot be answered from the rollup or the
  // in-memory mirror; they always fall back to the live scan.
  if (!params.q && (!params.attrFilters || params.attrFilters.length === 0)) {
    return aggregateHybrid(params, bucket);
  }
  return aggregateLive(params, bucket.interval);
}

// Serves any filter-free window entirely from the in-memory mirror.
//
// When memory covers the range, the (at most two) boundary slivers are served
// from the ten-second-resolution tier and the whole-minute interior from the
// minute-resolution tier.  This eliminates every SQL round-trip from the hot
// aggregate path — the only DB queries left are for windows that predate the
// memory floor.
async function aggregateHybrid(
  params: AggregateParams,
  bucket: { interval: string; ms: number },
): Promise<AggregateBucket[]> {
  const sinceMs = new Date(params.since).getTime();
  const untilMs = new Date(params.until).getTime();
  if (untilMs <= sinceMs) return [];

  const nextMinute = Math.ceil(sinceMs / MINUTE_MS) * MINUTE_MS;
  const prevMinute = Math.floor(untilMs / MINUTE_MS) * MINUTE_MS;

  // Less than one whole minute of interior: the window spans at most two
  // minutes, so scanning it live is cheap and exactly the old behaviour.
  if (nextMinute >= prevMinute) {
    return aggregateLive(params, bucket.interval);
  }

  if (memCoversSince(nextMinute)) {
    const sliver1 = bucketsToRows(sumSliverBuckets(sinceMs, nextMinute, bucket.ms), params);
    const interior = aggregateMemoryInterior(params, nextMinute, prevMinute, bucket.ms);
    const sliver2 = bucketsToRows(sumSliverBuckets(prevMinute, untilMs, bucket.ms), params);
    return mergeBuckets([sliver1, interior, sliver2]);
  }

  // Not covered by memory: 1h/1d use the hourly rollup (edges live + middle
  // from log_rollup), 1m/5m scan the whole window live — exactly what the
  // service did before the memory mirror existed.
  if (bucket.ms >= HOUR_MS) {
    return aggregateFromRollup(params, bucket);
  }
  return aggregateLive(params, bucket.interval);
}

// Sums the whole minutes in [fromMs, toMs) (both minute-aligned, guaranteed
// covered) into AggregateBuckets, applying the service/level filters and the
// group_by in JS. The bucket grid is anchored at the same 2000-01-01 UTC
// origin date_bin uses, so bucket starts match the SQL path byte for byte.
function aggregateMemoryInterior(
  params: AggregateParams,
  fromMs: number,
  toMs: number,
  bucketMs: number,
): AggregateBucket[] {
  const buckets = sumBuckets(fromMs, toMs, bucketMs);
  const rows: AggregateBucket[] = [];
  const levelFilter = params.level !== undefined ? LEVEL_MAP[params.level] : undefined;

  for (const [bStart, bySvc] of buckets) {
    const start = new Date(bStart).toISOString();
    const svcIt = [...bySvc.entries()].filter(([svc]) => !params.service || svc === params.service);
    if (svcIt.length === 0) continue;

    if (!params.group_by) {
      let count = 0;
      for (const [, byLvl] of svcIt) {
        for (const [lvl, cnt] of byLvl) {
          if (levelFilter === undefined || lvl === levelFilter) count += cnt;
        }
      }
      if (count > 0) rows.push({ start, group: null, count });
      continue;
    }

    if (params.group_by === "service") {
      for (const [svc, byLvl] of svcIt) {
        let count = 0;
        for (const [lvl, cnt] of byLvl) {
          if (levelFilter === undefined || lvl === levelFilter) count += cnt;
        }
        if (count > 0) rows.push({ start, group: svc, count });
      }
      continue;
    }

    const perLevel = new Map<number, number>();
    for (const [, byLvl] of svcIt) {
      for (const [lvl, cnt] of byLvl) {
        if (levelFilter !== undefined && lvl !== levelFilter) continue;
        perLevel.set(lvl, (perLevel.get(lvl) ?? 0) + cnt);
      }
    }
    for (const [lvl, count] of perLevel) {
      rows.push({ start, group: LEVEL_NAMES[lvl], count });
    }
  }
  return rows;
}

// Converts a bucket map (from sumSliverBuckets or sumBuckets) into the
// AggregateBucket[] format, applying service/level filters and group_by.
function bucketsToRows(
  buckets: Map<number, ServiceCounts>,
  params: AggregateParams,
): AggregateBucket[] {
  const rows: AggregateBucket[] = [];
  const levelFilter = params.level !== undefined ? LEVEL_MAP[params.level] : undefined;

  for (const [bStart, bySvc] of buckets) {
    const start = new Date(bStart).toISOString();
    const svcIt = [...bySvc.entries()].filter(([svc]) => !params.service || svc === params.service);
    if (svcIt.length === 0) continue;

    if (!params.group_by) {
      let count = 0;
      for (const [, byLvl] of svcIt) {
        for (const [lvl, cnt] of byLvl) {
          if (levelFilter === undefined || lvl === levelFilter) count += cnt;
        }
      }
      if (count > 0) rows.push({ start, group: null, count });
      continue;
    }

    if (params.group_by === "service") {
      for (const [svc, byLvl] of svcIt) {
        let count = 0;
        for (const [lvl, cnt] of byLvl) {
          if (levelFilter === undefined || lvl === levelFilter) count += cnt;
        }
        if (count > 0) rows.push({ start, group: svc, count });
      }
      continue;
    }

    const perLevel = new Map<number, number>();
    for (const [, byLvl] of svcIt) {
      for (const [lvl, cnt] of byLvl) {
        if (levelFilter !== undefined && lvl !== levelFilter) continue;
        perLevel.set(lvl, (perLevel.get(lvl) ?? 0) + cnt);
      }
    }
    for (const [lvl, count] of perLevel) {
      rows.push({ start, group: LEVEL_NAMES[lvl], count });
    }
  }
  return rows;
}

// Merges possibly-overlapping bucket lists (a boundary sliver and the memory
// interior can both contribute to the same bucket) by summing counts per
// (start, group), then sorts deterministically.
function mergeBuckets(lists: AggregateBucket[][]): AggregateBucket[] {
  const byKey = new Map<string, AggregateBucket>();
  for (const list of lists) {
    for (const row of list) {
      const key = `${row.start}\u0000${row.group ?? ""}`;
      const cur = byKey.get(key);
      if (cur) {
        cur.count += row.count;
      } else {
        byKey.set(key, { ...row });
      }
    }
  }
  const rows = [...byKey.values()];
  sortBuckets(rows);
  return rows;
}

function sortBuckets(rows: AggregateBucket[]): void {
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

  sortBuckets(rows);
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
    start: new Date(row.start).toISOString(),
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
  // date_bin on bucket_start is the identity for 1h (hour-aligned rows) and
  // sums the 24 hourly rows per day for 1d. Using one $N placeholder keeps the
  // bind-parameter count aligned with the interval value pushed below.
  const startExpr = `date_bin($${nextIdx}, bucket_start, '${BUCKET_ORIGIN}'::timestamptz)`;
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
    start: new Date(row.start).toISOString(),
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
    start: new Date(row.start).toISOString(),
    group: row.group_name,
    count: row.count,
  }));
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
