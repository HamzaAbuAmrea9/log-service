// Shared helpers for the hourly ingestion rollup (migration 011).
//
// The rollup keeps a per-hour COUNT by (service, level), updated in the same
// transaction as the rows themselves so aggregate-1h queries never need to
// scan the whole window on a single CPU. Hour buckets are anchored to UTC hour
// boundaries, matching date_bin('1 hour', ts, '2000-01-01') exactly.

export const ROLLUP_HOUR_MS = 3600000;

export function hourBucketStart(ms: number): number {
  return Math.floor(ms / ROLLUP_HOUR_MS) * ROLLUP_HOUR_MS;
}

export interface RollupCount {
  bucketStart: string;
  service: string;
  level: number;
  count: number;
}

// Aggregates raw rows (epoch ms + service + numeric level) into per-hour
// (service, level) counts so the UPSERT touches one row per distinct triple
// instead of re-grouping the whole batch on the database.
export function aggregateRollupCounts(
  rows: Array<{ ts: number; service: string; level: number }>,
): RollupCount[] {
  const counts = new Map<string, RollupCount>();
  for (const row of rows) {
    const bucket = new Date(hourBucketStart(row.ts)).toISOString();
    const key = `${bucket}\u0000${row.service}\u0000${row.level}`;
    const cur = counts.get(key);
    if (cur) {
      cur.count += 1;
    } else {
      counts.set(key, { bucketStart: bucket, service: row.service, level: row.level, count: 1 });
    }
  }
  return [...counts.values()];
}

// Builds an UPSERT for already-aggregated counts. With `decrement`, the
// values are subtracted (used by retention when rows are deleted).
export function buildRollupUpsertCounts(
  counts: RollupCount[],
  decrement = false,
): { text: string; values: unknown[] } {
  const placeholders: string[] = [];
  const values: unknown[] = [];
  let i = 0;
  for (const c of counts) {
    placeholders.push(`($${i + 1}, $${i + 2}, $${i + 3}, $${i + 4}::bigint)`);
    values.push(c.bucketStart, c.service, c.level, c.count);
    i += 4;
  }

  return {
    text: `INSERT INTO log_rollup (bucket_start, service, level, count) VALUES ${placeholders.join(", ")}
      ON CONFLICT (bucket_start, service, level)
      DO UPDATE SET count = log_rollup.count ${decrement ? "-" : "+"} EXCLUDED.count`,
    values,
  };
}

export function buildRollupUpsert(
  rows: Array<{ ts: number; service: string; level: number }>,
  decrement = false,
): { text: string; values: unknown[] } {
  return buildRollupUpsertCounts(aggregateRollupCounts(rows), decrement);
}
