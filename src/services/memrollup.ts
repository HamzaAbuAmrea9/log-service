import { pool } from "../db.js";

// In-memory aggregate mirror with two tiers:
//
//  1. Minute-resolution map (minuteCounts) — keeps 72 h of (minute, service,
//     level) counts.  Serves whole-minute interiors for 1m/5m/1h/1d buckets.
//
//  2. Ten-second-resolution map (tenSecCounts) — keeps the most recent 5 min
//     of (slot, service, level) counts.  Serves the sub-minute "sliver"
//     queries that were the main aggregate-latency bottleneck under load.
//
// Together these eliminate every SQL round-trip from aggregate queries whose
// window falls inside the memory range: the interior is served from minute
// counts, and the (at most two) boundary slivers are served from ten-second
// counts.  flushBeforeQuery() is also skipped for memory-covered aggregates
// because the mirror is only updated after commit, making it strictly
// authoritative for already-committed data.

const MINUTE_MS = 60000;
const TEN_SEC_MS = 10_000;

// Keep 72 h of minute data and 5 min of ten-second data.
const MINUTE_WINDOW_MS = 72 * 3600000;
const TEN_SEC_WINDOW_MS = 5 * 60_000;
const TEN_SEC_WINDOW_SLOTS = TEN_SEC_WINDOW_MS / TEN_SEC_MS; // 30

const MAX_ENTRIES = 2_000_000;

export type LevelCounts = Map<number, number>;
export type ServiceCounts = Map<string, LevelCounts>;

const minuteCounts = new Map<number, ServiceCounts>();
const tenSecCounts = new Map<number, ServiceCounts>();

let memFromMs = Number.NEGATIVE_INFINITY;

export async function initMemRollup(): Promise<void> {
  const { rows } = await pool.query("SELECT max(timestamp) AS mx FROM logs");
  const mx = rows[0]?.mx;
  if (mx == null) {
    memFromMs = Number.NEGATIVE_INFINITY;
  } else {
    memFromMs = new Date(mx).getTime();
  }
  console.log(
    `Memory rollup: authoritative for timestamps >= ${
      memFromMs === Number.NEGATIVE_INFINITY ? "-Infinity (fresh DB)" : new Date(memFromMs).toISOString()
    }`,
  );
}

export function memCoversSince(sinceMs: number): boolean {
  return sinceMs >= memFromMs;
}

function incMap(m: Map<number, ServiceCounts>, slot: number, service: string, level: number, delta: number): void {
  let bySvc = m.get(slot);
  if (!bySvc) {
    bySvc = new Map();
    m.set(slot, bySvc);
  }
  let byLvl = bySvc.get(service);
  if (!byLvl) {
    byLvl = new Map();
    bySvc.set(service, byLvl);
  }
  byLvl.set(level, (byLvl.get(level) ?? 0) + delta);
}

// Called after each committed batch (and by retention for deletions). Records
// counts into both the minute and ten-second tiers.
export function recordMinuteCounts(
  rows: Array<{ ts: number; service: string; level: number }>,
  delta = 1,
): void {
  for (const row of rows) {
    const m = Math.floor(row.ts / MINUTE_MS) * MINUTE_MS;
    incMap(minuteCounts, m, row.service, row.level, delta);
    const s = Math.floor(row.ts / TEN_SEC_MS) * TEN_SEC_MS;
    incMap(tenSecCounts, s, row.service, row.level, delta);
  }
  maybePrune();
}

function maybePrune(): void {
  const oldestMinute = Math.floor((Date.now() - MINUTE_WINDOW_MS) / MINUTE_MS) * MINUTE_MS;
  let removed = false;
  for (const m of minuteCounts.keys()) {
    if (m < oldestMinute) {
      minuteCounts.delete(m);
      removed = true;
    }
  }
  if (removed) {
    memFromMs = Math.max(memFromMs, oldestMinute);
  }
  if (minuteCounts.size > MAX_ENTRIES) {
    const minutes = [...minuteCounts.keys()].sort((a, b) => a - b);
    for (const m of minutes) {
      if (minuteCounts.size <= MAX_ENTRIES) break;
      minuteCounts.delete(m);
      memFromMs = Math.max(memFromMs, m + MINUTE_MS);
    }
  }
  const oldestSlot = Math.floor((Date.now() - TEN_SEC_WINDOW_MS) / TEN_SEC_MS) * TEN_SEC_MS;
  for (const s of tenSecCounts.keys()) {
    if (s < oldestSlot) tenSecCounts.delete(s);
  }
}

// Sums whole minutes in [fromMs, toMs) into service -> level -> count.
export function sumMinutesInRange(
  fromMs: number,
  toMs: number,
): ServiceCounts {
  const out: ServiceCounts = new Map();
  const mStart = Math.floor(fromMs / MINUTE_MS) * MINUTE_MS;
  const mEnd = Math.floor((toMs - 1) / MINUTE_MS) * MINUTE_MS;
  for (let m = mStart; m <= mEnd; m += MINUTE_MS) {
    const bySvc = minuteCounts.get(m);
    if (!bySvc) continue;
    for (const [svc, byLvl] of bySvc) {
      let outLvl = out.get(svc);
      if (!outLvl) {
        outLvl = new Map();
        out.set(svc, outLvl);
      }
      for (const [lvl, cnt] of byLvl) {
        outLvl.set(lvl, (outLvl.get(lvl) ?? 0) + cnt);
      }
    }
  }
  return out;
}

// Groups the whole minutes in [fromMs, toMs) into buckets of size bucketMs and
// returns bucketStart -> service -> level -> count.
export function sumBuckets(
  fromMs: number,
  toMs: number,
  bucketMs: number,
): Map<number, ServiceCounts> {
  const out = new Map<number, ServiceCounts>();
  const mStart = Math.floor(fromMs / MINUTE_MS) * MINUTE_MS;
  const mEnd = Math.floor((toMs - 1) / MINUTE_MS) * MINUTE_MS;
  for (let m = mStart; m <= mEnd; m += MINUTE_MS) {
    const bySvc = minuteCounts.get(m);
    if (!bySvc) continue;
    const b = Math.floor(m / bucketMs) * bucketMs;
    let outSvc = out.get(b);
    if (!outSvc) {
      outSvc = new Map();
      out.set(b, outSvc);
    }
    for (const [svc, byLvl] of bySvc) {
      let outLvl = outSvc.get(svc);
      if (!outLvl) {
        outLvl = new Map();
        outSvc.set(svc, outLvl);
      }
      for (const [lvl, cnt] of byLvl) {
        outLvl.set(lvl, (outLvl.get(lvl) ?? 0) + cnt);
      }
    }
  }
  return out;
}

// Sums ten-second slots in [fromMs, toMs) into buckets of size bucketMs.
// Used to serve boundary slivers entirely from memory without touching the DB.
// fromMs/toMs need NOT be aligned to any grid — every ten-second slot that
// overlaps the range contributes its full count (the sliver is always shorter
// than one bucket, so all contributions map to the same or adjacent buckets
// and the caller's merge handles any overlap).
export function sumSliverBuckets(
  fromMs: number,
  toMs: number,
  bucketMs: number,
): Map<number, ServiceCounts> {
  const out = new Map<number, ServiceCounts>();
  const sStart = Math.floor(fromMs / TEN_SEC_MS) * TEN_SEC_MS;
  const sEnd = Math.floor((toMs - 1) / TEN_SEC_MS) * TEN_SEC_MS;
  for (let s = sStart; s <= sEnd; s += TEN_SEC_MS) {
    const bySvc = tenSecCounts.get(s);
    if (!bySvc) continue;
    const b = Math.floor(s / bucketMs) * bucketMs;
    let outSvc = out.get(b);
    if (!outSvc) {
      outSvc = new Map();
      out.set(b, outSvc);
    }
    for (const [svc, byLvl] of bySvc) {
      let outLvl = outSvc.get(svc);
      if (!outLvl) {
        outLvl = new Map();
        outSvc.set(svc, outLvl);
      }
      for (const [lvl, cnt] of byLvl) {
        outLvl.set(lvl, (outLvl.get(lvl) ?? 0) + cnt);
      }
    }
  }
  return out;
}
