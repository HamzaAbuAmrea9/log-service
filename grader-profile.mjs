#!/usr/bin/env node
// Replicates the grader's four load scenarios against a running instance:
//   Load: 15000/s 120s
//   Stress: 15000/s 30s -> 22500/s 60s -> 30000/s 60s
//   Spike: 7500/s 30s -> 30000/s 10s -> 7500/s 60s
//   Breakpoint: 15000/s 30s -> 22500/s 30s -> 30000/s 30s -> 45000/s 30s
// Tracks: achieved (HTTP 200) rate, POST latency, DB committed rate (gap),
// and during-load query/aggregate latency.
// Run: node grader-profile.mjs [--batches N] [--batch 1000] [--phases load,stress]

import pg from "pg";

const BASE_URL = process.env.BASE_URL || "http://localhost:8080";
const DATABASE_URL = process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/logs";
const args = process.argv.slice(2);
const arg = (n, d) => { const i = args.indexOf(n); return i >= 0 ? args[i + 1] : d; };

const BATCH = parseInt(arg("--batch", "1000"), 10);
const MAX_INFLIGHT = parseInt(arg("--inflight", "20"), 10);
const QUERY_EVERY_MS = 1000; // one aggregate + one search per second, like the grader

const PHASES = {
  load: [[15000, 120]],
  stress: [[15000, 30], [22500, 60], [30000, 60]],
  spike: [[7500, 30], [30000, 10], [7500, 60]],
  breakpoint: [[15000, 30], [22500, 30], [30000, 30], [45000, 30]],
  pump: [[20000, 60]],
};
const requested = (arg("--phases", "load,stress,spike,breakpoint")).split(",");

const SERVICES = ["checkout", "auth", "api", "payment", "inventory", "search", "notification", "web"];
const LEVELS = ["debug", "info", "warn", "error"];
const MESSAGES = ["request handled", "timeout exceeded", "cache miss", "db query slow", "user login", "payment declined", "retrying request", "queue full"];
const REGIONS = ["us-east", "eu-west", "ap-south", "us-west"];

const pgPool = new pg.Pool({ connectionString: DATABASE_URL });
let counter = 0;

function makeBatch() {
  const logs = [];
  const baseTs = Date.now() - (counter % 100) * 60000;
  for (let i = 0; i < BATCH; i++) {
    const n = counter++;
    const ts = new Date(baseTs - Math.random() * 30 * 86400000).toISOString();
    logs.push({
      timestamp: ts,
      level: LEVELS[Math.floor(Math.random() * LEVELS.length)],
      service: SERVICES[Math.floor(Math.random() * SERVICES.length)],
      message: `${MESSAGES[Math.floor(Math.random() * MESSAGES.length)]} #${n}`,
      attributes: {
        user_id: String(Math.floor(Math.random() * 50000)),
        region: REGIONS[Math.floor(Math.random() * REGIONS.length)],
        request_id: `req-${n % 100000}`,
        retries: n % 4,
      },
    });
  }
  return JSON.stringify({ logs });
}

async function postBatch() {
  const t0 = performance.now();
  const res = await fetch(`${BASE_URL}/logs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: makeBatch(),
    signal: AbortSignal.timeout(30000),
  });
  const elapsed = performance.now() - t0;
  const data = await res.json();
  return { status: res.status, accepted: data.accepted ?? 0, rejected: data.rejected?.length ?? 0, elapsed };
}

const now0 = Date.now();
const fmt = (ms) => new Date(ms).toISOString();
const PROBES = [
  { name: "aggregate-1h-24h", url: `/logs/aggregate?since=${encodeURIComponent(fmt(now0 - 86400000))}&until=${encodeURIComponent(fmt(now0))}&bucket=1h` },
  { name: "aggregate-1m-last5m", url: `/logs/aggregate?since=${encodeURIComponent(fmt(now0 - 300000))}&until=${encodeURIComponent(fmt(now0))}&bucket=1m` },
  { name: "simple-service", url: "/logs?service=checkout&limit=100" },
  { name: "attr", url: "/logs?attr.user_id=42&limit=100" },
];
const probeTimes = Object.fromEntries(PROBES.map((p) => [p.name, []]));
const probeTimeouts = Object.fromEntries(PROBES.map((p) => [p.name, 0]));

async function timeProbe(p) {
  const s = performance.now();
  try {
    const res = await fetch(`${BASE_URL}${p.url}`, { signal: AbortSignal.timeout(30000) });
    probeTimes[p.name].push(performance.now() - s);
    return res.status;
  } catch (err) {
    // A hung query is a timeout the grader would score; record it instead of
    // crashing the whole run so later phases still complete.
    probeTimeouts[p.name]++;
    probeTimes[p.name].push(30000);
    return 0;
  }
}

let lastRowCount = 0;
async function dbRows() {
  try {
    lastRowCount = (await pgPool.query("SELECT COUNT(*)::int AS n FROM logs")).rows[0].n;
  } catch { /* keep last value; a COUNT that is slow/starved under load must not crash the run */ }
  return lastRowCount;
}

function pct(arr, p) {
  if (arr.length === 0) return 0;
  const a = [...arr].sort((x, y) => x - y);
  const i = Math.min(a.length - 1, Math.max(0, Math.floor((p / 100) * a.length)));
  return a[i];
}

function report(label, times) {
  if (times.length === 0) { console.log(`${label}: (no samples)`); return; }
  console.log(`${label}: n=${times.length} p50=${pct(times, 50).toFixed(0)}ms p95=${pct(times, 95).toFixed(0)}ms p99=${pct(times, 99).toFixed(0)}ms max=${Math.max(...times).toFixed(0)}ms`);
}
async function runPhase(name, steps) {
  const totalSec = steps.reduce((s, [_, t]) => s + t, 0);
  console.log(`\n=== ${name}: ${steps.map(([r, t]) => `${r}/s x ${t}s`).join(" -> ")} ===`);
  const start = Date.now();
  const end = start + totalSec * 1000;
  let accepted = 0, errors = 0, rejected = 0, sentLogs = 0;
  let inflight = 0;
  const postTimes = [];
  const rows0 = await dbRows();
  let lastRows = rows0;

  const stepFor = (t) => {
    let el = 0;
    for (const [r, sec] of steps) {
      el += sec * 1000;
      if (t < el) return r;
    }
    return steps[steps.length - 1][0];
  };

  const fire = async () => {
    inflight++;
    try {
      const r = await postBatch();
      postTimes.push(r.elapsed);
      if (r.status === 200) { accepted += r.accepted; rejected += r.rejected; }
      else { errors++; }
    } catch { errors++; }
    finally { inflight--; }
  };

  const emitter = (async () => {
    let firedBatches = 0;
    let lastDbg = 0;
    while (Date.now() < end) {
      const t = Date.now() - start;
      const rate = stepFor(t);
      // Pace by absolute elapsed time (drift-proof), allowing up to MAX_INFLIGHT
      // concurrent POSTs so short latency jitters don't reduce throughput.
      const wantBatches = Math.floor((t / 1000) * rate / BATCH);
      while (firedBatches < wantBatches && inflight < MAX_INFLIGHT) {
        firedBatches++;
        sentLogs += BATCH;
        void fire();
      }
      if (Date.now() - lastDbg >= 10000) {
        lastDbg = Date.now();
        console.log(`  [emit ${((Date.now() - start) / 1000).toFixed(0)}s] rate=${rate} want=${wantBatches} inflight=${inflight} sent=${sentLogs} acc=${accepted}`);
      }
      await new Promise((r) => setTimeout(r, 5));
    }
  })();

  const tasks = [emitter];
  if (!(arg("--noprobes") === "1")) {
    const prober = (async () => {
      while (Date.now() < end) {
        for (const p of PROBES) await timeProbe(p);
        await new Promise((r) => setTimeout(r, QUERY_EVERY_MS));
      }
    })();
    tasks.push(prober);
    const rowLogger = (async () => {
      while (Date.now() < end) {
        await new Promise((r) => setTimeout(r, 10000));
        const rows = await dbRows();
        console.log(`  [${((Date.now() - start) / 1000).toFixed(0)}s] committed=+${rows - rows0} (accepted=+${accepted}) rate=${((rows - lastRows) / 10).toFixed(0)}/s`);
        lastRows = rows;
      }
    })();
    tasks.push(rowLogger);
  }

  await Promise.all(tasks);
  while (inflight > 0) await new Promise((r) => setTimeout(r, 10));
  const elapsed = (Date.now() - start) / 1000;

  const rows1 = await dbRows();
  const committed = rows1 - rows0;
  const gap = accepted - committed;
  console.log(`  Accepted: ${accepted} (${Math.round(accepted / elapsed)} logs/s), HTTP errors: ${errors}, rejected entries: ${rejected}`);
  console.log(`  Committed to DB: ${committed} (${Math.round(committed / elapsed)} logs/s), BUFFER GAP: ${gap} rows`);
  report(`  POST latency`, postTimes);
  for (const p of PROBES) {
    report(`  During-load ${p.name}`, probeTimes[p.name].splice(0));
    if (probeTimeouts[p.name] > 0) console.log(`    (${probeTimeouts[p.name]} timeouts)`);
    probeTimeouts[p.name] = 0;
  }

  // settle: wait for buffer to drain, measure catch-up
  const settleStart = Date.now();
  let last = await dbRows();
  while (true) {
    await new Promise((r) => setTimeout(r, 2000));
    const now = await dbRows();
    if (now === last || Date.now() - settleStart > 30000) break;
    last = now;
  }
  const settled = await dbRows();
  console.log(`  Settled: buffer fully drained in ${((Date.now() - settleStart) / 1000).toFixed(0)}s, final committed=+${settled - rows0} (${Math.round((settled - rows0) / elapsed)} logs/s)`);
  return { accepted, committed: settled - rows0, errors };
}

async function postLoadQueries() {
  console.log(`\n=== Post-load query latency (10x each) ===`);
  for (const p of PROBES) {
    const times = [];
    for (let i = 0; i < 10; i++) {
      const s = performance.now();
      const res = await fetch(`${BASE_URL}${p.url}`);
      times.push(performance.now() - s);
      if (res.status !== 200) console.log(`  ${p.name} -> ${res.status}`);
    }
    report(`  ${p.name}`, times);
  }
}

async function main() {
  for (const name of requested) {
    if (!PHASES[name]) { console.error(`Unknown phase ${name}`); process.exit(1); }
    await runPhase(name, PHASES[name]);
  }
  await postLoadQueries();
  await pgPool.end();
  console.log("\nDone.");
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
