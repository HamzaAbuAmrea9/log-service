#!/usr/bin/env node
// Local benchmark harness that approximates the grading load generator:
//   - Phase A: max-throughput ingestion (concurrency 20), measures sustained rate + errors
//   - Phase B: sustained ingestion at a target rate with concurrent query probes
//   - Phase C: post-load query + aggregation latency percentiles
// Run: node benchmark.js [--rows N] [--rate R] [--total T]

const BASE_URL = process.env.BASE_URL || "http://localhost:8080";
const args = process.argv.slice(2);
const arg = (name, dflt) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : dflt;
};
const TOTAL_LOGS = parseInt(arg("--total", "1000000"), 10);
const BATCH_SIZE = 1000;
const NUM_BATCHES = Math.ceil(TOTAL_LOGS / BATCH_SIZE);
const TARGET_RATE = parseInt(arg("--rate", "15000"), 10);
const PHASE_B_SECONDS = 30;

const SERVICES = ["checkout", "auth", "api", "payment", "inventory", "search", "notification", "web"];
const LEVELS = ["debug", "info", "warn", "error"];
const MESSAGES = ["request handled", "timeout exceeded", "cache miss", "db query slow", "user login", "payment declined", "retrying request", "queue full"];
const REGIONS = ["us-east", "eu-west", "ap-south", "us-west"];

let counter = 0;
function makeBatch() {
  const logs = [];
  const baseTs = Date.now() - (counter % 100) * 60000;
  for (let i = 0; i < BATCH_SIZE; i++) {
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
  const res = await fetch(`${BASE_URL}/logs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: makeBatch(),
  });
  const data = await res.json();
  return { status: res.status, accepted: data.accepted ?? 0, rejected: data.rejected?.length ?? 0 };
}

const now = Date.now();
const fmt = (ms) => new Date(ms).toISOString();
const QUERIES = [
  { name: "simple-service", url: "/logs?service=checkout&limit=100" },
  { name: "complex", url: `/logs?service=checkout&level=error&since=${encodeURIComponent(fmt(now - 9 * 86400000))}&until=${encodeURIComponent(fmt(now - 4 * 86400000))}&limit=100` },
  { name: "attr", url: "/logs?attr.user_id=42&limit=100" },
  { name: "message-q", url: "/logs?q=timeout&limit=100" },
  { name: "aggregate-1d", url: `/logs/aggregate?since=${encodeURIComponent(fmt(now - 9 * 86400000))}&until=${encodeURIComponent(fmt(now - 4 * 86400000))}&bucket=1d` },
  { name: "aggregate-1h-24h", url: `/logs/aggregate?since=${encodeURIComponent(fmt(now - 86400000))}&until=${encodeURIComponent(fmt(now))}&bucket=1h` },
  { name: "aggregate-1m-last5m", url: `/logs/aggregate?since=${encodeURIComponent(fmt(now - 300000))}&until=${encodeURIComponent(fmt(now))}&bucket=1m` },
  { name: "aggregate-1h-group", url: `/logs/aggregate?since=${encodeURIComponent(fmt(now - 86400000))}&until=${encodeURIComponent(fmt(now))}&bucket=1h&group_by=service` },
];

async function timeQuery(q) {
  const s = performance.now();
  const res = await fetch(`${BASE_URL}${q.url}`);
  const elapsed = performance.now() - s;
  return { status: res.status, elapsed };
}

function pct(arr, p) {
  const a = [...arr].sort((x, y) => x - y);
  const i = Math.min(a.length - 1, Math.max(0, Math.floor((p / 100) * a.length)));
  return a[i];
}

function report(label, times) {
  console.log(
    `${label}: n=${times.length} p50=${pct(times, 50).toFixed(1)}ms p95=${pct(times, 95).toFixed(1)}ms p99=${pct(times, 99).toFixed(1)}ms max=${Math.max(...times).toFixed(1)}ms`,
  );
}

async function phaseA() {
  console.log(`\n=== Phase A: Max throughput (${NUM_BATCHES} batches x ${BATCH_SIZE}) ===`);
  const start = Date.now();
  let accepted = 0, errors = 0, rejected = 0;
  const concurrency = 20;
  let next = 0;
  const workers = [];
  for (let w = 0; w < concurrency; w++) {
    workers.push((async () => {
      while (true) {
        const i = next++;
        if (i >= NUM_BATCHES) break;
        const r = await postBatch();
        if (r.status === 200) { accepted += r.accepted; rejected += r.rejected; }
        else errors++;
      }
    })());
  }
  await Promise.all(workers);
  const elapsed = (Date.now() - start) / 1000;
  console.log(`Accepted: ${accepted}, HTTP errors: ${errors}, rejected entries: ${rejected}`);
  console.log(`Elapsed: ${elapsed.toFixed(1)}s, Sustained rate: ${Math.round(accepted / elapsed)} logs/s`);
  return accepted;
}

async function phaseB() {
  console.log(`\n=== Phase B: Sustained ${TARGET_RATE} logs/s for ${PHASE_B_SECONDS}s with query probes ===`);
  const interval = (BATCH_SIZE / TARGET_RATE) * 1000; // ms between 1000-log batches
  const stop = { now: false };
  const queryTimes = Object.fromEntries(QUERIES.map((q) => [q.name, []]));
  const errors = { http: 0, rejected: 0, accepted: 0 };

  const loader = (async () => {
    const start = Date.now();
    while (!stop.now) {
      const t0 = performance.now();
      const r = await postBatch();
      if (r.status === 200) { errors.accepted += r.accepted; errors.rejected += r.rejected; }
      else errors.http++;
      const wait = interval - (performance.now() - t0);
      if (wait > 0) await new Promise((res) => setTimeout(res, wait));
      if (Date.now() - start > PHASE_B_SECONDS * 1000) break;
    }
  })();

  const prober = (async () => {
    const probeEnd = Date.now() + PHASE_B_SECONDS * 1000;
    while (Date.now() < probeEnd) {
      for (const q of QUERIES) {
        const r = await timeQuery(q);
        queryTimes[q.name].push(r.elapsed);
        if (r.status !== 200) console.log(`  probe ${q.name} returned ${r.status}`);
      }
    }
  })();

  await Promise.all([loader, prober]);
  stop.now = true;
  const elapsed = PHASE_B_SECONDS;
  console.log(`Accepted: ${errors.accepted}, HTTP errors: ${errors.http}, rejected entries: ${errors.rejected}`);
  console.log(`Achieved: ${Math.round(errors.accepted / elapsed)} logs/s`);
  for (const q of QUERIES) report(`  During-load ${q.name}`, queryTimes[q.name]);
}

async function phaseC() {
  console.log(`\n=== Phase C: Post-load query latency (10x each) ===`);
  for (const q of QUERIES) {
    const times = [];
    for (let i = 0; i < 10; i++) {
      const r = await timeQuery(q);
      times.push(r.elapsed);
      if (r.status !== 200) console.log(`  ${q.name} returned ${r.status}`);
    }
    report(`  ${q.name}`, times);
  }
}

async function verifyVisibility() {
  console.log(`\n=== Visibility check ===`);
  const probe = `?limit=1`;
  const res = await fetch(`${BASE_URL}/logs${probe}`);
  const data = await res.json();
  console.log(`GET /logs: status=${res.status}, returned=${data.logs?.length ?? 0} logs`);
}

async function main() {
  console.log(`Benchmark: ${TOTAL_LOGS} logs, batch ${BATCH_SIZE}, ${NUM_BATCHES} batches`);
  await phaseA();
  await phaseB();
  await phaseC();
  await verifyVisibility();
  console.log("\nDone.");
}

main().catch((e) => { console.error(e); process.exit(1); });
