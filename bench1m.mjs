const BASE = "http://localhost:8080";
const TOTAL = 1000000;
const BATCH = 1000;
const CONCURRENCY = 10;

const SERVICES = ["checkout", "auth", "api", "payment", "inventory", "search", "notification", "web"];
const LEVELS = ["debug", "info", "warn", "error"];
const MESSAGES = ["request handled", "timeout exceeded", "cache miss", "db query slow", "user login", "payment declined", "retrying request", "queue full"];
const REGIONS = ["us-east", "eu-west", "ap-south", "us-west"];
let counter = 0;

function makeBatch() {
  const logs = [];
  const baseTs = Date.now() - 30 * 86400000;
  for (let i = 0; i < BATCH; i++) {
    const n = counter++;
    const ts = new Date(baseTs + Math.floor(Math.random() * 30 * 86400000)).toISOString();
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

async function postOne() {
  const res = await fetch(`${BASE}/logs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: makeBatch(),
  });
  const data = await res.json();
  return { status: res.status, accepted: data.accepted ?? 0 };
}

async function main() {
  const t0 = performance.now();
  let posted = 0;
  let errors = 0;
  let batches = 0;
  const numBatches = TOTAL / BATCH;

  while (posted < TOTAL) {
    const chunk = [];
    for (let i = 0; i < CONCURRENCY && posted < TOTAL; i++) {
      chunk.push(postOne());
      posted += BATCH;
      batches++;
    }
    const results = await Promise.all(chunk);
    for (const r of results) if (r.status !== 200 || r.accepted !== BATCH) { errors++; }
  }
  const t1 = performance.now();
  console.log(`Ingested ${posted} in ${((t1 - t0) / 1000).toFixed(1)}s = ${Math.round(posted / ((t1 - t0) / 1000))} logs/s, HTTP errors: ${errors}`);

  // Wait for full durability: poll the DB until all 1M rows are committed.
  const { pool } = await import("./dist/db.js");
  const tWait = performance.now();
  let committed = 0;
  while (performance.now() - tWait < 300000) {
    const r = await pool.query("SELECT count(*)::int AS c FROM logs");
    committed = r.rows[0].c;
    if (committed >= TOTAL) break;
    await new Promise((r2) => setTimeout(r2, 500));
  }
  console.log(`Committed: ${committed}/${TOTAL} (waited ${((performance.now() - tWait) / 1000).toFixed(1)}s after ingest)`);
  await pool.end();

  const QUERIES = [
    ["simple-service", "/logs?service=checkout&limit=100"],
    ["complex", "/logs?service=checkout&level=error&since=2026-08-01T00:00:00Z&until=2026-08-10T00:00:00Z&limit=100"],
    ["attr", "/logs?attr.user_id=42&limit=100"],
    ["message-q", "/logs?q=timeout&limit=100"],
    ["aggregate-1d", "/logs/aggregate?since=2026-08-01T00:00:00Z&until=2026-08-10T00:00:00Z&bucket=1d"],
    ["aggregate-1h-24h", "/logs/aggregate?since=2026-08-13T00:00:00Z&until=2026-08-14T00:00:00Z&bucket=1h"],
    ["aggregate-1m-last5m", "/logs/aggregate?since=2026-08-14T12:00:00Z&until=2026-08-14T12:05:00Z&bucket=1m"],
    ["aggregate-1h-group", "/logs/aggregate?since=2026-08-13T00:00:00Z&until=2026-08-14T00:00:00Z&bucket=1h&group_by=service"],
  ];

  for (const [name, url] of QUERIES) {
    const times = [];
    for (let i = 0; i < 10; i++) {
      const s = performance.now();
      const res = await fetch(`${BASE}${url}`);
      await res.json();
      times.push(performance.now() - s);
    }
    const sorted = [...times].sort((a, b) => a - b);
    const pct = (p) => sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
    console.log(`${name}: p50=${pct(50).toFixed(0)}ms p95=${pct(95).toFixed(0)}ms p99=${pct(99).toFixed(0)}ms max=${Math.max(...times).toFixed(0)}ms`);
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
