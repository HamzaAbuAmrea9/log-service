#!/usr/bin/env node

const BASE_URL = "http://localhost:8080";
const BATCH_SIZE = 1000;
const NUM_BATCHES = 50; // 50K logs total
const CONCURRENCY = 10;

function makeBatch(batchNum) {
  const logs = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
    const idx = batchNum * BATCH_SIZE + i;
    const ts = new Date(Date.now() - Math.random() * 30 * 24 * 60 * 60 * 1000).toISOString();
    const services = ["checkout", "auth", "api", "payment", "inventory"];
    const levels = ["debug", "info", "warn", "error"];
    const messages = ["request handled", "timeout exceeded", "cache miss", "db query slow", "user login"];
    logs.push({
      timestamp: ts,
      level: levels[idx % 4],
      service: services[idx % 5],
      message: messages[idx % 5],
      attributes: { user_id: String(idx), region: "us-east" },
    });
  }
  return JSON.stringify({ logs });
}

async function sendBatch(batchNum) {
  const body = makeBatch(batchNum);
  const start = performance.now();
  const res = await fetch(`${BASE_URL}/logs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });
  const elapsed = performance.now() - start;
  const data = await res.json();
  return { status: res.status, accepted: data.accepted, elapsed };
}

async function main() {
  console.log(`Load test: ${NUM_BATCHES} batches × ${BATCH_SIZE} = ${NUM_BATCHES * BATCH_SIZE} logs`);
  console.log(`Concurrency: ${CONCURRENCY}`);
  console.log("Warming up...");

  // Warm up
  await sendBatch(0);

  const start = performance.now();
  let totalAccepted = 0;
  let totalErrors = 0;

  // Process batches with concurrency limit
  for (let i = 0; i < NUM_BATCHES; i += CONCURRENCY) {
    const batchPromises = [];
    for (let j = 0; j < CONCURRENCY && i + j < NUM_BATCHES; j++) {
      batchPromises.push(sendBatch(i + j));
    }
    const results = await Promise.all(batchPromises);
    for (const r of results) {
      totalAccepted += r.accepted;
      if (r.status !== 200) totalErrors++;
    }
  }

  const totalElapsed = (performance.now() - start) / 1000;
  const logsPerSec = totalAccepted / totalElapsed;

  console.log("\n=== Results ===");
  console.log(`Total logs accepted: ${totalAccepted}`);
  console.log(`Errors: ${totalErrors}`);
  console.log(`Total time: ${totalElapsed.toFixed(2)}s`);
  console.log(`Ingestion rate: ${Math.round(logsPerSec)} logs/sec`);

  // Test query performance
  console.log("\n=== Query Performance ===");

  const queries = [
    { name: "Simple (service)", url: "/logs?service=checkout&limit=100" },
    { name: "Complex (service+level+time)", url: "/logs?service=checkout&level=error&since=2026-07-01T00:00:00Z&until=2026-08-01T00:00:00Z&limit=100" },
    { name: "Message search (q)", url: "/logs?q=timeout&limit=100" },
    { name: "Attribute filter", url: "/logs?attr.user_id=42&limit=100" },
    { name: "Aggregate 1m", url: "/logs/aggregate?since=2026-07-01T00:00:00Z&until=2026-08-01T00:00:00Z&bucket=1d" },
  ];

  for (const q of queries) {
    const times = [];
    for (let i = 0; i < 10; i++) {
      const s = performance.now();
      await fetch(`${BASE_URL}${q.url}`);
      times.push(performance.now() - s);
    }
    times.sort((a, b) => a - b);
    const p50 = times[Math.floor(times.length * 0.5)];
    const p95 = times[Math.floor(times.length * 0.95)];
    const p99 = times[Math.floor(times.length * 0.99)];
    console.log(`${q.name}: P50=${p50.toFixed(0)}ms P95=${p95.toFixed(0)}ms P99=${p99.toFixed(0)}ms`);
  }
}

main().catch(console.error);
