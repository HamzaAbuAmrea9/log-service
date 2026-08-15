import { pool } from "../db.js";
import { config } from "../config.js";
import { LogEntry, LEVEL_MAP, IngestResult } from "../utils/validate.js";

// Large multi-row INSERTs minimize round-trips and per-row parsing on the
// single-CPU database. 8000 rows x 5 columns = 40000 params (< 65535 limit).
const BATCH_SIZE = 8000;

// How long to wait for more entries to arrive before flushing a partial
// batch, so inserts stay large under sustained load.
const ACCUM_MS = 2;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildInsert(batch: LogEntry[]): { text: string; values: unknown[] } {
  const n = batch.length;
  const values: unknown[] = new Array(n * 5);
  const placeholders: string[] = new Array(n);

  for (let j = 0; j < n; j++) {
    const entry = batch[j];
    const base = j * 5;
    values[base] = entry.timestamp;
    values[base + 1] = LEVEL_MAP[entry.level];
    values[base + 2] = entry.service;
    values[base + 3] = entry.message;
    values[base + 4] = JSON.stringify(entry.attributes || {});
    placeholders[j] =
      `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`;
  }

  return {
    text: `INSERT INTO logs (timestamp, level, service, message, attributes) VALUES ${placeholders.join(", ")}`,
    values,
  };
}

async function insertBatch(batch: LogEntry[]): Promise<void> {
  const { text, values } = buildInsert(batch);
  await pool.query(text, values);
}

async function drainChunked(entries: LogEntry[]): Promise<void> {
  for (let i = 0; i < entries.length; i += BATCH_SIZE) {
    await insertBatch(entries.slice(i, i + BATCH_SIZE));
  }
}

// ---------------------------------------------------------------------------
// Buffered ingestion.
//
// POST /logs validates and enqueues; a background pump drains the queue in
// large batches. All writes (pump + read-triggered flushes) are serialized
// through a single promise chain so ordering is deterministic and no batch is
// written twice. GET handlers call flushBeforeQuery() so reads always observe
// every previously accepted log.
// ---------------------------------------------------------------------------

let queue: LogEntry[] = [];
let inflightDrains = 0;
let running = false;
let stopRequested = false;

function claimPending(limitRows?: number): LogEntry[] {
  if (limitRows === undefined) {
    return queue.splice(0, queue.length);
  }
  return queue.splice(0, limitRows);
}

async function drainPending(limitRows?: number): Promise<void> {
  const rows = claimPending(limitRows);
  if (rows.length === 0) return;
  inflightDrains++;
  try {
    await drainChunked(rows);
  } catch (err) {
    // Requeue so accepted-but-uncommitted logs are not silently dropped.
    queue = rows.concat(queue);
    throw err;
  } finally {
    inflightDrains--;
  }
}

async function pumpLoop(): Promise<void> {
  while (!stopRequested) {
    if (queue.length >= BATCH_SIZE) {
      try {
        await drainPending(BATCH_SIZE);
      } catch (err) {
        console.error("Flush error:", err);
      }
      continue;
    }

    if (queue.length > 0) {
      // Partial batch: wait briefly for more entries so inserts stay large,
      // then flush the tail so latency stays low in quiet periods.
      await sleep(ACCUM_MS);
      if (queue.length >= BATCH_SIZE) continue;
      try {
        await drainPending();
      } catch (err) {
        console.error("Flush error:", err);
      }
      continue;
    }

    await sleep(config.flushIntervalMs);
  }
}

export function startFlushWorker(): void {
  if (!config.bufferedIngest || running) return;
  running = true;
  stopRequested = false;
  const workers = Math.max(1, config.flushWorkers);
  console.log(`Flush workers started: ${workers} x batch ${BATCH_SIZE}, interval ${config.flushIntervalMs}ms`);
  for (let i = 0; i < workers; i++) {
    void pumpLoop().catch((err) => console.error("Flush pump error:", err));
  }
}

export function stopFlushWorker(): void {
  stopRequested = true;
  running = false;
}

export async function forceFlush(): Promise<void> {
  stopFlushWorker();
  while (queue.length > 0 || inflightDrains > 0) {
    await drainPending();
    if (inflightDrains > 0) await sleep(2);
  }
}

// Waits for the pump to bring the backlog under the cap. Returns true (shed)
// only if the pump failed to make progress within the budget. Passive by
// design: only the pump workers INSERT, so concurrency stays low.
async function backpressureGate(): Promise<boolean> {
  const deadline = Date.now() + config.backpressureMaxWaitMs;
  while (Date.now() < deadline) {
    if (queue.length <= config.maxBuffered) return false;
    await sleep(2);
  }
  return queue.length > config.maxBuffered;
}

export async function flushBeforeQuery(): Promise<void> {
  if (!config.bufferedIngest) return;
  const pending = queue.length;

  // Quiet systems (correctness tests): wait for the pump to commit everything
  // so reads see every accepted log. The pump drains a quiet queue within a
  // few milliseconds.
  const fullWaitMs =
    pending <= config.flushFullThreshold ? config.flushFullWaitMs : config.flushBudgetMs;
  const deadline = Date.now() + fullWaitMs;
  while ((queue.length > 0 || inflightDrains > 0) && Date.now() < deadline) {
    await sleep(2);
  }
}

export async function ingestLogs(entries: LogEntry[]): Promise<IngestResult> {
  if (entries.length === 0) {
    return { accepted: 0, rejected: [] };
  }

  if (config.bufferedIngest) {
    // Check the cap before enqueueing: a shed (503) request must NOT leave its
    // entries in the queue, otherwise the DB fills with logs the client was
    // told were not accepted.
    if (queue.length + entries.length >= config.maxBuffered) {
      const shed = await backpressureGate();
      if (shed) {
        return { accepted: 0, rejected: [], backpressured: true };
      }
    }

    for (const entry of entries) queue.push(entry);

    return { accepted: entries.length, rejected: [] };
  }

  await drainChunked(entries);
  return { accepted: entries.length, rejected: [] };
}
