import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { from as copyFrom } from "pg-copy-streams";
import { pool } from "../db.js";
import { config } from "../config.js";
import { LogEntry, LEVEL_MAP, IngestResult } from "../utils/validate.js";

const COPY_BATCH_SIZE = 1000;
const FLUSH_CONCURRENCY = 8;

// COPY text format: fields separated by tab, rows by newline.
// Backslash, tab, newline, and carriage return must be escaped.
function escapeCopyField(value: string): string {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
}

function toCopyRow(entry: LogEntry): string {
  return [
    entry.timestamp,
    String(LEVEL_MAP[entry.level]),
    entry.service,
    entry.message,
    JSON.stringify(entry.attributes || {}),
  ]
    .map(escapeCopyField)
    .join("\t") + "\n";
}

// COPY FROM STDIN is the fastest bulk write path in PostgreSQL (2-3x
// faster than multi-row INSERT at the same batch size).
async function copyBatch(batch: LogEntry[]): Promise<void> {
  const client = await pool.connect();
  try {
    const ingestStream = client.query(
      copyFrom("COPY logs (timestamp, level, service, message, attributes) FROM STDIN"),
    );
    await pipeline(Readable.from(batch.map(toCopyRow)), ingestStream);
  } finally {
    client.release();
  }
}

async function insertBatch(batch: LogEntry[]): Promise<void> {
  const values: unknown[][] = [];
  const placeholders: string[] = [];

  for (let j = 0; j < batch.length; j++) {
    const entry = batch[j];
    const levelNum = LEVEL_MAP[entry.level];

    values.push([
      entry.timestamp,
      levelNum,
      entry.service,
      entry.message,
      JSON.stringify(entry.attributes || {}),
    ]);

    const base = j * 5;
    placeholders.push(
      `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`,
    );
  }

  const query = `
    INSERT INTO logs (timestamp, level, service, message, attributes)
    VALUES ${placeholders.join(", ")}
  `;

  await pool.query(query, values.flat());
}

// Writes chunks in parallel (bounded concurrency) so a large request
// finishes in ~one COPY time instead of N sequential round-trips.
async function writeChunks(entries: LogEntry[]): Promise<void> {
  const chunks: LogEntry[][] = [];
  for (let i = 0; i < entries.length; i += COPY_BATCH_SIZE) {
    chunks.push(entries.slice(i, i + COPY_BATCH_SIZE));
  }

  let next = 0;
  const workers = Array.from({ length: Math.min(FLUSH_CONCURRENCY, chunks.length) }, async () => {
    while (next < chunks.length) {
      const chunk = chunks[next++];
      try {
        await copyBatch(chunk);
      } catch {
        // Fallback to multi-row INSERT if COPY is unavailable.
        await insertBatch(chunk);
      }
    }
  });

  await Promise.all(workers);
}

// ---------------------------------------------------------------------------
// Buffered ingestion. POST /logs validates and enqueues; a worker drains the
// queue every flushIntervalMs. GET handlers call flushBeforeQuery() so reads
// are always consistent with everything already accepted. forceFlush() is
// used on shutdown.
// ---------------------------------------------------------------------------

let queue: LogEntry[] = [];
let flushing: Promise<void> = Promise.resolve();
let interval: ReturnType<typeof setInterval> | null = null;

async function drain(entries: LogEntry[]): Promise<void> {
  try {
    await writeChunks(entries);
  } catch (err) {
    // Requeue so accepted-but-uncommitted logs are not silently dropped.
    queue.unshift(...entries);
    throw err;
  }
}

function flushAll(): Promise<void> {
  const run = flushing.then(async () => {
    const entries = queue.splice(0, queue.length);
    if (entries.length > 0) {
      await drain(entries);
    }
  });
  flushing = run;
  return run;
}

export function startFlushWorker(): void {
  if (!config.bufferedIngest || interval) return;
  console.log(`Flush worker started: every ${config.flushIntervalMs}ms`);
  interval = setInterval(() => {
    flushAll().catch((err) => console.error("Flush error:", err));
  }, config.flushIntervalMs);
}

export function stopFlushWorker(): void {
  if (interval) {
    clearInterval(interval);
    interval = null;
  }
}

export async function forceFlush(): Promise<void> {
  stopFlushWorker();
  await flushAll();
}

export async function flushBeforeQuery(): Promise<void> {
  if (!config.bufferedIngest) return;
  await flushAll();
}

export async function ingestLogs(entries: LogEntry[]): Promise<IngestResult> {
  if (entries.length === 0) {
    return { accepted: 0, rejected: [] };
  }

  if (config.bufferedIngest) {
    queue.push(...entries);

    // Bound memory + keep latency flat: if the queue is too deep, flush
    // synchronously so this request absorbs the cost.
    if (queue.length >= config.maxBuffered) {
      await flushAll();
    }

    return { accepted: entries.length, rejected: [] };
  }

  await writeChunks(entries);
  return { accepted: entries.length, rejected: [] };
}
