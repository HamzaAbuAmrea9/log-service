import { pool } from "../db.js";
import { LogEntry, LEVEL_MAP, IngestResult, ValidationError } from "../utils/validate.js";

const BATCH_SIZE = 1000;       // rows per INSERT
const FLUSH_INTERVAL_MS = 5;   // max wait before flush
const MAX_BUFFER_SIZE = 50000; // max buffered entries before backpressure

interface BufferedEntry {
  entry: LogEntry;
  resolve: () => void;
  reject: (err: Error) => void;
}

let buffer: BufferedEntry[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;
let flushing = false;

export function getBufferSize(): number {
  return buffer.length;
}

function scheduleFlush(): void {
  if (flushTimer) return;
  flushTimer = setTimeout(() => {
    flushTimer = null;
    flushBuffer().catch((err) => {
      console.error("Flush error:", err);
    });
  }, FLUSH_INTERVAL_MS);
}

export async function ingestLogs(entries: LogEntry[]): Promise<IngestResult> {
  // Direct fast path for small batches (<10 entries)
  if (entries.length < 10) {
    return directInsert(entries);
  }

  // Buffered path for high throughput
  const result: IngestResult = { accepted: entries.length, rejected: [] };

  // Apply backpressure if buffer is too large
  if (buffer.length >= MAX_BUFFER_SIZE) {
    // Flush synchronously before accepting more
    await flushBuffer();
  }

  // Add entries to buffer
  for (const entry of entries) {
    const promise = new Promise<void>((resolve, reject) => {
      buffer.push({ entry, resolve, reject });
    });
    // Don't await individual promises — they resolve on flush
  }

  scheduleFlush();
  return result;
}

async function flushBuffer(): Promise<void> {
  if (flushing || buffer.length === 0) return;
  flushing = true;

  const toFlush = buffer;
  buffer = [];

  try {
    // Process in sub-batches
    for (let i = 0; i < toFlush.length; i += BATCH_SIZE) {
      const batch = toFlush.slice(i, i + BATCH_SIZE);
      const values: unknown[][] = [];
      const placeholders: string[] = [];

      for (let j = 0; j < batch.length; j++) {
        const { entry } = batch[j];
        const levelNum = LEVEL_MAP[entry.level];

        values.push([
          entry.timestamp,
          levelNum,
          entry.service,
          entry.message,
          JSON.stringify(entry.attributes || {}),
        ]);

        const base = j * 5;
        placeholders.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
      }

      const query = `
        INSERT INTO logs (timestamp, level, service, message, attributes)
        VALUES ${placeholders.join(", ")}
      `;

      await pool.query(query, values.flat());

      // Resolve all promises in this batch
      for (const item of batch) {
        item.resolve();
      }
    }
  } catch (err) {
    // Reject all promises on error
    for (const item of toFlush) {
      item.reject(err instanceof Error ? err : new Error(String(err)));
    }
  } finally {
    flushing = false;

    // If more entries accumulated during flush, schedule another
    if (buffer.length > 0) {
      scheduleFlush();
    }
  }
}

// Direct insert for small batches (avoids buffer overhead)
async function directInsert(entries: LogEntry[]): Promise<IngestResult> {
  const result: IngestResult = { accepted: 0, rejected: [] };

  for (let i = 0; i < entries.length; i += BATCH_SIZE) {
    const batch = entries.slice(i, i + BATCH_SIZE);
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
      placeholders.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
    }

    if (values.length === 0) continue;

    const query = `
      INSERT INTO logs (timestamp, level, service, message, attributes)
      VALUES ${placeholders.join(", ")}
    `;

    await pool.query(query, values.flat());
    result.accepted += batch.length;
  }

  return result;
}

// Force flush on shutdown
export async function forceFlush(): Promise<void> {
  if (buffer.length > 0) {
    await flushBuffer();
  }
}
