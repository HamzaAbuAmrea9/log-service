import { Readable } from "stream";
import { pipeline } from "stream/promises";
import { from as copyFrom } from "pg-copy-streams";
import { pool } from "../db.js";
import { LogEntry, LEVEL_MAP, IngestResult } from "../utils/validate.js";

const BATCH_SIZE = 1000;

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

export async function ingestLogs(entries: LogEntry[]): Promise<IngestResult> {
  if (entries.length === 0) {
    return { accepted: 0, rejected: [] };
  }

  for (let i = 0; i < entries.length; i += BATCH_SIZE) {
    const batch = entries.slice(i, i + BATCH_SIZE);

    try {
      await copyBatch(batch);
    } catch {
      // Fallback to multi-row INSERT if COPY is unavailable
      // (e.g., restricted environments without the COPY protocol).
      await insertBatch(batch);
    }
  }

  return { accepted: entries.length, rejected: [] };
}

export async function forceFlush(): Promise<void> {
  // COPY and direct INSERT are already durable, so no-op is correct.
}
