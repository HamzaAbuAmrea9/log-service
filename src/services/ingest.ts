import { pool } from "../db.js";
import { LogEntry, LEVEL_MAP, IngestResult } from "../utils/validate.js";

const BATCH_SIZE = 1000;

export async function ingestLogs(entries: LogEntry[]): Promise<IngestResult> {
  if (entries.length === 0) {
    return { accepted: 0, rejected: [] };
  }

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

  return { accepted: entries.length, rejected: [] };
}

export async function forceFlush(): Promise<void> {
  // Direct INSERT execution is already durable, so no-op is correct.
}
