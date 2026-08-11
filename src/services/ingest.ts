import { pool } from "../db.js";
import { LogEntry, LEVEL_MAP, IngestResult } from "../utils/validate.js";

const BATCH_SIZE = 1000;

export async function ingestLogs(entries: LogEntry[]): Promise<IngestResult> {
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

// Force flush on shutdown (no-op now, kept for API compatibility)
export async function forceFlush(): Promise<void> {
  // No buffer to flush — all inserts are synchronous
}
