import { pool } from "../db.js";
import { config } from "../config.js";
import { buildRollupUpsert } from "./rollup.js";
import { recordMinuteCounts } from "./memrollup.js";

const BATCH_SIZE = 10000;
const CHECK_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

let running = false;

export function startRetentionWorker(): void {
  if (config.retentionDays <= 0) {
    console.log("Retention disabled (RETENTION_DAYS <= 0)");
    return;
  }

  console.log(`Retention worker started: deleting logs older than ${config.retentionDays} days`);

  setInterval(async () => {
    if (running) return;
    running = true;

    try {
      let totalDeleted = 0;
      let deleted = BATCH_SIZE;

      while (deleted === BATCH_SIZE) {
        deleted = await deleteExpiredBatch();
        totalDeleted += deleted;

        if (deleted > 0) {
          console.log(`Retention: deleted ${deleted} expired logs`);
        }
      }

      if (totalDeleted > 0) {
        console.log(`Retention cycle complete: ${totalDeleted} total logs deleted`);
      }
    } catch (err) {
      console.error("Retention worker error:", err);
    } finally {
      running = false;
    }
  }, CHECK_INTERVAL_MS);
}

// Deletes up to BATCH_SIZE expired rows and decrements the rollup in the same
// transaction, so the rollup never over-counts retained data. Rows are matched
// by ctid (stable for immutable log rows) so the delete does not depend on a
// primary-key index on `id`, which was dropped to cut per-row ingest cost.
async function deleteExpiredBatch(): Promise<number> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query(
      `WITH expired AS (
         SELECT ctid FROM logs
         WHERE timestamp < NOW() - INTERVAL '1 day' * $1
         LIMIT $2
       )
       DELETE FROM logs USING expired
       WHERE logs.ctid = expired.ctid
       RETURNING timestamp, service, level`,
      [config.retentionDays, BATCH_SIZE]
    );

    if (result.rows.length > 0) {
      const rows = result.rows.map((row) => ({
        ts: row.timestamp.getTime(),
        service: row.service,
        level: row.level,
      }));
      const rollup = buildRollupUpsert(rows, true);
      await client.query(rollup.text, rollup.values);
      recordMinuteCounts(rows, -1);
    }

    await client.query("COMMIT");
    return result.rows.length;
  } catch (err) {
    await client.query("ROLLBACK").catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}
