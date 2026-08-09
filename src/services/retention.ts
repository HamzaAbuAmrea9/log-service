import { pool } from "../db.js";
import { config } from "../config.js";

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
        const result = await pool.query(
          `DELETE FROM logs WHERE id IN (
            SELECT id FROM logs
            WHERE timestamp < NOW() - INTERVAL '1 day' * $1
            LIMIT $2
          )`,
          [config.retentionDays, BATCH_SIZE]
        );
        deleted = result.rowCount ?? 0;
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
