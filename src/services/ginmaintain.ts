import { pool } from "../db.js";

// GIN pending-list maintenance. Migration 013 enables fastupdate on the
// attributes index so per-row writes are lock-free pending-list appends
// instead of metapage-lock serialization. A pending list that grows without
// bound makes the next search pay a full merge (measured 6ms -> 5.5s on a
// 1.8M-row load), so this worker drains it periodically to keep both writes
// cheap and reads fast. gin_clean_pending_list is incremental: it only merges
// entries present when it runs, so a 5s cadence bounds the list to ~5s of
// writes and each cleanup does a small, cheap merge.

const CLEAN_INTERVAL_MS = 5000;
const INDEX_NAME = "idx_logs_attributes_path";

let cleaning = false;

export function startGinMaintain(): void {
  console.log(
    `GIN maintenance started: draining ${INDEX_NAME} pending list every ${CLEAN_INTERVAL_MS}ms`
  );
  setInterval(async () => {
    if (cleaning) return;
    cleaning = true;
    try {
      await pool.query("SELECT gin_clean_pending_list($1)", [INDEX_NAME]);
    } catch (err) {
      console.error("GIN cleanup error:", err);
    } finally {
      cleaning = false;
    }
  }, CLEAN_INTERVAL_MS);
}
