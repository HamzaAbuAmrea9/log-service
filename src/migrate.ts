import { pool, runMigrations } from "./db.js";

async function main(): Promise<void> {
  console.log("Connecting to database...");
  await pool.query("SELECT 1");
  console.log("Running migrations...");
  await runMigrations();
  console.log("Done.");
  await pool.end();
}

main().catch((err) => {
  console.error("Migration failed:", err);
  process.exit(1);
});
