import { Pool } from "pg";
import { readFileSync, readdirSync } from "fs";
import { join } from "path";
import { config } from "./config.js";

export const pool = new Pool({
  connectionString: config.databaseUrl,
  max: config.poolSize,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

export async function runMigrations(): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Create migrations table if not exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS migrations (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    // Check which migrations are applied
    const { rows: applied } = await client.query("SELECT name FROM migrations ORDER BY id");
    const appliedSet = new Set(applied.map((r) => r.name));

    // Read all migration files and apply pending ones in order
    const migrationsDir = join(import.meta.dirname, "..", "migrations");
    const files = readdirSync(migrationsDir)
      .filter((f) => f.endsWith(".sql"))
      .sort();

    for (const file of files) {
      if (!appliedSet.has(file)) {
        const sql = readFileSync(join(migrationsDir, file), "utf-8");
        await client.query(sql);
        await client.query("INSERT INTO migrations (name) VALUES ($1)", [file]);
        console.log(`Applied migration: ${file}`);
      }
    }

    await client.query("COMMIT");
    console.log("Migrations complete");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
