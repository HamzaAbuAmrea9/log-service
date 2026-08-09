import { FastifyInstance } from "fastify";
import { pool } from "../db.js";

let healthy = false;

export function markHealthy(): void {
  healthy = true;
}

export async function healthRoutes(app: FastifyInstance): Promise<void> {
  app.get("/health", async (_request, reply) => {
    if (!healthy) {
      return reply.status(503).send({ status: "starting" });
    }

    try {
      await pool.query("SELECT 1");
      return reply.status(200).send({ status: "ok" });
    } catch {
      return reply.status(503).send({ status: "database unreachable" });
    }
  });
}
