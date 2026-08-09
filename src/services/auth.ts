import { FastifyRequest, FastifyReply } from "fastify";
import { config } from "../config.js";

// In-memory API key store (seeded at startup)
const apiKeys = new Set<string>();

export function seedLoadgenKey(): void {
  if (config.authEnabled && config.loadgenApiKey) {
    apiKeys.add(config.loadgenApiKey);
    console.log(`Auth: seeded loadgen API key`);
  }
}

export async function authMiddleware(request: FastifyRequest, reply: FastifyReply): Promise<void> {
  // Auth is off by default — skip entirely
  if (!config.authEnabled) return;

  // Health endpoint is always unauthenticated
  if (request.url === "/health") return;

  const authHeader = request.headers.authorization;
  const apiKeyHeader = request.headers["x-api-key"];

  let token: string | null = null;

  // Primary: Authorization: Bearer <key>
  if (typeof authHeader === "string" && authHeader.startsWith("Bearer ")) {
    token = authHeader.slice(7);
  }

  // Secondary: X-API-Key: <key>
  if (!token && typeof apiKeyHeader === "string") {
    token = apiKeyHeader;
  }

  if (!token) {
    return reply.status(401).send({ error: "Missing or malformed credential" });
  }

  if (!apiKeys.has(token)) {
    return reply.status(401).send({ error: "Invalid API key" });
  }
}
