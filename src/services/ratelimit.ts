import { FastifyRequest, FastifyReply } from "fastify";
import { config } from "../config.js";

// Sliding window rate limiter (in-memory, per-process)
const windows = new Map<string, { count: number; resetAt: number }>();

const WINDOW_MS = 60_000; // 1 minute

export async function rateLimitMiddleware(request: FastifyRequest, reply: FastifyReply): Promise<void> {
  if (!config.rateLimitEnabled) return;

  // Health endpoint is always exempt
  if (request.url === "/health") return;

  const key = "global"; // Global rate limit (no per-tenant)
  const now = Date.now();
  const entry = windows.get(key);

  if (!entry || now > entry.resetAt) {
    windows.set(key, { count: 1, resetAt: now + WINDOW_MS });
    return;
  }

  entry.count++;

  if (entry.count > config.rateLimitMax) {
    const retryAfter = Math.ceil((entry.resetAt - now) / 1000);
    reply.header("Retry-After", retryAfter.toString());
    return reply.status(429).send({ error: "Rate limit exceeded" });
  }
}
