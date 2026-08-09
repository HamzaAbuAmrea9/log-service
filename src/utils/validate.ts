export const LEVEL_MAP: Record<string, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

export const LEVEL_NAMES: Record<number, string> = {
  0: "debug",
  1: "info",
  2: "warn",
  3: "error",
};

export interface LogEntry {
  timestamp: string;
  level: string;
  service: string;
  message: string;
  attributes?: Record<string, string | number | boolean>;
}

export interface StoredLog {
  id: number;
  timestamp: string;
  level: string;
  service: string;
  message: string;
  attributes: Record<string, string | number | boolean>;
}

export interface IngestResult {
  accepted: number;
  rejected: Array<{ index: number; reason: string }>;
}

export interface QueryResult {
  logs: StoredLog[];
  next_cursor: string | null;
}

export interface AggregateBucket {
  start: string;
  group: string | null;
  count: number;
}

export interface ValidationError {
  index: number;
  reason: string;
}

const VALID_LEVELS = new Set(["debug", "info", "warn", "error"]);

export function validateLogEntry(entry: unknown, index: number): ValidationError | null {
  if (typeof entry !== "object" || entry === null) {
    return { index, reason: "entry must be an object" };
  }

  const obj = entry as Record<string, unknown>;

  // timestamp
  if (typeof obj.timestamp !== "string") {
    return { index, reason: "timestamp is required and must be a string" };
  }
  const ts = new Date(obj.timestamp);
  if (isNaN(ts.getTime())) {
    return { index, reason: "invalid timestamp format" };
  }
  const fiveMinFromNow = Date.now() + 5 * 60 * 1000;
  if (ts.getTime() > fiveMinFromNow) {
    return { index, reason: "timestamp must not be more than five minutes in the future" };
  }

  // level
  if (typeof obj.level !== "string" || !VALID_LEVELS.has(obj.level)) {
    return { index, reason: `invalid level: '${obj.level}'` };
  }

  // service
  if (typeof obj.service !== "string" || obj.service.length === 0) {
    return { index, reason: "service is required and must be a non-empty string" };
  }

  // message
  if (typeof obj.message !== "string" || obj.message.length === 0) {
    return { index, reason: "message is required and must be a non-empty string" };
  }

  // attributes (optional)
  if (obj.attributes !== undefined) {
    if (typeof obj.attributes !== "object" || obj.attributes === null || Array.isArray(obj.attributes)) {
      return { index, reason: "attributes must be a flat object" };
    }
    const attrs = obj.attributes as Record<string, unknown>;
    for (const [key, val] of Object.entries(attrs)) {
      if (typeof val !== "string" && typeof val !== "number" && typeof val !== "boolean") {
        return { index, reason: `attribute '${key}' must be a string, number, or boolean` };
      }
    }
  }

  return null;
}

export function validateBatch(logs: unknown[]): ValidationError[] {
  const errors: ValidationError[] = [];
  for (let i = 0; i < logs.length; i++) {
    const err = validateLogEntry(logs[i], i);
    if (err) errors.push(err);
  }
  return errors;
}
