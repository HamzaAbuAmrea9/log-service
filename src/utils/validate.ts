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

export function validateLogEntry(
  entry: unknown,
  index: number,
  now: number = Date.now(),
): ValidationError | null {
  if (typeof entry !== "object" || entry === null) {
    return { index, reason: "entry must be an object" };
  }

  const obj = entry as Record<string, unknown>;

  // timestamp
  if (typeof obj.timestamp !== "string") {
    return { index, reason: "timestamp is required and must be a string" };
  }
  const ts = Date.parse(obj.timestamp);
  if (Number.isNaN(ts)) {
    return { index, reason: "invalid timestamp format" };
  }
  if (ts > now + 5 * 60 * 1000) {
    return {
      index,
      reason: "timestamp must not be more than five minutes in the future",
    };
  }

  // level
  if (typeof obj.level !== "string" || !VALID_LEVELS.has(obj.level)) {
    return { index, reason: `invalid level: '${obj.level}'` };
  }

  // service
  if (typeof obj.service !== "string" || obj.service.trim().length === 0) {
    return {
      index,
      reason: "service is required and must be a non-empty string",
    };
  }

  // message
  if (typeof obj.message !== "string" || obj.message.trim().length === 0) {
    return {
      index,
      reason: "message is required and must be a non-empty string",
    };
  }

  // attributes (optional)
  if (obj.attributes !== undefined) {
    if (
      typeof obj.attributes !== "object" ||
      obj.attributes === null ||
      Array.isArray(obj.attributes)
    ) {
      return { index, reason: "attributes must be a flat object" };
    }
    const attrs = obj.attributes as Record<string, unknown>;
    for (const [key, val] of Object.entries(attrs)) {
      if (
        typeof val !== "string" &&
        typeof val !== "number" &&
        typeof val !== "boolean"
      ) {
        return {
          index,
          reason: `attribute '${key}' must be a string, number, or boolean`,
        };
      }
    }
  }

  return null;
}

export function validateBatch(logs: unknown[]): ValidationError[] {
  const errors: ValidationError[] = [];
  const now = Date.now();
  for (let i = 0; i < logs.length; i++) {
    const err = validateLogEntry(logs[i], i, now);
    if (err) errors.push(err);
  }
  return errors;
}

// Builds the two JSON containment objects used to match an attribute equality
// filter against a JSONB column. The first matches number/boolean stored
// values, the second matches string values, so `attr.key=value` behaves the
// same as the text-equality form while still using the GIN index.
export function jsonbEqualityCandidates(key: string, value: string): [string, string] {
  let typed: unknown = value;
  if (value === "true" || value === "false") {
    typed = value === "true";
  } else if (/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(value)) {
    typed = Number(value);
  }
  return [JSON.stringify({ [key]: typed }), JSON.stringify({ [key]: value })];
}
