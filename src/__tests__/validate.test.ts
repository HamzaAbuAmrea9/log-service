import { describe, it, expect } from "vitest";
import {
  validateLogEntry,
  validateBatch,
  LEVEL_MAP,
  LEVEL_NAMES,
  jsonbEqualityCandidates,
} from "../utils/validate.js";
import { encodeCursor, decodeCursor } from "../utils/cursor.js";

describe("validateLogEntry", () => {
  const validEntry = {
    timestamp: "2026-07-20T14:32:01.123Z",
    level: "error",
    service: "checkout",
    message: "payment declined",
    attributes: { user_id: "42" },
  };

  it("accepts a valid entry", () => {
    expect(validateLogEntry(validEntry, 0)).toBeNull();
  });

  it("accepts entry without attributes", () => {
    const { attributes: _, ...noAttrs } = validEntry;
    expect(validateLogEntry(noAttrs, 0)).toBeNull();
  });

  it("rejects non-object", () => {
    expect(validateLogEntry(null, 0)).toEqual({
      index: 0,
      reason: "entry must be an object",
    });
    expect(validateLogEntry("string", 0)).toEqual({
      index: 0,
      reason: "entry must be an object",
    });
    expect(validateLogEntry(42, 0)).toEqual({
      index: 0,
      reason: "entry must be an object",
    });
  });

  it("rejects missing timestamp", () => {
    const { timestamp: _, ...noTs } = validEntry;
    expect(validateLogEntry(noTs, 0)).toEqual({
      index: 0,
      reason: "timestamp is required and must be a string",
    });
  });

  it("rejects invalid timestamp format", () => {
    expect(
      validateLogEntry({ ...validEntry, timestamp: "not-a-date" }, 0),
    ).toEqual({
      index: 0,
      reason: "invalid timestamp format",
    });
  });

  it("rejects timestamp more than 5 minutes in the future", () => {
    const future = new Date(Date.now() + 6 * 60 * 1000).toISOString();
    expect(validateLogEntry({ ...validEntry, timestamp: future }, 0)).toEqual({
      index: 0,
      reason: "timestamp must not be more than five minutes in the future",
    });
  });

  it("accepts timestamp within 5 minutes in the future", () => {
    const nearFuture = new Date(Date.now() + 4 * 60 * 1000).toISOString();
    expect(
      validateLogEntry({ ...validEntry, timestamp: nearFuture }, 0),
    ).toBeNull();
  });

  it("rejects invalid level", () => {
    expect(validateLogEntry({ ...validEntry, level: "critical" }, 0)).toEqual({
      index: 0,
      reason: "invalid level: 'critical'",
    });
    expect(validateLogEntry({ ...validEntry, level: "INFO" }, 0)).toEqual({
      index: 0,
      reason: "invalid level: 'INFO'",
    });
  });

  it("accepts all valid levels", () => {
    for (const level of ["debug", "info", "warn", "error"]) {
      expect(validateLogEntry({ ...validEntry, level }, 0)).toBeNull();
    }
  });

  it("rejects empty service", () => {
    expect(validateLogEntry({ ...validEntry, service: "" }, 0)).toEqual({
      index: 0,
      reason: "service is required and must be a non-empty string",
    });
  });

  it("rejects whitespace-only service", () => {
    expect(validateLogEntry({ ...validEntry, service: "   " }, 0)).toEqual({
      index: 0,
      reason: "service is required and must be a non-empty string",
    });
  });

  it("rejects non-string service", () => {
    expect(validateLogEntry({ ...validEntry, service: 123 }, 0)).toEqual({
      index: 0,
      reason: "service is required and must be a non-empty string",
    });
  });

  it("rejects empty message", () => {
    expect(validateLogEntry({ ...validEntry, message: "" }, 0)).toEqual({
      index: 0,
      reason: "message is required and must be a non-empty string",
    });
  });

  it("rejects whitespace-only message", () => {
    expect(validateLogEntry({ ...validEntry, message: "   " }, 0)).toEqual({
      index: 0,
      reason: "message is required and must be a non-empty string",
    });
  });

  it("rejects non-string message", () => {
    expect(validateLogEntry({ ...validEntry, message: 123 }, 0)).toEqual({
      index: 0,
      reason: "message is required and must be a non-empty string",
    });
  });

  it("rejects nested attributes", () => {
    expect(
      validateLogEntry(
        { ...validEntry, attributes: { nested: { deep: true } } },
        0,
      ),
    ).toEqual({
      index: 0,
      reason: "attribute 'nested' must be a string, number, or boolean",
    });
  });

  it("rejects array attributes", () => {
    expect(
      validateLogEntry({ ...validEntry, attributes: [1, 2, 3] }, 0),
    ).toEqual({
      index: 0,
      reason: "attributes must be a flat object",
    });
  });

  it("accepts boolean attribute values", () => {
    expect(
      validateLogEntry(
        { ...validEntry, attributes: { flag: true, count: 42, name: "test" } },
        0,
      ),
    ).toBeNull();
  });
});

describe("validateBatch", () => {
  it("returns no errors for all valid entries", () => {
    const logs = [
      {
        timestamp: "2026-07-20T14:00:00Z",
        level: "info",
        service: "a",
        message: "b",
      },
      {
        timestamp: "2026-07-20T14:00:01Z",
        level: "error",
        service: "c",
        message: "d",
      },
    ];
    expect(validateBatch(logs)).toEqual([]);
  });

  it("returns errors for invalid entries", () => {
    const logs = [
      {
        timestamp: "2026-07-20T14:00:00Z",
        level: "info",
        service: "a",
        message: "b",
      },
      { timestamp: "bad", level: "info", service: "c", message: "d" },
      {
        timestamp: "2026-07-20T14:00:02Z",
        level: "critical",
        service: "e",
        message: "f",
      },
    ];
    const errors = validateBatch(logs);
    expect(errors).toHaveLength(2);
    expect(errors[0].index).toBe(1);
    expect(errors[1].index).toBe(2);
  });
});

describe("jsonbEqualityCandidates", () => {
  it("matches stored string values", () => {
    const [num, str] = jsonbEqualityCandidates("user_id", "42");
    expect(JSON.parse(num)).toEqual({ user_id: 42 });
    expect(JSON.parse(str)).toEqual({ user_id: "42" });
  });

  it("matches stored numeric values", () => {
    const [num, str] = jsonbEqualityCandidates("retries", "3");
    expect(JSON.parse(num)).toEqual({ retries: 3 });
    expect(JSON.parse(str)).toEqual({ retries: "3" });
  });

  it("matches stored boolean values", () => {
    const [num, str] = jsonbEqualityCandidates("flag", "true");
    expect(JSON.parse(num)).toEqual({ flag: true });
    expect(JSON.parse(str)).toEqual({ flag: "true" });
  });

  it("handles plain string values", () => {
    const [num, str] = jsonbEqualityCandidates("region", "us-east");
    expect(JSON.parse(num)).toEqual({ region: "us-east" });
    expect(JSON.parse(str)).toEqual({ region: "us-east" });
  });

  it("escapes special characters in values", () => {
    const [, str] = jsonbEqualityCandidates("k", 'a"b\\c');
    expect(JSON.parse(str)).toEqual({ k: 'a"b\\c' });
  });
});

describe("LEVEL_MAP and LEVEL_NAMES", () => {
  it("maps levels correctly", () => {
    expect(LEVEL_MAP.debug).toBe(0);
    expect(LEVEL_MAP.info).toBe(1);
    expect(LEVEL_MAP.warn).toBe(2);
    expect(LEVEL_MAP.error).toBe(3);
  });

  it("maps names correctly", () => {
    expect(LEVEL_NAMES[0]).toBe("debug");
    expect(LEVEL_NAMES[1]).toBe("info");
    expect(LEVEL_NAMES[2]).toBe("warn");
    expect(LEVEL_NAMES[3]).toBe("error");
  });
});

describe("cursor", () => {
  it("round-trips timestamp and id", () => {
    const timestamp = "2026-07-20T14:32:01.123Z";
    const id = 42;
    const cursor = encodeCursor(timestamp, id);
    const decoded = decodeCursor(cursor);
    expect(decoded.timestamp).toBe(timestamp);
    expect(decoded.id).toBe(id);
  });

  it("rejects invalid cursor", () => {
    expect(() => decodeCursor("invalid")).toThrow(
      "Invalid or malformed cursor",
    );
  });

  it("rejects cursor with missing fields", () => {
    const bad = Buffer.from(
      JSON.stringify({ t: "2026-07-20T14:00:00Z" }),
    ).toString("base64url");
    expect(() => decodeCursor(bad)).toThrow("Invalid or malformed cursor");
  });

  it("rejects cursor with invalid timestamp", () => {
    const bad = Buffer.from(JSON.stringify({ t: "not-a-date", i: 1 })).toString(
      "base64url",
    );
    expect(() => decodeCursor(bad)).toThrow("Invalid or malformed cursor");
  });
});
