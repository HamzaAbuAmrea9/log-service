export function encodeCursor(timestamp: string, id: number): string {
  return Buffer.from(JSON.stringify({ t: timestamp, i: id })).toString("base64url");
}

export function decodeCursor(cursor: string): { timestamp: string; id: number } {
  try {
    const decoded = JSON.parse(Buffer.from(cursor, "base64url").toString("utf-8"));
    if (typeof decoded.t !== "string" || typeof decoded.i !== "number") {
      throw new Error("Invalid cursor structure");
    }
    const ts = new Date(decoded.t);
    if (isNaN(ts.getTime())) {
      throw new Error("Invalid cursor timestamp");
    }
    return { timestamp: decoded.t, id: decoded.i };
  } catch {
    throw new Error("Invalid or malformed cursor");
  }
}
