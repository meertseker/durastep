import { describe, expect, it } from "vitest";
import { canonicalize, computeIdempotencyKey } from "../src/core/idempotency";

const base = {
  tenantId: "t1",
  worker: "fetch",
  subjectId: "s1",
  inputs: { url: "https://example.com", depth: 2 },
  sessionId: "sess1",
  stepId: "fetch",
};

describe("canonicalize", () => {
  it("sorts object keys recursively and keeps array order", () => {
    const out = canonicalize({ b: 1, a: { d: [3, 1, { z: 1, y: 2 }], c: null } });
    expect(JSON.stringify(out)).toBe('{"a":{"c":null,"d":[3,1,{"y":2,"z":1}]},"b":1}');
  });

  it("serialises dates through toJSON instead of collapsing them to {}", () => {
    const a = canonicalize({ at: new Date("2026-01-01T00:00:00Z") });
    const b = canonicalize({ at: new Date("2026-06-01T00:00:00Z") });
    expect(JSON.stringify(a)).not.toBe(JSON.stringify(b));
  });
});

describe("computeIdempotencyKey", () => {
  it("is a 32-character base64url string", () => {
    const key = computeIdempotencyKey(base);
    expect(key).toMatch(/^[A-Za-z0-9_-]{32}$/);
  });

  it("is stable across calls", () => {
    expect(computeIdempotencyKey(base)).toBe(computeIdempotencyKey({ ...base }));
  });

  it("ignores input key order, including nested objects", () => {
    const a = computeIdempotencyKey({ ...base, inputs: { x: { b: 1, a: 2 }, y: 1 } });
    const b = computeIdempotencyKey({ ...base, inputs: { y: 1, x: { a: 2, b: 1 } } });
    expect(a).toBe(b);
  });

  it("treats array order as meaningful", () => {
    const a = computeIdempotencyKey({ ...base, inputs: { ids: [1, 2] } });
    const b = computeIdempotencyKey({ ...base, inputs: { ids: [2, 1] } });
    expect(a).not.toBe(b);
  });

  it("separates sessions and steps that run the same work", () => {
    const k = computeIdempotencyKey(base);
    expect(computeIdempotencyKey({ ...base, sessionId: "sess2" })).not.toBe(k);
    expect(computeIdempotencyKey({ ...base, stepId: "fetch-2" })).not.toBe(k);
  });

  it("separates tenants, workers and subjects", () => {
    const k = computeIdempotencyKey(base);
    expect(computeIdempotencyKey({ ...base, tenantId: "t2" })).not.toBe(k);
    expect(computeIdempotencyKey({ ...base, worker: "analyse" })).not.toBe(k);
    expect(computeIdempotencyKey({ ...base, subjectId: null })).not.toBe(k);
  });

  it("treats missing and empty inputs the same", () => {
    const { inputs: _drop, ...rest } = base;
    expect(computeIdempotencyKey(rest)).toBe(computeIdempotencyKey({ ...rest, inputs: {} }));
  });
});
