import { describe, expect, it } from "vitest";
import {
  classifyError,
  DeferError,
  isDeferError,
  isPermanentError,
  isRetryableError,
  PermanentError,
  RetryableError,
  toTaxonomyError,
} from "../src/core/errors";

function withStatus(message: string, status: number): Error {
  return Object.assign(new Error(message), { status });
}

function withCode(message: string, code: string): Error {
  return Object.assign(new Error(message), { code });
}

describe("error classes", () => {
  it("carry their class through the brand guards", () => {
    expect(isRetryableError(new RetryableError("x"))).toBe(true);
    expect(isPermanentError(new PermanentError("x", "schema"))).toBe(true);
    expect(isDeferError(new DeferError(1000))).toBe(true);
    expect(isRetryableError(new Error("x"))).toBe(false);
    expect(isPermanentError(null)).toBe(false);
  });

  it("recognise errors from a duplicated copy of the module by brand", () => {
    const foreign = Object.assign(new Error("from another bundle"), { durastepKind: "permanent" });
    expect(isPermanentError(foreign)).toBe(true);
  });

  it("PermanentError defaults its reason to other and keeps the cause", () => {
    const cause = new Error("root");
    const err = new PermanentError("bad", undefined, { cause });
    expect(err.reason).toBe("other");
    expect(err.cause).toBe(cause);
    expect(err.retryable).toBe(false);
  });

  it("DeferError clamps negative and fractional delays", () => {
    expect(new DeferError(-5).delayMs).toBe(0);
    expect(new DeferError(1500.7).delayMs).toBe(1500);
  });
});

describe("classifyError defaults", () => {
  it.each([
    ["timeouts", new Error("Request timed out after 30s")],
    ["the executor deadline", new Error("deadline_exceeded: fetch exceeded 90000ms")],
    ["aborts by name", Object.assign(new Error("x"), { name: "AbortError" })],
    ["aborts by message", new Error("This operation was aborted")],
    ["ECONNRESET by code", withCode("read failed", "ECONNRESET")],
    ["ENOTFOUND by message", new Error("getaddrinfo ENOTFOUND api.example.com")],
    ["socket hang up", new Error("socket hang up")],
    ["pool timeouts", new Error("Connection terminated due to connection timeout")],
    ["HTTP 429 by status", withStatus("slow down", 429)],
    ["HTTP 503 by status", withStatus("unavailable", 503)],
    ["HTTP 502 by message", new Error("upstream returned HTTP_502")],
    ["unknown errors", new Error("something odd happened")],
    ["non-error values", "a string was thrown"],
  ])("treats %s as retryable", (_label, err) => {
    expect(classifyError(err)).toEqual({ retryable: true });
  });

  it.each([
    ["schema violations", new Error("Output schema violation at $.title"), "schema"],
    ["invalid JSON", new Error("Model returned invalid JSON"), "schema"],
    ["JSON.parse failures", new Error("Unexpected token } in JSON at position 4"), "schema"],
    ["quota exhaustion", new Error("Monthly quota exceeded for tenant"), "quota"],
    ["HTTP 401", withStatus("unauthorised", 401), "input"],
    ["HTTP 422", withStatus("unprocessable", 422), "input"],
  ])("treats %s as permanent", (_label, err, reason) => {
    expect(classifyError(err)).toEqual({ retryable: false, reason });
  });

  it("checks retryable patterns before permanent ones", () => {
    expect(classifyError(new Error("timeout while validating schema violation"))).toEqual({
      retryable: true,
    });
  });

  it("keeps the class of explicit durastep errors", () => {
    expect(classifyError(new PermanentError("timeout", "input"))).toEqual({
      retryable: false,
      reason: "input",
    });
    expect(classifyError(new RetryableError("invalid json"))).toEqual({ retryable: true });
  });
});

describe("classifyError custom matchers", () => {
  it("evaluates user matchers before the defaults", () => {
    const matchers = [{ match: /content filtered/, retryable: false, reason: "input" as const }];
    expect(classifyError(new Error("Content filtered by provider"), matchers)).toEqual({
      retryable: false,
      reason: "input",
    });
  });

  it("supports predicate matchers on the raw error", () => {
    const matchers = [{ match: (e: unknown) => e instanceof TypeError, retryable: false }];
    expect(classifyError(new TypeError("x is undefined"), matchers)).toEqual({
      retryable: false,
      reason: "other",
    });
  });
});

describe("toTaxonomyError", () => {
  it("wraps plain errors and preserves the cause", () => {
    const original = new Error("socket hang up");
    const wrapped = toTaxonomyError(original);
    expect(wrapped).toBeInstanceOf(RetryableError);
    expect(wrapped.cause).toBe(original);
  });

  it("returns explicit durastep errors unchanged", () => {
    const err = new PermanentError("nope", "schema");
    expect(toTaxonomyError(err)).toBe(err);
  });
});
