import { describe, expect, it } from "vitest";
import { deadlineFor, watchdogVerdict } from "../src/core/deadline";

const T0 = Date.parse("2026-10-04T10:00:00Z");
const at = (minutes: number) => new Date(T0 + minutes * 60_000);
const LONG = { estimatedDurationMs: 60_000, deadlineMs: 300_000 };
const SHORT = { estimatedDurationMs: 30_000 };

describe("deadlineFor", () => {
  it("defaults to three times a one-minute estimate, capped at 180 s", () => {
    expect(deadlineFor(undefined)).toBe(180_000);
    expect(deadlineFor({})).toBe(180_000);
  });

  it("uses three times the estimate when it is under the cap", () => {
    expect(deadlineFor({ estimatedDurationMs: 30_000 })).toBe(90_000);
  });

  it("caps long estimates at 180 s", () => {
    expect(deadlineFor({ estimatedDurationMs: 120_000 })).toBe(180_000);
  });

  it("prefers an explicit deadline over the estimate", () => {
    expect(deadlineFor({ estimatedDurationMs: 60_000, deadlineMs: 300_000 })).toBe(300_000);
  });
});

describe("watchdogVerdict", () => {
  it("applies the three-minute rule to a worker without its own deadline", () => {
    const run = { createdAt: at(0), startedAt: at(0) };
    expect(watchdogVerdict(run, SHORT, T0 + 2 * 60_000).expired).toBe(false);
    expect(watchdogVerdict(run, SHORT, T0 + 4 * 60_000)).toMatchObject({
      expired: true,
      limitMs: 180_000,
      externalWait: false,
    });
  });

  it("gives a worker with a 300 s deadline six minutes", () => {
    const run = { createdAt: at(0), startedAt: at(0) };
    expect(watchdogVerdict(run, LONG, T0 + 4 * 60_000).expired).toBe(false);
    expect(watchdogVerdict(run, LONG, T0 + 7 * 60_000)).toMatchObject({
      expired: true,
      limitMs: 360_000,
    });
  });

  it("counts from the last start, not from creation", () => {
    const run = { createdAt: at(0), startedAt: at(10) };
    expect(watchdogVerdict(run, SHORT, T0 + 11 * 60_000).expired).toBe(false);
  });

  it("does not kill a run that was deferred a moment ago", () => {
    const run = { createdAt: at(0), startedAt: null, deferredAt: at(19) };
    expect(watchdogVerdict(run, LONG, T0 + 20 * 60_000).expired).toBe(false);
  });

  it("still fails a run that was never picked up", () => {
    const run = { createdAt: at(0), startedAt: null };
    expect(watchdogVerdict(run, SHORT, T0 + 4 * 60_000).expired).toBe(true);
  });

  it("gives external-wait runs ten minutes from creation", () => {
    const run = { createdAt: at(0), startedAt: at(0), externalWait: true };
    expect(watchdogVerdict(run, SHORT, T0 + 9 * 60_000)).toMatchObject({
      expired: false,
      externalWait: true,
      limitMs: 600_000,
    });
    expect(watchdogVerdict(run, SHORT, T0 + 11 * 60_000).expired).toBe(true);
  });

  it("accepts ISO strings for the timestamps", () => {
    const run = { createdAt: at(0).toISOString(), startedAt: at(0).toISOString() };
    expect(watchdogVerdict(run, SHORT, T0 + 4 * 60_000).expired).toBe(true);
  });

  it("treats an unknown worker like one without a deadline", () => {
    const run = { createdAt: at(0) };
    expect(watchdogVerdict(run, undefined, T0 + 4 * 60_000)).toMatchObject({
      expired: true,
      limitMs: 180_000,
    });
  });
});
