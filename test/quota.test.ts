import { beforeEach, describe, expect, it } from "vitest";
import {
  allQuotas,
  costCeiling,
  counterKeys,
  monthBucket,
  monthlyCount,
  UNLIMITED,
} from "../src/core/quota";
import { MemoryStore } from "../src/store/memory";

const now = new Date("2026-10-15T12:00:00Z");
let store: MemoryStore;
const input = (worker = "report", tenantId = "t1") => ({ tenantId, worker, now, counters: store });

beforeEach(() => {
  store = new MemoryStore();
});

describe("counter keys", () => {
  it("bucket by UTC month", () => {
    expect(monthBucket(new Date("2026-01-31T23:59:59Z"))).toBe("2026-01");
    expect(counterKeys.runs("t1", "report", now)).toBe("durastep:runs:t1:report:2026-10");
    expect(counterKeys.costCents("t1", now)).toBe("durastep:cost_cents:t1:2026-10");
  });
});

describe("monthlyCount", () => {
  it("allows runs under the limit and reports what is left", async () => {
    await store.incrementCounter(counterKeys.runs("t1", "report", now), 3);
    expect(await monthlyCount({ report: 5 }).check(input())).toEqual({
      allowed: true,
      remaining: 2,
    });
  });

  it("blocks once the limit is reached", async () => {
    await store.incrementCounter(counterKeys.runs("t1", "report", now), 5);
    const decision = await monthlyCount({ report: 5 }).check(input());
    expect(decision.allowed).toBe(false);
    expect(decision.reason).toMatch(/quota exceeded: report ran 5 of 5/);
  });

  it("starts fresh in a new month", async () => {
    await store.incrementCounter(
      counterKeys.runs("t1", "report", new Date("2026-09-30T00:00:00Z")),
      99,
    );
    expect((await monthlyCount({ report: 5 }).check(input())).allowed).toBe(true);
  });

  it("treats UNLIMITED and unlisted kinds as uncapped by default", async () => {
    await store.incrementCounter(counterKeys.runs("t1", "fetch", now), 10_000);
    expect(await monthlyCount({ fetch: UNLIMITED }).check(input("fetch"))).toEqual({
      allowed: true,
    });
    expect(await monthlyCount({}).check(input("fetch"))).toEqual({ allowed: true });
    expect((await monthlyCount({}, { defaultLimit: 1 }).check(input("fetch"))).allowed).toBe(false);
  });

  it("supports per-tenant limits", async () => {
    const policy = monthlyCount((tenantId) =>
      tenantId === "pro" ? { report: 100 } : { report: 0 },
    );
    expect((await policy.check(input("report", "pro"))).allowed).toBe(true);
    expect((await policy.check(input("report", "free"))).allowed).toBe(false);
  });
});

describe("costCeiling", () => {
  it("blocks once monthly spend reaches the ceiling", async () => {
    const policy = costCeiling({ monthlyUsdCents: 500 });
    await store.incrementCounter(counterKeys.costCents("t1", now), 499);
    expect(await policy.check(input())).toEqual({ allowed: true, remaining: 1 });
    await store.incrementCounter(counterKeys.costCents("t1", now), 1);
    const decision = await policy.check(input());
    expect(decision).toMatchObject({ allowed: false, remaining: 0 });
    expect(decision.reason).toMatch(/cost ceiling reached/);
  });
});

describe("allQuotas", () => {
  it("returns the first denial and otherwise the tightest remaining", async () => {
    await store.incrementCounter(counterKeys.runs("t1", "report", now), 1);
    const ok = allQuotas(monthlyCount({ report: 10 }), costCeiling({ monthlyUsdCents: 3 }));
    expect(await ok.check(input())).toEqual({ allowed: true, remaining: 3 });
    const denied = allQuotas(monthlyCount({ report: 1 }), costCeiling({ monthlyUsdCents: 3 }));
    expect((await denied.check(input())).reason).toMatch(/quota exceeded/);
  });
});
