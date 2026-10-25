import { beforeEach, describe, expect, it } from "vitest";
import { computeIdempotencyKey } from "../src/core/idempotency";
import { noopLogger } from "../src/core/logger";
import { advance, RETRY_PENDING_PREFIX } from "../src/core/orchestrator";
import type { PlanStep } from "../src/core/types";
import { MemoryStore } from "../src/store/memory";
import type { Store } from "../src/store/types";
import { interleavingStore, planOf, RecordingDispatcher, settleStep } from "./helpers";

let store: MemoryStore;
let dispatcher: RecordingDispatcher;

const deps = (s: Store = store) => ({ store: s, dispatcher, logger: noopLogger });

async function start(plan: PlanStep[], inputs: Record<string, unknown> = {}) {
  return store.createSession({ tenantId: "t1", subjectId: "subj", inputs, plan });
}

async function stepStatuses(sessionId: string) {
  const s = await store.getSession(sessionId);
  return Object.fromEntries((s?.plan ?? []).map((p) => [p.stepId, p.status]));
}

beforeEach(() => {
  store = new MemoryStore();
  dispatcher = new RecordingDispatcher();
});

describe("advance: scheduling", () => {
  it("fans out every zero-dependency step in one call", async () => {
    const s = await start(
      planOf([{ id: "r1" }, { id: "r2" }, { id: "r3" }, { id: "d", deps: ["r1"] }]),
    );
    const res = await advance(deps(), s.id);
    expect(res.scheduled).toEqual(["r1", "r2", "r3"]);
    expect(res.status).toBe("EXECUTING");
    expect(dispatcher.runs).toHaveLength(3);
    expect(await stepStatuses(s.id)).toEqual({
      r1: "RUNNING",
      r2: "RUNNING",
      r3: "RUNNING",
      d: "PENDING",
    });
  });

  it("schedules a dependent step only after its upstream succeeded", async () => {
    const s = await start(planOf([{ id: "a" }, { id: "b", deps: ["a"] }]));
    await advance(deps(), s.id);
    expect((await advance(deps(), s.id)).scheduled).toEqual([]);
    await settleStep(store, s.id, "a", "SUCCEEDED");
    const res = await advance(deps(), s.id);
    expect(res.scheduled).toEqual(["b"]);
    expect(await stepStatuses(s.id)).toEqual({ a: "SUCCEEDED", b: "RUNNING" });
  });

  it("waits for every dependency of a fan-in step", async () => {
    const s = await start(planOf([{ id: "a" }, { id: "b" }, { id: "join", deps: ["a", "b"] }]));
    await advance(deps(), s.id);
    await settleStep(store, s.id, "a", "SUCCEEDED");
    expect((await advance(deps(), s.id)).scheduled).toEqual([]);
    await settleStep(store, s.id, "b", "SUCCEEDED");
    expect((await advance(deps(), s.id)).scheduled).toEqual(["join"]);
  });

  it("creates runs with merged inputs and the derived idempotency key", async () => {
    const plan = planOf([{ id: "a", worker: "fetch" }]);
    plan[0]!.inputs = { url: "https://example.com", mode: "step" };
    const s = await start(plan, { mode: "session", locale: "en" });
    await advance(deps(), s.id);
    const [run] = await store.listRuns({ sessionId: s.id });
    expect(run?.inputs).toEqual({ url: "https://example.com", mode: "step", locale: "en" });
    expect(run?.idempotencyKey).toBe(
      computeIdempotencyKey({
        tenantId: "t1",
        worker: "fetch",
        subjectId: "subj",
        inputs: { url: "https://example.com", mode: "step", locale: "en" },
        sessionId: s.id,
        stepId: "a",
      }),
    );
  });
});

describe("advance: failures", () => {
  it("marks a failed optional step SKIPPED and continues downstream", async () => {
    const s = await start(
      planOf([{ id: "a" }, { id: "opt", optional: true }, { id: "z", deps: ["a", "opt"] }]),
    );
    await advance(deps(), s.id);
    await settleStep(store, s.id, "a", "SUCCEEDED");
    await settleStep(store, s.id, "opt", "FAILED", "boom");
    const res = await advance(deps(), s.id);
    expect(res.scheduled).toEqual(["z"]);
    expect(await stepStatuses(s.id)).toEqual({ a: "SUCCEEDED", opt: "SKIPPED", z: "RUNNING" });
  });

  it("fails the session on a non-optional failure and schedules nothing further", async () => {
    const s = await start(
      planOf([{ id: "a" }, { id: "b", deps: ["a"] }, { id: "c", deps: ["b"] }]),
    );
    await advance(deps(), s.id);
    await settleStep(store, s.id, "a", "FAILED", "boom");
    const res = await advance(deps(), s.id);
    expect(res).toMatchObject({ status: "FAILED", completed: true, scheduled: [] });
    expect(await stepStatuses(s.id)).toEqual({ a: "FAILED", b: "SKIPPED", c: "SKIPPED" });
    expect(dispatcher.runs).toHaveLength(1);
  });

  it("waits for in-flight siblings before declaring the session FAILED", async () => {
    const s = await start(planOf([{ id: "a" }, { id: "b" }, { id: "c", deps: ["a", "b"] }]));
    await advance(deps(), s.id);
    await settleStep(store, s.id, "a", "FAILED", "boom");
    const mid = await advance(deps(), s.id);
    expect(mid).toMatchObject({ status: "EXECUTING", completed: false, scheduled: [] });
    await settleStep(store, s.id, "b", "SUCCEEDED");
    const end = await advance(deps(), s.id);
    expect(end).toMatchObject({ status: "FAILED", completed: true });
    expect(await stepStatuses(s.id)).toEqual({ a: "FAILED", b: "SUCCEEDED", c: "SKIPPED" });
  });

  it("treats a CANCELLED run as a failed step", async () => {
    const s = await start(planOf([{ id: "a" }]));
    await advance(deps(), s.id);
    await settleStep(store, s.id, "a", "CANCELLED");
    expect(await advance(deps(), s.id)).toMatchObject({ status: "FAILED", completed: true });
  });

  it("does not fail a step whose run is FAILED but waiting for a retry", async () => {
    const s = await start(planOf([{ id: "a" }, { id: "b" }]));
    await advance(deps(), s.id);
    await settleStep(store, s.id, "a", "FAILED", `${RETRY_PENDING_PREFIX}socket hang up`);
    await settleStep(store, s.id, "b", "SUCCEEDED");
    const res = await advance(deps(), s.id);
    expect(res).toMatchObject({ status: "EXECUTING", completed: false });
    expect(await stepStatuses(s.id)).toEqual({ a: "RUNNING", b: "SUCCEEDED" });
  });
});

describe("advance: completion", () => {
  it("marks the session COMPLETED when every step succeeded", async () => {
    const s = await start(planOf([{ id: "a" }, { id: "b", deps: ["a"] }]));
    await advance(deps(), s.id);
    await settleStep(store, s.id, "a", "SUCCEEDED");
    await advance(deps(), s.id);
    await settleStep(store, s.id, "b", "SUCCEEDED");
    expect(await advance(deps(), s.id)).toMatchObject({ status: "COMPLETED", completed: true });
    expect((await store.getSession(s.id))?.status).toBe("COMPLETED");
  });

  it("is a no-op on a terminal session", async () => {
    const s = await start(planOf([{ id: "a" }]));
    await store.updatePlan(s.id, s.plan, 0, "CANCELLED");
    expect(await advance(deps(), s.id)).toEqual({
      sessionId: s.id,
      status: "CANCELLED",
      scheduled: [],
      completed: true,
    });
    expect(dispatcher.runs).toHaveLength(0);
  });

  it("reports a missing session as FAILED", async () => {
    expect(await advance(deps(), "nope")).toMatchObject({ status: "FAILED", completed: true });
  });
});

describe("advance: concurrency", () => {
  it("never double-schedules when two advance() calls race (session lock)", async () => {
    const slow = interleavingStore(store);
    const s = await start(planOf([{ id: "a" }, { id: "b" }, { id: "c" }]));
    const [x, y] = await Promise.all([advance(deps(slow), s.id), advance(deps(slow), s.id)]);
    expect([x.locked, y.locked].filter(Boolean)).toHaveLength(1);
    expect([...x.scheduled, ...y.scheduled].sort()).toEqual(["a", "b", "c"]);
    expect(dispatcher.runs).toHaveLength(3);
    expect(new Set(dispatcher.runs).size).toBe(3);
    expect(await store.listRuns({ sessionId: s.id })).toHaveLength(3);
  });

  it("never double-schedules without the lock (idempotency key + version check)", async () => {
    const unlocked = interleavingStore(store, { lock: false });
    const s = await start(planOf([{ id: "a" }, { id: "b" }, { id: "c" }]));
    const results = await Promise.all([
      advance(deps(unlocked), s.id),
      advance(deps(unlocked), s.id),
      advance(deps(unlocked), s.id),
    ]);
    // Every caller read version 0; exactly one persisted, the rest conflicted.
    expect(results.filter((r) => r.conflict)).toHaveLength(2);
    expect(dispatcher.runs).toHaveLength(3);
    expect(new Set(dispatcher.runs).size).toBe(3);
    expect(await store.listRuns({ sessionId: s.id })).toHaveLength(3);
    // A re-advance after the race schedules nothing new.
    expect((await advance(deps(), s.id)).scheduled).toEqual([]);
    expect(dispatcher.runs).toHaveLength(3);
  });

  it("dispatches nothing on a version conflict and reuses the runs on the next call", async () => {
    let failNext = true;
    const flaky = new Proxy(store, {
      get(target, prop, receiver) {
        if (prop === "updatePlan") {
          return async (...args: Parameters<Store["updatePlan"]>) => {
            if (failNext) {
              failNext = false;
              return false;
            }
            return target.updatePlan(...args);
          };
        }
        const v = Reflect.get(target, prop, receiver);
        return typeof v === "function" ? v.bind(target) : v;
      },
    });
    const s = await start(planOf([{ id: "a" }, { id: "b" }]));
    const first = await advance(deps(flaky), s.id);
    expect(first).toMatchObject({ conflict: true, scheduled: [] });
    expect(dispatcher.runs).toHaveLength(0);
    const runsAfterConflict = (await store.listRuns({ sessionId: s.id })).map((r) => r.id).sort();
    expect(runsAfterConflict).toHaveLength(2);

    const second = await advance(deps(flaky), s.id);
    expect(second.scheduled).toEqual(["a", "b"]);
    expect([...dispatcher.runs].sort()).toEqual(runsAfterConflict);
  });

  it("resolves a step to an existing terminal run left by a crashed scheduler", async () => {
    const s = await start(planOf([{ id: "a" }, { id: "b", deps: ["a"] }]));
    // A previous advance created the run but died before persisting the plan,
    // and something else executed it.
    const key = computeIdempotencyKey({
      tenantId: "t1",
      worker: "w",
      subjectId: "subj",
      inputs: {},
      sessionId: s.id,
      stepId: "a",
    });
    const { run } = await store.createRunIfAbsent({
      sessionId: s.id,
      tenantId: "t1",
      subjectId: "subj",
      worker: "w",
      stepId: "a",
      inputs: {},
      idempotencyKey: key,
    });
    await store.updateRun(run.id, { status: "SUCCEEDED" });
    const res = await advance(deps(), s.id);
    expect(res.scheduled).toEqual(["a", "b"]);
    expect(dispatcher.runs).toHaveLength(1);
    expect(await stepStatuses(s.id)).toEqual({ a: "SUCCEEDED", b: "RUNNING" });
  });
});
