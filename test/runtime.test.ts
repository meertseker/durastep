import { beforeEach, describe, expect, it } from "vitest";
import {
  ChainValidationError,
  createRegistry,
  createRuntime,
  DeferError,
  defineChain,
  defineWorker,
  InlineDispatcher,
  MemoryStore,
  PermanentError,
  RetryableError,
  type RunFinishedEvent,
} from "../src/index";
import type { Dispatcher, RuntimeHandlers } from "../src/queue/types";
import { sleep } from "./helpers";

const spans: Record<string, { start: number; end: number }> = {};
const timed = async <T>(id: string, ms: number, value: T): Promise<T> => {
  const start = Date.now();
  await sleep(ms);
  spans[id] = { start, end: Date.now() };
  return value;
};

let flakyCalls = 0;
let flakyAlwaysFails = false;
const fetchW = defineWorker({
  kind: "fetch",
  run: async (_ctx, input: { url: string }) => timed("fetch", 5, { body: `page ${input.url}` }),
});
const analyseW = defineWorker({
  kind: "analyse",
  run: async (ctx, input: { lens: string }) =>
    timed(`analyse-${input.lens}`, 30, { lens: input.lens, from: ctx.upstream.fetch }),
});
const flakyW = defineWorker({
  kind: "flaky",
  async run() {
    flakyCalls += 1;
    if (flakyCalls === 1 || flakyAlwaysFails) throw new RetryableError("HTTP 503 from upstream");
    return { ok: true };
  },
});
const failW = defineWorker({
  kind: "fail",
  async run() {
    throw new PermanentError("not available", "input");
  },
});
const joinW = defineWorker({
  kind: "join",
  run: async (ctx) => ({ keys: Object.keys(ctx.upstream).sort() }),
});
const deferW = defineWorker({
  kind: "defer",
  async run() {
    throw new DeferError(1000);
  },
});
const registry = createRegistry([fetchW, analyseW, flakyW, failW, joinW, deferW]);

let store: MemoryStore;
let events: RunFinishedEvent[];
const make = (dispatcher: Dispatcher = new InlineDispatcher()) =>
  createRuntime({
    store,
    dispatcher,
    registry,
    telemetry: { onRunFinished: (e) => void events.push(e) },
  });

beforeEach(() => {
  store = new MemoryStore();
  events = [];
  flakyCalls = 0;
  flakyAlwaysFails = false;
});

describe("runtime with InlineDispatcher (end to end)", () => {
  it("runs a fan-out / fan-in chain to COMPLETED, in parallel where it can", async () => {
    const chain = defineChain(registry, [
      { stepId: "fetch", worker: "fetch" },
      { stepId: "a", worker: "analyse", dependsOn: ["fetch"], inputs: { lens: "a" } },
      { stepId: "b", worker: "analyse", dependsOn: ["fetch"], inputs: { lens: "b" } },
      { stepId: "join", worker: "join", dependsOn: ["a", "b"] },
    ]);
    const runtime = make();
    const session = await runtime.startSession(chain, { tenantId: "t1", inputs: { url: "x" } });
    expect(session.status).toBe("COMPLETED");
    expect(session.plan.every((s) => s.status === "SUCCEEDED")).toBe(true);
    // a and b overlapped: each started before the other finished.
    expect(spans["analyse-a"]!.start).toBeLessThan(spans["analyse-b"]!.end);
    expect(spans["analyse-b"]!.start).toBeLessThan(spans["analyse-a"]!.end);
    const runs = await runtime.getRuns(session.id);
    expect(runs).toHaveLength(4);
    expect(runs.find((r) => r.stepId === "a")?.output).toEqual({
      lens: "a",
      from: { body: "page x" },
    });
    expect(runs.find((r) => r.stepId === "join")?.output).toEqual({ keys: ["a", "b"] });
  });

  it("skips a failed optional step and retries a flaky step to success", async () => {
    const chain = defineChain(registry, [
      { stepId: "fetch", worker: "fetch", inputs: { url: "x" } },
      { stepId: "enrich", worker: "fail", dependsOn: ["fetch"], optional: true },
      { stepId: "flaky", worker: "flaky", dependsOn: ["fetch"] },
      { stepId: "join", worker: "join", dependsOn: ["enrich", "flaky"] },
    ]);
    const session = await make().startSession(chain, { tenantId: "t1" });
    expect(session.status).toBe("COMPLETED");
    const status = Object.fromEntries(session.plan.map((p) => [p.stepId, p.status]));
    expect(status).toEqual({
      fetch: "SUCCEEDED",
      enrich: "SKIPPED",
      flaky: "SUCCEEDED",
      join: "SUCCEEDED",
    });
    const flaky = events.filter((e) => e.worker === "flaky");
    expect(flaky.map((e) => [e.attempt, e.status, e.willRetry])).toEqual([
      [1, "FAILED", true],
      [2, "SUCCEEDED", false],
    ]);
  });

  it("fails the session on a hard failure and never runs downstream steps", async () => {
    const chain = defineChain(registry, [
      { stepId: "boom", worker: "fail" },
      { stepId: "after", worker: "join", dependsOn: ["boom"] },
    ]);
    const runtime = make();
    const session = await runtime.startSession(chain, { tenantId: "t1" });
    expect(session.status).toBe("FAILED");
    expect(session.plan.map((p) => p.status)).toEqual(["FAILED", "SKIPPED"]);
    expect(await runtime.getRuns(session.id)).toHaveLength(1);
  });

  it("fails a step whose retries are exhausted", async () => {
    flakyAlwaysFails = true;
    const chain = defineChain(registry, [{ stepId: "flaky", worker: "flaky" }]);
    const session = await make(new InlineDispatcher({ attempts: 2 })).startSession(chain, {
      tenantId: "t1",
    });
    expect(session.status).toBe("FAILED");
    const [run] = await store.listRuns({ sessionId: session.id });
    expect(run).toMatchObject({ attempt: 2, error: "retries exhausted: HTTP 503 from upstream" });
  });

  it("fails a deferring run inline because nothing would re-deliver it", async () => {
    const chain = defineChain(registry, [{ stepId: "d", worker: "defer" }]);
    const session = await make().startSession(chain, { tenantId: "t1" });
    expect(session.status).toBe("FAILED");
  });

  it("validates the chain before creating a session", async () => {
    const runtime = make();
    await expect(
      runtime.startSession([{ stepId: "x", worker: "nope", dependsOn: [] }], { tenantId: "t1" }),
    ).rejects.toBeInstanceOf(ChainValidationError);
    expect(await store.listSessions({})).toEqual([]);
  });

  it("coalesces concurrent advance calls in one process without double-scheduling", async () => {
    const recorded: string[] = [];
    const runtime = make({
      enqueueRun: async (id) => void recorded.push(id),
      enqueueAdvance: async () => {},
    });
    const chain = defineChain(registry, [
      { stepId: "a", worker: "join" },
      { stepId: "b", worker: "join" },
    ]);
    const s = await store.createSession({
      tenantId: "t1",
      subjectId: null,
      inputs: {},
      plan: chain.map((c) => ({ ...c, dependsOn: [], status: "PENDING" as const })),
    });
    const results = await Promise.all(Array.from({ length: 5 }, () => runtime.advance(s.id)));
    expect(results.filter((r) => r.locked)).toHaveLength(4);
    expect(recorded).toHaveLength(2);
  });
});

describe("recovery", () => {
  /** Inline dispatcher that drops advance notifications while `drop` is true. */
  class LossyDispatcher implements Dispatcher {
    drop = true;
    private readonly inner = new InlineDispatcher();
    bind(h: RuntimeHandlers) {
      this.inner.bind(h);
    }
    enqueueRun(id: string) {
      return this.inner.enqueueRun(id);
    }
    async enqueueAdvance(id: string) {
      if (!this.drop) await this.inner.enqueueAdvance(id);
    }
  }

  const twoStep = () =>
    defineChain(registry, [
      { stepId: "a", worker: "join" },
      { stepId: "b", worker: "join", dependsOn: ["a"] },
    ]);

  it("re-advances a session whose advance notification was lost", async () => {
    const dispatcher = new LossyDispatcher();
    const runtime = make(dispatcher);
    const session = await runtime.startSession(twoStep(), { tenantId: "t1" });
    expect(session.status).toBe("EXECUTING");
    dispatcher.drop = false;
    const later = new Date(Date.now() + 5 * 60_000);
    const res = await runtime.recoverStuckSessions({ now: later });
    expect(res.sessionIds).toEqual([session.id]);
    expect((await runtime.getSession(session.id))?.status).toBe("COMPLETED");
  });

  it("leaves fresh sessions and sessions with runs in flight alone", async () => {
    const dispatcher = new LossyDispatcher();
    const runtime = make(dispatcher);
    const session = await runtime.startSession(twoStep(), { tenantId: "t1" });
    expect((await runtime.recoverStuckSessions({ olderThanMs: 60_000 })).recovered).toBe(0);
    const [run] = await runtime.getRuns(session.id);
    await store.updateRun(run!.id, { status: "RUNNING" });
    const later = new Date(Date.now() + 5 * 60_000);
    expect((await runtime.recoverStuckSessions({ now: later })).recovered).toBe(0);
  });

  it("expires a dead RUNNING run and fails its session", async () => {
    const recorded: string[] = [];
    const inline = new InlineDispatcher();
    const runtime = make({
      bind: (h) => inline.bind(h),
      enqueueRun: async (id) => void recorded.push(id),
      enqueueAdvance: (id) => inline.enqueueAdvance(id),
    });
    const session = await runtime.startSession(twoStep(), { tenantId: "t1" });
    await store.updateRun(recorded[0]!, { status: "RUNNING", startedAt: new Date() });
    expect((await runtime.expireStuckRuns()).expired).toBe(0);
    const res = await runtime.expireStuckRuns({ now: new Date(Date.now() + 4 * 60_000) });
    expect(res.runIds).toEqual([recorded[0]]);
    expect((await store.getRun(recorded[0]!))?.error).toMatch(/^watchdog: no progress/);
    expect((await runtime.getSession(session.id))?.status).toBe("FAILED");
  });
});

describe("crash recovery", () => {
  const twoStep = () =>
    defineChain(registry, [
      { stepId: "a", worker: "join" },
      { stepId: "b", worker: "join", dependsOn: ["a"] },
    ]);
  const later = (minutes: number) => new Date(Date.now() + minutes * 60_000);

  it("adopts the orphan run of an advance that died before persisting its plan", async () => {
    const runtime = make();
    const s = await store.createSession({
      tenantId: "t1",
      subjectId: null,
      inputs: {},
      plan: twoStep().map((c) => ({
        ...c,
        dependsOn: [...c.dependsOn],
        status: "PENDING" as const,
      })),
    });
    // Simulate the dead advance: the run row exists, the plan never recorded it.
    const dying = createRuntime({
      store: {
        ...store,
        getSession: store.getSession.bind(store),
        listRuns: store.listRuns.bind(store),
        createRunIfAbsent: store.createRunIfAbsent.bind(store),
        withSessionLock: store.withSessionLock.bind(store),
        updatePlan: async () => {
          throw new Error("process died");
        },
      } as unknown as MemoryStore,
      dispatcher: new InlineDispatcher(),
      registry,
    });
    await expect(dying.advance(s.id)).rejects.toThrow("process died");
    expect(await store.listRuns({ sessionId: s.id })).toHaveLength(1);

    const res = await runtime.recoverStuckSessions({ now: later(5) });
    expect(res.sessionIds).toEqual([s.id]);
    expect((await runtime.getSession(s.id))?.status).toBe("COMPLETED");
    expect(await store.listRuns({ sessionId: s.id })).toHaveLength(2);
  });

  it("re-dispatches a PENDING run whose enqueue was lost, then fails it after maxPendingMs", async () => {
    const inline = new InlineDispatcher();
    let dropRuns = true;
    const runtime = make({
      bind: (h) => inline.bind(h),
      enqueueRun: async (id) => {
        if (!dropRuns) await inline.enqueueRun(id);
      },
      enqueueAdvance: (id) => inline.enqueueAdvance(id),
    });
    const session = await runtime.startSession(twoStep(), { tenantId: "t1" });
    expect(session.status).toBe("EXECUTING");

    dropRuns = false;
    const res = await runtime.expireStuckRuns({ now: later(4) });
    expect(res).toMatchObject({ expired: 0 });
    expect(res.redispatched).toHaveLength(1);
    expect((await runtime.getSession(session.id))?.status).toBe("COMPLETED");

    // A run that stays undeliverable is eventually failed.
    dropRuns = true;
    const stuck = await runtime.startSession(twoStep(), { tenantId: "t2" });
    const giveUp = await runtime.expireStuckRuns({ now: later(45) });
    expect(giveUp.expired).toBe(1);
    expect((await runtime.getSession(stuck.id))?.status).toBe("FAILED");
  });
});
