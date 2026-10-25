import { beforeEach, describe, expect, it, vi } from "vitest";
import { DeferError, PermanentError, RetryableError } from "../src/core/errors";
import { type ExecutorDeps, executeRun } from "../src/core/executor";
import { noopLogger } from "../src/core/logger";
import { advance, RETRY_PENDING_PREFIX } from "../src/core/orchestrator";
import { counterKeys, monthlyCount } from "../src/core/quota";
import { createRegistry, defineWorker } from "../src/core/registry";
import type { RunFinishedEvent, WorkerContext, WorkerDefinition } from "../src/core/types";
import { MemoryStore } from "../src/store/memory";
import { planOf, RecordingDispatcher } from "./helpers";

type Impl = (ctx: WorkerContext, input: Record<string, unknown>) => Promise<unknown>;

let store: MemoryStore;
let dispatcher: RecordingDispatcher;
let events: RunFinishedEvent[];
let impl: ReturnType<typeof vi.fn<Impl>>;

function depsFor(
  worker: WorkerDefinition<"w", Record<string, unknown>, unknown>,
  extra: Partial<ExecutorDeps> = {},
): ExecutorDeps {
  return {
    store,
    dispatcher,
    registry: createRegistry([worker]),
    logger: noopLogger,
    telemetry: { onRunFinished: (e) => void events.push(e) },
    now: () => new Date(),
    ...extra,
  };
}

const makeWorker = (extra: Partial<WorkerDefinition<"w", Record<string, unknown>, unknown>> = {}) =>
  defineWorker<"w", Record<string, unknown>, unknown>({
    kind: "w",
    run: (ctx, input) => impl(ctx, input),
    ...extra,
  });

/** Creates a one-step session and schedules it; returns the run id. */
async function scheduledRun(): Promise<string> {
  const s = await store.createSession({
    tenantId: "t1",
    subjectId: null,
    inputs: { q: 1 },
    plan: planOf([{ id: "a" }]),
  });
  await advance({ store, dispatcher, logger: noopLogger }, s.id);
  const id = dispatcher.runs.at(-1);
  if (!id) throw new Error("not scheduled");
  dispatcher.runs.length = 0;
  return id;
}

beforeEach(() => {
  store = new MemoryStore();
  dispatcher = new RecordingDispatcher();
  events = [];
  impl = vi.fn<Impl>(async () => ({ ok: true }));
});

describe("executeRun: success", () => {
  it("persists output and cost, records usage, fires telemetry and advances", async () => {
    impl.mockImplementation(async (ctx) => {
      ctx.reportCost({ usdCents: 7, tokens: 1200 });
      return { answer: 42 };
    });
    const runId = await scheduledRun();
    const res = await executeRun(depsFor(makeWorker()), runId);
    expect(res).toEqual({ runId, outcome: "succeeded" });
    const run = await store.getRun(runId);
    expect(run).toMatchObject({
      status: "SUCCEEDED",
      output: { answer: 42 },
      costUsdCents: 7,
      costTokens: 1200,
      attempt: 1,
    });
    expect(await store.getCounter(counterKeys.runs("t1", "w", new Date()))).toBe(1);
    expect(await store.getCounter(counterKeys.costCents("t1", new Date()))).toBe(7);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      runId,
      worker: "w",
      status: "SUCCEEDED",
      attempt: 1,
      costUsdCents: 7,
      willRetry: false,
    });
    expect(dispatcher.advances).toEqual([run?.sessionId]);
  });

  it("hands the worker its context: signal, defer permission, counts and inputs", async () => {
    let seen: WorkerContext | undefined;
    let seenInput: unknown;
    impl.mockImplementation(async (ctx, input) => {
      seen = ctx;
      seenInput = input;
      return null;
    });
    const runId = await scheduledRun();
    await executeRun(depsFor(makeWorker()), runId, { canDefer: true });
    expect(seen?.signal).toBeInstanceOf(AbortSignal);
    expect(seen?.signal.aborted).toBe(false);
    expect(seen).toMatchObject({
      runId,
      tenantId: "t1",
      stepId: "a",
      attempt: 1,
      canDefer: true,
      deferCount: 0,
    });
    expect(seen?.queuedAt).toBeInstanceOf(Date);
    expect(seenInput).toEqual({ q: 1 });
  });

  it("gives a step the outputs of its upstream steps", async () => {
    const s = await store.createSession({
      tenantId: "t1",
      subjectId: null,
      inputs: {},
      plan: planOf([{ id: "a" }, { id: "opt", optional: true }, { id: "b", deps: ["a", "opt"] }]),
    });
    const orch = { store, dispatcher, logger: noopLogger };
    await advance(orch, s.id);
    const [aRun, optRun] = dispatcher.runs;
    await store.updateRun(aRun!, { status: "SUCCEEDED", output: { html: "<p>" } });
    await store.updateRun(optRun!, { status: "FAILED", error: "nope" });
    await advance(orch, s.id);
    let upstream: unknown;
    impl.mockImplementation(async (ctx) => {
      upstream = ctx.upstream;
      return null;
    });
    await executeRun(depsFor(makeWorker()), dispatcher.runs.at(-1)!);
    expect(upstream).toEqual({ a: { html: "<p>" }, opt: undefined });
  });
});

describe("executeRun: failures", () => {
  it("marks a retryable failure FAILED for observability and rethrows", async () => {
    impl.mockRejectedValue(new RetryableError("socket hang up"));
    const runId = await scheduledRun();
    await expect(executeRun(depsFor(makeWorker()), runId)).rejects.toBeInstanceOf(RetryableError);
    const run = await store.getRun(runId);
    expect(run?.status).toBe("FAILED");
    expect(run?.error).toBe(`${RETRY_PENDING_PREFIX}socket hang up`);
    expect(dispatcher.advances).toEqual([]);
    expect(events[0]).toMatchObject({ status: "FAILED", willRetry: true });
  });

  it("classifies plain errors and rethrows unknown ones as retryable with the cause", async () => {
    const original = new Error("weird upstream glitch");
    impl.mockRejectedValue(original);
    const runId = await scheduledRun();
    const err = await executeRun(depsFor(makeWorker()), runId).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RetryableError);
    expect((err as RetryableError).cause).toBe(original);
  });

  it("resets a FAILED run to RUNNING on retry and counts the attempt", async () => {
    impl.mockRejectedValueOnce(new RetryableError("flake")).mockResolvedValueOnce({ ok: 2 });
    const runId = await scheduledRun();
    const deps = depsFor(makeWorker());
    await expect(executeRun(deps, runId)).rejects.toThrow("flake");
    expect(await executeRun(deps, runId, { isRetry: true })).toMatchObject({
      outcome: "succeeded",
    });
    expect(await store.getRun(runId)).toMatchObject({
      status: "SUCCEEDED",
      attempt: 2,
      error: null,
    });
    expect(events.map((e) => e.attempt)).toEqual([1, 2]);
  });

  it("refuses to run a FAILED run that is not a retry", async () => {
    impl.mockRejectedValue(new PermanentError("bad", "schema"));
    const runId = await scheduledRun();
    await executeRun(depsFor(makeWorker()), runId);
    expect(await executeRun(depsFor(makeWorker()), runId)).toEqual({
      runId,
      outcome: "not_runnable",
      status: "FAILED",
    });
    expect(impl).toHaveBeenCalledTimes(1);
  });

  it("marks a permanent failure FAILED, advances and does not rethrow", async () => {
    impl.mockRejectedValue(new PermanentError("output schema violation", "schema"));
    const runId = await scheduledRun();
    const res = await executeRun(depsFor(makeWorker()), runId);
    expect(res).toEqual({ runId, outcome: "failed", error: "output schema violation" });
    expect((await store.getRun(runId))?.status).toBe("FAILED");
    expect(dispatcher.advances).toHaveLength(1);
  });

  it("treats a retryable failure on the final attempt as terminal", async () => {
    impl.mockRejectedValue(new RetryableError("still down"));
    const runId = await scheduledRun();
    const res = await executeRun(depsFor(makeWorker()), runId, { finalAttempt: true });
    expect(res).toMatchObject({ outcome: "failed", error: "retries exhausted: still down" });
    expect(dispatcher.advances).toHaveLength(1);
    expect(events[0]?.willRetry).toBe(false);
  });

  it("fails a run whose input does not parse, without retrying", async () => {
    const runId = await scheduledRun();
    const worker = makeWorker({
      parseInput: () => {
        throw new Error("url is required");
      },
    });
    const res = await executeRun(depsFor(worker), runId);
    expect(res).toMatchObject({ outcome: "failed", error: "invalid input: url is required" });
    expect(impl).not.toHaveBeenCalled();
  });

  it("fails a run whose worker is not registered", async () => {
    const runId = await scheduledRun();
    const other = defineWorker({ kind: "other", run: async () => null });
    const deps = { ...depsFor(makeWorker()), registry: createRegistry([other]) };
    expect(await executeRun(deps, runId)).toMatchObject({
      outcome: "failed",
      error: 'unknown worker "w"',
    });
  });

  it("reports a missing run", async () => {
    expect(await executeRun(depsFor(makeWorker()), "ghost")).toEqual({
      runId: "ghost",
      outcome: "missing",
    });
  });
});

describe("executeRun: re-delivery", () => {
  it("re-runs a retry-pending FAILED run that is re-delivered without isRetry", async () => {
    impl.mockRejectedValueOnce(new RetryableError("flake")).mockResolvedValueOnce({ ok: 1 });
    const runId = await scheduledRun();
    const deps = depsFor(makeWorker());
    await expect(executeRun(deps, runId)).rejects.toThrow("flake");
    // The process died after marking the run; the queue re-delivered the job
    // as if it were the same attempt.
    expect(await executeRun(deps, runId)).toMatchObject({ outcome: "succeeded" });
    expect((await store.getRun(runId))?.attempt).toBe(2);
  });

  it("re-runs a RUNNING run re-delivered after a stalled worker (at-least-once)", async () => {
    const runId = await scheduledRun();
    await store.updateRun(runId, { status: "RUNNING", attempt: 1, startedAt: new Date() });
    expect(await executeRun(depsFor(makeWorker()), runId)).toMatchObject({ outcome: "succeeded" });
    expect(impl).toHaveBeenCalledTimes(1);
    expect((await store.getRun(runId))?.attempt).toBe(2);
  });
});

describe("executeRun: deferral", () => {
  it("puts the run back to PENDING with deferCount + 1 and returns the delay", async () => {
    impl.mockRejectedValue(new DeferError(20_000));
    const runId = await scheduledRun();
    const res = await executeRun(depsFor(makeWorker()), runId, { canDefer: true });
    expect(res).toEqual({ runId, outcome: "deferred", deferred: 20_000 });
    const run = await store.getRun(runId);
    expect(run).toMatchObject({ status: "PENDING", deferCount: 1, attempt: 0, startedAt: null });
    expect(run?.deferredAt).toBeInstanceOf(Date);
    expect(dispatcher.advances).toEqual([]);
    expect(events).toEqual([]);
  });

  it("fails the run instead of stranding it when the dispatcher cannot defer", async () => {
    impl.mockRejectedValue(new DeferError(20_000));
    const runId = await scheduledRun();
    const res = await executeRun(depsFor(makeWorker()), runId);
    expect(res).toMatchObject({ outcome: "failed" });
    expect((await store.getRun(runId))?.status).toBe("FAILED");
    expect(dispatcher.advances).toHaveLength(1);
  });
});

describe("executeRun: deadline", () => {
  it("aborts the signal and fails with a RetryableError when the deadline fires", async () => {
    let aborted = false;
    impl.mockImplementation(
      (ctx) =>
        new Promise(() => {
          ctx.signal.addEventListener("abort", () => {
            aborted = true;
          });
        }),
    );
    const runId = await scheduledRun();
    const worker = makeWorker({ deadlineMs: 30 });
    await expect(executeRun(depsFor(worker), runId)).rejects.toBeInstanceOf(RetryableError);
    expect(aborted).toBe(true);
    const run = await store.getRun(runId);
    expect(run?.status).toBe("FAILED");
    expect(run?.error).toContain("deadline_exceeded: w exceeded 30ms");
  });
});

describe("executeRun: quota", () => {
  it("fails with PermanentError(quota) before running the worker", async () => {
    await store.incrementCounter(counterKeys.runs("t1", "w", new Date()), 3);
    const runId = await scheduledRun();
    const res = await executeRun(depsFor(makeWorker(), { quota: monthlyCount({ w: 3 }) }), runId);
    expect(res).toMatchObject({ outcome: "failed" });
    expect((res as { error: string }).error).toMatch(/quota exceeded/);
    expect(impl).not.toHaveBeenCalled();
  });
});

describe("executeRun: best-effort after success", () => {
  it("keeps the run SUCCEEDED when the advance notification fails", async () => {
    const runId = await scheduledRun();
    const errors: string[] = [];
    const deps = depsFor(makeWorker(), {
      dispatcher: {
        enqueueRun: async () => {},
        enqueueAdvance: async () => {
          throw new Error("redis down");
        },
      },
      logger: { ...noopLogger, error: (m) => void errors.push(m) },
    });
    expect(await executeRun(deps, runId)).toMatchObject({ outcome: "succeeded" });
    expect((await store.getRun(runId))?.status).toBe("SUCCEEDED");
    expect(errors).toContain("durastep.execute.notify_failed");
  });

  it("keeps the run SUCCEEDED when the telemetry hook throws", async () => {
    const runId = await scheduledRun();
    const deps = depsFor(makeWorker(), {
      telemetry: {
        onRunFinished: () => {
          throw new Error("metrics backend down");
        },
      },
    });
    expect(await executeRun(deps, runId)).toMatchObject({ outcome: "succeeded" });
    expect((await store.getRun(runId))?.status).toBe("SUCCEEDED");
    expect(dispatcher.advances).toHaveLength(1);
  });
});
