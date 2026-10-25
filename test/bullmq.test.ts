import { DelayedError, UnrecoverableError } from "bullmq";
import { describe, expect, it, vi } from "vitest";
import { chain, registry } from "../examples/basic/pipeline";
import {
  createRuntime,
  MemoryStore,
  noopLogger,
  PermanentError,
  RetryableError,
} from "../src/index";
import {
  BullMQDispatcher,
  DEFAULT_JOB_OPTIONS,
  type DurastepJob,
  type JobLike,
  processJob,
  type QueueLike,
  startBullMQWorker,
} from "../src/queue/bullmq/index";
import type { RuntimeHandlers } from "../src/queue/types";
import { sleep } from "./helpers";

function fakeJob(data: DurastepJob, attemptsMade = 0, attempts = 3) {
  const moveToDelayed = vi.fn(async () => {});
  const job = {
    id: "j1",
    data,
    attemptsMade,
    opts: { attempts },
    moveToDelayed,
  } as unknown as JobLike;
  return { job, moveToDelayed };
}

function handlers(overrides: Partial<RuntimeHandlers> = {}): RuntimeHandlers {
  return {
    executeRun: vi.fn(async (runId: string) => ({ runId, outcome: "succeeded" as const })),
    advance: vi.fn(async (sessionId: string) => ({
      sessionId,
      status: "EXECUTING" as const,
      scheduled: [],
      completed: false,
    })),
    logger: noopLogger,
    ...overrides,
  };
}

describe("BullMQDispatcher", () => {
  it("does not connect on construction and enqueues both job types with the default options", async () => {
    const added: Array<[string, DurastepJob, unknown]> = [];
    const queue: QueueLike = {
      add: async (name, data, opts) => void added.push([name, data, opts]),
      close: async () => {},
    };
    const lazy = new BullMQDispatcher({ connection: { host: "127.0.0.1", port: 1 } });
    expect(lazy.jobOptions).toMatchObject({
      attempts: 3,
      backoff: { type: "exponential", delay: 2000 },
    });
    const d = new BullMQDispatcher({ queue });
    await d.enqueueRun("r1");
    await d.enqueueAdvance("s1");
    expect(added).toEqual([
      ["run", { type: "run", runId: "r1" }, DEFAULT_JOB_OPTIONS],
      ["advance", { type: "advance", sessionId: "s1" }, DEFAULT_JOB_OPTIONS],
    ]);
  });
});

describe("processJob", () => {
  it("executes runs with canDefer and maps attempts to isRetry / finalAttempt", async () => {
    const h = handlers();
    await processJob(h, fakeJob({ type: "run", runId: "r1" }, 0, 3).job);
    await processJob(h, fakeJob({ type: "run", runId: "r1" }, 2, 3).job);
    expect(h.executeRun).toHaveBeenNthCalledWith(1, "r1", {
      isRetry: false,
      canDefer: true,
      finalAttempt: false,
    });
    expect(h.executeRun).toHaveBeenNthCalledWith(2, "r1", {
      isRetry: true,
      canDefer: true,
      finalAttempt: true,
    });
  });

  it("moves a deferred run back to delayed and throws DelayedError", async () => {
    const h = handlers({
      executeRun: async (runId) => ({ runId, outcome: "deferred", deferred: 5000 }),
    });
    const { job, moveToDelayed } = fakeJob({ type: "run", runId: "r1" });
    const before = Date.now();
    await expect(processJob(h, job, "tok")).rejects.toBeInstanceOf(DelayedError);
    const [when, token] = moveToDelayed.mock.calls[0] as unknown as [number, string];
    expect(when).toBeGreaterThanOrEqual(before + 5000);
    expect(token).toBe("tok");
  });

  it("maps permanent errors to UnrecoverableError and rethrows retryable ones", async () => {
    const permanent = handlers({
      executeRun: async () => {
        throw new PermanentError("schema violation", "schema");
      },
    });
    await expect(
      processJob(permanent, fakeJob({ type: "run", runId: "r" }).job),
    ).rejects.toBeInstanceOf(UnrecoverableError);
    const retryable = new RetryableError("socket hang up");
    const flaky = handlers({
      executeRun: async () => {
        throw retryable;
      },
    });
    await expect(processJob(flaky, fakeJob({ type: "run", runId: "r" }).job)).rejects.toBe(
      retryable,
    );
  });

  it("retries an advance that found the session locked", async () => {
    const h = handlers({
      advance: async (sessionId) => ({
        sessionId,
        status: "EXECUTING",
        scheduled: [],
        completed: false,
        locked: true,
      }),
    });
    await expect(
      processJob(h, fakeJob({ type: "advance", sessionId: "s" }).job),
    ).rejects.toBeInstanceOf(RetryableError);
  });

  it("rejects unknown payloads as unrecoverable", async () => {
    const job = fakeJob({ type: "nope" } as unknown as DurastepJob).job;
    await expect(processJob(handlers(), job)).rejects.toBeInstanceOf(UnrecoverableError);
  });
});

const redisUrl = process.env.REDIS_URL;

describe.skipIf(!redisUrl)("BullMQ end to end (REDIS_URL)", () => {
  it("runs the example chain through a real queue and worker", async () => {
    const { default: IORedis } = await import("ioredis");
    const connection = new IORedis(redisUrl as string, { maxRetriesPerRequest: null });
    const queueName = `durastep-test-${Date.now()}`;
    const dispatcher = new BullMQDispatcher({
      connection,
      queueName,
      jobOptions: { backoff: { type: "fixed", delay: 50 } },
    });
    const runtime = createRuntime({ store: new MemoryStore(), dispatcher, registry });
    const handle = startBullMQWorker(runtime, { connection, queueName, recovery: false });
    try {
      const session = await runtime.startSession(chain, { tenantId: "t", inputs: { url: "u" } });
      let status = session.status;
      for (let i = 0; i < 100 && status !== "COMPLETED" && status !== "FAILED"; i++) {
        await sleep(100);
        status = (await runtime.getSession(session.id))?.status ?? status;
      }
      expect(status).toBe("COMPLETED");
      const runs = await runtime.getRuns(session.id);
      expect(runs.find((r) => r.stepId === "analyse-b")?.attempt).toBe(2);
    } finally {
      await handle.close();
      await dispatcher.close();
      await connection.quit();
    }
  }, 20_000);
});
