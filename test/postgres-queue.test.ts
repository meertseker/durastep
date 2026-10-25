import { randomUUID } from "node:crypto";
import pg from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { chain, registry } from "../examples/basic/pipeline";
import {
  createRuntime,
  type ExecuteOptions,
  type ExecuteResult,
  noopLogger,
  PermanentError,
  RetryableError,
  type RunFinishedEvent,
} from "../src/index";
import type { RuntimeHandlers } from "../src/queue/types";
import {
  applySchema,
  backoffDelay,
  type PgQueryable,
  PostgresDispatcher,
  PostgresStore,
  type PostgresWorkerHandle,
  type PostgresWorkerOptions,
  planCompletion,
  processClaimedJob,
  startPostgresWorker,
} from "../src/store/postgres/index";
import { sleep } from "./helpers";

const url = process.env.DATABASE_URL;

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

function fakeRuntime(h: RuntimeHandlers): PostgresWorkerOptions["runtime"] {
  return {
    handlers: h,
    logger: noopLogger,
    recoverStuckSessions: async () => ({ recovered: 0, sessionIds: [] }),
    expireStuckRuns: async () => ({ expired: 0, runIds: [], redispatched: [] }),
  };
}

describe("Postgres queue without a database", () => {
  it("computes fixed and capped exponential backoff", () => {
    expect(backoffDelay({ type: "fixed", delayMs: 500 }, 1)).toBe(500);
    expect(backoffDelay({ type: "fixed", delayMs: 500 }, 9)).toBe(500);
    const exp = { type: "exponential" as const, delayMs: 2_000 };
    expect([1, 2, 3, 4].map((n) => backoffDelay(exp, n))).toEqual([2_000, 4_000, 8_000, 16_000]);
    expect(backoffDelay(exp, 100)).toBe(5 * 60_000);
    expect(backoffDelay({ ...exp, maxMs: 5_000 }, 3)).toBe(5_000);
  });

  it("maps outcomes onto row updates", () => {
    const job = { attempts: 1, maxAttempts: 3, backoff: { type: "fixed" as const, delayMs: 100 } };
    expect(planCompletion(job, { type: "done" })).toEqual({ action: "delete" });
    expect(planCompletion(job, { type: "deferred", delayMs: 700 })).toEqual({
      action: "requeue",
      delayMs: 700,
      refundAttempt: true,
      error: null,
    });
    expect(
      planCompletion(job, { type: "failed", error: new Error("503"), retryable: true }),
    ).toEqual({ action: "requeue", delayMs: 100, refundAttempt: false, error: "503" });
    expect(
      planCompletion({ ...job, attempts: 3 }, { type: "failed", error: "boom", retryable: true }),
    ).toEqual({ action: "dead", error: "attempts exhausted (3): boom" });
    expect(planCompletion(job, { type: "failed", error: "bad", retryable: false })).toEqual({
      action: "dead",
      error: "permanent: bad",
    });
  });

  it("passes isRetry / finalAttempt from the claimed attempt, like the BullMQ adapter", async () => {
    const h = handlers();
    await processClaimedJob(h, { kind: "run", ref: "r1", attempts: 1, maxAttempts: 3 });
    await processClaimedJob(h, { kind: "run", ref: "r1", attempts: 3, maxAttempts: 3 });
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

  it("classifies thrown errors, deferrals and locked advances", async () => {
    const job = { kind: "run" as const, ref: "r", attempts: 1, maxAttempts: 3 };
    const deferred = handlers({
      executeRun: async (runId) => ({ runId, outcome: "deferred", deferred: 250 }),
    });
    expect(await processClaimedJob(deferred, job)).toEqual({ type: "deferred", delayMs: 250 });
    const permanent = handlers({
      executeRun: async () => {
        throw new PermanentError("schema violation", "schema");
      },
    });
    expect(await processClaimedJob(permanent, job)).toMatchObject({
      type: "failed",
      retryable: false,
    });
    const flaky = handlers({
      executeRun: async () => {
        throw new RetryableError("socket hang up");
      },
    });
    expect(await processClaimedJob(flaky, job)).toMatchObject({ type: "failed", retryable: true });
    const locked = handlers({
      advance: async (sessionId) => ({
        sessionId,
        status: "EXECUTING",
        scheduled: [],
        completed: false,
        locked: true,
      }),
    });
    expect(
      await processClaimedJob(locked, { kind: "advance", ref: "s", attempts: 1, maxAttempts: 3 }),
    ).toMatchObject({ type: "failed", retryable: true });
  });

  it("inserts with coalescing and notifies the queue's channel", async () => {
    const calls: Array<{ text: string; values: unknown[] | undefined }> = [];
    const fake: PgQueryable = {
      async query(text, values) {
        calls.push({ text, values });
        return { rows: [], rowCount: 0 };
      },
    };
    const d = new PostgresDispatcher({ pool: fake, queue: "q1", maxAttempts: 5 });
    await d.enqueueRun("r1");
    await d.withClient(fake).enqueueAdvance("s1");
    expect(calls[0]?.text).toMatch(/ON CONFLICT \(queue, kind, ref\) WHERE status = 'queued'/);
    expect(calls[0]?.text).toMatch(/pg_notify\('durastep_jobs', \$1\)/);
    expect(calls[0]?.values).toEqual(["q1", "run", "r1", 5, "exponential", 2000, null]);
    expect(calls[1]?.values?.slice(0, 4)).toEqual(["q1", "advance", "s1", 5]);
    expect(() => new PostgresDispatcher({} as never)).toThrow(TypeError);
  });
});

describe.skipIf(!url)("Postgres queue (DATABASE_URL)", () => {
  let pool: pg.Pool;
  const workers: PostgresWorkerHandle[] = [];

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: url, max: 30 });
    await applySchema(pool);
  });

  afterEach(async () => {
    await Promise.all(workers.splice(0).map((w) => w.close()));
  });

  afterAll(async () => {
    await pool?.end();
  });

  const newQueue = () => `test-${randomUUID()}`;

  function worker(
    queue: string,
    h: RuntimeHandlers,
    options: Partial<PostgresWorkerOptions> = {},
  ): PostgresWorkerHandle {
    const w = startPostgresWorker({
      pool,
      runtime: fakeRuntime(h),
      queue,
      pollIntervalMs: 50,
      recovery: false,
      ...options,
    });
    workers.push(w);
    return w;
  }

  async function jobs(queue: string) {
    const { rows } = await pool.query(
      "SELECT kind, ref, status, attempts, last_error, locked_by, run_at FROM durastep_jobs WHERE queue = $1 ORDER BY id",
      [queue],
    );
    return rows as Array<{
      kind: string;
      ref: string;
      status: string;
      attempts: number;
      last_error: string | null;
      locked_by: string | null;
      run_at: Date;
    }>;
  }

  async function waitFor(check: () => Promise<boolean> | boolean, timeoutMs = 5_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (await check()) return;
      await sleep(20);
    }
    throw new Error("waitFor: timed out");
  }

  it("executes an enqueued run, woken by NOTIFY rather than polling", async () => {
    const queue = newQueue();
    const h = handlers();
    // A poll interval far longer than the wait proves LISTEN/NOTIFY woke it.
    worker(queue, h, { pollIntervalMs: 60_000 });
    await sleep(200); // let the listener connect
    const started = Date.now();
    await new PostgresDispatcher({ pool, queue }).enqueueRun("r1");
    await waitFor(() => vi.mocked(h.executeRun).mock.calls.length === 1, 3_000);
    expect(Date.now() - started).toBeLessThan(2_000);
    await waitFor(async () => (await jobs(queue)).length === 0);
  });

  it("works by polling alone with listen: false", async () => {
    const queue = newQueue();
    const h = handlers();
    worker(queue, h, { listen: false, pollIntervalMs: 50 });
    await new PostgresDispatcher({ pool, queue }).enqueueAdvance("s1");
    await waitFor(() => vi.mocked(h.advance).mock.calls.length === 1);
    expect(h.advance).toHaveBeenCalledWith("s1");
    await waitFor(async () => (await jobs(queue)).length === 0);
  });

  it("retries a retryable failure with backoff, then succeeds", async () => {
    const queue = newQueue();
    const seen: Array<{ at: number; opts: ExecuteOptions | undefined }> = [];
    const h = handlers({
      executeRun: async (runId, opts): Promise<ExecuteResult> => {
        seen.push({ at: Date.now(), opts });
        if (seen.length < 3) throw new RetryableError("HTTP 503");
        return { runId, outcome: "succeeded" };
      },
    });
    worker(queue, h);
    await new PostgresDispatcher({
      pool,
      queue,
      backoff: { type: "exponential", delayMs: 150 },
    }).enqueueRun("r1");
    await waitFor(() => seen.length === 3);
    expect(seen.map((s) => s.opts)).toEqual([
      { isRetry: false, canDefer: true, finalAttempt: false },
      { isRetry: true, canDefer: true, finalAttempt: false },
      { isRetry: true, canDefer: true, finalAttempt: true },
    ]);
    const gap1 = seen[1]!.at - seen[0]!.at;
    const gap2 = seen[2]!.at - seen[1]!.at;
    expect(gap1).toBeGreaterThanOrEqual(130);
    expect(gap2).toBeGreaterThanOrEqual(280); // 150 * 2
    await waitFor(async () => (await jobs(queue)).length === 0);
  });

  it("marks a job dead once its attempts are used up", async () => {
    const queue = newQueue();
    const h = handlers({
      executeRun: vi.fn(async () => {
        throw new RetryableError("still down");
      }),
    });
    worker(queue, h);
    await new PostgresDispatcher({
      pool,
      queue,
      maxAttempts: 2,
      backoff: { type: "fixed", delayMs: 10 },
    }).enqueueRun("r1");
    await waitFor(async () => (await jobs(queue))[0]?.status === "dead");
    const [row] = await jobs(queue);
    expect(row).toMatchObject({ status: "dead", attempts: 2 });
    expect(row?.last_error).toMatch(/attempts exhausted \(2\): still down/);
    expect(h.executeRun).toHaveBeenCalledTimes(2);
  });

  it("marks a permanent failure dead without retrying", async () => {
    const queue = newQueue();
    const h = handlers({
      executeRun: vi.fn(async () => {
        throw new PermanentError("schema violation", "schema");
      }),
    });
    worker(queue, h);
    await new PostgresDispatcher({
      pool,
      queue,
      backoff: { type: "fixed", delayMs: 10 },
    }).enqueueRun("r1");
    await waitFor(async () => (await jobs(queue))[0]?.status === "dead");
    await sleep(150);
    expect(h.executeRun).toHaveBeenCalledTimes(1);
    expect((await jobs(queue))[0]).toMatchObject({ status: "dead", attempts: 1 });
    expect((await jobs(queue))[0]?.last_error).toBe("permanent: schema violation");
  });

  it("re-delivers a deferred run after the delay without spending an attempt", async () => {
    const queue = newQueue();
    const seen: Array<{ at: number; opts: ExecuteOptions | undefined }> = [];
    const h = handlers({
      executeRun: async (runId, opts): Promise<ExecuteResult> => {
        seen.push({ at: Date.now(), opts });
        return seen.length === 1
          ? { runId, outcome: "deferred", deferred: 400 }
          : { runId, outcome: "succeeded" };
      },
    });
    worker(queue, h);
    await new PostgresDispatcher({ pool, queue, maxAttempts: 1 }).enqueueRun("r1");
    await waitFor(() => seen.length === 1);
    await waitFor(async () => (await jobs(queue))[0]?.status === "queued");
    expect((await jobs(queue))[0]).toMatchObject({ status: "queued", attempts: 0 });
    await waitFor(() => seen.length === 2);
    expect(seen[1]!.at - seen[0]!.at).toBeGreaterThanOrEqual(380);
    // maxAttempts is 1: had the defer spent it, the second delivery would be
    // a retry on its final attempt and the first would never have come back.
    expect(seen[1]!.opts).toEqual({ isRetry: false, canDefer: true, finalAttempt: true });
    await waitFor(async () => (await jobs(queue)).length === 0);
  });

  it("reclaims a job whose worker died once the lease expires", async () => {
    const queue = newQueue();
    await new PostgresDispatcher({ pool, queue }).enqueueRun("r1");
    // A worker that claimed the job and died: running, lease about to lapse.
    await pool.query(
      `UPDATE durastep_jobs
          SET status = 'running', locked_by = 'dead-worker', attempts = 1,
              locked_until = now() + interval '400 milliseconds'
        WHERE queue = $1`,
      [queue],
    );
    const h = handlers();
    const started = Date.now();
    worker(queue, h);
    await waitFor(() => vi.mocked(h.executeRun).mock.calls.length === 1);
    expect(Date.now() - started).toBeGreaterThanOrEqual(300);
    expect(h.executeRun).toHaveBeenCalledWith("r1", {
      isRetry: true,
      canDefer: true,
      finalAttempt: false,
    });
    await waitFor(async () => (await jobs(queue)).length === 0);
  });

  it("hands over a job left running by a closed worker, and ignores its late completion", async () => {
    const queue = newQueue();
    let release: (() => void) | undefined;
    const hung = handlers({
      executeRun: async (runId) => {
        await new Promise<void>((r) => {
          release = r;
        });
        return { runId, outcome: "succeeded" as const };
      },
    });
    const a = worker(queue, hung, { leaseMs: 500, heartbeatMs: 100, shutdownTimeoutMs: 50 });
    await new PostgresDispatcher({ pool, queue }).enqueueRun("r1");
    await waitFor(() => release !== undefined);
    await a.close(); // times out: the job is still running, heartbeats stop
    expect((await jobs(queue))[0]).toMatchObject({ status: "running", attempts: 1 });

    let bRuns = 0;
    const b = handlers({
      executeRun: async (runId) => {
        bRuns += 1;
        await sleep(300);
        return { runId, outcome: "succeeded" as const };
      },
    });
    worker(queue, b);
    await waitFor(() => bRuns === 1);
    expect((await jobs(queue))[0]).toMatchObject({ status: "running", attempts: 2 });
    // A finishes now: its guarded completion must not delete B's claim.
    release?.();
    await sleep(100);
    expect((await jobs(queue))[0]).toMatchObject({ status: "running", attempts: 2 });
    await waitFor(async () => (await jobs(queue)).length === 0);
    expect(bRuns).toBe(1);
  });

  it("keeps a long job's lease alive with heartbeats", async () => {
    const queue = newQueue();
    let runs = 0;
    const slow = handlers({
      executeRun: async (runId) => {
        runs += 1;
        await sleep(1_200);
        return { runId, outcome: "succeeded" as const };
      },
    });
    const opts = { leaseMs: 400, heartbeatMs: 100 };
    worker(queue, slow, opts);
    worker(queue, slow, opts);
    await new PostgresDispatcher({ pool, queue }).enqueueRun("r1");
    await waitFor(async () => runs === 1 && (await jobs(queue)).length === 0, 5_000);
    expect(runs).toBe(1);
  });

  it("ends a job that keeps killing its worker as dead", async () => {
    const queue = newQueue();
    await new PostgresDispatcher({ pool, queue, maxAttempts: 2 }).enqueueRun("r1");
    await pool.query(
      `UPDATE durastep_jobs
          SET status = 'running', locked_by = 'dead-worker', attempts = 2,
              locked_until = now() - interval '1 second'
        WHERE queue = $1`,
      [queue],
    );
    const h = handlers();
    worker(queue, h);
    await waitFor(async () => (await jobs(queue))[0]?.status === "dead");
    expect(h.executeRun).not.toHaveBeenCalled();
    expect((await jobs(queue))[0]?.last_error).toMatch(/lease expired on the final attempt/);
  });

  it("executes every job exactly once across competing workers (SKIP LOCKED)", async () => {
    const queue = newQueue();
    const counts = new Map<string, number>();
    const h = handlers({
      executeRun: async (runId) => {
        counts.set(runId, (counts.get(runId) ?? 0) + 1);
        await sleep(Math.floor(Math.random() * 5));
        return { runId, outcome: "succeeded" as const };
      },
    });
    const dispatcher = new PostgresDispatcher({ pool, queue });
    const total = 200;
    for (let w = 0; w < 4; w++) worker(queue, h, { concurrency: 5, pollIntervalMs: 20 });
    await Promise.all(Array.from({ length: total }, (_, i) => dispatcher.enqueueRun(`r${i}`)));
    await waitFor(async () => (await jobs(queue)).length === 0, 15_000);
    expect(counts.size).toBe(total);
    expect([...counts.values()].every((n) => n === 1)).toBe(true);
  }, 20_000);

  it("coalesces duplicate pending enqueues and pulls a delayed one forward", async () => {
    const queue = newQueue();
    const d = new PostgresDispatcher({ pool, queue });
    for (let i = 0; i < 5; i++) await d.enqueueAdvance("s1");
    await d.enqueueRun("s1"); // a different kind is different work
    expect((await jobs(queue)).map((j) => `${j.kind}:${j.ref}`)).toEqual(["advance:s1", "run:s1"]);
    await pool.query(
      "UPDATE durastep_jobs SET run_at = now() + interval '1 hour' WHERE queue = $1 AND kind = 'advance'",
      [queue],
    );
    await d.enqueueAdvance("s1");
    const advance = (await jobs(queue)).find((j) => j.kind === "advance");
    expect(advance!.run_at.getTime()).toBeLessThanOrEqual(Date.now() + 1_000);

    // While a job runs, a new enqueue is new work and gets its own row.
    let release: (() => void) | undefined;
    const h = handlers({
      advance: vi.fn(async (sessionId: string) => {
        if (!release) await new Promise<void>((r) => (release = r));
        return { sessionId, status: "EXECUTING" as const, scheduled: [], completed: false };
      }),
    });
    worker(queue, h);
    await waitFor(() => release !== undefined);
    await d.enqueueAdvance("s1");
    expect((await jobs(queue)).filter((j) => j.kind === "advance").map((j) => j.status)).toEqual([
      "running",
      "queued",
    ]);
    release?.();
    await waitFor(async () => (await jobs(queue)).length === 0);
    expect(h.advance).toHaveBeenCalledTimes(2);
  });

  it("drops a retrying job when the same work was enqueued again meanwhile", async () => {
    const queue = newQueue();
    const d = new PostgresDispatcher({ pool, queue, backoff: { type: "fixed", delayMs: 10 } });
    let calls = 0;
    const h = handlers({
      advance: async (sessionId) => {
        calls += 1;
        if (calls === 1) {
          await d.enqueueAdvance(sessionId); // the same work, queued while this runs
          throw new Error("transient");
        }
        return { sessionId, status: "EXECUTING", scheduled: [], completed: false };
      },
    });
    worker(queue, h, { concurrency: 1 });
    await d.enqueueAdvance("s1");
    await waitFor(async () => calls === 2 && (await jobs(queue)).length === 0);
    await sleep(100);
    expect(calls).toBe(2);
  });

  it("joins the caller's transaction through withClient", async () => {
    const queue = newQueue();
    const d = new PostgresDispatcher({ pool, queue });
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await d.withClient(client).enqueueRun("rolled-back");
      await client.query("ROLLBACK");
      await client.query("BEGIN");
      await d.withClient(client).enqueueRun("committed");
      await client.query("COMMIT");
    } finally {
      client.release();
    }
    expect((await jobs(queue)).map((j) => j.ref)).toEqual(["committed"]);
  });

  it("closes gracefully: waits for in-flight jobs and claims nothing after", async () => {
    const queue = newQueue();
    let finished = 0;
    const h = handlers({
      executeRun: vi.fn(async (runId: string) => {
        await sleep(400);
        finished += 1;
        return { runId, outcome: "succeeded" as const };
      }),
    });
    const w = worker(queue, h);
    const d = new PostgresDispatcher({ pool, queue });
    await d.enqueueRun("r1");
    await waitFor(() => w.inFlight === 1);
    await w.close();
    expect(finished).toBe(1);
    expect(w.inFlight).toBe(0);
    expect(await jobs(queue)).toEqual([]);
    await d.enqueueRun("r2");
    await sleep(300);
    expect(h.executeRun).toHaveBeenCalledTimes(1);
    expect((await jobs(queue))[0]).toMatchObject({ ref: "r2", status: "queued" });
  });

  it("runs the example chain on PostgresStore + PostgresDispatcher with two workers", async () => {
    const queue = newQueue();
    const attempts: RunFinishedEvent[] = [];
    const pools = [
      new pg.Pool({ connectionString: url, max: 8 }),
      new pg.Pool({ connectionString: url, max: 8 }),
    ];
    // Two independent runtimes, as two processes would have.
    const runtimes = pools.map((p) =>
      createRuntime({
        store: new PostgresStore({ pool: p }),
        dispatcher: new PostgresDispatcher({
          pool: p,
          queue,
          backoff: { type: "fixed", delayMs: 50 },
        }),
        registry,
        telemetry: { onRunFinished: (e) => void attempts.push(e) },
      }),
    );
    const handles = runtimes.map((runtime, i) =>
      startPostgresWorker({
        pool: pools[i]!,
        runtime,
        queue,
        pollIntervalMs: 50,
        recovery: false,
      }),
    );
    try {
      const started = await runtimes[0]!.startSession(chain, {
        tenantId: `pgq-e2e-${randomUUID()}`,
        inputs: { url: "https://example.com" },
      });
      await waitFor(async () => {
        const s = await runtimes[1]!.getSession(started.id);
        return s?.status === "COMPLETED" || s?.status === "FAILED";
      }, 15_000);
      const session = await runtimes[1]!.getSession(started.id);
      expect(session?.status).toBe("COMPLETED");
      expect(Object.fromEntries(session!.plan.map((s) => [s.stepId, s.status]))).toEqual({
        fetch: "SUCCEEDED",
        "analyse-a": "SUCCEEDED",
        "analyse-b": "SUCCEEDED",
        enrich: "SKIPPED",
        summarise: "SUCCEEDED",
      });
      const runs = await runtimes[0]!.getRuns(started.id);
      expect(runs).toHaveLength(5);
      expect(runs.find((r) => r.stepId === "analyse-b")?.attempt).toBe(2);
      // Every (step, attempt) ran once: no attempt was executed twice.
      const keys = attempts
        .filter((a) => a.sessionId === started.id)
        .map((a) => `${a.stepId}#${a.attempt}`);
      expect(new Set(keys).size).toBe(keys.length);
      expect(keys.sort()).toEqual(
        ["analyse-a#1", "analyse-b#1", "analyse-b#2", "enrich#1", "fetch#1", "summarise#1"].sort(),
      );
      await waitFor(async () => (await jobs(queue)).length === 0);
    } finally {
      await Promise.all(handles.map((h) => h.close()));
      await Promise.all(pools.map((p) => p.end()));
    }
  }, 20_000);
});
