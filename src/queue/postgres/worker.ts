import { randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { classifyError, RetryableError } from "../../core/errors";
import { errorMessage, type Logger } from "../../core/logger";
import type { Runtime } from "../../core/runtime";
import type { PgClientLike, PgPoolLike } from "../../store/postgres/index";
import type { RuntimeHandlers } from "../types";
import {
  type BackoffPolicy,
  backoffDelay,
  DEFAULT_BACKOFF_MAX_MS,
  DEFAULT_QUEUE,
  JOBS_CHANNEL,
} from "./dispatcher";

/** A claimed job, as the worker sees it. */
export interface ClaimedJob {
  /** Row id. */
  id: string;
  /** Queue name. */
  queue: string;
  /** `run` (ref is a run id) or `advance` (ref is a session id). */
  kind: "run" | "advance";
  /** Run id or session id. */
  ref: string;
  /** Attempts including this one (incremented at claim). */
  attempts: number;
  /** Total attempts allowed. */
  maxAttempts: number;
  /** Backoff stored on the row. */
  backoff: BackoffPolicy;
}

/** What processing a job produced. */
export type JobOutcome =
  | { type: "done" }
  | { type: "deferred"; delayMs: number }
  | { type: "failed"; error: unknown; retryable: boolean };

/** What the worker writes back to the job row. */
export type JobCompletion =
  | { action: "delete" }
  | { action: "requeue"; delayMs: number; refundAttempt: boolean; error: string | null }
  | { action: "dead"; error: string };

const MAX_ERROR_LENGTH = 2000;

/**
 * Maps an outcome onto the row update. Pure, so the retry rules are
 * testable without a database:
 *
 * - done → delete the row;
 * - deferred → queued again after the delay, the attempt refunded;
 * - permanent failure, or attempts used up → dead;
 * - retryable failure → queued again after the backoff.
 *
 * @param job - the claimed job.
 * @param outcome - what processing produced.
 */
export function planCompletion(
  job: Pick<ClaimedJob, "attempts" | "maxAttempts" | "backoff">,
  outcome: JobOutcome,
): JobCompletion {
  if (outcome.type === "done") return { action: "delete" };
  if (outcome.type === "deferred") {
    return {
      action: "requeue",
      delayMs: Math.max(0, outcome.delayMs),
      refundAttempt: true,
      error: null,
    };
  }
  const message = errorMessage(outcome.error).slice(0, MAX_ERROR_LENGTH);
  if (!outcome.retryable) return { action: "dead", error: `permanent: ${message}` };
  if (job.attempts >= job.maxAttempts) {
    return { action: "dead", error: `attempts exhausted (${job.attempts}): ${message}` };
  }
  return {
    action: "requeue",
    delayMs: backoffDelay(job.backoff, job.attempts),
    refundAttempt: false,
    error: message,
  };
}

/**
 * Processes one claimed job against the runtime's handlers and reports the
 * outcome. Never throws.
 *
 * - `run`: `executeRun(ref, { isRetry: attempts > 1, canDefer: true,
 *   finalAttempt: attempts >= maxAttempts })`, as the BullMQ adapter.
 *   A thrown error is retried when `classifyError` says it is retryable.
 * - `advance`: `advance(ref)`. Any error, and a session lock held by
 *   another process, is retried.
 *
 * @param handlers - the runtime's handlers.
 * @param job - the claimed job.
 */
export async function processClaimedJob(
  handlers: RuntimeHandlers,
  job: Pick<ClaimedJob, "kind" | "ref" | "attempts" | "maxAttempts">,
): Promise<JobOutcome> {
  if (job.kind === "run") {
    try {
      const result = await handlers.executeRun(job.ref, {
        isRetry: job.attempts > 1,
        canDefer: true,
        finalAttempt: job.attempts >= job.maxAttempts,
      });
      if (result.outcome === "deferred") return { type: "deferred", delayMs: result.deferred };
      return { type: "done" };
    } catch (err) {
      return { type: "failed", error: err, retryable: classifyError(err).retryable };
    }
  }
  if (job.kind === "advance") {
    try {
      const result = await handlers.advance(job.ref);
      if (result.locked) {
        // Another advance holds the session lock and may have read the plan
        // before this job's trigger; retry so the trigger is not lost.
        return {
          type: "failed",
          error: new RetryableError(`session ${job.ref} is locked by another advance`),
          retryable: true,
        };
      }
      return { type: "done" };
    } catch (err) {
      return { type: "failed", error: err, retryable: true };
    }
  }
  return {
    type: "failed",
    error: new Error(`unknown durastep job kind "${String(job.kind)}"`),
    retryable: false,
  };
}

/** Options for `startPostgresWorker`. */
export interface PostgresWorkerOptions {
  /**
   * The pool jobs are claimed and completed on. The worker never ends it.
   * With `listen` on, it holds one extra connection for `LISTEN`.
   */
  pool: PgPoolLike;
  /** The runtime whose handlers execute jobs. Its dispatcher should be a `PostgresDispatcher` on the same queue. */
  runtime: Pick<Runtime, "handlers" | "logger" | "recoverStuckSessions" | "expireStuckRuns">;
  /** Queue name. Default `"durastep"`. */
  queue?: string;
  /** Jobs processed in parallel by this worker. Default 4. */
  concurrency?: number;
  /** How often to poll when idle, in ms. Default 1000. */
  pollIntervalMs?: number;
  /**
   * How long a claim is valid without a heartbeat, in ms. A running job
   * whose lease expired is claimed by another worker. Default 30 000.
   */
  leaseMs?: number;
  /** How often to extend the lease of running jobs. Default `leaseMs / 3`. */
  heartbeatMs?: number;
  /** Identifies this worker in `locked_by`. Default `hostname:pid:random`. */
  workerId?: string;
  /**
   * `LISTEN durastep_jobs` on a dedicated connection for low-latency
   * wakeups. Polling is always on as the fallback. Set `false` behind
   * PgBouncer in transaction pooling mode. Default true.
   */
  listen?: boolean;
  /** How long `close()` waits for in-flight jobs, in ms. Default 30 000. */
  shutdownTimeoutMs?: number;
  /**
   * Periodic recovery: `expireStuckRuns` then `recoverStuckSessions` every
   * `intervalMs` (default 60 s), as `startBullMQWorker`. Pass `false` to
   * disable and schedule recovery yourself.
   */
  recovery?: false | { intervalMs?: number; olderThanMs?: number };
  /** Log sink. Defaults to the runtime's logger. */
  logger?: Logger;
}

/** A running Postgres worker. */
export interface PostgresWorkerHandle {
  /** The id written to `locked_by`. */
  readonly workerId: string;
  /** Jobs this worker is processing right now. */
  readonly inFlight: number;
  /**
   * Stops claiming, stops listening and waits for in-flight jobs (up to
   * `shutdownTimeoutMs`). Jobs still running after that keep their row in
   * `running`; their lease expires and another worker picks them up.
   */
  close(): Promise<void>;
}

interface ListenClient extends PgClientLike {
  on(event: string, listener: (...args: never[]) => void): unknown;
  removeAllListeners?(event?: string): unknown;
}

type Row = Record<string, unknown>;

function jobFromRow(r: Row): ClaimedJob {
  const maxMs = r.backoff_max_ms;
  return {
    id: String(r.id),
    queue: String(r.queue),
    kind: r.kind as ClaimedJob["kind"],
    ref: String(r.ref),
    attempts: Number(r.attempts),
    maxAttempts: Number(r.max_attempts),
    backoff: {
      type: r.backoff_type === "fixed" ? "fixed" : "exponential",
      delayMs: Number(r.backoff_ms),
      maxMs: maxMs === null || maxMs === undefined ? DEFAULT_BACKOFF_MAX_MS : Number(maxMs),
    },
  };
}

const CLAIM_SQL = `WITH picked AS (
  SELECT id FROM durastep_jobs
   WHERE queue = $1
     AND ((status = 'queued' AND run_at <= now())
       OR (status = 'running' AND locked_until < now()))
   ORDER BY run_at, id
   FOR UPDATE SKIP LOCKED
   LIMIT $2
)
UPDATE durastep_jobs AS j
   SET status = 'running',
       locked_by = $3,
       locked_until = now() + ($4::integer * interval '1 millisecond'),
       attempts = j.attempts + 1,
       updated_at = now()
  FROM picked
 WHERE j.id = picked.id
RETURNING j.*`;

const GUARD = "id = $1 AND locked_by = $2 AND attempts = $3 AND status = 'running'";

/**
 * Starts a worker that claims jobs from `durastep_jobs` and executes them
 * with the runtime's handlers. Postgres only: no Redis.
 *
 * - Claims with `FOR UPDATE SKIP LOCKED` in one short statement and commits
 *   before running anything; no transaction or row lock is held while a
 *   job executes.
 * - Holds a lease (`locked_until`) and extends it every `heartbeatMs`
 *   while the job runs. A job whose lease expired (its worker died) is
 *   claimed again; attempts count at claim, so a job that keeps killing
 *   its worker ends `dead`.
 * - Completion is guarded by `locked_by` and the claimed attempt number, so
 *   a worker that lost its lease cannot overwrite the new owner's state.
 * - Wakes on `LISTEN durastep_jobs` and polls every `pollIntervalMs`.
 *
 * @example
 * const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
 * const runtime = createRuntime({ store: new PostgresStore({ pool }), dispatcher: new PostgresDispatcher({ pool }), registry });
 * const worker = startPostgresWorker({ pool, runtime, concurrency: 8 });
 * process.on("SIGTERM", () => void worker.close());
 */
export function startPostgresWorker(options: PostgresWorkerOptions): PostgresWorkerHandle {
  const { pool, runtime } = options;
  const logger = options.logger ?? runtime.logger;
  const queue = options.queue ?? DEFAULT_QUEUE;
  const concurrency = Math.max(1, Math.floor(options.concurrency ?? 4));
  const pollIntervalMs = Math.max(10, options.pollIntervalMs ?? 1_000);
  const leaseMs = Math.max(100, Math.floor(options.leaseMs ?? 30_000));
  const heartbeatMs = Math.max(10, Math.floor(options.heartbeatMs ?? leaseMs / 3));
  const shutdownTimeoutMs = Math.max(0, options.shutdownTimeoutMs ?? 30_000);
  const workerId = options.workerId ?? `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
  const listen = options.listen !== false;

  const inFlight = new Map<string, Promise<void>>();
  const timers = new Set<ReturnType<typeof setTimeout>>();
  const heartbeats = new Set<ReturnType<typeof setInterval>>();
  let closing = false;
  let closed: Promise<void> | undefined;

  // Wakeup: a pending flag plus the resolver of the current idle wait, so a
  // wake that arrives while the loop is busy is not lost.
  let wakePending = false;
  let wakeIdle: (() => void) | undefined;
  const wake = () => {
    wakePending = true;
    wakeIdle?.();
  };
  const idle = (ms: number) =>
    new Promise<void>((resolve) => {
      if (wakePending || closing) {
        wakePending = false;
        resolve();
        return;
      }
      const done = () => {
        clearTimeout(t);
        timers.delete(t);
        wakeIdle = undefined;
        wakePending = false;
        resolve();
      };
      const t = setTimeout(done, ms);
      timers.add(t);
      wakeIdle = done;
    });
  const wakeLater = (ms: number) => {
    const t = setTimeout(() => {
      timers.delete(t);
      wake();
    }, ms);
    timers.add(t);
  };

  async function claim(limit: number): Promise<ClaimedJob[]> {
    const { rows } = await pool.query(CLAIM_SQL, [queue, limit, workerId, leaseMs]);
    return rows.map((r) => jobFromRow(r as Row));
  }

  async function complete(job: ClaimedJob, completion: JobCompletion): Promise<void> {
    const guard = [job.id, workerId, job.attempts];
    let rowCount: number | null;
    if (completion.action === "delete") {
      ({ rowCount } = await pool.query(`DELETE FROM durastep_jobs WHERE ${GUARD}`, guard));
    } else if (completion.action === "dead") {
      ({ rowCount } = await pool.query(
        `UPDATE durastep_jobs
            SET status = 'dead', locked_until = NULL, last_error = $4, updated_at = now()
          WHERE ${GUARD}`,
        [...guard, completion.error],
      ));
    } else {
      try {
        ({ rowCount } = await pool.query(
          `UPDATE durastep_jobs
              SET status = 'queued',
                  run_at = now() + ($4::integer * interval '1 millisecond'),
                  attempts = attempts - $5::integer,
                  locked_by = NULL,
                  locked_until = NULL,
                  last_error = COALESCE($6, last_error),
                  updated_at = now()
            WHERE ${GUARD}`,
          [
            ...guard,
            Math.min(completion.delayMs, 2_147_483_647),
            completion.refundAttempt ? 1 : 0,
            completion.error,
          ],
        ));
      } catch (err) {
        // 23505: the same work was enqueued again while this job ran, and
        // that queued row will do it. Drop this one instead of duplicating.
        if ((err as { code?: unknown }).code !== "23505") throw err;
        ({ rowCount } = await pool.query(`DELETE FROM durastep_jobs WHERE ${GUARD}`, guard));
        logger.info("durastep.pg.job_coalesced", { jobId: job.id, kind: job.kind, ref: job.ref });
      }
      if (rowCount === 1 && !closing) wakeLater(completion.delayMs + 5);
    }
    if (rowCount !== 1) {
      logger.warn("durastep.pg.lease_lost", {
        jobId: job.id,
        kind: job.kind,
        ref: job.ref,
        attempts: job.attempts,
        action: completion.action,
      });
    }
  }

  async function handle(job: ClaimedJob): Promise<void> {
    let leaseLost = false;
    const heartbeat = setInterval(() => {
      void pool
        .query(
          `UPDATE durastep_jobs
              SET locked_until = now() + ($4::integer * interval '1 millisecond'), updated_at = now()
            WHERE ${GUARD}`,
          [job.id, workerId, job.attempts, leaseMs],
        )
        .then(({ rowCount }) => {
          if (rowCount !== 1 && !leaseLost) {
            leaseLost = true;
            logger.warn("durastep.pg.lease_lost", { jobId: job.id, kind: job.kind, ref: job.ref });
          }
        })
        .catch((err) =>
          logger.error("durastep.pg.heartbeat_failed", { jobId: job.id, err: errorMessage(err) }),
        );
    }, heartbeatMs);
    heartbeats.add(heartbeat);
    try {
      let completion: JobCompletion;
      if (job.attempts > job.maxAttempts) {
        // The lease of its final attempt expired: the worker running it died
        // (or stalled) every time. Stop instead of crash-looping; the
        // watchdog settles the run, recovery the session.
        completion = {
          action: "dead",
          error: `lease expired on the final attempt (${job.maxAttempts}); the worker likely died`,
        };
      } else {
        const outcome = await processClaimedJob(runtime.handlers, job);
        completion = planCompletion(job, outcome);
        if (outcome.type === "failed") {
          logger.warn("durastep.pg.job_failed", {
            jobId: job.id,
            kind: job.kind,
            ref: job.ref,
            attempts: job.attempts,
            next: completion.action,
            err: errorMessage(outcome.error),
          });
        }
      }
      clearInterval(heartbeat);
      await complete(job, completion);
    } catch (err) {
      logger.error("durastep.pg.complete_failed", { jobId: job.id, err: errorMessage(err) });
    } finally {
      clearInterval(heartbeat);
      heartbeats.delete(heartbeat);
    }
  }

  function start(job: ClaimedJob) {
    const p = handle(job).finally(() => {
      inFlight.delete(job.id);
      wake();
    });
    inFlight.set(job.id, p);
  }

  async function loop() {
    let failures = 0;
    while (!closing) {
      const free = concurrency - inFlight.size;
      let claimed = 0;
      if (free > 0) {
        try {
          const jobs = await claim(free);
          failures = 0;
          claimed = jobs.length;
          for (const job of jobs) start(job);
        } catch (err) {
          failures += 1;
          logger.error("durastep.pg.claim_failed", { err: errorMessage(err) });
          await idle(Math.min(pollIntervalMs * 2 ** Math.min(failures, 5), 30_000));
          continue;
        }
      }
      if (closing) break;
      // A full batch means more jobs are probably due: claim again at once.
      if (free > 0 && claimed === free) continue;
      await idle(pollIntervalMs);
    }
  }

  // LISTEN on a dedicated connection, reconnecting with backoff.
  let listener: ListenClient | undefined;
  let listenFailures = 0;
  async function startListener(): Promise<void> {
    if (!listen || closing) return;
    let client: ListenClient | undefined;
    try {
      client = (await pool.connect()) as ListenClient;
      if (typeof client.on !== "function") {
        client.release();
        logger.warn("durastep.pg.listen_unsupported", {});
        return;
      }
      const c = client;
      c.on("notification", ((msg: { channel?: string; payload?: string }) => {
        if (msg.channel === JOBS_CHANNEL && (!msg.payload || msg.payload === queue)) wake();
      }) as never);
      c.on("error", ((err: Error) => listenerLost(c, err)) as never);
      c.on("end", (() => listenerLost(c)) as never);
      await c.query(`LISTEN ${JOBS_CHANNEL}`);
      if (closing) {
        c.release(true);
        return;
      }
      listener = c;
      listenFailures = 0;
      // Anything enqueued while nobody listened is picked up now.
      wake();
    } catch (err) {
      logger.warn("durastep.pg.listen_failed", { err: errorMessage(err) });
      if (client) {
        try {
          client.release(true);
        } catch {
          // already released
        }
      }
      scheduleListen();
    }
  }
  function listenerLost(client: ListenClient, err?: Error) {
    if (listener !== client) return;
    listener = undefined;
    logger.warn("durastep.pg.listener_lost", { err: err ? errorMessage(err) : "connection ended" });
    try {
      client.release(err ?? true);
    } catch {
      // already released
    }
    scheduleListen();
  }
  function scheduleListen() {
    if (closing) return;
    listenFailures += 1;
    const t = setTimeout(
      () => {
        timers.delete(t);
        void startListener();
      },
      Math.min(1_000 * 2 ** Math.min(listenFailures - 1, 5), 30_000),
    );
    timers.add(t);
  }

  let recoveryTimer: ReturnType<typeof setInterval> | undefined;
  if (options.recovery !== false) {
    const intervalMs = options.recovery?.intervalMs ?? 60_000;
    const olderThanMs = options.recovery?.olderThanMs;
    let running = false;
    recoveryTimer = setInterval(() => {
      if (running || closing) return;
      running = true;
      void (async () => {
        try {
          await runtime.expireStuckRuns();
          await runtime.recoverStuckSessions(olderThanMs === undefined ? {} : { olderThanMs });
        } catch (err) {
          logger.error("durastep.pg.recovery_failed", { err: errorMessage(err) });
        } finally {
          running = false;
        }
      })();
    }, intervalMs);
    recoveryTimer.unref?.();
  }

  void startListener();
  const looping = loop().catch((err) =>
    logger.error("durastep.pg.loop_crashed", { err: errorMessage(err) }),
  );
  logger.info("durastep.pg.worker_started", { workerId, queue, concurrency });

  return {
    workerId,
    get inFlight() {
      return inFlight.size;
    },
    close() {
      closed ??= (async () => {
        closing = true;
        if (recoveryTimer) clearInterval(recoveryTimer);
        wakeIdle?.();
        await looping;
        for (const t of timers) clearTimeout(t);
        timers.clear();
        const l = listener;
        listener = undefined;
        if (l) {
          try {
            l.release(true);
          } catch {
            // already released
          }
        }
        if (inFlight.size > 0) {
          let timer: ReturnType<typeof setTimeout> | undefined;
          const timeout = new Promise<"timeout">((resolve) => {
            timer = setTimeout(() => resolve("timeout"), shutdownTimeoutMs);
          });
          const result = await Promise.race([
            Promise.allSettled([...inFlight.values()]).then(() => "done" as const),
            timeout,
          ]);
          clearTimeout(timer);
          if (result === "timeout") {
            // Stop extending the leases so another worker can take over.
            for (const h of heartbeats) clearInterval(h);
            heartbeats.clear();
            logger.warn("durastep.pg.close_timeout", { workerId, inFlight: inFlight.size });
          }
        }
        logger.info("durastep.pg.worker_closed", { workerId });
      })();
      return closed;
    },
  };
}
