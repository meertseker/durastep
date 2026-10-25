/**
 * BullMQ dispatcher and worker for durastep. Import from `durastep/bullmq`.
 *
 * Requires the optional peer dependencies `bullmq` and `ioredis`.
 *
 * One queue carries two job types:
 *
 *   { type: "run", runId }          -> runtime.executeRun
 *   { type: "advance", sessionId }  -> runtime.advance
 *
 * Error mapping in the worker:
 *   - executeRun returned `deferred`  -> job.moveToDelayed + DelayedError (no attempt spent)
 *   - permanent error                  -> UnrecoverableError (BullMQ stops retrying)
 *   - retryable error                  -> rethrown; BullMQ retries with exponential backoff
 */
import {
  type ConnectionOptions,
  DelayedError,
  type Job,
  type JobsOptions,
  Queue,
  UnrecoverableError,
  Worker,
} from "bullmq";
import { classifyError, RetryableError } from "../../core/errors";
import { errorMessage } from "../../core/logger";
import type { Runtime } from "../../core/runtime";
import type { Dispatcher, RuntimeHandlers } from "../types";

/** Payload of a durastep job. */
export type DurastepJob = { type: "run"; runId: string } | { type: "advance"; sessionId: string };

/** Default queue name. */
export const DEFAULT_QUEUE_NAME = "durastep";

/**
 * Default job options: 3 attempts with exponential backoff starting at 2 s,
 * bounded retention of finished jobs.
 */
export const DEFAULT_JOB_OPTIONS: JobsOptions = {
  attempts: 3,
  backoff: { type: "exponential", delay: 2_000 },
  removeOnComplete: 1_000,
  removeOnFail: 5_000,
};

/** The subset of `Queue` the dispatcher uses (injectable for tests). */
export interface QueueLike {
  /** Adds a job. */
  add(name: string, data: DurastepJob, opts?: JobsOptions): Promise<unknown>;
  /** Closes the queue's connection. */
  close(): Promise<void>;
}

/** Options for `BullMQDispatcher`. */
export interface BullMQDispatcherOptions {
  /** Redis connection: an ioredis instance or connection options. */
  connection?: ConnectionOptions;
  /** Queue name. Default `"durastep"`. Must match the worker's. */
  queueName?: string;
  /** BullMQ key prefix. */
  prefix?: string;
  /** Job options merged over `DEFAULT_JOB_OPTIONS`. */
  jobOptions?: JobsOptions;
  /** Use this queue instead of creating one (for tests or shared queues). */
  queue?: QueueLike;
}

/**
 * Sends runs and advances to a BullMQ queue. The queue (and its Redis
 * connection) is created lazily on the first enqueue, so constructing the
 * dispatcher does not connect.
 */
export class BullMQDispatcher implements Dispatcher {
  private queueInstance: QueueLike | undefined;
  private readonly options: BullMQDispatcherOptions;
  /** Job options applied to every job. */
  readonly jobOptions: JobsOptions;

  /** @param options - connection, queue name and job options. */
  constructor(options: BullMQDispatcherOptions) {
    if (!options.queue && !options.connection) {
      throw new TypeError("BullMQDispatcher: pass a connection or a queue");
    }
    this.options = options;
    this.queueInstance = options.queue;
    this.jobOptions = { ...DEFAULT_JOB_OPTIONS, ...options.jobOptions };
  }

  /** The underlying queue, created on first use. */
  get queue(): QueueLike {
    if (!this.queueInstance) {
      this.queueInstance = new Queue<DurastepJob>(this.options.queueName ?? DEFAULT_QUEUE_NAME, {
        connection: this.options.connection as ConnectionOptions,
        ...(this.options.prefix ? { prefix: this.options.prefix } : {}),
      }) as unknown as QueueLike;
    }
    return this.queueInstance;
  }

  /** Adds a `run` job. */
  async enqueueRun(runId: string): Promise<void> {
    await this.queue.add("run", { type: "run", runId }, this.jobOptions);
  }

  /** Adds an `advance` job. */
  async enqueueAdvance(sessionId: string): Promise<void> {
    await this.queue.add("advance", { type: "advance", sessionId }, this.jobOptions);
  }

  /** Closes the queue if it was created. */
  async close(): Promise<void> {
    await this.queueInstance?.close();
  }
}

/** The subset of a BullMQ `Job` that `processJob` reads. */
export type JobLike = Pick<
  Job<DurastepJob>,
  "id" | "data" | "attemptsMade" | "opts" | "moveToDelayed"
>;

/**
 * Processes one durastep job. This is the worker's processor function,
 * exported so it can be unit-tested or wrapped.
 *
 * @param handlers - the runtime's handlers (`runtime.handlers`).
 * @param job - the BullMQ job.
 * @param token - the worker's lock token (needed for `moveToDelayed`).
 */
export async function processJob(
  handlers: RuntimeHandlers,
  job: JobLike,
  token?: string,
): Promise<unknown> {
  const data = job.data;
  if (data?.type === "run" && typeof data.runId === "string") {
    const maxAttempts = job.opts.attempts ?? 1;
    let result: Awaited<ReturnType<RuntimeHandlers["executeRun"]>>;
    try {
      result = await handlers.executeRun(data.runId, {
        isRetry: job.attemptsMade > 0,
        canDefer: true,
        finalAttempt: job.attemptsMade + 1 >= maxAttempts,
      });
    } catch (err) {
      if (!classifyError(err).retryable) {
        throw new UnrecoverableError(`permanent: ${errorMessage(err)}`);
      }
      throw err;
    }
    if (result.outcome === "deferred") {
      // No capacity: hand the job back for later without holding a worker
      // slot and without spending an attempt. The run row is PENDING again.
      await job.moveToDelayed(Date.now() + result.deferred, token);
      throw new DelayedError();
    }
    return result;
  }
  if (data?.type === "advance" && typeof data.sessionId === "string") {
    const result = await handlers.advance(data.sessionId);
    if (result.locked) {
      // Another process holds the session lock; retry shortly so this
      // advance is not lost. Recovery covers the case where retries run out.
      throw new RetryableError(`session ${data.sessionId} is locked by another advance`);
    }
    return result;
  }
  throw new UnrecoverableError(`unknown durastep job payload (job ${String(job.id)})`);
}

/** Options for `startBullMQWorker`. */
export interface BullMQWorkerOptions {
  /** Redis connection: an ioredis instance (with `maxRetriesPerRequest: null`) or options. */
  connection: ConnectionOptions;
  /** Jobs processed in parallel by this process. Default 10. */
  concurrency?: number;
  /** Queue name. Default `"durastep"`. */
  queueName?: string;
  /** BullMQ key prefix. */
  prefix?: string;
  /**
   * Periodic recovery: `expireStuckRuns` then `recoverStuckSessions` every
   * `intervalMs` (default 60 s). Safe to run in every worker process.
   * Pass `false` to disable and schedule recovery yourself.
   */
  recovery?: false | { intervalMs?: number; olderThanMs?: number };
}

/** A running worker. */
export interface BullMQWorkerHandle {
  /** The BullMQ worker. */
  readonly worker: Worker<DurastepJob>;
  /** Stops recovery and closes the worker, waiting for active jobs. */
  close(): Promise<void>;
}

/**
 * Starts a BullMQ worker that executes durastep jobs for `runtime`. The
 * runtime should use a `BullMQDispatcher` on the same queue so that the
 * next steps are enqueued, not executed inline.
 *
 * @example
 * const runtime = createRuntime({ store, dispatcher: new BullMQDispatcher({ connection }), registry });
 * const { close } = startBullMQWorker(runtime, { connection, concurrency: 20 });
 * process.on("SIGTERM", () => void close());
 */
export function startBullMQWorker(
  runtime: Pick<Runtime, "handlers" | "logger" | "recoverStuckSessions" | "expireStuckRuns">,
  options: BullMQWorkerOptions,
): BullMQWorkerHandle {
  const logger = runtime.logger;
  const worker = new Worker<DurastepJob>(
    options.queueName ?? DEFAULT_QUEUE_NAME,
    (job, token) => processJob(runtime.handlers, job, token),
    {
      connection: options.connection,
      concurrency: options.concurrency ?? 10,
      ...(options.prefix ? { prefix: options.prefix } : {}),
    },
  );
  worker.on("failed", (job, err) => {
    if (err instanceof DelayedError) return;
    logger.warn("durastep.bullmq.job_failed", {
      jobId: job?.id,
      type: job?.data?.type,
      attemptsMade: job?.attemptsMade,
      err: err.message,
    });
  });
  worker.on("error", (err) => logger.error("durastep.bullmq.worker_error", { err: err.message }));

  let timer: ReturnType<typeof setInterval> | undefined;
  if (options.recovery !== false) {
    const intervalMs = options.recovery?.intervalMs ?? 60_000;
    const olderThanMs = options.recovery?.olderThanMs;
    let running = false;
    timer = setInterval(() => {
      if (running) return;
      running = true;
      void (async () => {
        try {
          await runtime.expireStuckRuns();
          await runtime.recoverStuckSessions(olderThanMs === undefined ? {} : { olderThanMs });
        } catch (err) {
          logger.error("durastep.bullmq.recovery_failed", { err: errorMessage(err) });
        } finally {
          running = false;
        }
      })();
    }, intervalMs);
    timer.unref?.();
  }

  return {
    worker,
    async close() {
      if (timer) clearInterval(timer);
      await worker.close();
    },
  };
}
