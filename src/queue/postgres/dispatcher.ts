import type { PgQueryable } from "../../store/postgres/index";
import type { Dispatcher } from "../types";

/** Default queue name, shared with the BullMQ adapter. */
export const DEFAULT_QUEUE = "durastep";

/** The channel `PostgresDispatcher` notifies and `startPostgresWorker` listens on. */
export const JOBS_CHANNEL = "durastep_jobs";

/** How long to wait before retry number `n` (1-based count of failed attempts). */
export interface BackoffPolicy {
  /** `fixed`: always `delayMs`. `exponential`: `delayMs * 2^(n-1)`. */
  type: "fixed" | "exponential";
  /** Base delay in milliseconds. */
  delayMs: number;
  /** Upper bound for the delay. Default 5 minutes. */
  maxMs?: number;
}

/** Default backoff: exponential from 2 s, capped at 5 minutes (as the BullMQ adapter). */
export const DEFAULT_BACKOFF: BackoffPolicy = { type: "exponential", delayMs: 2_000 };

/** Default cap applied when a policy has no `maxMs`. */
export const DEFAULT_BACKOFF_MAX_MS = 5 * 60_000;

/**
 * The delay before retry number `failedAttempts` (1 after the first failure).
 *
 * @param policy - the backoff policy.
 * @param failedAttempts - attempts that failed so far, at least 1.
 */
export function backoffDelay(policy: BackoffPolicy, failedAttempts: number): number {
  const base = Math.max(0, Math.floor(policy.delayMs));
  const cap = Math.max(0, Math.floor(policy.maxMs ?? DEFAULT_BACKOFF_MAX_MS));
  if (policy.type === "fixed") return Math.min(base, cap);
  const n = Math.max(1, Math.floor(failedAttempts));
  // 2^30 is far past any sensible cap; clamping avoids Infinity.
  return Math.min(cap, base * 2 ** Math.min(n - 1, 30));
}

/** Options for `PostgresDispatcher`. */
export interface PostgresDispatcherOptions {
  /**
   * Where jobs are inserted: a `pg.Pool`, or a `pg.PoolClient` to enqueue
   * inside that client's open transaction. Only `query` is used.
   */
  pool: PgQueryable;
  /** Queue name. Default `"durastep"`. Must match the worker's. */
  queue?: string;
  /** Total attempts per job, including the first. Default 3. */
  maxAttempts?: number;
  /** Delay between attempts. Default exponential from 2 s, capped at 5 minutes. */
  backoff?: BackoffPolicy;
}

/**
 * Sends runs and advances to the `durastep_jobs` table, where
 * `startPostgresWorker` claims them. One Postgres, no Redis.
 *
 * - Each enqueue is one `INSERT` plus `pg_notify('durastep_jobs', queue)`.
 *   Inside a transaction the notification is delivered on commit.
 * - Duplicate enqueues of the same pending work (same queue, kind and ref,
 *   still `queued`) coalesce into one row; the row's due time moves to the
 *   earlier of the two.
 * - Attempts and backoff are stored on the row, so producers decide them
 *   and every worker applies the same policy.
 *
 * @example
 * const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
 * const runtime = createRuntime({
 *   store: new PostgresStore({ pool }),
 *   dispatcher: new PostgresDispatcher({ pool }),
 *   registry,
 * });
 */
export class PostgresDispatcher implements Dispatcher {
  /** Queue name. */
  readonly queue: string;
  /** Total attempts per job. */
  readonly maxAttempts: number;
  /** Backoff written to every job. */
  readonly backoff: BackoffPolicy;
  private readonly db: PgQueryable;

  /** @param options - pool, queue name, attempts and backoff. */
  constructor(options: PostgresDispatcherOptions) {
    if (!options?.pool || typeof options.pool.query !== "function") {
      throw new TypeError("PostgresDispatcher: pass a pool (or client) with a query method");
    }
    this.db = options.pool;
    this.queue = options.queue ?? DEFAULT_QUEUE;
    this.maxAttempts = Math.max(1, Math.floor(options.maxAttempts ?? 3));
    this.backoff = { ...DEFAULT_BACKOFF, ...options.backoff };
  }

  /**
   * A dispatcher with the same settings that inserts through `db`, for
   * example a client inside your own transaction: the job becomes visible
   * (and the notification fires) only if that transaction commits.
   *
   * @param db - a pool or client.
   */
  withClient(db: PgQueryable): PostgresDispatcher {
    return new PostgresDispatcher({
      pool: db,
      queue: this.queue,
      maxAttempts: this.maxAttempts,
      backoff: this.backoff,
    });
  }

  /** Inserts a `run` job for `runId`. */
  async enqueueRun(runId: string): Promise<void> {
    await this.enqueue("run", runId);
  }

  /** Inserts an `advance` job for `sessionId`. */
  async enqueueAdvance(sessionId: string): Promise<void> {
    await this.enqueue("advance", sessionId);
  }

  private async enqueue(kind: "run" | "advance", ref: string): Promise<void> {
    await this.db.query(
      `WITH job AS (
         INSERT INTO durastep_jobs
           (queue, kind, ref, max_attempts, backoff_type, backoff_ms, backoff_max_ms)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (queue, kind, ref) WHERE status = 'queued'
           DO UPDATE SET run_at = LEAST(durastep_jobs.run_at, now()), updated_at = now()
         RETURNING id
       )
       SELECT id, pg_notify('${JOBS_CHANNEL}', $1) FROM job`,
      [
        this.queue,
        kind,
        ref,
        this.maxAttempts,
        this.backoff.type,
        Math.max(0, Math.floor(this.backoff.delayMs)),
        this.backoff.maxMs === undefined ? null : Math.max(0, Math.floor(this.backoff.maxMs)),
      ],
    );
  }
}
