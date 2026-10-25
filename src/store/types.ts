import type { PlanStep, Run, RunStatus, Session, SessionStatus } from "../core/types";

/** Returned by `withSessionLock` when another holder has the lock. */
export interface Locked {
  /** Always true. */
  readonly locked: true;
}

/** True when a `withSessionLock` result means "someone else holds the lock". */
export function isLocked(value: unknown): value is Locked {
  return (
    value !== null &&
    typeof value === "object" &&
    (value as { locked?: unknown }).locked === true &&
    Object.keys(value).length === 1
  );
}

/** Input to `Store.createSession`. */
export interface NewSession {
  /** Tenant id. */
  tenantId: string;
  /** Subject id, or null. */
  subjectId: string | null;
  /** Session-level inputs. */
  inputs: Record<string, unknown>;
  /** Initial plan; every step PENDING. */
  plan: PlanStep[];
}

/** Input to `Store.createRunIfAbsent`. */
export interface NewRun {
  /** Owning session. */
  sessionId: string;
  /** Owning tenant. */
  tenantId: string;
  /** Subject id, or null. */
  subjectId: string | null;
  /** Worker kind. */
  worker: string;
  /** Step id. */
  stepId: string;
  /** Merged worker inputs. */
  inputs: Record<string, unknown>;
  /** Deterministic key; `(tenantId, idempotencyKey)` must be unique. */
  idempotencyKey: string;
}

/** Fields of a run the runtime updates. */
export type RunPatch = Partial<
  Pick<
    Run,
    | "status"
    | "output"
    | "error"
    | "attempt"
    | "deferCount"
    | "deferredAt"
    | "startedAt"
    | "finishedAt"
    | "costUsdCents"
    | "costTokens"
  >
>;

/** Optional compare-and-set condition for `updateRun`. */
export interface RunGuard {
  /** Apply the patch only while the run is in one of these statuses. */
  ifStatus: readonly RunStatus[];
}

/** Filter for `listRuns`. All fields are ANDed. */
export interface RunFilter {
  /** Only runs of this session. */
  sessionId?: string;
  /** Only runs of this tenant. */
  tenantId?: string;
  /** Only runs in one of these statuses. */
  status?: readonly RunStatus[];
  /** Only these run ids. */
  ids?: readonly string[];
  /** Maximum number of rows, oldest first. */
  limit?: number;
}

/** Filter for `listSessions`. All fields are ANDed. */
export interface SessionFilter {
  /** Only sessions of this tenant. */
  tenantId?: string;
  /** Only sessions in one of these statuses. */
  status?: readonly SessionStatus[];
  /** Only sessions whose plan was last written before this time. */
  updatedBefore?: Date;
  /** Maximum number of rows, least recently updated first. */
  limit?: number;
}

/**
 * Persistence used by the runtime. Implementations must provide:
 *
 * - a unique `(tenantId, idempotencyKey)` constraint enforced atomically by
 *   `createRunIfAbsent`;
 * - an optimistic version check in `updatePlan`;
 * - a non-blocking per-session lock in `withSessionLock` (try-lock: return
 *   `{ locked: true }` immediately when held, never wait).
 *
 * `MemoryStore` and `PostgresStore` are the reference implementations.
 */
export interface Store {
  /** Inserts a session with status PENDING and version 0. */
  createSession(input: NewSession): Promise<Session>;
  /** Loads a session, or null. */
  getSession(id: string): Promise<Session | null>;
  /**
   * Writes the plan and status when the stored version equals
   * `expectedVersion`, bumping the version. Returns false on conflict.
   */
  updatePlan(
    id: string,
    plan: PlanStep[],
    expectedVersion: number,
    status: SessionStatus,
  ): Promise<boolean>;
  /**
   * Runs `fn` while holding the session's lock. Returns `{ locked: true }`
   * without running `fn` when another holder has it.
   */
  withSessionLock<T>(id: string, fn: () => Promise<T>): Promise<T | Locked>;
  /**
   * Inserts a PENDING run, or returns the existing run with the same
   * `(tenantId, idempotencyKey)`. `created` tells which happened.
   */
  createRunIfAbsent(input: NewRun): Promise<{ run: Run; created: boolean }>;
  /** Loads a run, or null. */
  getRun(id: string): Promise<Run | null>;
  /** Applies a patch. Throws when the run does not exist. */
  updateRun(id: string, patch: RunPatch): Promise<Run>;
  /**
   * Applies a patch only when the guard holds. Returns null when the run is
   * missing or the guard does not hold.
   */
  updateRun(id: string, patch: RunPatch, guard: RunGuard): Promise<Run | null>;
  /** Lists runs, oldest first. */
  listRuns(filter: RunFilter): Promise<Run[]>;
  /** Lists sessions, least recently updated first. */
  listSessions(filter: SessionFilter): Promise<Session[]>;
  /** Atomically adds `by` to a counter (created at 0) and returns the new value. */
  incrementCounter(key: string, by: number): Promise<number>;
  /** Reads a counter; 0 when absent. */
  getCounter(key: string): Promise<number>;
}
