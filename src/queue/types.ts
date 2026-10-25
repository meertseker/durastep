import type { Logger } from "../core/logger";
import type { AdvanceResult, ExecuteOptions, ExecuteResult } from "../core/types";

/**
 * The entry points a dispatcher calls back into. `createRuntime` hands them
 * to `Dispatcher.bind`.
 */
export interface RuntimeHandlers {
  /** Executes one run. Throws `RetryableError` when the dispatcher should retry. */
  executeRun(runId: string, options?: ExecuteOptions): Promise<ExecuteResult>;
  /** Advances one session. */
  advance(sessionId: string): Promise<AdvanceResult>;
  /** The runtime's logger. */
  readonly logger: Logger;
}

/**
 * Moves work between the orchestrator and the executor. The orchestrator
 * calls `enqueueRun` after scheduling a step; the executor calls
 * `enqueueAdvance` after a run reaches a terminal state.
 *
 * Contract for implementations:
 * - `enqueueRun` must eventually call `executeRun(runId, ...)` at least
 *   once, retrying when it throws `RetryableError` and passing
 *   `isRetry: true` on retries and `finalAttempt: true` on the last one.
 * - On `{ outcome: "deferred", deferred: ms }` re-deliver the run after
 *   `ms` without spending an attempt (or pass `canDefer: false`).
 * - `enqueueAdvance` must eventually call `advance(sessionId)`.
 */
export interface Dispatcher {
  /** Arranges for `executeRun(runId)` to be called. */
  enqueueRun(runId: string): Promise<void>;
  /** Arranges for `advance(sessionId)` to be called. */
  enqueueAdvance(sessionId: string): Promise<void>;
  /** Receives the runtime's handlers. Called once by `createRuntime`. */
  bind?(handlers: RuntimeHandlers): void;
}
