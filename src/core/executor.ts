/**
 * Executor: runs one worker execution and records the outcome.
 *
 * Contract (shared by every dispatcher):
 *
 * - Only PENDING and RUNNING runs execute. With `isRetry`, a FAILED run
 *   (left by a previous retryable attempt) is reset to RUNNING first.
 * - The quota policy is checked before the worker runs; a denial fails the
 *   run with `PermanentError(reason: "quota")`.
 * - The worker races an outer deadline. When it fires, `ctx.signal` is
 *   aborted (so the work stops, not just the wait) and the attempt fails
 *   with a `RetryableError`.
 * - Success: output and cost are persisted, usage counters incremented,
 *   telemetry fired and the session advanced. Everything after the
 *   SUCCEEDED write is best-effort: a failure there is logged and never
 *   flips the run back to FAILED.
 * - `DeferError` with `canDefer`: back to PENDING, `deferCount + 1`,
 *   returns `{ outcome: "deferred", deferred: delayMs }`. Without
 *   `canDefer` nothing would re-queue the run, so it fails permanently.
 * - Retryable failure: FAILED (with the retry-pending marker) for
 *   observability, then the `RetryableError` is rethrown for the
 *   dispatcher to retry. On `finalAttempt` it is treated as permanent.
 * - Permanent failure: FAILED, telemetry, session advanced, no rethrow.
 */

import type { Dispatcher } from "../queue/types";
import type { Store } from "../store/types";
import { deadlineFor } from "./deadline";
import {
  type ErrorMatcher,
  isDeferError,
  isRetryableError,
  PermanentError,
  RetryableError,
  toTaxonomyError,
} from "./errors";
import { errorMessage, type Logger } from "./logger";
import { RETRY_PENDING_PREFIX } from "./orchestrator";
import { counterKeys, type QuotaPolicy } from "./quota";
import type { Registry } from "./registry";
import type {
  ExecuteOptions,
  ExecuteResult,
  Run,
  RunFinishedEvent,
  Telemetry,
  WorkerContext,
} from "./types";

/** What the executor needs. */
export interface ExecutorDeps {
  /** Persistence. */
  readonly store: Store;
  /** Used to advance the session after a terminal outcome. */
  readonly dispatcher: Dispatcher;
  /** Worker lookup. */
  readonly registry: Pick<Registry, "get">;
  /** Optional quota policy, checked before every attempt. */
  readonly quota?: QuotaPolicy;
  /** Optional telemetry hooks. */
  readonly telemetry?: Telemetry;
  /** Log sink. */
  readonly logger: Logger;
  /** Extra error classification rules. */
  readonly errorMatchers?: readonly ErrorMatcher[];
  /** Clock. */
  readonly now: () => Date;
}

const MAX_ERROR_LENGTH = 2000;

async function raceDeadline<T>(
  work: () => Promise<T>,
  deadlineMs: number,
  abort: AbortController,
  worker: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const err = new RetryableError(
        `deadline_exceeded: ${worker} exceeded ${deadlineMs}ms outer deadline`,
      );
      // Stop the work, not just the wait: a worker that honours ctx.signal
      // releases its resources here, so a retry does not run on top of a
      // still-running first attempt.
      abort.abort(err);
      reject(err);
    }, deadlineMs);
  });
  try {
    return await Promise.race([Promise.resolve().then(work), deadline]);
  } finally {
    clearTimeout(timer);
  }
}

async function loadUpstream(store: Store, run: Run): Promise<Record<string, unknown>> {
  const session = await store.getSession(run.sessionId);
  const step = session?.plan.find((s) => s.stepId === run.stepId);
  if (!session || !step || step.dependsOn.length === 0) return {};
  const depSteps = session.plan.filter((s) => step.dependsOn.includes(s.stepId));
  const ids = depSteps.flatMap((s) => (s.runId ? [s.runId] : []));
  const runs = ids.length > 0 ? await store.listRuns({ ids }) : [];
  const byId = new Map(runs.map((r) => [r.id, r]));
  const upstream: Record<string, unknown> = {};
  for (const dep of depSteps) {
    const depRun = dep.runId ? byId.get(dep.runId) : undefined;
    upstream[dep.stepId] = depRun?.status === "SUCCEEDED" ? depRun.output : undefined;
  }
  return upstream;
}

/**
 * Executes one run. See the module comment for the full contract.
 *
 * @param deps - store, dispatcher, registry and optional quota / telemetry.
 * @param runId - the run to execute.
 * @param options - delivery details from the dispatcher.
 * @throws RetryableError when the attempt failed transiently and more attempts may follow.
 */
export async function executeRun(
  deps: ExecutorDeps,
  runId: string,
  options: ExecuteOptions = {},
): Promise<ExecuteResult> {
  const { store, logger } = deps;
  const run = await store.getRun(runId);
  if (!run) {
    logger.warn("durastep.execute.missing", { runId });
    return { runId, outcome: "missing" };
  }

  const retrying = run.status === "FAILED" && options.isRetry === true;
  if (!retrying && run.status !== "PENDING" && run.status !== "RUNNING") {
    logger.warn("durastep.execute.not_runnable", { runId, status: run.status });
    return { runId, outcome: "not_runnable", status: run.status };
  }

  const attempt = run.attempt + 1;
  const startedAt = deps.now();
  await store.updateRun(runId, {
    status: "RUNNING",
    startedAt,
    finishedAt: null,
    error: null,
    attempt,
  });

  const worker = deps.registry.get(run.worker);
  const abort = new AbortController();
  const cost = { usdCents: 0, tokens: 0 };
  const telemetryBase = {
    runId,
    sessionId: run.sessionId,
    stepId: run.stepId,
    tenantId: run.tenantId,
    worker: run.worker,
    attempt,
  };

  let output: unknown;
  try {
    if (!worker) {
      throw new PermanentError(`unknown worker "${run.worker}"`, "input");
    }
    if (deps.quota) {
      const decision = await deps.quota.check({
        tenantId: run.tenantId,
        worker: run.worker,
        now: startedAt,
        counters: store,
      });
      if (!decision.allowed) {
        throw new PermanentError(decision.reason ?? `quota exceeded: ${run.worker}`, "quota");
      }
    }

    let input: unknown = run.inputs;
    if (worker.parseInput) {
      try {
        input = worker.parseInput(run.inputs);
      } catch (err) {
        throw new PermanentError(`invalid input: ${errorMessage(err)}`, "input", { cause: err });
      }
    }

    const ctx: WorkerContext = {
      runId,
      sessionId: run.sessionId,
      stepId: run.stepId,
      tenantId: run.tenantId,
      subjectId: run.subjectId,
      attempt,
      signal: abort.signal,
      canDefer: options.canDefer === true,
      deferCount: run.deferCount,
      queuedAt: run.createdAt,
      logger,
      upstream: Object.freeze(await loadUpstream(store, run)),
      reportCost(c) {
        if (c.usdCents && Number.isFinite(c.usdCents)) cost.usdCents += c.usdCents;
        if (c.tokens && Number.isFinite(c.tokens)) cost.tokens += c.tokens;
      },
    };

    output = await raceDeadline(
      () => worker.run(ctx, input),
      deadlineFor(worker),
      abort,
      run.worker,
    );
  } catch (err) {
    const finishedAt = deps.now();
    const durationMs = finishedAt.getTime() - startedAt.getTime();

    if (isDeferError(err) && options.canDefer === true) {
      // Not a failure: no capacity. Back to PENDING without spending an
      // attempt; no telemetry (nothing ran) and no advance (not terminal).
      await store.updateRun(runId, {
        status: "PENDING",
        startedAt: null,
        attempt: run.attempt,
        deferCount: run.deferCount + 1,
        deferredAt: finishedAt,
      });
      logger.info("durastep.execute.deferred", { runId, worker: run.worker, delayMs: err.delayMs });
      return { runId, outcome: "deferred", deferred: err.delayMs };
    }

    const classified = isDeferError(err)
      ? new PermanentError(`${err.message} (dispatcher cannot defer)`, "other", { cause: err })
      : toTaxonomyError(err, deps.errorMatchers);
    const message = classified.message;

    if (isRetryableError(classified) && options.finalAttempt !== true) {
      // FAILED for observability, rethrown for the dispatcher to retry. The
      // marker tells the orchestrator the step is still in flight.
      logger.warn("durastep.execute.retryable_failure", {
        runId,
        worker: run.worker,
        err: message,
      });
      await store.updateRun(runId, {
        status: "FAILED",
        finishedAt,
        error: `${RETRY_PENDING_PREFIX}${message}`.slice(0, MAX_ERROR_LENGTH),
      });
      await fireTelemetry(deps, {
        ...telemetryBase,
        status: "FAILED",
        willRetry: true,
        durationMs,
        costUsdCents: cost.usdCents,
        costTokens: cost.tokens,
        error: message,
      });
      throw classified;
    }

    const error = (isRetryableError(classified) ? `retries exhausted: ${message}` : message).slice(
      0,
      MAX_ERROR_LENGTH,
    );
    logger.error("durastep.execute.failed", { runId, worker: run.worker, err: error });
    await store.updateRun(runId, { status: "FAILED", finishedAt, error });
    await fireTelemetry(deps, {
      ...telemetryBase,
      status: "FAILED",
      willRetry: false,
      durationMs,
      costUsdCents: cost.usdCents,
      costTokens: cost.tokens,
      error,
    });
    await notify(deps, run.sessionId, runId);
    return { runId, outcome: "failed", error };
  }

  const finishedAt = deps.now();
  await store.updateRun(runId, {
    status: "SUCCEEDED",
    output: output === undefined ? null : output,
    finishedAt,
    error: null,
    costUsdCents: cost.usdCents,
    costTokens: cost.tokens,
  });

  // Everything below is best-effort. The worker did its job; a failure to
  // record usage, emit telemetry or enqueue the advance must never turn the
  // SUCCEEDED row back into FAILED. Recovery re-advances a stalled session.
  try {
    await store.incrementCounter(counterKeys.runs(run.tenantId, run.worker, startedAt), 1);
    if (cost.usdCents > 0) {
      await store.incrementCounter(counterKeys.costCents(run.tenantId, startedAt), cost.usdCents);
    }
  } catch (err) {
    logger.error("durastep.execute.usage_failed", { runId, err: errorMessage(err) });
  }
  await fireTelemetry(deps, {
    ...telemetryBase,
    status: "SUCCEEDED",
    willRetry: false,
    durationMs: finishedAt.getTime() - startedAt.getTime(),
    costUsdCents: cost.usdCents,
    costTokens: cost.tokens,
  });
  logger.info("durastep.execute.done", { runId, worker: run.worker, attempt });
  await notify(deps, run.sessionId, runId);
  return { runId, outcome: "succeeded" };
}

async function fireTelemetry(deps: ExecutorDeps, event: RunFinishedEvent): Promise<void> {
  if (!deps.telemetry?.onRunFinished) return;
  try {
    await deps.telemetry.onRunFinished(event);
  } catch (err) {
    deps.logger.error("durastep.execute.telemetry_failed", {
      runId: event.runId,
      err: errorMessage(err),
    });
  }
}

async function notify(deps: ExecutorDeps, sessionId: string, runId: string): Promise<void> {
  try {
    await deps.dispatcher.enqueueAdvance(sessionId);
  } catch (err) {
    deps.logger.error("durastep.execute.notify_failed", {
      runId,
      sessionId,
      err: errorMessage(err),
    });
  }
}
