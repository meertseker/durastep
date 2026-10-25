/**
 * Orchestrator: walks a session's DAG.
 *
 * Each `advance(sessionId)` call:
 *
 *   1. takes the per-session lock (non-blocking; a held lock is a no-op);
 *   2. reloads the session and reconciles every step's status from its
 *      run's status, because runs finish asynchronously and the persisted
 *      step status may be stale;
 *   3. stops scheduling if a non-optional step failed;
 *   4. schedules every PENDING step whose dependencies are SUCCEEDED or
 *      SKIPPED, through `createRunIfAbsent` with a deterministic
 *      idempotency key;
 *   5. persists the plan with an optimistic version check;
 *   6. marks the session COMPLETED or FAILED when every step is terminal;
 *   7. after releasing the lock, dispatches the runs it created.
 *
 * Race safety is layered: the lock serialises cooperating callers, the
 * idempotency key makes a second scheduler re-use the first one's run, and
 * the version check stops a writer that read a stale plan from persisting
 * (and dispatching) anything.
 */

import type { Dispatcher } from "../queue/types";
import type { Store } from "../store/types";
import { isLocked } from "../store/types";
import { computeIdempotencyKey } from "./idempotency";
import { errorMessage, type Logger } from "./logger";
import type {
  AdvanceResult,
  PlanStep,
  Run,
  RunStatus,
  Session,
  SessionStatus,
  StepStatus,
} from "./types";

/**
 * Prefix the executor puts on `Run.error` when a retryable failure will be
 * retried. Such a FAILED run is still in flight: the orchestrator treats it
 * as RUNNING, and the watchdog gives up on it if no retry ever arrives.
 */
export const RETRY_PENDING_PREFIX = "retryable: ";

/** True when a FAILED run is waiting for a dispatcher retry. */
export function isRetryPending(run: Pick<Run, "status" | "error">): boolean {
  return run.status === "FAILED" && (run.error ?? "").startsWith(RETRY_PENDING_PREFIX);
}

/** What the orchestrator needs. */
export interface OrchestratorDeps {
  /** Persistence. */
  readonly store: Store;
  /** Where created runs are sent. */
  readonly dispatcher: Dispatcher;
  /** Log sink. */
  readonly logger: Logger;
}

const TERMINAL_STEP: ReadonlySet<StepStatus> = new Set(["SUCCEEDED", "FAILED", "SKIPPED"]);
const TERMINAL_SESSION: ReadonlySet<SessionStatus> = new Set(["COMPLETED", "FAILED", "CANCELLED"]);

/**
 * Maps a run status onto the step status it implies, or undefined when the
 * step status should stay as it is.
 */
function stepStatusFromRun(
  run: Pick<Run, "status" | "error">,
  step: PlanStep,
): StepStatus | undefined {
  if (isRetryPending(run)) return step.status === "PENDING" ? "RUNNING" : undefined;
  const actual: RunStatus = run.status;
  if (actual === "SUCCEEDED") return "SUCCEEDED";
  // A CANCELLED run is terminal too; treating it like FAILED keeps the
  // session from sitting in EXECUTING forever.
  if (actual === "FAILED" || actual === "CANCELLED") return step.optional ? "SKIPPED" : "FAILED";
  if ((actual === "RUNNING" || actual === "PENDING") && step.status === "PENDING") return "RUNNING";
  return undefined;
}

/**
 * True when every dependency is SUCCEEDED or SKIPPED (SKIPPED only arises
 * from optional steps, or from a hard failure, in which case nothing is
 * scheduled anyway).
 */
function depsSatisfied(step: PlanStep, byId: ReadonlyMap<string, PlanStep>): boolean {
  for (const depId of step.dependsOn) {
    const dep = byId.get(depId);
    if (!dep || (dep.status !== "SUCCEEDED" && dep.status !== "SKIPPED")) return false;
  }
  return true;
}

interface LockedOutcome {
  result: AdvanceResult;
  toDispatch: Array<{ stepId: string; runId: string }>;
}

async function advanceLocked(deps: OrchestratorDeps, sessionId: string): Promise<LockedOutcome> {
  const { store, logger } = deps;
  const session = await store.getSession(sessionId);
  if (!session) {
    logger.warn("durastep.advance.session_missing", { sessionId });
    return {
      result: { sessionId, status: "FAILED", scheduled: [], completed: true },
      toDispatch: [],
    };
  }
  if (TERMINAL_SESSION.has(session.status)) {
    return {
      result: { sessionId, status: session.status, scheduled: [], completed: true },
      toDispatch: [],
    };
  }

  const plan = session.plan;
  const byId = new Map(plan.map((s) => [s.stepId, s]));

  // Step 1: reconcile step status from run status (the source of truth).
  const runIds = plan.flatMap((s) => (s.runId ? [s.runId] : []));
  const runs = runIds.length > 0 ? await store.listRuns({ ids: runIds }) : [];
  const runById = new Map(runs.map((r) => [r.id, r]));
  for (const step of plan) {
    const run = step.runId ? runById.get(step.runId) : undefined;
    if (!run) continue;
    const next = stepStatusFromRun(run, step);
    if (next) step.status = next;
  }

  // Step 2: a failed non-optional step fails the session; nothing new is
  // scheduled after that.
  let hardFailure = plan.some((s) => s.status === "FAILED" && !s.optional);

  // Step 3: schedule ready steps. Loops because a step can resolve to an
  // existing terminal run (a scheduler that died between creating the run
  // and persisting the plan), which may unblock further steps.
  const scheduled: string[] = [];
  const toDispatch: LockedOutcome["toDispatch"] = [];
  let progressed = true;
  while (!hardFailure && progressed) {
    progressed = false;
    for (const step of plan) {
      if (step.status !== "PENDING" || !depsSatisfied(step, byId)) continue;
      const inputs = { ...session.inputs, ...(step.inputs ?? {}) };
      const idempotencyKey = computeIdempotencyKey({
        tenantId: session.tenantId,
        worker: step.worker,
        subjectId: session.subjectId,
        inputs,
        sessionId: session.id,
        stepId: step.stepId,
      });
      const { run, created } = await store.createRunIfAbsent({
        sessionId: session.id,
        tenantId: session.tenantId,
        subjectId: session.subjectId,
        worker: step.worker,
        stepId: step.stepId,
        inputs,
        idempotencyKey,
      });
      if (!created) {
        logger.info("durastep.advance.reused_run", {
          sessionId,
          stepId: step.stepId,
          runId: run.id,
        });
      }
      step.runId = run.id;
      step.status = "RUNNING";
      scheduled.push(step.stepId);
      // A fresh run, or one left PENDING by a scheduler that never persisted
      // its plan, needs dispatching. A run that already started does not.
      if (created || run.status === "PENDING") {
        toDispatch.push({ stepId: step.stepId, runId: run.id });
        continue;
      }
      const resolved = stepStatusFromRun(run, { ...step, status: "PENDING" });
      if (resolved && TERMINAL_STEP.has(resolved)) {
        step.status = resolved;
        progressed = true;
        if (resolved === "FAILED") hardFailure = true;
      }
    }
  }

  // Step 4: after a hard failure, steps that never started will never run.
  if (hardFailure) {
    for (const step of plan) if (step.status === "PENDING") step.status = "SKIPPED";
  }

  // Step 5: aggregate status.
  const allTerminal = plan.every((s) => TERMINAL_STEP.has(s.status));
  const anyRunning = plan.some((s) => s.status === "RUNNING");
  let nextStatus: SessionStatus = session.status;
  if (allTerminal) nextStatus = hardFailure ? "FAILED" : "COMPLETED";
  else if (anyRunning || scheduled.length > 0) nextStatus = "EXECUTING";

  // Step 6: optimistic persist. On conflict nothing is dispatched; the
  // writer that won owns dispatch, and runs we created stay PENDING for the
  // next advance to pick up through their idempotency key.
  const persisted = await store.updatePlan(session.id, plan, session.version, nextStatus);
  if (!persisted) {
    logger.warn("durastep.advance.version_conflict", { sessionId, version: session.version });
    return {
      result: {
        sessionId,
        status: session.status,
        scheduled: [],
        completed: false,
        conflict: true,
      },
      toDispatch: [],
    };
  }

  logger.info("durastep.advance", { sessionId, scheduled: scheduled.length, status: nextStatus });
  return {
    result: {
      sessionId,
      status: nextStatus,
      scheduled,
      completed: nextStatus === "COMPLETED" || nextStatus === "FAILED",
    },
    toDispatch,
  };
}

/**
 * Runs one iteration of the DAG walk for `sessionId`. Safe to call any
 * number of times, concurrently, from any process: see the module comment
 * for the layered race protection.
 *
 * Returns `locked: true` without doing anything when another `advance`
 * holds the session lock, and `conflict: true` when the plan version moved
 * underneath this call. In both cases the caller should advance again later;
 * `createRuntime` does this automatically.
 *
 * @param deps - store, dispatcher and logger.
 * @param sessionId - the session to advance.
 */
export async function advance(deps: OrchestratorDeps, sessionId: string): Promise<AdvanceResult> {
  const outcome = await deps.store.withSessionLock(sessionId, () => advanceLocked(deps, sessionId));
  if (isLocked(outcome)) {
    deps.logger.info("durastep.advance.skipped_locked", { sessionId });
    const existing: Session | null = await deps.store.getSession(sessionId);
    return {
      sessionId,
      status: existing?.status ?? "EXECUTING",
      scheduled: [],
      completed: false,
      locked: true,
    };
  }

  // Dispatch outside the lock so an in-process dispatcher that executes the
  // run immediately can advance the session again when the run finishes.
  const results = await Promise.allSettled(
    outcome.toDispatch.map((d) => deps.dispatcher.enqueueRun(d.runId)),
  );
  results.forEach((r, i) => {
    if (r.status === "rejected") {
      const d = outcome.toDispatch[i];
      deps.logger.error("durastep.advance.dispatch_failed", {
        sessionId,
        stepId: d?.stepId,
        runId: d?.runId,
        err: errorMessage(r.reason),
      });
    }
  });
  return outcome.result;
}
