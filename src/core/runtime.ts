import type { Dispatcher, RuntimeHandlers } from "../queue/types";
import type { Store } from "../store/types";
import { validateChain } from "./chain";
import { watchdogVerdict } from "./deadline";
import type { ErrorMatcher } from "./errors";
import { executeRun as execute } from "./executor";
import { errorMessage, type Logger, noopLogger } from "./logger";
import { isRetryPending, advance as orchestratorAdvance } from "./orchestrator";
import type { QuotaPolicy } from "./quota";
import type { Registry } from "./registry";
import type {
  AdvanceResult,
  AnyWorker,
  Chain,
  ExecuteOptions,
  ExecuteResult,
  PlanStep,
  Run,
  Session,
  Telemetry,
} from "./types";

/** Options for `createRuntime`. */
export interface RuntimeOptions<W extends AnyWorker = AnyWorker> {
  /** Persistence: `MemoryStore`, `PostgresStore` or your own. */
  store: Store;
  /** Work transport: `InlineDispatcher`, `BullMQDispatcher` or your own. */
  dispatcher: Dispatcher;
  /** The workers this runtime can execute. */
  registry: Registry<W>;
  /** Checked before every attempt. */
  quota?: QuotaPolicy;
  /** Hooks fired after every attempt. */
  telemetry?: Telemetry;
  /** Log sink. Defaults to a no-op logger. */
  logger?: Logger;
  /** Extra error classification rules, evaluated before the defaults. */
  errorMatchers?: readonly ErrorMatcher[];
  /** Clock. Defaults to `() => new Date()`. */
  now?: () => Date;
}

/** Options for `startSession`. */
export interface StartSessionOptions {
  /** Tenant the session belongs to. Scopes idempotency keys and quotas. */
  tenantId: string;
  /** The entity the chain is about, if any. */
  subjectId?: string | null;
  /** Inputs every step receives, under its own static inputs. */
  inputs?: Record<string, unknown>;
}

/** Options for `recoverStuckSessions`. */
export interface RecoverOptions {
  /** Only sessions whose plan has not been written for this long. Default 2 minutes. */
  olderThanMs?: number;
  /** Maximum sessions per call. Default 100. */
  limit?: number;
  /** Only this tenant's sessions. */
  tenantId?: string;
  /** Override the current time. */
  now?: Date;
}

/** Options for `expireStuckRuns`. */
export interface ExpireOptions {
  /** Override the current time. */
  now?: Date;
  /** Maximum runs inspected per status per call. Default 500. */
  limit?: number;
  /** Only this tenant's runs. */
  tenantId?: string;
}

/** The durastep runtime: the public entry points bound to a store and dispatcher. */
export interface Runtime<W extends AnyWorker = AnyWorker> {
  /** The registry the runtime executes. */
  readonly registry: Registry<W>;
  /** The store. */
  readonly store: Store;
  /** The dispatcher. */
  readonly dispatcher: Dispatcher;
  /** The logger. */
  readonly logger: Logger;
  /** Entry points handed to the dispatcher. */
  readonly handlers: RuntimeHandlers;
  /**
   * Validates the chain, creates a session and advances it once (which
   * schedules the root steps). Returns the session as persisted after that
   * first advance; with `InlineDispatcher` the whole chain has run by then.
   *
   * @throws ChainValidationError when the chain is invalid for the registry.
   */
  startSession(chain: Chain, options: StartSessionOptions): Promise<Session>;
  /**
   * Advances a session: reconciles, schedules ready steps, detects
   * completion. Retries version conflicts, and coalesces concurrent calls
   * in this process into re-runs of the active call.
   */
  advance(sessionId: string): Promise<AdvanceResult>;
  /** Executes a run. Called by dispatchers. */
  executeRun(runId: string, options?: ExecuteOptions): Promise<ExecuteResult>;
  /**
   * Re-advances PENDING / EXECUTING sessions that have no run in flight and
   * have not been written for `olderThanMs`: sessions whose advance
   * notification was lost. Safe to run on a timer from every process.
   */
  recoverStuckSessions(
    options?: RecoverOptions,
  ): Promise<{ recovered: number; sessionIds: string[] }>;
  /**
   * Marks PENDING / RUNNING runs that the watchdog declares dead as FAILED
   * (and FAILED runs whose retry never arrived as finally FAILED), then
   * advances their sessions. Safe to run on a timer from every process.
   */
  expireStuckRuns(options?: ExpireOptions): Promise<{ expired: number; runIds: string[] }>;
  /** Loads a session. */
  getSession(sessionId: string): Promise<Session | null>;
  /** Lists a session's runs, oldest first. */
  getRuns(sessionId: string): Promise<Run[]>;
}

const MAX_CONSECUTIVE_CONFLICTS = 5;

/**
 * Creates a runtime and binds it to the dispatcher.
 *
 * @example
 * const runtime = createRuntime({
 *   store: new MemoryStore(),
 *   dispatcher: new InlineDispatcher(),
 *   registry,
 * });
 * const session = await runtime.startSession(chain, { tenantId: "acme" });
 */
export function createRuntime<W extends AnyWorker>(options: RuntimeOptions<W>): Runtime<W> {
  const { store, dispatcher, registry } = options;
  const logger = options.logger ?? noopLogger;
  const now = options.now ?? (() => new Date());
  const orchestratorDeps = { store, dispatcher, logger };
  const executorDeps = {
    store,
    dispatcher,
    registry,
    logger,
    now,
    ...(options.quota ? { quota: options.quota } : {}),
    ...(options.telemetry ? { telemetry: options.telemetry } : {}),
    ...(options.errorMatchers ? { errorMatchers: options.errorMatchers } : {}),
  };

  // Sessions being advanced by this process. A concurrent call for the same
  // session does not touch the store: it flags a re-run and returns, and the
  // active call loops once more after it finishes. The flag is checked and
  // the entry removed in the same synchronous block, so no request is lost.
  const active = new Map<string, { rerun: boolean }>();

  async function advance(sessionId: string): Promise<AdvanceResult> {
    const current = active.get(sessionId);
    if (current) {
      current.rerun = true;
      const session = await store.getSession(sessionId);
      return {
        sessionId,
        status: session?.status ?? "EXECUTING",
        scheduled: [],
        completed: false,
        locked: true,
      };
    }
    const entry = { rerun: false };
    active.set(sessionId, entry);
    try {
      const scheduled: string[] = [];
      let conflicts = 0;
      for (;;) {
        const result = await orchestratorAdvance(orchestratorDeps, sessionId);
        scheduled.push(...result.scheduled);
        if (result.conflict && conflicts < MAX_CONSECUTIVE_CONFLICTS) {
          conflicts += 1;
          continue;
        }
        conflicts = 0;
        if (entry.rerun && !result.locked) {
          entry.rerun = false;
          continue;
        }
        return { ...result, scheduled };
      }
    } finally {
      active.delete(sessionId);
    }
  }

  const handlers: RuntimeHandlers = {
    executeRun: (runId, opts) => execute(executorDeps, runId, opts),
    advance,
    logger,
  };

  async function startSession(chain: Chain, opts: StartSessionOptions): Promise<Session> {
    validateChain(chain, registry);
    const plan: PlanStep[] = chain.map((s) => ({
      stepId: s.stepId,
      worker: s.worker,
      dependsOn: [...s.dependsOn],
      ...(s.optional ? { optional: true } : {}),
      ...(s.inputs ? { inputs: { ...s.inputs } } : {}),
      status: "PENDING" as const,
    }));
    const session = await store.createSession({
      tenantId: opts.tenantId,
      subjectId: opts.subjectId ?? null,
      inputs: { ...(opts.inputs ?? {}) },
      plan,
    });
    logger.info("durastep.session.started", { sessionId: session.id, steps: plan.length });
    await advance(session.id);
    return (await store.getSession(session.id)) ?? session;
  }

  async function recoverStuckSessions(opts: RecoverOptions = {}) {
    const olderThanMs = opts.olderThanMs ?? 2 * 60_000;
    const at = opts.now ?? now();
    const candidates = await store.listSessions({
      status: ["PENDING", "EXECUTING"],
      updatedBefore: new Date(at.getTime() - olderThanMs),
      limit: opts.limit ?? 100,
      ...(opts.tenantId ? { tenantId: opts.tenantId } : {}),
    });
    const sessionIds: string[] = [];
    for (const session of candidates) {
      const runs = await store.listRuns({
        sessionId: session.id,
        status: ["PENDING", "RUNNING", "FAILED"],
      });
      const inFlight = runs.some((r) => r.status !== "FAILED" || isRetryPending(r));
      if (inFlight) continue;
      try {
        await dispatcher.enqueueAdvance(session.id);
        sessionIds.push(session.id);
      } catch (err) {
        logger.error("durastep.recover.enqueue_failed", {
          sessionId: session.id,
          err: errorMessage(err),
        });
      }
    }
    if (sessionIds.length > 0) {
      logger.info("durastep.recover.sessions", { recovered: sessionIds.length, olderThanMs });
    }
    return { recovered: sessionIds.length, sessionIds };
  }

  async function expireStuckRuns(opts: ExpireOptions = {}) {
    const at = opts.now ?? now();
    const scope = opts.tenantId ? { tenantId: opts.tenantId } : {};
    const limit = opts.limit ?? 500;
    const live = await store.listRuns({ status: ["PENDING", "RUNNING"], limit, ...scope });
    const retrying = (await store.listRuns({ status: ["FAILED"], limit, ...scope })).filter(
      isRetryPending,
    );
    const runIds: string[] = [];
    const sessions = new Set<string>();
    for (const run of [...live, ...retrying]) {
      const verdict = watchdogVerdict(
        { ...run, externalWait: run.inputs.externalWait === true },
        registry.get(run.worker),
        at.getTime(),
      );
      if (!verdict.expired) continue;
      const updated = await store.updateRun(
        run.id,
        {
          status: "FAILED",
          finishedAt: at,
          error: `watchdog: no progress for ${verdict.ageMs}ms (limit ${verdict.limitMs}ms)`,
        },
        { ifStatus: [run.status] },
      );
      if (!updated) continue;
      runIds.push(run.id);
      sessions.add(run.sessionId);
    }
    for (const sessionId of sessions) {
      try {
        await dispatcher.enqueueAdvance(sessionId);
      } catch (err) {
        logger.error("durastep.expire.enqueue_failed", { sessionId, err: errorMessage(err) });
      }
    }
    if (runIds.length > 0) logger.warn("durastep.expire.runs", { expired: runIds.length });
    return { expired: runIds.length, runIds };
  }

  const runtime: Runtime<W> = {
    registry,
    store,
    dispatcher,
    logger,
    handlers,
    startSession,
    advance,
    executeRun: handlers.executeRun,
    recoverStuckSessions,
    expireStuckRuns,
    getSession: (id) => store.getSession(id),
    getRuns: (sessionId) => store.listRuns({ sessionId }),
  };
  dispatcher.bind?.(handlers);
  return runtime;
}
