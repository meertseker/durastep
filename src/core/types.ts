import type { Logger } from "./logger";

/** Lifecycle of a session (one chain instance). */
export type SessionStatus = "PENDING" | "EXECUTING" | "COMPLETED" | "FAILED" | "CANCELLED";

/** Lifecycle of one step inside a session's plan. */
export type StepStatus = "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "SKIPPED";

/** Lifecycle of one worker execution (a run). */
export type RunStatus = "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED";

/** A JSON-friendly bag of inputs handed to a worker. */
export type Inputs = Readonly<Record<string, unknown>>;

/**
 * Cost a worker reports for one execution. Both fields are optional and
 * additive: calling `ctx.reportCost` twice sums the values.
 */
export interface Cost {
  /** Spend in hundredths of a US dollar (integer cents). */
  usdCents?: number;
  /** Tokens consumed (prompt + completion, or whatever unit you bill by). */
  tokens?: number;
}

/**
 * What a worker sees while it runs. Built fresh by the executor for every
 * attempt.
 */
export interface WorkerContext {
  /** Id of the run being executed. */
  readonly runId: string;
  /** Id of the session the run belongs to. */
  readonly sessionId: string;
  /** Step id inside the session's chain. */
  readonly stepId: string;
  /** Tenant the session was started for. */
  readonly tenantId: string;
  /** Optional subject (the entity the chain is about), or null. */
  readonly subjectId: string | null;
  /** 1-based attempt number. Deferrals do not consume attempts. */
  readonly attempt: number;
  /**
   * Aborted when the outer deadline fires. Pass it to fetch / SDK calls so
   * the work stops, not just the wait.
   */
  readonly signal: AbortSignal;
  /** True when the dispatcher can re-queue the run later (throw `DeferError`). */
  readonly canDefer: boolean;
  /** How many times this run has been deferred so far. */
  readonly deferCount: number;
  /** When the run row was created (first queued). */
  readonly queuedAt: Date;
  /** The runtime's logger. */
  readonly logger: Logger;
  /**
   * Outputs of the steps this step depends on, keyed by their `stepId`.
   * A skipped optional dependency is present with the value `undefined`.
   */
  readonly upstream: Readonly<Record<string, unknown>>;
  /** Records spend for this attempt. Persisted on success and fed to quotas. */
  reportCost(cost: Cost): void;
}

/**
 * A unit of work durastep can schedule. Create one with `defineWorker`.
 *
 * @typeParam TKind - the literal worker kind, e.g. `"fetch-page"`.
 * @typeParam TInput - the input shape `run` receives.
 * @typeParam TOutput - the value `run` resolves with; persisted as the run output.
 */
export interface WorkerDefinition<
  TKind extends string = string,
  TInput = unknown,
  TOutput = unknown,
> {
  /** Unique, stable identifier. Persisted on every run row. */
  readonly kind: TKind;
  /** Does the work. Throw a durastep error class to steer retries. */
  run(ctx: WorkerContext, input: TInput): Promise<TOutput>;
  /**
   * Optional input validation. Receives the merged session + step inputs and
   * returns the typed input. A throw marks the run FAILED with
   * `PermanentError(reason: "input")`; it is never retried.
   */
  parseInput?(raw: unknown): TInput;
  /** Typical duration. The default outer deadline is three times this, capped at 180 s. */
  readonly estimatedDurationMs?: number;
  /** Explicit outer deadline in milliseconds. Overrides the estimate-based default. */
  readonly deadlineMs?: number;
  /**
   * Advisory concurrency hint. Recorded for dispatchers that want to honour
   * it; the built-in dispatchers do not enforce it in v0.1.
   */
  readonly concurrency?: number;
  /** Free-form labels for your own routing and dashboards. */
  readonly tags?: readonly string[];
}

/** Any worker definition, whatever its kind, input and output. */
export type AnyWorker = WorkerDefinition<string, unknown, unknown>;

/** Extracts the kind literal of a worker definition. */
export type WorkerKindOf<W> = W extends { readonly kind: infer K extends string } ? K : never;

/** Extracts the input type of a worker definition. */
export type WorkerInputOf<W> = W extends WorkerDefinition<string, infer I, unknown> ? I : never;

/** Extracts the output type of a worker definition. */
export type WorkerOutputOf<W> = W extends WorkerDefinition<string, infer _I, infer O> ? O : never;

/** Worker metadata the deadline and watchdog helpers read. */
export interface WorkerTiming {
  /** Typical duration in milliseconds. */
  readonly estimatedDurationMs?: number;
  /** Explicit outer deadline in milliseconds. */
  readonly deadlineMs?: number;
}

/** One node in a chain (a DAG of worker invocations). */
export interface ChainStep {
  /** Unique id within the chain. Other steps reference it in `dependsOn`. */
  readonly stepId: string;
  /** Worker kind to run for this step. */
  readonly worker: string;
  /** Step ids that must finish first. Empty means a root step (fans out in parallel). */
  readonly dependsOn: readonly string[];
  /** When true the chain continues if this step fails; the step ends SKIPPED. */
  readonly optional?: boolean;
  /** Static inputs merged over the session inputs for this step. */
  readonly inputs?: Inputs;
}

/** A validated-shape list of chain steps. Produce one with `defineChain`. */
export type Chain = readonly ChainStep[];

/** A chain step as persisted inside a session plan, with its live status. */
export interface PlanStep {
  /** Same as `ChainStep.stepId`. */
  stepId: string;
  /** Same as `ChainStep.worker`. */
  worker: string;
  /** Same as `ChainStep.dependsOn`. */
  dependsOn: string[];
  /** Same as `ChainStep.optional`. */
  optional?: boolean;
  /** Same as `ChainStep.inputs`. */
  inputs?: Record<string, unknown>;
  /** Current status of the step. */
  status: StepStatus;
  /** Id of the run scheduled for this step, once scheduled. */
  runId?: string;
}

/** A chain instance: the plan plus its aggregate status. */
export interface Session {
  /** Session id. */
  id: string;
  /** Tenant the session belongs to. Every run inherits it. */
  tenantId: string;
  /** Optional entity the chain is about. Part of every idempotency key. */
  subjectId: string | null;
  /** Aggregate status. */
  status: SessionStatus;
  /** Inputs supplied to `startSession`, merged under every step's inputs. */
  inputs: Record<string, unknown>;
  /** The persisted DAG with per-step status. */
  plan: PlanStep[];
  /** Optimistic concurrency token, bumped on every plan write. */
  version: number;
  /** Creation time. */
  createdAt: Date;
  /** Last plan write. */
  updatedAt: Date;
}

/** One execution record of a worker for one step. */
export interface Run {
  /** Run id. */
  id: string;
  /** Owning session. */
  sessionId: string;
  /** Owning tenant (copied from the session). */
  tenantId: string;
  /** Subject (copied from the session). */
  subjectId: string | null;
  /** Worker kind. */
  worker: string;
  /** Step id inside the session plan. */
  stepId: string;
  /** Current status. */
  status: RunStatus;
  /** Merged inputs the worker receives. */
  inputs: Record<string, unknown>;
  /** Worker output on success, otherwise null. */
  output: unknown;
  /** Last error message, otherwise null. */
  error: string | null;
  /** Deterministic key; unique per tenant. See `computeIdempotencyKey`. */
  idempotencyKey: string;
  /** Attempts started so far (0 before the first). Deferrals do not count. */
  attempt: number;
  /** Times the run was deferred. */
  deferCount: number;
  /** Last deferral time, used by the watchdog. */
  deferredAt: Date | null;
  /** Row creation time. */
  createdAt: Date;
  /** Start of the current or last attempt. */
  startedAt: Date | null;
  /** End of the last attempt. */
  finishedAt: Date | null;
  /** Reported spend of the successful attempt, in US cents. */
  costUsdCents: number | null;
  /** Reported tokens of the successful attempt. */
  costTokens: number | null;
}

/** Result of one `advance(sessionId)` call. */
export interface AdvanceResult {
  /** Session that was advanced. */
  sessionId: string;
  /** Session status after this call. */
  status: SessionStatus;
  /** Step ids scheduled (run created and dispatched) by this call. */
  scheduled: string[];
  /** True when the session is terminal. */
  completed: boolean;
  /**
   * True when another writer bumped the plan version first. Nothing was
   * dispatched; call `advance` again (the runtime does this for you).
   */
  conflict?: boolean;
  /**
   * True when another `advance` held the session lock. The runtime
   * re-advances once the holder finishes when both are in the same process.
   */
  locked?: boolean;
}

/** Options for one `executeRun` call, set by the dispatcher. */
export interface ExecuteOptions {
  /** This delivery is a retry of an attempt that threw `RetryableError`. */
  isRetry?: boolean;
  /** The dispatcher can re-queue the run later, so `DeferError` is honoured. */
  canDefer?: boolean;
  /**
   * No further attempts will follow. A retryable failure is then treated as
   * final: the run stays FAILED, the session is advanced, nothing is rethrown.
   */
  finalAttempt?: boolean;
}

/** What `executeRun` did. */
export type ExecuteResult =
  | { runId: string; outcome: "succeeded" }
  | { runId: string; outcome: "failed"; error: string }
  | { runId: string; outcome: "deferred"; deferred: number }
  | { runId: string; outcome: "not_runnable"; status: RunStatus }
  | { runId: string; outcome: "missing" };

/** Payload of the `onRunFinished` telemetry hook. Fired once per attempt that ran. */
export interface RunFinishedEvent {
  /** Run id. */
  runId: string;
  /** Session id. */
  sessionId: string;
  /** Step id. */
  stepId: string;
  /** Tenant id. */
  tenantId: string;
  /** Worker kind. */
  worker: string;
  /** Status the attempt left the run in. */
  status: "SUCCEEDED" | "FAILED";
  /** True when the failure will be retried by the dispatcher. */
  willRetry: boolean;
  /** Wall time of the attempt. */
  durationMs: number;
  /** 1-based attempt number. */
  attempt: number;
  /** Reported spend for the attempt. */
  costUsdCents: number;
  /** Reported tokens for the attempt. */
  costTokens: number;
  /** Error message when status is FAILED. */
  error?: string;
}

/** Telemetry hooks. Exceptions thrown by hooks are logged and swallowed. */
export interface Telemetry {
  /** Called after every attempt that actually ran the worker. */
  onRunFinished?(event: RunFinishedEvent): void | Promise<void>;
}
