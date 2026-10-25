/**
 * durastep: durable, idempotent multi-step agent pipelines on your own
 * Postgres and Redis. A library, not a platform.
 *
 * @packageDocumentation
 */

export { type ChainStepFor, defineChain, validateChain } from "./core/chain";
export {
  DEFAULT_ESTIMATED_DURATION_MS,
  deadlineFor,
  MAX_DEFAULT_DEADLINE_MS,
  WATCHDOG_DEFAULT_MS,
  WATCHDOG_EXTERNAL_WAIT_MS,
  WATCHDOG_GRACE_MS,
  type WatchdogRun,
  type WatchdogVerdict,
  watchdogVerdict,
} from "./core/deadline";
export {
  ChainValidationError,
  classifyError,
  DeferError,
  defaultErrorMatchers,
  type ErrorClass,
  type ErrorMatcher,
  isDeferError,
  isPermanentError,
  isRetryableError,
  PermanentError,
  type PermanentReason,
  RetryableError,
  toTaxonomyError,
} from "./core/errors";
export { type ExecutorDeps, executeRun } from "./core/executor";
export { canonicalize, computeIdempotencyKey, type IdempotencyKeyInput } from "./core/idempotency";
export { consoleLogger, errorMessage, type Logger, type LogMeta, noopLogger } from "./core/logger";
export {
  advance,
  isRetryPending,
  type OrchestratorDeps,
  RETRY_PENDING_PREFIX,
} from "./core/orchestrator";
export {
  allQuotas,
  type CounterReader,
  costCeiling,
  counterKeys,
  monthBucket,
  monthlyCount,
  type QuotaCheckInput,
  type QuotaDecision,
  type QuotaPolicy,
  UNLIMITED,
} from "./core/quota";
export { createRegistry, defineWorker, type Registry, type WorkersOf } from "./core/registry";
export {
  createRuntime,
  type ExpireOptions,
  type RecoverOptions,
  type Runtime,
  type RuntimeOptions,
  type StartSessionOptions,
} from "./core/runtime";
export type {
  AdvanceResult,
  AnyWorker,
  Chain,
  ChainStep,
  Cost,
  ExecuteOptions,
  ExecuteResult,
  Inputs,
  PlanStep,
  Run,
  RunFinishedEvent,
  RunStatus,
  Session,
  SessionStatus,
  StepStatus,
  Telemetry,
  WorkerContext,
  WorkerDefinition,
  WorkerInputOf,
  WorkerKindOf,
  WorkerOutputOf,
  WorkerTiming,
} from "./core/types";
export { InlineDispatcher, type InlineDispatcherOptions } from "./queue/inline";
export type { Dispatcher, RuntimeHandlers } from "./queue/types";
export { MemoryStore, type MemoryStoreOptions } from "./store/memory";
export {
  isLocked,
  type Locked,
  type NewRun,
  type NewSession,
  type RunFilter,
  type RunGuard,
  type RunPatch,
  type SessionFilter,
  type Store,
} from "./store/types";
