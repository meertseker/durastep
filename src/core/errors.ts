/**
 * Error taxonomy.
 *
 * Retrying everything wastes money on work that will deterministically
 * fail again (a schema violation, an exhausted quota). Retrying nothing
 * loses runs to a flaky network. durastep splits failures into three
 * classes and the executor and dispatchers act on the class:
 *
 * - `RetryableError`: transient. The run is marked FAILED for
 *   observability, the error is rethrown and the dispatcher retries.
 * - `PermanentError`: deterministic. The run is marked FAILED, the
 *   session is advanced, nothing is retried.
 * - `DeferError`: "not now". No capacity; the run goes back to PENDING
 *   and the dispatcher re-queues it later without spending an attempt.
 *
 * Plain errors are classified by `classifyError`. Unknown errors default
 * to retryable: retrying once is cheaper than losing a run to a flaky
 * dependency.
 */

type Brand = "retryable" | "permanent" | "defer";

function brandOf(err: unknown): Brand | undefined {
  if (err === null || typeof err !== "object") return undefined;
  const value = (err as { durastepKind?: unknown }).durastepKind;
  return value === "retryable" || value === "permanent" || value === "defer" ? value : undefined;
}

/** Why a permanent error will never succeed on retry. */
export type PermanentReason = "schema" | "input" | "quota" | "other";

/** A transient failure. Dispatchers retry it with backoff. */
export class RetryableError extends Error {
  /** Brand used by `isRetryableError`; survives duplicated module copies. */
  readonly durastepKind = "retryable" as const;
  /** Always true. */
  readonly retryable = true as const;

  /**
   * @param message - human-readable description.
   * @param options - standard `ErrorOptions`; set `cause` to the original error.
   */
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "RetryableError";
  }
}

/** A deterministic failure. Never retried. */
export class PermanentError extends Error {
  /** Brand used by `isPermanentError`; survives duplicated module copies. */
  readonly durastepKind = "permanent" as const;
  /** Always false. */
  readonly retryable = false as const;
  /** Category of the failure. */
  readonly reason: PermanentReason;

  /**
   * @param message - human-readable description.
   * @param reason - category; defaults to `"other"`.
   * @param options - standard `ErrorOptions`; set `cause` to the original error.
   */
  constructor(message: string, reason: PermanentReason = "other", options?: ErrorOptions) {
    super(message, options);
    this.name = "PermanentError";
    this.reason = reason;
  }
}

/**
 * "Not now": the worker has no capacity. Only honoured when
 * `ctx.canDefer` is true; otherwise the run fails permanently, because
 * nothing would re-queue it.
 */
export class DeferError extends Error {
  /** Brand used by `isDeferError`; survives duplicated module copies. */
  readonly durastepKind = "defer" as const;
  /** How long the dispatcher should wait before re-queueing. */
  readonly delayMs: number;

  /**
   * @param delayMs - delay before the run is re-queued, in milliseconds.
   * @param message - human-readable description.
   */
  constructor(delayMs: number, message = "deferred: no capacity") {
    super(message);
    this.name = "DeferError";
    this.delayMs = Math.max(0, Math.floor(delayMs));
  }
}

/** True when `err` is a `RetryableError` (from any copy of durastep). */
export function isRetryableError(err: unknown): err is RetryableError {
  return brandOf(err) === "retryable";
}

/** True when `err` is a `PermanentError` (from any copy of durastep). */
export function isPermanentError(err: unknown): err is PermanentError {
  return brandOf(err) === "permanent";
}

/** True when `err` is a `DeferError` (from any copy of durastep). */
export function isDeferError(err: unknown): err is DeferError {
  return brandOf(err) === "defer";
}

/** The outcome of classifying an error. */
export type ErrorClass = { retryable: true } | { retryable: false; reason: PermanentReason };

/**
 * A rule that classifies errors. `match` is tested against the lower-cased
 * message when it is a RegExp, or called with the raw error when it is a
 * function. The first matching rule wins.
 */
export interface ErrorMatcher {
  /** Pattern for the lower-cased message, or a predicate on the raw error. */
  match: RegExp | ((err: unknown) => boolean);
  /** Whether a match is transient. */
  retryable: boolean;
  /** Category when `retryable` is false. Defaults to `"other"`. */
  reason?: PermanentReason;
}

function statusOf(err: unknown): number | undefined {
  if (err === null || typeof err !== "object") return undefined;
  const e = err as { status?: unknown; statusCode?: unknown; response?: { status?: unknown } };
  for (const v of [e.status, e.statusCode, e.response?.status]) {
    if (typeof v === "number" && Number.isInteger(v)) return v;
  }
  return undefined;
}

function codeOf(err: unknown): string {
  if (err === null || typeof err !== "object") return "";
  const code = (err as { code?: unknown }).code;
  return typeof code === "string" ? code.toUpperCase() : "";
}

const TRANSIENT_CODES = new Set([
  "ECONNRESET",
  "ECONNREFUSED",
  "ENOTFOUND",
  "ETIMEDOUT",
  "EAI_AGAIN",
  "EPIPE",
  "ECONNABORTED",
  "UND_ERR_SOCKET",
  "UND_ERR_CONNECT_TIMEOUT",
]);

/**
 * The generic default rules, in evaluation order. Retryable patterns are
 * checked before permanent ones, so "timeout while validating schema"
 * still retries.
 */
export const defaultErrorMatchers: readonly ErrorMatcher[] = [
  // Timeouts and aborts, including the executor's own outer deadline.
  {
    match: (err) => {
      const name = err instanceof Error ? err.name.toLowerCase() : "";
      return name === "aborterror" || name === "timeouterror";
    },
    retryable: true,
  },
  { match: /deadline_exceeded|timed? ?out|timeout|aborted/, retryable: true },
  // Network transients, by code or by message.
  { match: (err) => TRANSIENT_CODES.has(codeOf(err)), retryable: true },
  {
    match: /socket hang up|econnreset|econnrefused|enotfound|etimedout|eai_again/,
    retryable: true,
  },
  // Connection pool back-pressure (node-postgres and friends).
  { match: /connection terminated|connection timeout|too many connections/, retryable: true },
  // HTTP rate limits and server errors, by status field or by message.
  {
    match: (err) => {
      const status = statusOf(err);
      return status === 408 || status === 429 || (status !== undefined && status >= 500);
    },
    retryable: true,
  },
  { match: /\bhttp[ _]?(408|429|5\d\d)\b|rate.?limit|too many requests/, retryable: true },
  // Deterministic failures.
  { match: /schema.*violation|violat.*schema/, retryable: false, reason: "schema" },
  {
    match: /invalid json|malformed json|unexpected token .* in json|is not valid json/,
    retryable: false,
    reason: "schema",
  },
  { match: /quota exceeded|budget exceeded|cost ceiling/, retryable: false, reason: "quota" },
  {
    match: (err) => {
      const status = statusOf(err);
      return status === 400 || status === 401 || status === 403 || status === 404 || status === 422;
    },
    retryable: false,
    reason: "input",
  },
];

function messageOf(err: unknown): string {
  if (err instanceof Error) return err.message.toLowerCase();
  if (typeof err === "string") return err.toLowerCase();
  return "";
}

/**
 * Classifies any thrown value. Explicit durastep errors keep their class;
 * everything else goes through `matchers` (checked first) and then the
 * default rules. No match means retryable.
 *
 * @param err - the thrown value.
 * @param matchers - extra rules evaluated before the defaults.
 */
export function classifyError(err: unknown, matchers: readonly ErrorMatcher[] = []): ErrorClass {
  if (isPermanentError(err)) return { retryable: false, reason: err.reason };
  if (isRetryableError(err)) return { retryable: true };
  const message = messageOf(err);
  for (const rule of [...matchers, ...defaultErrorMatchers]) {
    const hit = typeof rule.match === "function" ? rule.match(err) : rule.match.test(message);
    if (hit) {
      return rule.retryable
        ? { retryable: true }
        : { retryable: false, reason: rule.reason ?? "other" };
    }
  }
  return { retryable: true };
}

/**
 * Converts any thrown value into a `RetryableError` or `PermanentError`,
 * preserving the original as `cause`. `DeferError` is not handled here.
 *
 * @param err - the thrown value.
 * @param matchers - extra rules evaluated before the defaults.
 */
export function toTaxonomyError(
  err: unknown,
  matchers: readonly ErrorMatcher[] = [],
): RetryableError | PermanentError {
  if (isRetryableError(err) || isPermanentError(err)) return err;
  const cls = classifyError(err, matchers);
  const message = err instanceof Error ? err.message : String(err);
  return cls.retryable
    ? new RetryableError(message, { cause: err })
    : new PermanentError(message, cls.reason, { cause: err });
}

/** Thrown by `validateChain` with a stable, user-presentable message. */
export class ChainValidationError extends Error {
  /**
   * @param message - what is wrong with the chain.
   */
  constructor(message: string) {
    super(message);
    this.name = "ChainValidationError";
  }
}
