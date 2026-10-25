/**
 * Quotas and cost ceilings.
 *
 * A `QuotaPolicy` is consulted by the executor before each attempt. A
 * denial fails the run with `PermanentError(reason: "quota")`, which is
 * never retried. The built-in policies read monthly counters that the
 * executor increments after every successful run.
 *
 * Limits are soft under concurrency: two runs that check at the same
 * moment can both pass and overshoot by the number of concurrent runs.
 * See docs/guarantees.md.
 */

/** Read access to the store's counters. */
export interface CounterReader {
  /** Reads a counter; 0 when absent. */
  getCounter(key: string): Promise<number>;
}

/** What a policy is asked about. */
export interface QuotaCheckInput {
  /** Tenant the run belongs to. */
  tenantId: string;
  /** Worker kind about to run. */
  worker: string;
  /** The current time (the month bucket is derived from it, in UTC). */
  now: Date;
  /** Counter access. */
  counters: CounterReader;
}

/** A policy's answer. */
export interface QuotaDecision {
  /** False blocks the run. */
  allowed: boolean;
  /** Why the run was blocked; becomes the run's error message. */
  reason?: string;
  /** Units left in the current window, when the policy can tell. */
  remaining?: number;
}

/** Decides whether a tenant may run a worker right now. */
export interface QuotaPolicy {
  /** Returns a decision; may be async. */
  check(input: QuotaCheckInput): QuotaDecision | Promise<QuotaDecision>;
}

/** A limit value meaning "no limit". */
export const UNLIMITED = Number.POSITIVE_INFINITY;

/** The UTC month bucket of a date, as `YYYY-MM`. */
export function monthBucket(now: Date): string {
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, "0")}`;
}

/** Counter keys the executor writes and the built-in policies read. */
export const counterKeys = {
  /** Successful runs of `worker` for `tenantId` in the month of `now`. */
  runs: (tenantId: string, worker: string, now: Date): string =>
    `durastep:runs:${tenantId}:${worker}:${monthBucket(now)}`,
  /** Reported spend in US cents for `tenantId` in the month of `now`. */
  costCents: (tenantId: string, now: Date): string =>
    `durastep:cost_cents:${tenantId}:${monthBucket(now)}`,
} as const;

type PerTenant<T> = T | ((tenantId: string) => T);

const resolve = <T>(value: PerTenant<T>, tenantId: string): T =>
  typeof value === "function" ? (value as (t: string) => T)(tenantId) : value;

/**
 * Caps successful runs per worker kind per tenant per calendar month (UTC).
 *
 * @param limits - `{ [workerKind]: maxRunsPerMonth }`, or a function of the
 *   tenant id returning that map (for per-tenant tiers). Use `UNLIMITED` to
 *   leave a kind uncapped.
 * @param options.defaultLimit - limit for kinds not listed. Defaults to `UNLIMITED`.
 *
 * @example
 * monthlyCount({ "generate-report": 100, "fetch-page": UNLIMITED })
 */
export function monthlyCount(
  limits: PerTenant<Readonly<Record<string, number>>>,
  options: { defaultLimit?: number } = {},
): QuotaPolicy {
  return {
    async check({ tenantId, worker, now, counters }) {
      const limit = resolve(limits, tenantId)[worker] ?? options.defaultLimit ?? UNLIMITED;
      if (limit === UNLIMITED) return { allowed: true };
      const used = await counters.getCounter(counterKeys.runs(tenantId, worker, now));
      if (used >= limit) {
        return {
          allowed: false,
          reason: `quota exceeded: ${worker} ran ${used} of ${limit} times this month`,
          remaining: 0,
        };
      }
      return { allowed: true, remaining: limit - used };
    },
  };
}

/**
 * Caps total reported spend per tenant per calendar month (UTC). Spend is
 * what workers report with `ctx.reportCost`, recorded on success.
 *
 * @param options.monthlyUsdCents - the ceiling in US cents, or a function of
 *   the tenant id returning it.
 */
export function costCeiling(options: { monthlyUsdCents: PerTenant<number> }): QuotaPolicy {
  return {
    async check({ tenantId, now, counters }) {
      const ceiling = resolve(options.monthlyUsdCents, tenantId);
      if (ceiling === UNLIMITED) return { allowed: true };
      const spent = await counters.getCounter(counterKeys.costCents(tenantId, now));
      if (spent >= ceiling) {
        return {
          allowed: false,
          reason: `cost ceiling reached: spent ${spent} of ${ceiling} US cents this month`,
          remaining: 0,
        };
      }
      return { allowed: true, remaining: ceiling - spent };
    },
  };
}

/**
 * Combines policies: the first denial wins; otherwise `remaining` is the
 * smallest remaining value reported.
 *
 * @param policies - policies evaluated in order.
 */
export function allQuotas(...policies: readonly QuotaPolicy[]): QuotaPolicy {
  return {
    async check(input) {
      let remaining: number | undefined;
      for (const policy of policies) {
        const decision = await policy.check(input);
        if (!decision.allowed) return decision;
        if (decision.remaining !== undefined) {
          remaining =
            remaining === undefined ? decision.remaining : Math.min(remaining, decision.remaining);
        }
      }
      return remaining === undefined ? { allowed: true } : { allowed: true, remaining };
    },
  };
}
