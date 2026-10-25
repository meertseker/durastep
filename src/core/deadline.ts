import type { WorkerTiming } from "./types";

/** Default estimate when a worker declares none: one minute. */
export const DEFAULT_ESTIMATED_DURATION_MS = 60_000;
/** Cap on the estimate-derived outer deadline: three minutes. */
export const MAX_DEFAULT_DEADLINE_MS = 180_000;
/** Watchdog limit for a worker without an explicit deadline: three minutes. */
export const WATCHDOG_DEFAULT_MS = 3 * 60_000;
/** Watchdog limit for a run that waits on an external callback: ten minutes. */
export const WATCHDOG_EXTERNAL_WAIT_MS = 10 * 60_000;
/** Grace added to an explicit deadline before the watchdog gives up: one minute. */
export const WATCHDOG_GRACE_MS = 60_000;

/**
 * Outer deadline of one worker execution: the worker's own `deadlineMs`
 * when it declares one, otherwise three times its estimated duration,
 * capped at 180 s. A worker that is legitimately long-running in-process
 * should declare `deadlineMs`.
 *
 * @param meta - the worker's timing metadata (or nothing).
 */
export function deadlineFor(meta: WorkerTiming | null | undefined): number {
  if (meta?.deadlineMs && meta.deadlineMs > 0) return meta.deadlineMs;
  return Math.min(
    (meta?.estimatedDurationMs ?? DEFAULT_ESTIMATED_DURATION_MS) * 3,
    MAX_DEFAULT_DEADLINE_MS,
  );
}

/** The run fields the watchdog reads. Timestamps may be Dates or ISO strings. */
export interface WatchdogRun {
  /** When the run row was created. */
  createdAt: Date | string;
  /** Start of the current or last attempt, if any. */
  startedAt?: Date | string | null;
  /** Last deferral, if any. */
  deferredAt?: Date | string | null;
  /**
   * True when the run is parked waiting for an external system (a webhook,
   * a human). Such runs get a longer limit counted from creation.
   */
  externalWait?: boolean;
}

/** The watchdog's decision for one PENDING or RUNNING run. */
export interface WatchdogVerdict {
  /** True when the run should be considered dead. */
  expired: boolean;
  /** The limit that was applied, in milliseconds. */
  limitMs: number;
  /** Age of the run relative to the base the limit is counted from. */
  ageMs: number;
  /** Whether the external-wait branch was used. */
  externalWait: boolean;
}

function toMs(value: Date | string | null | undefined): number {
  if (!value) return 0;
  const t = value instanceof Date ? value.getTime() : Date.parse(value);
  return Number.isFinite(t) ? t : 0;
}

/**
 * Decides whether a PENDING or RUNNING run is dead.
 *
 * - External-wait runs: ten minutes from creation.
 * - Otherwise: the worker's deadline plus a minute of grace when it declares
 *   one, else three minutes, counted from the latest of creation, start and
 *   deferral. A run that is waiting for capacity or being retried is
 *   therefore not declared dead while it is alive.
 *
 * @param run - timestamps of the run.
 * @param meta - the worker's timing metadata (or nothing for an unknown worker).
 * @param nowMs - the current time in epoch milliseconds.
 */
export function watchdogVerdict(
  run: WatchdogRun,
  meta: WorkerTiming | null | undefined,
  nowMs: number = Date.now(),
): WatchdogVerdict {
  if (run.externalWait === true) {
    const ageMs = nowMs - toMs(run.createdAt);
    return {
      expired: ageMs > WATCHDOG_EXTERNAL_WAIT_MS,
      limitMs: WATCHDOG_EXTERNAL_WAIT_MS,
      ageMs,
      externalWait: true,
    };
  }
  const limitMs =
    meta?.deadlineMs && meta.deadlineMs > 0
      ? meta.deadlineMs + WATCHDOG_GRACE_MS
      : WATCHDOG_DEFAULT_MS;
  const base = Math.max(toMs(run.createdAt), toMs(run.startedAt), toMs(run.deferredAt));
  const ageMs = nowMs - base;
  return { expired: ageMs > limitMs, limitMs, ageMs, externalWait: false };
}
