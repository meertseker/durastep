import { createHash } from "node:crypto";

/**
 * The tuple that identifies one logical unit of work. Two callers that try
 * to schedule the same tuple collide on the store's unique
 * `(tenantId, idempotencyKey)` constraint, and the second one re-uses the
 * first one's run instead of creating a duplicate.
 *
 * Time-varying fields (timestamps, the caller's user id, trace ids) must not
 * be part of `inputs`; strip them before scheduling.
 */
export interface IdempotencyKeyInput {
  /** Tenant the work belongs to. */
  tenantId: string;
  /** Worker kind. */
  worker: string;
  /** Subject the work is about, or null. */
  subjectId?: string | null;
  /** The inputs the worker will receive. Key order does not matter. */
  inputs?: Readonly<Record<string, unknown>>;
  /**
   * Session that schedules the work. Included so two sessions for the same
   * tenant + worker + subject each get their own run rather than the second
   * silently joining the first one's pending work.
   */
  sessionId?: string | null;
  /** Step inside the session. Two steps running the same worker stay distinct. */
  stepId?: string | null;
}

/**
 * Returns a structurally equal value whose object keys are sorted
 * recursively, so `JSON.stringify` is stable across key insertion orders.
 * Arrays keep their order (it is usually meaningful). Values with a
 * `toJSON` method (Date, for example) are replaced by its result first.
 *
 * @param value - any JSON-friendly value.
 */
export function canonicalize(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  const withJson = value as { toJSON?: () => unknown };
  if (typeof withJson.toJSON === "function") return canonicalize(withJson.toJSON());
  if (Array.isArray(value)) return value.map(canonicalize);
  const record = value as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(record).sort()) {
    out[key] = canonicalize(record[key]);
  }
  return out;
}

/**
 * Derives the deterministic idempotency key for a unit of work: sha256 over
 * the canonicalised tuple, base64url, first 32 characters (192 bits).
 *
 * @param input - the identifying tuple.
 * @returns a 32-character URL-safe string.
 */
export function computeIdempotencyKey(input: IdempotencyKeyInput): string {
  const canonical = {
    t: input.tenantId,
    w: input.worker,
    s: input.subjectId ?? "_",
    i: canonicalize(input.inputs ?? {}),
    ss: input.sessionId ?? "_",
    st: input.stepId ?? "_",
  };
  return createHash("sha256").update(JSON.stringify(canonical)).digest("base64url").slice(0, 32);
}
