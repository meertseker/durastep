import type { AnyWorker, WorkerDefinition, WorkerKindOf } from "./types";

/**
 * Declares a worker. The kind literal and the input / output types are
 * inferred from the definition and flow into `createRegistry` and
 * `defineChain`.
 *
 * @example
 * const fetchPage = defineWorker({
 *   kind: "fetch-page",
 *   estimatedDurationMs: 5_000,
 *   async run(ctx, input: { url: string }) {
 *     const res = await fetch(input.url, { signal: ctx.signal });
 *     return { html: await res.text() };
 *   },
 * });
 */
export function defineWorker<const TKind extends string, TInput, TOutput>(
  definition: WorkerDefinition<TKind, TInput, TOutput>,
): WorkerDefinition<TKind, TInput, TOutput> {
  if (typeof definition.kind !== "string" || definition.kind.length === 0) {
    throw new TypeError("defineWorker: kind must be a non-empty string");
  }
  if (typeof definition.run !== "function") {
    throw new TypeError(`defineWorker(${definition.kind}): run must be a function`);
  }
  for (const field of ["estimatedDurationMs", "deadlineMs", "concurrency"] as const) {
    const value = definition[field];
    if (value !== undefined && (!Number.isFinite(value) || value <= 0)) {
      throw new TypeError(`defineWorker(${definition.kind}): ${field} must be a positive number`);
    }
  }
  return Object.freeze({ ...definition });
}

/** A typed, immutable lookup of worker definitions by kind. */
export interface Registry<W extends AnyWorker = AnyWorker> {
  /** The registered workers, in registration order. */
  readonly workers: readonly W[];
  /** Every registered kind, in registration order. */
  kinds(): WorkerKindOf<W>[];
  /** True when `kind` is registered. */
  has(kind: string): kind is WorkerKindOf<W>;
  /** Returns the worker for a known kind. */
  get<K extends WorkerKindOf<W>>(kind: K): Extract<W, { readonly kind: K }>;
  /** Returns the worker for `kind`, or undefined when it is not registered. */
  get(kind: string): W | undefined;
}

/** The union of worker definitions held by a registry. */
export type WorkersOf<R> = R extends Registry<infer W> ? W : never;

/**
 * Builds a registry from worker definitions. Duplicate kinds throw.
 *
 * @param workers - the worker definitions, typically from `defineWorker`.
 */
export function createRegistry<const T extends readonly AnyWorker[]>(
  workers: T,
): Registry<T[number]> {
  const byKind = new Map<string, T[number]>();
  for (const worker of workers) {
    if (byKind.has(worker.kind)) {
      throw new TypeError(`createRegistry: duplicate worker kind "${worker.kind}"`);
    }
    byKind.set(worker.kind, worker);
  }
  const list = Object.freeze([...workers]) as readonly T[number][];
  const registry = {
    workers: list,
    kinds: () => [...byKind.keys()] as WorkerKindOf<T[number]>[],
    has: (kind: string): kind is WorkerKindOf<T[number]> => byKind.has(kind),
    get: (kind: string) => byKind.get(kind),
  };
  return Object.freeze(registry) as Registry<T[number]>;
}
