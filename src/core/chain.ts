import { ChainValidationError } from "./errors";
import type { Registry } from "./registry";
import type { AnyWorker, Chain, ChainStep, Inputs, WorkerInputOf, WorkerKindOf } from "./types";

type InputsFor<I> = [unknown] extends [I] ? Inputs : I extends object ? Partial<I> : Inputs;

/**
 * The shape of one step accepted by `defineChain`, typed against a union of
 * worker definitions. `worker` is the kind string or the definition itself;
 * `inputs` must be a partial of that worker's input (the session inputs
 * supply the rest).
 */
export type ChainStepFor<W extends AnyWorker> = W extends AnyWorker
  ? {
      /** Unique id within the chain. */
      readonly stepId: string;
      /** Worker kind, or the worker definition. */
      readonly worker: WorkerKindOf<W> | W;
      /** Step ids that must finish first. Defaults to none (a root step). */
      readonly dependsOn?: readonly string[];
      /** Continue the chain when this step fails; the step ends SKIPPED. */
      readonly optional?: boolean;
      /** Static inputs merged over the session inputs. */
      readonly inputs?: InputsFor<WorkerInputOf<W>>;
    }
  : never;

function normalise(step: ChainStepFor<AnyWorker>): ChainStep {
  const worker = typeof step.worker === "string" ? step.worker : step.worker.kind;
  const out: ChainStep = {
    stepId: step.stepId,
    worker,
    dependsOn: Object.freeze([...(step.dependsOn ?? [])]),
    ...(step.optional ? { optional: true } : {}),
    ...(step.inputs ? { inputs: Object.freeze({ ...(step.inputs as Inputs) }) } : {}),
  };
  return Object.freeze(out);
}

/**
 * Declares a chain: a DAG of steps. Steps with no `dependsOn` are roots and
 * fan out in parallel.
 *
 * Pass a registry first to get typed `worker` / `inputs` checking and
 * immediate validation; pass only the steps to validate later (at
 * `startSession`, or with `validateChain`).
 *
 * @example
 * const chain = defineChain(registry, [
 *   { stepId: "fetch", worker: "fetch-page" },
 *   { stepId: "summarise", worker: "summarise", dependsOn: ["fetch"] },
 * ]);
 */
export function defineChain<W extends AnyWorker>(
  registry: Registry<W>,
  steps: readonly ChainStepFor<NoInfer<W>>[],
): Chain;
/**
 * Declares a chain without a registry. Supply the worker union as the type
 * argument for typed inputs: `defineChain<typeof a | typeof b>([...])`.
 */
export function defineChain<W extends AnyWorker = AnyWorker>(
  steps: readonly ChainStepFor<W>[],
): Chain;
export function defineChain(
  registryOrSteps: Registry | readonly ChainStepFor<AnyWorker>[],
  maybeSteps?: readonly ChainStepFor<AnyWorker>[],
): Chain {
  const registry = Array.isArray(registryOrSteps) ? undefined : (registryOrSteps as Registry);
  const steps = (registry ? maybeSteps : registryOrSteps) as readonly ChainStepFor<AnyWorker>[];
  if (!Array.isArray(steps)) {
    throw new ChainValidationError("Chain must be an array of steps");
  }
  const chain: Chain = Object.freeze(
    steps.map((s) => (s && typeof s === "object" ? normalise(s) : (s as ChainStep))),
  );
  if (registry) validateChain(chain, registry);
  return chain;
}

/**
 * Validates a chain against a registry. Throws `ChainValidationError` on:
 * an empty chain, a malformed step, a duplicate `stepId`, an unknown worker,
 * a `dependsOn` that references no step, or a cycle (the message names the
 * cycle path).
 *
 * @param chain - the chain to check.
 * @param registry - the registry the chain will run against.
 */
export function validateChain(chain: Chain, registry: Pick<Registry, "has">): void {
  if (!Array.isArray(chain) || chain.length === 0) {
    throw new ChainValidationError("Chain must contain at least one step");
  }

  const seen = new Set<string>();
  for (const step of chain) {
    if (!step || typeof step !== "object") {
      throw new ChainValidationError("Step is not an object");
    }
    if (typeof step.stepId !== "string" || step.stepId.length === 0) {
      throw new ChainValidationError("Step is missing stepId");
    }
    if (seen.has(step.stepId)) {
      throw new ChainValidationError(`Duplicate stepId "${step.stepId}"`);
    }
    seen.add(step.stepId);
    if (typeof step.worker !== "string" || !registry.has(step.worker)) {
      throw new ChainValidationError(
        `Step "${step.stepId}" uses unknown worker "${String(step.worker)}"`,
      );
    }
    if (
      !Array.isArray(step.dependsOn) ||
      step.dependsOn.some((d: unknown) => typeof d !== "string")
    ) {
      throw new ChainValidationError(`Step "${step.stepId}".dependsOn must be an array of strings`);
    }
  }

  for (const step of chain) {
    for (const dep of step.dependsOn) {
      if (!seen.has(dep)) {
        throw new ChainValidationError(`Step "${step.stepId}" depends on unknown step "${dep}"`);
      }
    }
  }

  // Cycle detection: depth-first search with three colours. A grey node
  // reached again is on the current path, so the path from it is a cycle.
  const deps = new Map<string, readonly string[]>(chain.map((s) => [s.stepId, s.dependsOn]));
  const colour = new Map<string, "grey" | "black">();
  const path: string[] = [];
  const visit = (id: string): void => {
    const c = colour.get(id);
    if (c === "black") return;
    if (c === "grey") {
      const cycle = [...path.slice(path.indexOf(id)), id].join(" -> ");
      throw new ChainValidationError(`Cycle detected: ${cycle}`);
    }
    colour.set(id, "grey");
    path.push(id);
    for (const next of deps.get(id) ?? []) visit(next);
    path.pop();
    colour.set(id, "black");
  };
  for (const step of chain) visit(step.stepId);
}
