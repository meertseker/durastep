import { noopLogger } from "../src/core/logger";
import type { PlanStep, RunStatus } from "../src/core/types";
import type { Dispatcher } from "../src/queue/types";
import { MemoryStore } from "../src/store/memory";
import type { Store } from "../src/store/types";

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** A dispatcher that only records what it was asked to do. */
export class RecordingDispatcher implements Dispatcher {
  readonly runs: string[] = [];
  readonly advances: string[] = [];
  async enqueueRun(runId: string): Promise<void> {
    this.runs.push(runId);
  }
  async enqueueAdvance(sessionId: string): Promise<void> {
    this.advances.push(sessionId);
  }
}

/**
 * Wraps a store so every call yields to the event loop first, forcing
 * concurrent callers to interleave between every read and write.
 * `lock: false` makes withSessionLock a pass-through, to exercise the
 * idempotency-key and version-check layers on their own.
 */
export function interleavingStore(inner: Store, options: { lock?: boolean } = {}): Store {
  return new Proxy(inner, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (typeof value !== "function") return value;
      if (prop === "withSessionLock" && options.lock === false) {
        return async (_id: string, fn: () => Promise<unknown>) => {
          await sleep(1);
          return fn();
        };
      }
      return async (...args: unknown[]) => {
        await sleep(1);
        return (value as (...a: unknown[]) => unknown).apply(target, args);
      };
    },
  });
}

/** Builds a plan from compact step specs. */
export function planOf(
  steps: Array<{ id: string; deps?: string[]; optional?: boolean; worker?: string }>,
): PlanStep[] {
  return steps.map((s) => ({
    stepId: s.id,
    worker: s.worker ?? "w",
    dependsOn: s.deps ?? [],
    ...(s.optional ? { optional: true } : {}),
    status: "PENDING" as const,
  }));
}

/** Sets the status of the run currently assigned to `stepId`. */
export async function settleStep(
  store: Store,
  sessionId: string,
  stepId: string,
  status: RunStatus,
  error: string | null = null,
): Promise<void> {
  const session = await store.getSession(sessionId);
  const runId = session?.plan.find((s) => s.stepId === stepId)?.runId;
  if (!runId) throw new Error(`step ${stepId} has no run`);
  await store.updateRun(runId, { status, error, finishedAt: new Date() });
}

export function newMemoryStore(): MemoryStore {
  return new MemoryStore();
}

export const quietLogger = noopLogger;
