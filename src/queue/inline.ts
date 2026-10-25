import { classifyError } from "../core/errors";
import { errorMessage } from "../core/logger";
import type { Dispatcher, RuntimeHandlers } from "./types";

/** Options for `InlineDispatcher`. */
export interface InlineDispatcherOptions {
  /** Total attempts per run, including the first. Default 3. */
  attempts?: number;
  /**
   * Delay before retry number `attempt` (1-based count of failed attempts).
   * A number is a fixed delay. Default 0.
   */
  backoffMs?: number | ((attempt: number) => number);
}

const sleep = (ms: number) =>
  ms > 0 ? new Promise<void>((resolve) => setTimeout(resolve, ms)) : Promise.resolve();

/**
 * Executes work in-process, immediately, and awaits it: `enqueueRun`
 * resolves after the run (and its retries) finished. Retries retryable
 * failures up to `attempts` times. Deferral is not supported (runs that
 * throw `DeferError` fail), because nothing would re-deliver them.
 *
 * For development, tests and scripts. Nothing survives a process exit; use
 * `BullMQDispatcher` in production.
 */
export class InlineDispatcher implements Dispatcher {
  private handlers: RuntimeHandlers | undefined;
  private readonly attempts: number;
  private readonly backoff: (attempt: number) => number;

  /** @param options - retry count and backoff. */
  constructor(options: InlineDispatcherOptions = {}) {
    this.attempts = Math.max(1, Math.floor(options.attempts ?? 3));
    const b = options.backoffMs ?? 0;
    this.backoff = typeof b === "function" ? b : () => b;
  }

  /** Receives the runtime's handlers. Called by `createRuntime`. */
  bind(handlers: RuntimeHandlers): void {
    this.handlers = handlers;
  }

  private bound(): RuntimeHandlers {
    if (!this.handlers) {
      throw new Error("InlineDispatcher is not bound; pass it to createRuntime first");
    }
    return this.handlers;
  }

  /** Executes the run now, retrying retryable failures. Never throws for run failures. */
  async enqueueRun(runId: string): Promise<void> {
    const h = this.bound();
    for (let attempt = 1; ; attempt++) {
      const finalAttempt = attempt >= this.attempts;
      try {
        await h.executeRun(runId, { isRetry: attempt > 1, canDefer: false, finalAttempt });
        return;
      } catch (err) {
        if (!finalAttempt && classifyError(err).retryable) {
          await sleep(this.backoff(attempt));
          continue;
        }
        h.logger.error("durastep.inline.run_failed", { runId, attempt, err: errorMessage(err) });
        return;
      }
    }
  }

  /** Advances the session now. Never throws. */
  async enqueueAdvance(sessionId: string): Promise<void> {
    const h = this.bound();
    try {
      await h.advance(sessionId);
    } catch (err) {
      h.logger.error("durastep.inline.advance_failed", { sessionId, err: errorMessage(err) });
    }
  }
}
