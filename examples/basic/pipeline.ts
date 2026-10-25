/**
 * The example pipeline, shared by examples/basic and examples/bullmq.
 *
 *   fetch ──┬── analyse-a ──┐
 *           ├── analyse-b ──┼── summarise
 *           └── enrich (optional) ┘
 *
 * analyse-b is flaky (fails once with a retryable error), enrich always
 * fails permanently and is optional, so the chain still completes.
 */
import {
  createRegistry,
  defineChain,
  defineWorker,
  PermanentError,
  RetryableError,
} from "../../src/index";

const sleep = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(t);
      reject(signal.reason);
    });
  });

export const fetchPage = defineWorker({
  kind: "fetch-page",
  estimatedDurationMs: 1_000,
  async run(ctx, input: { url: string }) {
    await sleep(40, ctx.signal);
    return { url: input.url, text: "durastep schedules each step exactly once per session." };
  },
});

export const analyse = defineWorker({
  kind: "analyse",
  estimatedDurationMs: 1_000,
  async run(ctx, input: { lens: "tone" | "facts"; flakyOnce?: boolean }) {
    await sleep(60, ctx.signal);
    if (input.flakyOnce && ctx.attempt === 1) {
      throw new RetryableError("HTTP 503 from model provider");
    }
    const page = ctx.upstream["fetch"] as { text: string };
    ctx.reportCost({ usdCents: 2, tokens: 900 });
    return { lens: input.lens, words: page.text.split(" ").length };
  },
});

export const enrich = defineWorker({
  kind: "enrich",
  async run(ctx) {
    await sleep(20, ctx.signal);
    throw new PermanentError("no public profile for this subject", "input");
  },
});

export const summarise = defineWorker({
  kind: "summarise",
  async run(ctx) {
    await sleep(30, ctx.signal);
    const parts = Object.entries(ctx.upstream)
      .filter(([, v]) => v !== undefined)
      .map(([k]) => k);
    ctx.reportCost({ usdCents: 3, tokens: 1500 });
    return { summary: `combined ${parts.join(" + ")}` };
  },
});

export const registry = createRegistry([fetchPage, analyse, enrich, summarise]);

export const chain = defineChain(registry, [
  { stepId: "fetch", worker: "fetch-page" },
  { stepId: "analyse-a", worker: "analyse", dependsOn: ["fetch"], inputs: { lens: "tone" } },
  {
    stepId: "analyse-b",
    worker: "analyse",
    dependsOn: ["fetch"],
    inputs: { lens: "facts", flakyOnce: true },
  },
  { stepId: "enrich", worker: "enrich", dependsOn: ["fetch"], optional: true },
  { stepId: "summarise", worker: "summarise", dependsOn: ["analyse-a", "analyse-b", "enrich"] },
]);
