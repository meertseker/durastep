import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createRegistry,
  createRuntime,
  defineChain,
  defineWorker,
  InlineDispatcher,
  MemoryStore,
  RetryableError,
} from "../src/index";

// Mirrors the hero snippet in README.md (plus a retry on 503) so it cannot silently rot.
describe("README hero snippet", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("runs to COMPLETED and retries a 503", async () => {
    let calls = 0;
    vi.stubGlobal("fetch", async () => {
      calls += 1;
      return calls === 1
        ? new Response("busy", { status: 503 })
        : new Response("<h1>Example Domain</h1>", { status: 200 });
    });

    const fetchPage = defineWorker({
      kind: "fetch-page",
      async run(ctx, input: { url: string }) {
        const res = await fetch(input.url, { signal: ctx.signal });
        if (res.status >= 500) throw new RetryableError(`HTTP ${res.status}`);
        return { text: await res.text() };
      },
    });
    const summarise = defineWorker({
      kind: "summarise",
      async run(ctx) {
        const page = ctx.upstream.fetch as { text: string };
        return { summary: page.text.slice(0, 200) };
      },
    });

    const registry = createRegistry([fetchPage, summarise]);
    const chain = defineChain(registry, [
      { stepId: "fetch", worker: "fetch-page" },
      { stepId: "summary", worker: "summarise", dependsOn: ["fetch"] },
    ]);

    const runtime = createRuntime({
      store: new MemoryStore(),
      dispatcher: new InlineDispatcher(),
      registry,
    });
    const session = await runtime.startSession(chain, {
      tenantId: "acme",
      inputs: { url: "https://example.com" },
    });
    expect(session.status).toBe("COMPLETED");
    const runs = await runtime.getRuns(session.id);
    expect(runs.find((r) => r.stepId === "fetch")?.attempt).toBe(2);
    expect(runs.find((r) => r.stepId === "summary")?.output).toEqual({
      summary: "<h1>Example Domain</h1>",
    });
  });
});
