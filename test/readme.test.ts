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

// Mirrors the Postgres quick start in README.md (with its own queue name so it
// cannot pick up other suites' jobs).
describe.skipIf(!process.env.DATABASE_URL)("README Postgres quick start (DATABASE_URL)", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("runs to COMPLETED through PostgresDispatcher and startPostgresWorker", async () => {
    const { default: pg } = await import("pg");
    const { applySchema, PostgresDispatcher, PostgresStore, startPostgresWorker } = await import(
      "../src/store/postgres/index"
    );
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

    const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
    await applySchema(pool);
    const queue = `readme-${Date.now()}`;
    const runtime = createRuntime({
      store: new PostgresStore({ pool }),
      dispatcher: new PostgresDispatcher({ pool, queue, backoff: { type: "fixed", delayMs: 50 } }),
      registry,
    });
    const worker = startPostgresWorker({ pool, runtime, queue, pollIntervalMs: 50 });
    try {
      const session = await runtime.startSession(chain, {
        tenantId: `acme-${Date.now()}`,
        inputs: { url: "https://example.com" },
      });
      let status = session.status;
      for (let i = 0; i < 100 && status !== "COMPLETED" && status !== "FAILED"; i++) {
        await new Promise((r) => setTimeout(r, 50));
        status = (await runtime.getSession(session.id))?.status ?? status;
      }
      expect(status).toBe("COMPLETED");
      const runs = await runtime.getRuns(session.id);
      expect(runs.find((r) => r.stepId === "fetch")?.attempt).toBe(2);
      expect(runs.find((r) => r.stepId === "summary")?.output).toEqual({
        summary: "<h1>Example Domain</h1>",
      });
    } finally {
      await worker.close();
      await pool.end();
    }
  });
});
