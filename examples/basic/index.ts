/**
 * In-memory example: no Postgres, no Redis.
 *
 *   npx tsx examples/basic/index.ts     (or: npm run example)
 */
import { createRuntime, InlineDispatcher, MemoryStore } from "../../src/index";
import { chain, registry } from "./pipeline";
import { recorder, renderTimeline } from "./timeline";

const telemetry = recorder();

const runtime = createRuntime({
  store: new MemoryStore(),
  dispatcher: new InlineDispatcher({ attempts: 3, backoffMs: 25 }),
  registry,
  telemetry,
});

const session = await runtime.startSession(chain, {
  tenantId: "tenant-demo",
  subjectId: "subject-42",
  inputs: { url: "https://example.com/about" },
});

console.log(renderTimeline(session, telemetry.attempts));

const runs = await runtime.getRuns(session.id);
const summary = runs.find((r) => r.stepId === "summarise");
const cents = runs.reduce((sum, r) => sum + (r.costUsdCents ?? 0), 0);
console.log(`\nsummarise output: ${JSON.stringify(summary?.output)}`);
console.log(`runs: ${runs.length}, reported spend: ${cents} US cents`);

if (session.status !== "COMPLETED") process.exitCode = 1;
