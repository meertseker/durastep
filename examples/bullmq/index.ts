/**
 * The basic example chain on Redis (BullMQ), optionally with Postgres.
 *
 *   REDIS_URL=redis://localhost:6379 npx tsx examples/bullmq/index.ts
 *   REDIS_URL=... DATABASE_URL=postgres://... npx tsx examples/bullmq/index.ts
 *
 * For the demo the producer and the worker share one process. In
 * production they are usually separate: your API calls startSession, a
 * worker deployment calls startBullMQWorker. Both build the runtime the
 * same way.
 */
import IORedis from "ioredis";
import { createRuntime, MemoryStore, type Store } from "../../src/index";
import { BullMQDispatcher, startBullMQWorker } from "../../src/queue/bullmq/index";
import { PostgresStore } from "../../src/store/postgres/index";
import { chain, registry } from "../basic/pipeline";
import { recorder, renderTimeline } from "../basic/timeline";

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
  console.error("Set REDIS_URL, e.g. REDIS_URL=redis://localhost:6379");
  process.exit(1);
}

// BullMQ workers need maxRetriesPerRequest: null on their connection.
const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });

let store: Store;
let closeStore = async () => {};
if (process.env.DATABASE_URL) {
  const pg = await PostgresStore.connect(process.env.DATABASE_URL, { applySchema: true });
  store = pg;
  closeStore = () => pg.close();
} else {
  store = new MemoryStore();
}

const telemetry = recorder();
const dispatcher = new BullMQDispatcher({
  connection,
  queueName: "durastep-example",
  // Short backoff so the flaky step's retry is visible quickly.
  jobOptions: { backoff: { type: "fixed", delay: 200 } },
});
const runtime = createRuntime({ store, dispatcher, registry, telemetry });
const worker = startBullMQWorker(runtime, {
  connection,
  queueName: "durastep-example",
  concurrency: 10,
});

const started = await runtime.startSession(chain, {
  tenantId: "tenant-demo",
  inputs: { url: "https://example.com/about" },
});
console.log(`session ${started.id} started; waiting for the worker...`);

let session = started;
for (let i = 0; i < 200 && session.status !== "COMPLETED" && session.status !== "FAILED"; i++) {
  await new Promise((r) => setTimeout(r, 100));
  session = (await runtime.getSession(started.id)) ?? session;
}

console.log(renderTimeline(session, telemetry.attempts));

await worker.close();
await dispatcher.close();
await connection.quit();
await closeStore();
if (session.status !== "COMPLETED") process.exitCode = 1;
