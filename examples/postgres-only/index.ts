/**
 * The basic example chain on one Postgres: PostgresStore keeps the state,
 * PostgresDispatcher + startPostgresWorker move the work. No Redis.
 *
 *   DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres npm run example:postgres
 *
 * For the demo the producer and the worker share one process. In
 * production they are usually separate: your API calls startSession, a
 * worker deployment calls startPostgresWorker. Both build the runtime the
 * same way, on the same database.
 */
import pg from "pg";
import { createRuntime } from "../../src/index";
import {
  applySchema,
  PostgresDispatcher,
  PostgresStore,
  startPostgresWorker,
} from "../../src/store/postgres/index";
import { chain, registry } from "../basic/pipeline";
import { recorder, renderTimeline } from "../basic/timeline";

const url = process.env.DATABASE_URL;
if (!url) {
  console.error(
    [
      "DATABASE_URL is not set. This example needs one Postgres, e.g.:",
      "",
      "  docker run -d --name durastep-pg -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16-alpine",
      "  DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres npm run example:postgres",
    ].join("\n"),
  );
  process.exit(1);
}

const pool = new pg.Pool({ connectionString: url, max: 10 });
await applySchema(pool); // idempotent: sessions, runs, counters and the job queue

const queue = "durastep-example";
const telemetry = recorder();
const runtime = createRuntime({
  store: new PostgresStore({ pool }),
  // Short fixed backoff so the flaky step's retry is visible quickly.
  dispatcher: new PostgresDispatcher({ pool, queue, backoff: { type: "fixed", delayMs: 200 } }),
  registry,
  telemetry,
});
const worker = startPostgresWorker({ pool, runtime, queue, concurrency: 4 });

const started = await runtime.startSession(chain, {
  tenantId: "tenant-demo",
  subjectId: "subject-42",
  inputs: { url: "https://example.com/about" },
});
console.log(`session ${started.id} started; the worker claims its jobs from durastep_jobs...\n`);

let session = started;
for (let i = 0; i < 200 && session.status !== "COMPLETED" && session.status !== "FAILED"; i++) {
  await new Promise((r) => setTimeout(r, 50));
  session = (await runtime.getSession(started.id)) ?? session;
}

console.log(renderTimeline(session, telemetry.attempts));

const runs = await runtime.getRuns(session.id);
const { rows } = await pool.query(
  "SELECT count(*)::int AS n FROM durastep_jobs WHERE queue = $1 AND status <> 'dead'",
  [queue],
);
console.log(`\nruns: ${runs.length}, jobs left in the queue: ${(rows[0] as { n: number }).n}`);

await worker.close();
await pool.end();
if (session.status !== "COMPLETED") process.exitCode = 1;
