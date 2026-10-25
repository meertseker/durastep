# BullMQ dispatcher

The same chain as `examples/basic`, but runs and advances travel through a
Redis-backed BullMQ queue and a worker executes them.

```sh
npm install durastep bullmq ioredis

# in this repo:
REDIS_URL=redis://localhost:6379 npx tsx examples/bullmq/index.ts
# with Postgres as the store too:
REDIS_URL=redis://localhost:6379 DATABASE_URL=postgres://... npx tsx examples/bullmq/index.ts
```

No Redis handy? `docker run --rm -p 6379:6379 redis:7-alpine`.

## Production shape (about 20 lines)

```ts
// runtime.ts: shared by your API and your worker deployment
import IORedis from "ioredis";
import pg from "pg";
import { createRuntime } from "durastep";
import { PostgresStore } from "durastep/postgres";
import { BullMQDispatcher } from "durastep/bullmq";
import { registry } from "./workers";

export const connection = new IORedis(process.env.REDIS_URL!, { maxRetriesPerRequest: null });
export const runtime = createRuntime({
  store: new PostgresStore({ pool: new pg.Pool({ connectionString: process.env.DATABASE_URL }) }),
  dispatcher: new BullMQDispatcher({ connection }),
  registry,
});

// api.ts
await runtime.startSession(chain, { tenantId, subjectId, inputs });

// worker.ts
import { startBullMQWorker } from "durastep/bullmq";
const worker = startBullMQWorker(runtime, { connection, concurrency: 20 });
process.on("SIGTERM", () => void worker.close());
```

## Job types and error mapping

One queue (default name `durastep`) carries two job types:

| job       | payload                          | handler                                                                |
| --------- | -------------------------------- | ---------------------------------------------------------------------- |
| `run`     | `{ type: "run", runId }`         | `runtime.executeRun(runId, { isRetry, canDefer: true, finalAttempt })` |
| `advance` | `{ type: "advance", sessionId }` | `runtime.advance(sessionId)`                                           |

| outcome of the handler                  | what the worker does                                                        |
| --------------------------------------- | --------------------------------------------------------------------------- |
| `{ outcome: "deferred", deferred: ms }` | `job.moveToDelayed(now + ms)` and `DelayedError`: no attempt spent          |
| permanent error (by `classifyError`)    | `UnrecoverableError`: BullMQ stops retrying                                 |
| retryable error                         | rethrown: BullMQ retries (default 3 attempts, exponential backoff from 2 s) |
| `advance` found the session locked      | `RetryableError`: retried so the advance is not lost                        |

`isRetry` is `job.attemptsMade > 0`; `finalAttempt` is true on the last
configured attempt, so a run whose retries run out ends FAILED and its session
is advanced instead of waiting for the watchdog.

## Recovery

`startBullMQWorker` runs `expireStuckRuns()` and `recoverStuckSessions()`
every 60 s by default (`recovery: { intervalMs }`, or `recovery: false` to
schedule them yourself). Both are idempotent and safe to run in every worker
process at once.

## Not covered by CI

The adapter is type-checked and its job processor is unit-tested with fake
jobs. The end-to-end test against a real Redis runs only when `REDIS_URL` is
set: `REDIS_URL=redis://localhost:6379 npm test`.
