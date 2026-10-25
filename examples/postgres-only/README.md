# Postgres only

The whole runtime on one database: `PostgresStore` keeps sessions, runs and
counters, `PostgresDispatcher` writes jobs to `durastep_jobs`, and
`startPostgresWorker` claims them with `FOR UPDATE SKIP LOCKED`. No Redis.

```sh
docker run -d --name durastep-pg -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16-alpine
DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres npm run example:postgres
```

It runs the same five-step chain as `examples/basic` (a fan-out, an optional
step that fails and is skipped, a flaky step that succeeds on its second
attempt) and prints the timeline:

```
session c4a9b689-2718-4481-b043-73daff1c72c7 started; the worker claims its jobs from durastep_jobs...

step        worker      try result     start    end  timeline
---------------------------------------------------------------------------------------------
fetch       fetch-page  1   ok           0ms   54ms  ####
enrich      enrich      1   failed      87ms  114ms        xx
analyse-b   analyse     1   retry       88ms  168ms        xxxxxx
analyse-a   analyse     1   ok          92ms  172ms        ######
analyse-b   analyse     2   ok         397ms  473ms                               ######
summarise   summarise   1   ok         502ms  535ms                                       ##

session COMPLETED
  fetch       SUCCEEDED
  analyse-a   SUCCEEDED
  analyse-b   SUCCEEDED
  enrich      SKIPPED
  summarise   SUCCEEDED

runs: 5, jobs left in the queue: 0
```

## The code that matters

```ts
import pg from "pg";
import { createRuntime } from "durastep";
import { applySchema, PostgresDispatcher, PostgresStore, startPostgresWorker } from "durastep/postgres";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
await applySchema(pool); // or apply schema.sql from your migrations

const runtime = createRuntime({
  store: new PostgresStore({ pool }),
  dispatcher: new PostgresDispatcher({ pool }), // 3 attempts, exponential backoff from 2 s
  registry,
});

// In your API:
await runtime.startSession(chain, { tenantId, inputs });

// In your worker process (or the same one):
const worker = startPostgresWorker({ pool, runtime, concurrency: 8 });
process.on("SIGTERM", () => void worker.close());
```

## Worker options

| option              | default            | meaning                                                                                   |
| ------------------- | ------------------ | ----------------------------------------------------------------------------------------- |
| `queue`             | `"durastep"`       | queue name; must match the dispatcher's                                                   |
| `concurrency`       | `4`                | jobs processed in parallel by this worker                                                 |
| `pollIntervalMs`    | `1000`             | idle poll interval (the fallback when no notification arrives)                            |
| `leaseMs`           | `30000`            | how long a claim is valid without a heartbeat; after that another worker may take the job |
| `heartbeatMs`       | `leaseMs / 3`      | how often running jobs extend their lease                                                 |
| `listen`            | `true`             | `LISTEN durastep_jobs` on one dedicated connection; set `false` behind PgBouncer (transaction mode) |
| `shutdownTimeoutMs` | `30000`            | how long `close()` waits for in-flight jobs                                               |
| `recovery`          | every 60 s         | `expireStuckRuns` + `recoverStuckSessions`; `false` to schedule them yourself             |
| `workerId`          | `host:pid:random`  | written to `locked_by`                                                                    |

## Pool size

A worker uses one connection for `LISTEN` (unless `listen: false`), one per
concurrent claim/heartbeat/completion query, and `advance()` holds one per
session lock. `max: concurrency + 4` is a comfortable start.

## Inspecting the queue

```sql
SELECT status, kind, count(*) FROM durastep_jobs GROUP BY 1, 2;
SELECT * FROM durastep_jobs WHERE status = 'dead' ORDER BY updated_at DESC LIMIT 20;
```

Finished jobs are deleted; dead jobs stay until you delete them. What the
queue guarantees, and what it does not, is in
[docs/guarantees.md](../../docs/guarantees.md#the-postgres-queue).
