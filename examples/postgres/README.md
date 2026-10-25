# Postgres store

`PostgresStore` keeps sessions, runs and counters in three tables in your
own database. A fourth table, `durastep_jobs`, is the optional Postgres job
queue (`PostgresDispatcher` + `startPostgresWorker`); a complete Postgres-only
setup is in [examples/postgres-only](../postgres-only/README.md).

It needs `pg` (an optional peer dependency):

```sh
npm install durastep pg
```

## 1. Apply the schema

The DDL ships with the package and is idempotent (`CREATE ... IF NOT EXISTS`).
Pick one:

```sh
# from a shell or your migration tool
psql "$DATABASE_URL" -f node_modules/durastep/dist/postgres/schema.sql
```

```ts
// from code, e.g. on boot
import pg from "pg";
import { applySchema } from "durastep/postgres";

await applySchema(new pg.Pool({ connectionString: process.env.DATABASE_URL }));
```

If you manage migrations with Prisma, Drizzle, Knex or plain SQL files, copy
`schema.sql` into a migration. The SQL text is also exported as `SCHEMA_SQL`.

Tables created:

| table               | purpose                                                                                    |
| ------------------- | ------------------------------------------------------------------------------------------ |
| `durastep_sessions` | one row per chain instance; `plan` is jsonb, `version` is the optimistic lock              |
| `durastep_runs`     | one row per worker execution; `UNIQUE (tenant_id, idempotency_key)` is the duplicate guard |
| `durastep_counters` | monthly usage counters read by the quota policies                                          |
| `durastep_jobs`     | the Postgres job queue; unused if you dispatch through BullMQ                              |

## 2. Swap the store

```ts
import pg from "pg";
import { createRuntime, InlineDispatcher } from "durastep";
import { PostgresStore } from "durastep/postgres";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

const runtime = createRuntime({
  store: new PostgresStore({ pool }),
  dispatcher: new InlineDispatcher(), // or PostgresDispatcher (../postgres-only), or BullMQDispatcher (../bullmq)
  registry,
});
```

`PostgresStore.connect(url, { applySchema: true })` is a shortcut that creates
its own pool (closed by `store.close()`).

## Notes

- **Locks.** `advance()` takes `pg_try_advisory_lock(0x44535450, hash(sessionId))`
  on a dedicated pooled connection and releases it in `finally`. Advisory locks
  are session-scoped, so they work behind PgBouncer only in session pooling
  mode. In transaction pooling mode the lock is unreliable; correctness then
  rests on the idempotency key and the plan version check (see
  docs/guarantees.md), which still prevent double scheduling.
- **Pool size.** Each concurrent `advance()` holds one extra connection for the
  duration of the lock. Size the pool for worker concurrency + advance
  concurrency.
- **Retention.** durastep never deletes session or run rows (the job queue deletes
  finished jobs; dead jobs stay). Delete old sessions with
  `DELETE FROM durastep_sessions WHERE status IN ('COMPLETED','FAILED','CANCELLED') AND updated_at < now() - interval '30 days'`
  (runs cascade).
- **Running the adapter tests.** `DATABASE_URL=postgres://... npm test` runs the
  shared store contract suite and an end-to-end chain against your database.
  It also runs the Postgres queue suite (`test/postgres-queue.test.ts`). They are
  skipped when `DATABASE_URL` is unset.
