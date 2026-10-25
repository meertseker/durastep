# Design

## Goal

Give TypeScript teams that already run Postgres and Redis the hard parts of
multi-step agent pipelines (typed workers, DAG chains, race-safe scheduling,
an error taxonomy, deadlines, quotas, recovery) as a library with no runtime
dependencies, instead of a platform to adopt.

## Shape

```
src/
  core/      types, errors, idempotency, registry, chain, deadline, quota,
             orchestrator (advance), executor (executeRun), runtime, logger
  store/     Store interface, MemoryStore, postgres/ (PostgresStore, schema.sql)
  queue/     Dispatcher interface, InlineDispatcher, bullmq/ (dispatcher + worker)
```

Three entry points: `durastep` (core + in-memory implementations),
`durastep/postgres` and `durastep/bullmq`. `pg`, `bullmq` and `ioredis` are
optional peer dependencies, loaded only by their adapters. The build uses
shared chunks so all entry points share one copy of the core; the error
classes also carry a string brand so `isRetryableError` and friends work
across duplicated copies (dual ESM/CJS installs).

## Hard rules

- Strict TypeScript, ESM + CJS, types shipped, Node >= 20.
- Zero runtime dependencies in the core.
- Domain-free vocabulary: tenant, subject, worker, chain, session, run, step.
- Everything runs locally with `MemoryStore` + `InlineDispatcher`.

## Core decisions in one paragraph each

**Idempotency key over locks** ([ADR 0002](./adr/0002-idempotency-key-over-locks.md)).
The advisory lock is an optimisation; the unique `(tenant_id, idempotency_key)`
constraint is the guarantee. A plan version check stops a writer with a stale
plan from persisting or dispatching.

**Error taxonomy** ([ADR 0003](./adr/0003-error-taxonomy.md)). Three classes
with distinct handling (retry, stop, re-queue later) plus a heuristic
classifier whose unknown default is "retry".

**Library, not platform** ([ADR 0001](./adr/0001-library-not-platform.md)).
No server, no UI, no workflow-as-code. You own the tables.

## What was left behind from the production version, and why

durastep was extracted from the production AI pipeline described in the
README. That code was coupled to Prisma, a specific LLM provider,
a scraping provider and a sales domain. These parts were not carried over:

| left behind                                                           | why                                                                                                                                                                                           |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Semantic memory (pgvector reads before a run, writes after)           | A storage concern of one application. Workers can read and write whatever memory they like; the executor should not own it. It also forced a `SUCCEEDED_NO_MEMORY` status, which is gone too. |
| Provider clients (LLM, embeddings, scraping actors) and their prompts | Domain code. durastep classifies their errors generically (timeouts, 429, 5xx, schema violations) and lets you add matchers.                                                                  |
| The "async actor + webhook finalize" execution mode                   | It depended on one provider's callback API. Its watchdog branch survives as the generic `externalWait` flag; a provider-neutral "complete this run from a webhook" API is on the roadmap.     |
| Plan tiers, minimum-plan gating, per-subject daily caps               | Product pricing. Replaced by the `QuotaPolicy` interface with two built-ins (`monthlyCount`, `costCeiling`) that take per-tenant limits as a function.                                        |
| Sentinel steps (inline DB side effects inside `advance`)              | They made the orchestrator call domain code while holding the lock. A small worker does the same job with retries and telemetry.                                                              |
| Stale-version guard (skip a run whose subject changed after queueing) | Domain-specific invalidation. Can be done in a worker by comparing a version passed in `inputs`.                                                                                              |
| Pre-run cost estimator                                                | Tied to provider price lists. Workers report actual cost with `ctx.reportCost`.                                                                                                               |
| A chain-telemetry table                                               | Replaced by the `onRunFinished` hook; write it wherever you keep metrics.                                                                                                                     |
| "Redis unavailable, run inline" fallback in the enqueue path          | Silent mode switches made production behaviour hard to reason about. Use `InlineDispatcher` explicitly, or let the enqueue fail and recovery re-dispatch.                                     |
| Multi-tenant Prisma `where` scoping                                   | The store interface takes `tenantId` where it matters (idempotency uniqueness, filters); access control is the application's job.                                                             |

## Decisions made during the build

The spec left these open or was ambiguous; this is what was chosen and why.

1. **`withSessionLock` is a try-lock everywhere.** The spec described the
   MemoryStore lock as a promise-chain mutex but gave it a `{ locked: true }`
   result. A waiting mutex would deadlock the inline dispatcher (an advance
   executing a run whose completion advances the same session), so both
   stores return `{ locked: true }` immediately, like `pg_try_advisory_lock`.
2. **Locked advances are not dropped.** Within one process the runtime
   coalesces a concurrent `advance` into a re-run of the active call (checked
   and cleared synchronously, so none is lost). Across processes the BullMQ
   worker retries an advance job that found the lock held.
3. **Runs are dispatched after the lock is released, and only by the writer
   whose plan persisted.** This keeps the inline dispatcher re-entrant and
   makes the version check meaningful: a conflicting writer dispatches
   nothing. The production code enqueued inside the lock, before persisting.
4. **After a hard failure, steps that never started end SKIPPED**, and the
   session is FAILED once running siblings finish. Without this, a session
   with a failed step and untouched downstream steps never became terminal.
5. **Retry-pending marker.** A retryable failure is stored as FAILED (as the
   spec says) with an error starting `retryable: `. The orchestrator treats
   such a run as in flight, so a sibling's advance does not fail the session
   while a retry is coming. On the final attempt the error reads
   `retries exhausted: ...` and the run is terminal.
6. **`finalAttempt` execute option.** Dispatchers pass it on the last
   attempt, so a run whose retries run out is finalised and its session
   advanced at once, instead of waiting for the watchdog.
7. **`executeRun` returns a discriminated result**
   (`succeeded | failed | deferred | not_runnable | missing`); the deferred
   variant carries `deferred: delayMs` as the spec describes. Retryable
   failures throw.
8. **`DeferError` without `canDefer` fails the run permanently**, as in
   production: the inline path has nothing that would re-deliver it.
9. **Additions to the data model:** `Session.inputs` (the spec's
   `startSession` takes inputs but the session shape had nowhere to keep
   them), `Run.subjectId` and `Run.deferredAt` (needed by the watchdog).
   Nullable fields are `T | null` rather than optional, matching SQL.
10. **Additions to the worker contract:** `ctx.upstream` (dependency outputs;
    without it a DAG step cannot use its inputs' results), `ctx.reportCost`
    (feeds `costUsdCents` / `costTokens` and the cost ceiling), `ctx.stepId`,
    and an optional `parseInput` whose failure is `PermanentError("input")`.
    `concurrency` is advisory metadata; neither built-in dispatcher enforces
    it in v0.1.
11. **Typed chains.** `defineChain(registry, steps)` type-checks worker kinds
    and step inputs against the registry and validates immediately;
    `defineChain(steps)` without a registry stays untyped unless given the
    worker union as a type argument.
12. **`QuotaPolicy.check` receives the counters** (`counters: CounterReader`)
    since the built-ins must read the store. The executor increments a run
    counter on every success and a cost counter when cost was reported.
    `allQuotas` combines policies. Limits are soft under concurrency.
13. **Guarded `updateRun`.** An optional `{ ifStatus }` guard (returning
    `null` when it does not hold) lets the watchdog avoid overwriting a run
    that finished between its read and its write. The executor's own writes
    stay unguarded, as in production, so a late success still lands.
14. **Canonicalisation honours `toJSON`.** The original collapsed `Date`
    values to `{}`, so two different dates produced the same key.
15. **`classifyError` returns a class** (`{ retryable }` or
    `{ retryable: false, reason }`); `toTaxonomyError` converts to an error
    instance. Defaults gained network error codes, HTTP status fields (408 /
    429 / 5xx retry; 400 / 401 / 403 / 404 / 422 permanent "input") and
    rate-limit phrases. Retryable rules are checked first.
16. **`externalWait` comes from the run's inputs** (`inputs.externalWait ===
    true`), since v0.1 has no other place a step can declare it.
17. **Recovery goes further than the original.** Recovery also picks up
    PENDING sessions (a crash between `createSession` and the first advance).
    It counts only runs the plan references when deciding whether a session
    is in flight, so it adopts orphan runs from a crashed advance. The
    watchdog re-dispatches never-started PENDING runs (up to `maxPendingMs`)
    instead of failing them, and a retry-pending run re-delivered without
    `isRetry` is re-executed. Each came out of writing
    [guarantees.md](./guarantees.md).
18. **Unknown worker at execution time** (registry changed under a running
    session) fails the run with `PermanentError("input")`.
19. **Postgres details:** the schema is shipped as `dist/postgres/schema.sql`
    and inlined as `SCHEMA_SQL` (a test keeps them identical; `tsx` cannot
    import `.sql`). The adapter takes any pool with `query`/`connect` and
    imports `pg` only in `PostgresStore.connect()`. The lock namespace is
    `0x44535450` ("DSTP"). Runs reference sessions with `ON DELETE CASCADE`.
20. **BullMQ details:** the queue connects lazily, job retention is bounded
    (`removeOnComplete: 1000`, `removeOnFail: 5000`), and the worker runs
    recovery every 60 s unless `recovery: false`.
21. **MemoryStore deep-copies** on every read and write (`structuredClone`) so
    it behaves like a database, not like shared mutable state.
22. **Cycle errors name the path** (`Cycle detected: a -> c -> b -> a`).
