# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). Until 1.0, minor versions may
contain breaking changes; they will be called out here.

## [Unreleased]

### Added

- `durastep/postgres`: a Postgres-only job queue. `PostgresDispatcher`
  inserts jobs into the new `durastep_jobs` table (with `pg_notify`,
  coalescing of duplicate pending work, and `withClient` for enqueueing
  inside your transaction); `startPostgresWorker` claims them with
  `FOR UPDATE SKIP LOCKED`, holds a lease with heartbeats, retries with
  backoff, keeps permanently failed jobs as `dead`, wakes on
  `LISTEN durastep_jobs` with polling as the fallback, runs recovery, and
  closes gracefully. Also exported: `backoffDelay`, `planCompletion`,
  `processClaimedJob`, `SCHEMA_LOCK_KEY`.
- `examples/postgres-only` and `npm run example:postgres`: the example chain
  on one `DATABASE_URL`.
- `npm run demo:kill` (`scripts/kill-demo.ts`): SIGKILLs a worker mid-step,
  finishes on a fresh worker, and checks that no completed step ran twice.
  `assets/kill-demo.gif` and `scripts/kill-demo.tape`.

### Changed

- `schema.sql` / `SCHEMA_SQL` add the `durastep_jobs` table and its indexes.
  Re-run `applySchema` (or the SQL) when upgrading; it is idempotent.
- `applySchema` holds a transaction-level advisory lock, so processes booting
  at the same time no longer race `CREATE ... IF NOT EXISTS`.
- The README leads with the Postgres-only setup; BullMQ is the optional second
  dispatcher.

## [0.1.0] - 2026-10-25

### Added

- `defineWorker`, `createRegistry`: typed worker registry.
- `defineChain`, `validateChain`: DAG chains with duplicate, unknown-worker,
  unknown-dependency and cycle checks; registry-typed step inputs.
- `createRuntime` with `startSession`, `advance`, `executeRun`,
  `recoverStuckSessions`, `expireStuckRuns`, `getSession`, `getRuns`.
- Orchestrator with session try-lock, idempotency keys, optimistic plan
  versions and terminal detection.
- Executor with quota checks, outer deadlines raced against an
  `AbortSignal`, defer / retry / permanent handling and best-effort
  post-success hooks.
- Error taxonomy: `RetryableError`, `PermanentError`, `DeferError`,
  `classifyError`, `toTaxonomyError`.
- `deadlineFor`, `watchdogVerdict`.
- Quota policies: `monthlyCount`, `costCeiling`, `allQuotas`.
- `MemoryStore`, `InlineDispatcher`.
- `durastep/postgres`: `PostgresStore`, `applySchema`, `SCHEMA_SQL`, shipped
  `schema.sql`.
- `durastep/bullmq`: `BullMQDispatcher`, `startBullMQWorker`, `processJob`.

[Unreleased]: https://github.com/meertseker/durastep/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/meertseker/durastep/releases/tag/v0.1.0
