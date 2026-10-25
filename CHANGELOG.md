# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). Until 1.0, minor versions may
contain breaking changes; they will be called out here.

## [Unreleased]

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
