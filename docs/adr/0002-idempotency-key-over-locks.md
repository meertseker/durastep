# ADR 0002: Idempotency keys over locks

- Status: accepted
- Date: 2026-10-25

## Context

`advance(sessionId)` is called from many places: after every run, by
recovery timers, by the application. Two concurrent calls can both see a
PENDING step whose dependencies are satisfied. If both create a run and
enqueue it, the work runs twice, and with paid model and scraping APIs that
is real money.

The production system first relied on a Postgres advisory lock around
`advance`. Locks alone proved insufficient as the guarantee:

- advisory locks are session-scoped and silently ineffective behind a
  transaction-mode connection pooler;
- a try-lock that is held makes the second caller skip, and a skipped advance
  can strand a session;
- any bug in lock handling turns directly into duplicate work.

## Decision

The guarantee is a deterministic idempotency key plus a unique constraint;
the lock is an optimisation.

- Every run is created through `createRunIfAbsent` with
  `key = sha256(canonical(tenantId, worker, subjectId, inputs, sessionId, stepId))`
  (base64url, 32 chars, keys sorted recursively).
- The store enforces `UNIQUE (tenant_id, idempotency_key)` atomically; a
  second scheduler gets the existing row back.
- The plan is persisted with an optimistic version check. Only the writer
  whose plan persisted dispatches runs, so a scheduler with a stale view
  neither overwrites the plan nor enqueues anything.
- The per-session try-lock still serialises cooperating callers, which keeps
  the common path free of conflicts and retries.

`sessionId` and `stepId` are part of the key so that two sessions doing the
same work, or two steps running the same worker with the same inputs, stay
separate runs.

## Consequences

- Double scheduling is prevented even when the lock is lost; the test suite
  races `advance()` with the lock disabled to prove it.
- Scheduling becomes recoverable: a scheduler that died after creating a run
  but before persisting the plan leaves a row the next advance adopts.
- The key must not contain time-varying values; callers strip timestamps and
  trace ids from inputs.
- This prevents duplicate _scheduling_, not duplicate _execution_: a
  re-delivered job can still run a worker twice. Workers use `ctx.runId` as
  the idempotency key for external side effects (ADR 0003, guarantees.md).
