# Guarantees

This page states what durastep guarantees, what it does not, and what happens
when a process dies at each point in a run's lifecycle. It assumes the
production setup: `PostgresStore` + `BullMQDispatcher`, with recovery
(`expireStuckRuns` + `recoverStuckSessions`) running periodically, which
`startBullMQWorker` does by default every 60 s.

## Summary

| property                                                                 | guaranteed?                                                                                                                                    |
| ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| At most one run row per (session, step)                                  | **Yes.** Enforced by the unique `(tenant_id, idempotency_key)` constraint, not by the lock.                                                    |
| A step is scheduled only after its dependencies are SUCCEEDED or SKIPPED | **Yes.** Decided from persisted run status, inside the session lock, behind a version check.                                                   |
| A scheduled run is executed at least once                                | **Yes**, given a dispatcher that delivers at least once (BullMQ does) and recovery running.                                                    |
| A run is executed at most once                                           | **No.** A worker can run more than once for the same run id. See "Duplicate execution".                                                        |
| Exactly-once side effects                                                | **No.** durastep cannot make your external calls exactly-once. Make them idempotent.                                                           |
| Every session eventually reaches COMPLETED or FAILED                     | **Yes**, given recovery runs and the store is reachable.                                                                                       |
| Quotas and cost ceilings are hard limits                                 | **No.** They are checked before an attempt and counted after success; concurrent runs can overshoot by the number of runs in flight.           |
| Run status writes are linearisable with the watchdog                     | **Partly.** The watchdog's write is guarded (`ifStatus`); the executor's success write is not, so a late success overwrites a watchdog FAILED. |

## The three layers that prevent double scheduling

1. **Session lock.** `advance()` holds `pg_try_advisory_lock(namespace,
   hash(sessionId))` (MemoryStore: an in-process set). A second caller does
   not wait; it returns `{ locked: true }`. In-process the runtime folds that
   call into a re-run of the active one; across processes the BullMQ worker
   retries the advance job.
2. **Idempotency key.** Every run row carries
   `sha256(tenant, worker, subject, inputs, session, step)`. The store's
   unique constraint makes `createRunIfAbsent` return the existing row to a
   second scheduler. This holds even if the lock is lost (PgBouncer in
   transaction mode, a lock bug, a hash collision).
3. **Plan version.** The plan is written with `WHERE version = expected`. A
   scheduler that read a stale plan gets `false`, dispatches nothing and
   re-advances. Only the writer whose plan persisted dispatches.

The test suite races `advance()` calls with and without the lock and asserts
that each step's run is created and dispatched exactly once.

## Duplicate execution (why at-least-once, not exactly-once)

A run's worker can execute more than once when:

- a BullMQ worker stalls (crash, OOM, event loop blocked past the job lock)
  and BullMQ re-delivers the job: `executeRun` accepts a RUNNING run and
  starts a new attempt;
- the outer deadline fires but the worker ignores `ctx.signal`: the attempt
  is marked failed and retried while the first attempt may still be running;
- the watchdog expires a run that was in fact alive (a worker slower than its
  declared `deadlineMs` + 60 s);
- a process dies after the worker returned but before the SUCCEEDED write.

So: treat `ctx.runId` (stable across attempts) as the idempotency key for
anything external (payments, emails, CRM writes, paid API calls), and honour
`ctx.signal`.

## Process dies at point X

The lifecycle of one step, and what happens if the process doing the work
dies right after each point. "Recovery" means the periodic
`expireStuckRuns` + `recoverStuckSessions`.

### Starting a session

**1. After `createSession`, before the first `advance`.**
The session is PENDING with no runs. `recoverStuckSessions` picks up PENDING
and EXECUTING sessions with no referenced run in flight once they are older
than `olderThanMs` (default 2 min) and advances them. Nothing is lost; the
start is delayed.

### Inside `advance` (holding the lock)

**2. After `createRunIfAbsent`, before `updatePlan`.**
A PENDING run row exists that the plan does not reference. Postgres releases
the advisory lock when the connection dies. Recovery ignores unreferenced
runs when deciding whether a session is in flight, so it re-advances; the
step is still PENDING in the plan, the idempotency key resolves to the
orphan row, and because it is PENDING it is adopted and dispatched. No
duplicate row, no duplicate work.

**3. After `updatePlan`, before dispatch.**
The plan says RUNNING and references a PENDING run that was never enqueued.
The watchdog expires it after the worker's limit (3 min, or `deadlineMs` +
60 s); because it never started, `expireStuckRuns` re-dispatches it instead
of failing it, and restarts its watchdog clock. If it is still undelivered
after `maxPendingMs` (default 30 min) it is failed, which fails the step.
The same path covers a dispatch that threw (Redis down at enqueue time).

### Inside `executeRun`

**4. After the RUNNING write, during the worker.**
BullMQ notices the stalled job and re-delivers it; `executeRun` starts a new
attempt on the RUNNING row (the worker runs twice; see above). If BullMQ
gives up on the stalled job, the watchdog marks the run FAILED after the
worker's limit and advances the session: the step fails, or is SKIPPED if
optional.

**5. After the worker returned, before the SUCCEEDED write.**
Same as 4. The result of the first execution is lost and the worker runs
again.

**6. After the SUCCEEDED write, before the advance notification.**
The run is SUCCEEDED, the session is not advanced. Usage counters and
telemetry for that run may be missing (they are best-effort and come after
the write). `recoverStuckSessions` sees a session with no run in flight and
re-advances it after `olderThanMs`. A failure to enqueue the notification
(Redis down) is handled the same way, and never flips the run back to
FAILED.

**7. After a retryable failure was marked, before the error reached the
queue.**
The run is FAILED with the `retryable: ` marker; the orchestrator treats it as
still in flight. BullMQ re-delivers the stalled job without counting the
attempt; `executeRun` re-runs a retry-pending FAILED run even without
`isRetry`. If no delivery ever comes, the watchdog finalises it as FAILED.

**8. After a permanent failure was marked, before the notification.**
The run is FAILED; recovery re-advances the session, which fails (or skips
the optional step and continues).

**9. After `DeferError` set the run back to PENDING, before
`moveToDelayed`.**
The run is PENDING with a fresh `deferredAt`. BullMQ re-delivers the stalled
job, or, if it is lost, the watchdog re-dispatches the never-started run
(point 3).

### Between steps

**10. An `advance` job is lost (Redis restarted without persistence).**
Recovery re-advances. A lost `run` job is point 3.

**11. Two processes advance the same session at once.**
One holds the lock; the other's advance job is retried with backoff. If its
retries run out, recovery covers it. Even without the lock, the idempotency
key and the version check prevent double scheduling.

## What recovery needs from you

- Run `startBullMQWorker` (recovery on by default) or call
  `runtime.expireStuckRuns()` and `runtime.recoverStuckSessions()` on a timer.
- Declare `deadlineMs` on workers that legitimately run longer than three
  minutes, otherwise the watchdog will expire them.
- Mark steps that wait on an external callback with
  `inputs: { externalWait: true }` to give them ten minutes from creation.
  (durastep v0.1 has no API for completing such a run from a webhook; see
  ROADMAP.md.)

## Limits of the in-memory setup

`MemoryStore` + `InlineDispatcher` keep everything in one process. They give
the same scheduling guarantees within that process, but nothing survives a
restart, and `InlineDispatcher` does not support deferral. Use them for tests,
scripts and development.
