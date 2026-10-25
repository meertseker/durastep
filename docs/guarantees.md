# Guarantees

This page states what durastep guarantees, what it does not, and what happens
when a process dies at each point in a run's lifecycle. It assumes the
production setup: `PostgresStore` with `PostgresDispatcher` (or
`BullMQDispatcher`), with recovery (`expireStuckRuns` +
`recoverStuckSessions`) running periodically, which `startPostgresWorker` and
`startBullMQWorker` do by default every 60 s.

The Postgres queue's own delivery rules are in
[The Postgres queue](#the-postgres-queue) below; the lifecycle walk-through
after it applies to both queues.

## Summary

| property                                                                 | guaranteed?                                                                                                                                    |
| ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| At most one run row per (session, step)                                  | **Yes.** Enforced by the unique `(tenant_id, idempotency_key)` constraint, not by the lock.                                                    |
| A step is scheduled only after its dependencies are SUCCEEDED or SKIPPED | **Yes.** Decided from persisted run status, inside the session lock, behind a version check.                                                   |
| A scheduled run is executed at least once                                | **Yes**, given a dispatcher that delivers at least once (the Postgres queue and BullMQ do) and recovery running.                               |
| A run is executed at most once                                           | **No.** A worker can run more than once for the same run id. See "Duplicate execution".                                                        |
| A SUCCEEDED run is executed again                                        | **No.** `executeRun` only executes PENDING, RUNNING and retry-pending FAILED runs; a re-delivered job for a SUCCEEDED run is a no-op.          |
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
- a Postgres queue worker dies or stalls past its lease (`leaseMs`) and
  another worker claims the job: the same thing, as the kill -9 demo shows;
- the outer deadline fires but the worker ignores `ctx.signal`: the attempt
  is marked failed and retried while the first attempt may still be running;
- the watchdog expires a run that was in fact alive (a worker slower than its
  declared `deadlineMs` + 60 s);
- a process dies after the worker returned but before the SUCCEEDED write.

So: treat `ctx.runId` (stable across attempts) as the idempotency key for
anything external (payments, emails, CRM writes, paid API calls), and honour
`ctx.signal`.

## The Postgres queue

`PostgresDispatcher` inserts rows into `durastep_jobs`; `startPostgresWorker`
claims and executes them. Its rules:

- **At-least-once delivery.** A job row is deleted only after its handler
  returned and the completion write succeeded. If the worker dies anywhere
  before that, the row stays `running` until its lease passes, then any worker
  claims it again.
- **Lease (visibility timeout).** A claim sets `locked_until = now() +
  leaseMs` (default 30 s). While the job runs, a heartbeat every `heartbeatMs`
  (default `leaseMs / 3`) extends it. A `running` row whose lease passed is
  claimable exactly like a due `queued` row. So a crashed worker's job is
  picked up within `leaseMs` of its last heartbeat. Lease times use the
  database clock (`now()`), never the worker's.
- **Claiming.** One statement selects due rows `FOR UPDATE SKIP LOCKED`,
  marks them `running` and commits. No transaction or row lock is held while
  user code runs. Two workers never hold a valid lease on the same row at the
  same time.
- **Attempts count at claim.** `attempts` is incremented when a job is
  claimed, so an attempt that crashed its worker counts. A job claimed again
  after its final attempt's lease expired (it kept killing its worker) is
  marked `dead` without running.
- **Retries.** A retryable error re-queues the job with the row's backoff
  (default exponential from 2 s, capped at 5 minutes) until `max_attempts`
  (default 3), then marks it `dead`. `executeRun` gets `isRetry: attempts > 1`
  and `finalAttempt: attempts >= max_attempts`, as with BullMQ; on the final
  attempt the executor finalises a retryable failure itself (run FAILED,
  session advanced), so a run job usually ends deleted, not dead.
- **Defer does not spend an attempt.** A `DeferError` re-queues the job after
  the requested delay with its attempt refunded.
- **Permanent errors end `dead`** with `last_error` kept, and are not
  retried. Dead rows stay in the table for inspection until you delete them.
- **Completion is guarded** by `locked_by` and the claimed attempt number. A
  worker that lost its lease (another worker re-claimed the job) cannot
  delete, re-queue or kill the new owner's row; it logs
  `durastep.pg.lease_lost` instead. Heartbeats are guarded the same way, so a
  late worker cannot extend a lease it no longer holds.
- **Coalescing.** At most one `queued` row exists per (queue, kind, ref): a
  duplicate enqueue of the same pending work merges into it and moves its due
  time to the earlier of the two. A job that is already `running` is not
  merged into: the new enqueue gets its own row, because the running handler
  may have read state before the new trigger. If a failing job would be
  re-queued while such a row is pending, it is dropped in favour of that row.
- **Notifications are hints.** `LISTEN durastep_jobs` only shortens latency;
  polling (`pollIntervalMs`) guarantees progress when notifications are lost
  or `listen: false`.
- **Transactional enqueue.** `dispatcher.withClient(client)` inserts through
  your client: the job exists, and the notification fires, only if your
  transaction commits.

### How it composes with the idempotency key and the plan version

The queue only decides *when* `executeRun` and `advance` are called, and it
may call them more than once. What makes repeats safe is the layer above:

- A re-delivered `run` job calls `executeRun` on the same run row. A run that
  already SUCCEEDED (or failed for good) is not executed again. A RUNNING run,
  left by a worker that died mid-step, is executed again as the next attempt.
- A re-delivered or duplicated `advance` job cannot create a second run for a
  step: the `(tenant_id, idempotency_key)` constraint returns the existing
  row, and a writer with a stale plan loses the version check and dispatches
  nothing.
- So a crash costs at most the interrupted attempt of the steps that were
  running, plus some no-op deliveries; finished steps stay finished.

### What the kill -9 demo shows

`npm run demo:kill` ([docs/kill-demo.md](./kill-demo.md)) kills a worker
process with SIGKILL while step 3 of 5 is running and starts a fresh worker.
It checks, from a ledger written by the steps and from the durastep tables,
that the session completes, that every step succeeded exactly once, that the
steps that had completed before the kill never started again, and that the
interrupted step started twice (attempt 1 killed, attempt 2 succeeded). In
short: **no completed step ran twice; the interrupted step was retried once.**

It covers one crash point (during a step) with one worker per process. The
other crash points are walked through below. It does not show exactly-once
side effects, because there are none: the killed attempt of step 3 had
already started its work.

### Limits

- **Side effects must be idempotent.** An interrupted attempt may have done
  part of its work (sent the email, charged the card) before dying, and the
  retry does it again unless you key it on `ctx.runId`.
- **A lease that is too short causes concurrent duplicates.** If a job runs
  without a heartbeat for longer than `leaseMs` (an event loop blocked by CPU
  work, a long GC pause, a network partition from the database), another
  worker claims it while the first is still executing. Both attempts run. The
  run status only partly guards this: an execution that starts after the run
  reached SUCCEEDED is a no-op, but two overlapping attempts both run to the
  end, and the later SUCCEEDED write wins. The stale worker's completion is
  rejected by the guard. Keep `leaseMs` well above your worst pause, and let
  heartbeats do the work.
- **A crash-looping run job ends `dead` while its run is RUNNING.** The
  watchdog in `expireStuckRuns` fails that run later and advances the session.
- **Throughput.** Each job costs a few small queries (claim, heartbeats,
  delete) and the table churns, so autovacuum matters. This suits pipeline
  steps that take seconds; for very high job rates of tiny jobs, BullMQ on
  Redis is the better transport.
- **PgBouncer.** `LISTEN` needs a session connection: use `listen: false` in
  transaction pooling mode (the session lock has the same caveat; see
  examples/postgres).

## Process dies at point X

The lifecycle of one step, and what happens if the process doing the work
dies right after each point. "Recovery" means the periodic
`expireStuckRuns` + `recoverStuckSessions`. The text uses BullMQ's terms;
with the Postgres queue, "BullMQ re-delivers the stalled job" reads "the
job's lease expires and another worker claims it", "Redis down" reads
"Postgres down" (in which case the store is down too and nothing progresses
until it is back), and a job whose retries ran out is kept as `dead`.

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

- Run `startPostgresWorker` or `startBullMQWorker` (recovery on by default) or call
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
