# Roadmap

Directional, not a promise. Items move up when someone needs them in
production; open an issue describing your case.

## Next (0.2)

- **Cancel a session.** `runtime.cancelSession(id)`: mark it CANCELLED, mark
  PENDING runs CANCELLED, abort in-process attempts.
- **Complete a run from outside.** `runtime.completeRun(runId, output)` /
  `failRun` for steps that hand work to an external system and wait for a
  webhook (`externalWait`).
- **Per-worker concurrency** honoured by the dispatchers (the `concurrency`
  field is advisory today).
- **OpenTelemetry adapter** for `onRunFinished` (spans per attempt).

## Later

- Retry a failed session from the failed step (`runtime.resumeSession`).
- Hard quotas with reservation (check-and-increment in one statement) for
  teams that cannot tolerate overshoot.
- A retention helper for old sessions and runs.
- Additional stores (SQLite for single-node deployments) and dispatchers
  (pg-boss for Postgres-only shops).
- A read-only CLI to inspect sessions and runs.

## Not planned

- Long-lived workflows, timers, signals, deterministic replay. Use Temporal or
  Inngest.
- A hosted service or UI.
