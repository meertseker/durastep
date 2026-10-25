# ADR 0001: A library, not a platform

- Status: accepted
- Date: 2026-10-25

## Context

Teams building multi-step LLM pipelines (crawl, analyse, generate) need
retries, idempotent scheduling, deadlines and recovery. The established
answers are platforms: Temporal (a cluster plus a history store), Inngest and
Trigger.dev (a hosted or self-hosted service). They are excellent at
long-lived workflows, timers, signals and replay, and they bring their own
operational footprint, their own storage and their own programming model.

The pipeline durastep comes from ran on Postgres and BullMQ, which the team
already operated. Its jobs finished in seconds to minutes. What hurt was not
a missing platform but a handful of hard details: double scheduling under
concurrency, retrying errors that could never succeed, runs hanging past any
reasonable time, and sessions stalling after a lost notification.

## Decision

durastep is a library that runs inside the user's processes, stores state in
tables the user owns, and moves work through a queue the user already runs.

- The core has zero runtime dependencies.
- Persistence and transport are two small interfaces (`Store`, `Dispatcher`)
  with in-memory and production implementations.
- There is no server, no UI and no workflow-as-code. A chain is data (a DAG),
  not a function that is replayed.

## Consequences

- Adoption is `npm install` plus three tables. Operating it is operating
  Postgres and Redis, which the target user already does.
- State is plain rows: query it with SQL, join it to your own tables, build
  your own dashboards.
- Not supported, by design: workflows that sleep for days, signals, child
  workflows, deterministic replay, a hosted UI. Users who need those should
  use Temporal or Inngest.
- Guarantees depend on the user's infrastructure (BullMQ's at-least-once
  delivery, Postgres' constraints), and are documented precisely in
  `docs/guarantees.md` rather than abstracted away.
