-- durastep schema (v0.1). Idempotent: safe to run on every boot.
-- Tables are prefixed durastep_ so they can live in your application schema.

CREATE TABLE IF NOT EXISTS durastep_sessions (
  id          text        PRIMARY KEY,
  tenant_id   text        NOT NULL,
  subject_id  text,
  status      text        NOT NULL CHECK (status IN ('PENDING', 'EXECUTING', 'COMPLETED', 'FAILED', 'CANCELLED')),
  inputs      jsonb       NOT NULL DEFAULT '{}'::jsonb,
  plan        jsonb       NOT NULL,
  version     integer     NOT NULL DEFAULT 0,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- recoverStuckSessions scans by status and staleness.
CREATE INDEX IF NOT EXISTS durastep_sessions_status_updated_idx
  ON durastep_sessions (status, updated_at);
CREATE INDEX IF NOT EXISTS durastep_sessions_tenant_idx
  ON durastep_sessions (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS durastep_runs (
  id               text        PRIMARY KEY,
  session_id       text        NOT NULL REFERENCES durastep_sessions (id) ON DELETE CASCADE,
  tenant_id        text        NOT NULL,
  subject_id       text,
  worker           text        NOT NULL,
  step_id          text        NOT NULL,
  status           text        NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
  inputs           jsonb       NOT NULL DEFAULT '{}'::jsonb,
  output           jsonb,
  error            text,
  idempotency_key  text        NOT NULL,
  attempt          integer     NOT NULL DEFAULT 0,
  defer_count      integer     NOT NULL DEFAULT 0,
  deferred_at      timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,
  finished_at      timestamptz,
  cost_usd_cents   integer,
  cost_tokens      integer,
  -- The authoritative duplicate guard: one run per logical unit of work.
  CONSTRAINT durastep_runs_tenant_idempotency_key UNIQUE (tenant_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS durastep_runs_session_idx
  ON durastep_runs (session_id, created_at);
-- expireStuckRuns scans by status.
CREATE INDEX IF NOT EXISTS durastep_runs_status_created_idx
  ON durastep_runs (status, created_at);

CREATE TABLE IF NOT EXISTS durastep_counters (
  key         text        PRIMARY KEY,
  value       bigint      NOT NULL DEFAULT 0,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Postgres job queue (PostgresDispatcher + startPostgresWorker). Optional:
-- unused when you dispatch through BullMQ. Workers claim with
-- FOR UPDATE SKIP LOCKED and hold a lease (locked_until) while they run; a
-- running job whose lease expired is claimable again. Finished jobs are
-- deleted; jobs that ran out of attempts stay as 'dead'.
CREATE TABLE IF NOT EXISTS durastep_jobs (
  id              bigserial   PRIMARY KEY,
  queue           text        NOT NULL DEFAULT 'durastep',
  kind            text        NOT NULL CHECK (kind IN ('run', 'advance')),
  ref             text        NOT NULL,
  status          text        NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'dead')),
  run_at          timestamptz NOT NULL DEFAULT now(),
  attempts        integer     NOT NULL DEFAULT 0,
  max_attempts    integer     NOT NULL,
  backoff_type    text        NOT NULL DEFAULT 'exponential' CHECK (backoff_type IN ('fixed', 'exponential')),
  backoff_ms      integer     NOT NULL DEFAULT 2000,
  backoff_max_ms  integer,
  locked_by       text,
  locked_until    timestamptz,
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

-- The claim query scans by queue, status and due time.
CREATE INDEX IF NOT EXISTS durastep_jobs_claim_idx
  ON durastep_jobs (queue, status, run_at);
-- Duplicate enqueues of the same pending work coalesce into one row.
CREATE UNIQUE INDEX IF NOT EXISTS durastep_jobs_pending_key
  ON durastep_jobs (queue, kind, ref) WHERE status = 'queued';
