# The kill -9 demo

`npm run demo:kill` (`scripts/kill-demo.ts`) shows what happens when a worker
process dies in the middle of a step: the job's lease expires, another worker
claims it, the interrupted step runs again, and the session completes. No
completed step runs twice.

## Run it

Any Postgres you can write to:

```sh
DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres npm run demo:kill
```

Or let the demo start a throwaway `postgres:16-alpine` container (random local
port, removed at the end; needs Docker):

```sh
npm run demo:kill -- --docker
```

Without `DATABASE_URL` and without `--docker` it prints both options and exits
1. It takes about 12 seconds and exits 0 only if every check holds. CI runs it
on every push.

## What it does

1. The parent process applies the schema, creates a `demo_ledger` table, and
   starts a session with five sequential steps (`fetch -> parse -> analyse ->
   score -> report`), each sleeping 1.5 s.
2. Every step inserts a `start` row into `demo_ledger` when it begins. The
   telemetry hook inserts a `success` row after the run is durably marked
   SUCCEEDED.
3. The parent spawns worker 1 (`worker` mode of the same script; lease 3 s,
   heartbeat 1 s). When `parse` has succeeded and `analyse` has been running
   for 0.75 s, the parent kills worker 1 with SIGKILL (TerminateProcess on
   Windows) and prints `kill -9 worker pid=...`.
4. The `analyse` job is still `running` in `durastep_jobs` with a lease that
   nobody renews. The parent starts worker 2, which claims the job once the
   lease expires, runs `analyse` as attempt 2, then `score` and `report`.
5. The parent prints a table from the ledger and `durastep_runs`, and these
   checks:
   - the session is COMPLETED;
   - every step succeeded exactly once;
   - no completed step ran twice: the steps that had succeeded before the kill
     never started again;
   - the interrupted step started twice: attempt 1 was killed, attempt 2
     succeeded on worker 2;
   - the steps after the kill started once each.

The retry of the interrupted step is the at-least-once delivery the queue
promises. It is safe for durastep's own state (one run row per step, guarded
by the idempotency key; a SUCCEEDED run is never executed again), and it is
why a step's external side effects should be idempotent, keyed on
`ctx.runId`. See [guarantees.md](./guarantees.md#the-postgres-queue).

## Real output

```
[parent] session 40138e50-3e5d-4bb6-909e-19b1f6de5044: fetch -> parse -> analyse -> score -> report (1500 ms each)
[worker 1] started, pid=22908, lease 3000 ms
[worker 1] fetch    attempt 1 started
[worker 1] fetch    attempt 1 succeeded (1511 ms)
[worker 1] parse    attempt 1 started
[worker 1] parse    attempt 1 succeeded (1520 ms)
[worker 1] analyse  attempt 1 started
[parent] "parse" has succeeded; "analyse" is mid-execution (attempt 1)
[parent] kill -9 worker pid=22908
[parent] its job is still 'running' (attempt 1); the lease expires in 2.2 s
[parent] starting worker 2
[worker 2] started, pid=17032, lease 3000 ms
[worker 2] analyse  attempt 2 started (retry: the previous attempt's lease expired)
[worker 2] analyse  attempt 2 succeeded (1516 ms)
[worker 2] score    attempt 1 started
[worker 2] score    attempt 1 succeeded (1520 ms)
[worker 2] report   attempt 1 started
[worker 2] report   attempt 1 succeeded (1520 ms)
[parent] session COMPLETED 7.1 s after the kill

step     started  succeeded  attempts started          run row
----------------------------------------------------------------------
fetch    1        1          1                         SUCCEEDED, attempt 1
parse    1        1          1                         SUCCEEDED, attempt 1
analyse  2        1          1 (killed), 2             SUCCEEDED, attempt 2
score    1        1          1                         SUCCEEDED, attempt 1
report   1        1          1                         SUCCEEDED, attempt 1

[ok]   session COMPLETED
[ok]   every step succeeded exactly once
[ok]   no completed step ran twice (fetch, parse had succeeded before the kill and never started again)
[ok]   the interrupted step (analyse) started twice: attempt 1 was killed, attempt 2 succeeded on worker 2
[ok]   steps after the kill started once each

The retry of "analyse" is at-least-once delivery: its job's lease expired and worker 2
claimed it. No completed step ran twice: each step is a run row behind an idempotency key,
and a SUCCEEDED run is never executed again. An interrupted step does run again, so its
external side effects must be idempotent (ctx.runId is stable across attempts).

total 11.3 s
```

## Record the GIF

`assets/kill-demo.gif` is rendered by [vhs](https://github.com/charmbracelet/vhs)
from `scripts/kill-demo.tape`. With vhs installed (it needs ttyd and ffmpeg):

```sh
DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres vhs scripts/kill-demo.tape
```

Without a local vhs, render it in Docker. Build an image once (the official
vhs image plus Node 22):

```sh
docker build -t durastep-vhs - <<'DOCKERFILE'
FROM node:22-bookworm-slim AS node
FROM ghcr.io/charmbracelet/vhs:latest
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm
DOCKERFILE
```

Then, from the repository root, with Postgres reachable from the container
(here: a container publishing port 5432 on the host):

```sh
docker run --rm --add-host=host.docker.internal:host-gateway \
  -v "$PWD:/src:ro" -v "$PWD/assets:/out" \
  -e DATABASE_URL=postgres://postgres:pw@host.docker.internal:5432/postgres \
  --entrypoint bash durastep-vhs -c '
    mkdir /work && cd /src &&
    tar --exclude=./node_modules --exclude=./dist --exclude=./.git -cf - . | (cd /work && tar xf -) &&
    cd /work && npm ci --no-audit --no-fund >/dev/null &&
    vhs scripts/kill-demo.tape && cp assets/kill-demo.gif /out/'
```

The copy into `/work` keeps the host's `node_modules` (which may hold native
binaries for another OS) out of the container. On Git Bash for Windows, prefix
the command with `MSYS_NO_PATHCONV=1` and use `C:/...` paths for the volumes.
