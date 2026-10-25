/**
 * kill -9 demo: a worker process is hard-killed in the middle of a step,
 * a fresh worker takes over, and the session completes with no completed
 * step running twice.
 *
 *   DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres npm run demo:kill
 *   npm run demo:kill -- --docker     # starts (and removes) a throwaway postgres:16-alpine container
 *
 * The parent process creates a session with five sequential steps (about
 * 1.5 s each) and spawns a worker child (`worker` mode of this script).
 * Every step writes a `start` row to demo_ledger when it begins; the
 * telemetry hook writes a `success` row after the run is durably marked
 * SUCCEEDED. Once step 2 has succeeded and step 3 is mid-execution, the
 * parent kills the worker with SIGKILL (TerminateProcess on Windows), then
 * spawns a second worker. The interrupted step's job is still `running` in
 * durastep_jobs; when its lease expires (3 s here) the new worker claims
 * it, retries the step, and finishes the session.
 *
 * The exit code is 0 only if every check holds.
 */
import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  createRegistry,
  createRuntime,
  defineChain,
  defineWorker,
  type Runtime,
  type Telemetry,
} from "../src/index";
import {
  applySchema,
  PostgresDispatcher,
  PostgresStore,
  startPostgresWorker,
} from "../src/store/postgres/index";

const QUEUE = "durastep-kill-demo";
const LEASE_MS = 3_000;
const STEP_MS = 1_500;
const STEPS = ["fetch", "parse", "analyse", "score", "report"] as const;
const KILL_STEP = 2; // index into STEPS: the third step

const LEDGER_SQL = `CREATE TABLE IF NOT EXISTS demo_ledger (
  id          bigserial   PRIMARY KEY,
  session_id  text        NOT NULL,
  step        text        NOT NULL,
  run_id      text        NOT NULL,
  attempt     integer     NOT NULL,
  event       text        NOT NULL CHECK (event IN ('start', 'success')),
  pid         integer     NOT NULL,
  at          timestamptz NOT NULL DEFAULT clock_timestamp()
)`;

const sleep = (ms: number, signal?: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(t);
      reject(signal.reason);
    });
  });

function buildRuntime(pool: pg.Pool, label: string): Runtime {
  const say = (msg: string) => console.log(`[${label}] ${msg}`);
  const step = defineWorker({
    kind: "demo-step",
    estimatedDurationMs: STEP_MS,
    async run(ctx, input: { name: string }) {
      await pool.query(
        "INSERT INTO demo_ledger (session_id, step, run_id, attempt, event, pid) VALUES ($1, $2, $3, $4, 'start', $5)",
        [ctx.sessionId, ctx.stepId, ctx.runId, ctx.attempt, process.pid],
      );
      const note = ctx.attempt > 1 ? " (retry: the previous attempt's lease expired)" : "";
      say(`${ctx.stepId.padEnd(8)} attempt ${ctx.attempt} started${note}`);
      await sleep(STEP_MS, ctx.signal);
      return { step: input.name, by: process.pid };
    },
  });
  const telemetry: Telemetry = {
    // Fires after the SUCCEEDED write, so a `success` row means the result
    // is durable, not just that the function returned.
    async onRunFinished(e) {
      if (e.status !== "SUCCEEDED") return;
      await pool.query(
        "INSERT INTO demo_ledger (session_id, step, run_id, attempt, event, pid) VALUES ($1, $2, $3, $4, 'success', $5)",
        [e.sessionId, e.stepId, e.runId, e.attempt, process.pid],
      );
      say(`${e.stepId.padEnd(8)} attempt ${e.attempt} succeeded (${e.durationMs} ms)`);
    },
  };
  return createRuntime({
    store: new PostgresStore({ pool }),
    dispatcher: new PostgresDispatcher({
      pool,
      queue: QUEUE,
      backoff: { type: "fixed", delayMs: 500 },
    }),
    registry: createRegistry([step]),
    telemetry,
  });
}

const chain = defineChain(
  STEPS.map((name, i) => ({
    stepId: name,
    worker: "demo-step",
    dependsOn: i === 0 ? [] : [STEPS[i - 1] as string],
    inputs: { name },
  })),
);

// ---------------------------------------------------------------- worker mode

async function worker(label: string): Promise<void> {
  const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 8 });
  pool.on("error", () => {}); // idle-client errors during shutdown
  const runtime = buildRuntime(pool, label);
  const handle = startPostgresWorker({
    pool,
    runtime,
    queue: QUEUE,
    concurrency: 4,
    leaseMs: LEASE_MS,
    heartbeatMs: 1_000,
    pollIntervalMs: 250,
    workerId: `${label}:pid-${process.pid}`,
  });
  console.log(`[${label}] started, pid=${process.pid}, lease ${LEASE_MS} ms`);
  const stop = async () => {
    await handle.close();
    await pool.end();
    process.exit(0);
  };
  process.on("message", (m) => {
    if (m === "stop") void stop();
  });
  process.on("disconnect", () => void stop()); // parent went away
}

// ---------------------------------------------------------------- parent mode

function log(msg: string) {
  console.log(`[parent] ${msg}`);
}

const color = process.stdout.isTTY === true && !process.env.NO_COLOR;
const paint = (code: number, text: string) => (color ? `\x1b[${code}m${text}\x1b[0m` : text);

function spawnWorker(label: string, url: string): ChildProcess {
  const script = fileURLToPath(import.meta.url);
  // Run node directly (not through npx / a shell), so SIGKILL hits the worker itself.
  const child = spawn(process.execPath, [...process.execArgv, script, "worker", label], {
    env: { ...process.env, DATABASE_URL: url },
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  for (const stream of [child.stdout, child.stderr]) {
    if (stream) createInterface({ input: stream }).on("line", (line) => console.log(line));
  }
  return child;
}

async function startDocker(): Promise<{ url: string; stop: () => void }> {
  const name = `durastep-kill-demo-${randomUUID().slice(0, 8)}`;
  log(`starting a throwaway postgres:16-alpine container (${name})...`);
  execFileSync(
    "docker",
    [
      "run",
      "-d",
      "--rm",
      "--name",
      name,
      "-e",
      "POSTGRES_PASSWORD=pw",
      "-p",
      "127.0.0.1::5432",
      "postgres:16-alpine",
    ],
    { stdio: "ignore" },
  );
  const stop = () => {
    try {
      execFileSync("docker", ["rm", "-f", name], { stdio: "ignore" });
    } catch {
      // already gone
    }
  };
  try {
    const port = execFileSync("docker", ["port", name, "5432/tcp"], { encoding: "utf8" })
      .trim()
      .split("\n")[0]
      ?.split(":")
      .pop();
    const url = `postgres://postgres:pw@127.0.0.1:${port}/postgres`;
    for (let i = 0; ; i++) {
      const client = new pg.Client({ connectionString: url });
      try {
        await client.connect();
        await client.query("SELECT 1");
        await client.end();
        return { url, stop };
      } catch {
        await client.end().catch(() => {});
        if (i > 120) throw new Error("postgres container did not become ready");
        await sleep(250);
      }
    }
  } catch (err) {
    stop();
    throw err;
  }
}

interface LedgerRow {
  step: string;
  attempt: number;
  event: "start" | "success";
  pid: number;
  at: Date;
}

async function parent(): Promise<number> {
  const t0 = Date.now();
  let url = process.env.DATABASE_URL;
  let stopDocker = () => {};
  if (!url) {
    if (!process.argv.includes("--docker")) {
      console.error(
        [
          "DATABASE_URL is not set. Point it at any Postgres you can write to, e.g.:",
          "",
          "  docker run -d --name durastep-pg -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16-alpine",
          "  DATABASE_URL=postgres://postgres:pw@localhost:5432/postgres npm run demo:kill",
          "",
          "or let the demo start and remove a throwaway container itself:",
          "",
          "  npm run demo:kill -- --docker",
        ].join("\n"),
      );
      return 1;
    }
    const docker = await startDocker();
    url = docker.url;
    stopDocker = docker.stop;
  }

  const pool = new pg.Pool({ connectionString: url, max: 4 });
  const children: ChildProcess[] = [];
  try {
    await applySchema(pool);
    await pool.query(LEDGER_SQL);
    // Leftovers from an aborted earlier run would compete for the workers.
    await pool.query("DELETE FROM durastep_jobs WHERE queue = $1", [QUEUE]);

    const runtime = buildRuntime(pool, "parent");
    const session = await runtime.startSession(chain, { tenantId: `kill-demo-${randomUUID()}` });
    log(`session ${session.id}: ${STEPS.join(" -> ")} (${STEP_MS} ms each)`);

    const ledger = async (): Promise<LedgerRow[]> =>
      (
        await pool.query(
          "SELECT step, attempt, event, pid, at FROM demo_ledger WHERE session_id = $1 ORDER BY id",
          [session.id],
        )
      ).rows as LedgerRow[];

    const first = spawnWorker("worker 1", url);
    children.push(first);

    // Wait until step 2 has succeeded and step 3 is well into its work.
    const killStep = STEPS[KILL_STEP] as string;
    const before = STEPS[KILL_STEP - 1] as string;
    let atKill: LedgerRow[] = [];
    for (let deadline = Date.now() + 30_000; ; ) {
      atKill = await ledger();
      const start = atKill.find((r) => r.step === killStep && r.event === "start");
      const prevDone = atKill.some((r) => r.step === before && r.event === "success");
      if (prevDone && start && Date.now() - start.at.getTime() >= STEP_MS / 2) break;
      if (Date.now() > deadline) throw new Error("timed out waiting for the step to start");
      await sleep(50);
    }
    log(`"${before}" has succeeded; "${killStep}" is mid-execution (attempt 1)`);
    log(paint(31, `kill -9 worker pid=${first.pid}`));
    const exited = new Promise((r) => first.once("exit", r));
    first.kill("SIGKILL");
    await exited;
    const killedAt = Date.now();
    atKill = await ledger();
    const succeededBeforeKill = new Set(
      atKill.filter((r) => r.event === "success").map((r) => r.step),
    );

    const { rows: jobRows } = await pool.query(
      `SELECT j.status, j.attempts,
              GREATEST(0, EXTRACT(EPOCH FROM (j.locked_until - now())) * 1000)::int AS lease_left_ms
         FROM durastep_jobs j JOIN durastep_runs r ON r.id = j.ref
        WHERE j.queue = $1 AND j.kind = 'run' AND r.session_id = $2 AND r.step_id = $3`,
      [QUEUE, session.id, killStep],
    );
    const job = jobRows[0] as
      | { status: string; attempts: number; lease_left_ms: number }
      | undefined;
    if (job) {
      log(
        `its job is still '${job.status}' (attempt ${job.attempts}); the lease expires in ${(job.lease_left_ms / 1000).toFixed(1)} s`,
      );
    }

    log("starting worker 2");
    const second = spawnWorker("worker 2", url);
    children.push(second);

    let status = "";
    for (let deadline = Date.now() + 60_000; ; ) {
      status = (await runtime.getSession(session.id))?.status ?? "";
      if (status === "COMPLETED" || status === "FAILED") break;
      if (Date.now() > deadline) throw new Error("timed out waiting for the session");
      await sleep(100);
    }
    log(`session ${status} ${((Date.now() - killedAt) / 1000).toFixed(1)} s after the kill`);
    second.send("stop");
    await new Promise((r) => second.once("exit", r));

    // ---- report
    const rows = await ledger();
    const runs = await runtime.getRuns(session.id);
    console.log("");
    console.log(
      `${"step".padEnd(9)}${"started".padEnd(9)}${"succeeded".padEnd(11)}${"attempts started".padEnd(26)}run row`,
    );
    console.log("-".repeat(70));
    const perStep = STEPS.map((step) => {
      const starts = rows.filter((r) => r.step === step && r.event === "start");
      const successes = rows.filter((r) => r.step === step && r.event === "success");
      const run = runs.find((r) => r.stepId === step);
      const attempts = starts
        .map((s) =>
          s.pid === first.pid && !successes.some((x) => x.attempt === s.attempt)
            ? `${s.attempt} (killed)`
            : `${s.attempt}`,
        )
        .join(", ");
      console.log(
        `${step.padEnd(9)}${String(starts.length).padEnd(9)}${String(successes.length).padEnd(11)}${attempts.padEnd(26)}${run?.status ?? "-"}, attempt ${run?.attempt ?? "-"}`,
      );
      return { step, starts, successes, run };
    });

    const interrupted = perStep.find((s) => s.step === killStep);
    const checks: Array<[boolean, string]> = [
      [status === "COMPLETED", "session COMPLETED"],
      [
        perStep.every((s) => s.successes.length === 1 && s.run?.status === "SUCCEEDED"),
        "every step succeeded exactly once",
      ],
      [
        perStep
          .filter((s) => succeededBeforeKill.has(s.step))
          .every((s) => s.starts.length === 1) && succeededBeforeKill.size === KILL_STEP,
        `no completed step ran twice (${[...succeededBeforeKill].join(", ")} had succeeded before the kill and never started again)`,
      ],
      [
        interrupted?.starts.map((s) => s.attempt).join(",") === "1,2" &&
          interrupted.successes[0]?.attempt === 2 &&
          interrupted.starts[0]?.pid === first.pid &&
          interrupted.starts[1]?.pid === second.pid,
        `the interrupted step (${killStep}) started twice: attempt 1 was killed, attempt 2 succeeded on worker 2`,
      ],
      [
        perStep.slice(KILL_STEP + 1).every((s) => s.starts.length === 1),
        "steps after the kill started once each",
      ],
    ];
    console.log("");
    for (const [ok, text] of checks) {
      console.log(`${ok ? paint(32, "[ok]  ") : paint(31, "[FAIL]")} ${text}`);
    }
    console.log("");
    for (const line of [
      `The retry of "${killStep}" is at-least-once delivery: its job's lease expired and worker 2`,
      "claimed it. No completed step ran twice: each step is a run row behind an idempotency key,",
      "and a SUCCEEDED run is never executed again. An interrupted step does run again, so its",
      "external side effects must be idempotent (ctx.runId is stable across attempts).",
    ]) {
      console.log(line);
    }
    console.log(`\ntotal ${((Date.now() - t0) / 1000).toFixed(1)} s`);
    return checks.every(([ok]) => ok) ? 0 : 1;
  } finally {
    for (const c of children) if (c.exitCode === null && c.signalCode === null) c.kill("SIGKILL");
    await pool.end();
    stopDocker();
  }
}

if (process.argv[2] === "worker") {
  await worker(process.argv[3] ?? "worker");
} else {
  try {
    process.exitCode = await parent();
  } catch (err) {
    console.error(`[parent] ${err instanceof Error ? err.message : String(err)}`);
    process.exitCode = 1;
  }
}
