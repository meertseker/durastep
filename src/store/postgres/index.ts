/**
 * PostgreSQL store and job queue for durastep. Import from `durastep/postgres`.
 *
 * `pg` is an optional peer dependency: this module never imports it at
 * load time. Pass your own `pg.Pool` (or anything with the same `query` /
 * `connect` shape), or call `PostgresStore.connect(url)`, which loads `pg`
 * on demand.
 */
import { randomUUID } from "node:crypto";
import type { PlanStep, Run, RunStatus, Session, SessionStatus } from "../../core/types";
import type {
  Locked,
  NewRun,
  NewSession,
  RunFilter,
  RunGuard,
  RunPatch,
  SessionFilter,
  Store,
} from "../types";
import { SCHEMA_SQL } from "./schema";

export { SCHEMA_SQL };

/** Result shape of a query, compatible with `pg`. */
export interface PgQueryResult {
  /** Returned rows. */
  rows: unknown[];
  /** Affected rows, when reported. */
  rowCount: number | null;
}

/** Anything that can run a parameterised query (a `pg.Pool` or `pg.PoolClient`). */
export interface PgQueryable {
  /** Runs `text` with positional `$n` parameters. */
  query(text: string, values?: unknown[]): Promise<PgQueryResult>;
}

/** A pooled client that must be released (a `pg.PoolClient`). */
export interface PgClientLike extends PgQueryable {
  /** Returns the client to the pool. */
  release(err?: Error | boolean): void;
}

/** A connection pool (a `pg.Pool`). */
export interface PgPoolLike extends PgQueryable {
  /** Checks out a dedicated client. */
  connect(): Promise<PgClientLike>;
  /** Closes the pool. */
  end?(): Promise<void>;
}

/** Options for `PostgresStore`. */
export interface PostgresStoreOptions {
  /** The pool to use. The store never closes it unless created by `connect`. */
  pool: PgPoolLike;
  /** Id generator. Defaults to `crypto.randomUUID`. */
  generateId?: (kind: "session" | "run") => string;
}

/**
 * First key of `pg_try_advisory_lock(int4, int4)` for session locks.
 * Arbitrary constant ("DSTP") chosen to avoid colliding with other users of
 * advisory locks in the same database.
 */
export const SESSION_LOCK_NAMESPACE = 0x44535450;

/**
 * Hashes a session id into a signed 32-bit integer (djb2-xor), the second
 * key of the advisory lock. A collision only makes two unrelated sessions
 * briefly serialise; the namespace keeps it within durastep.
 *
 * @param sessionId - the session id.
 */
export function sessionLockKey(sessionId: string): number {
  let h = 5381 | 0;
  for (let i = 0; i < sessionId.length; i++) {
    h = (Math.imul(33, h) ^ sessionId.charCodeAt(i)) | 0;
  }
  return h;
}

/**
 * Key of the transaction-level advisory lock `applySchema` holds, so that
 * processes booting at the same time do not race `CREATE ... IF NOT EXISTS`
 * (which can fail with a duplicate-key error under concurrency). The
 * single-bigint key space does not overlap the two-int4 session locks.
 */
export const SCHEMA_LOCK_KEY = 0x44535453;

/**
 * Applies `SCHEMA_SQL`. Idempotent and safe to call from several processes
 * at once; run it from a migration or on boot.
 *
 * @param db - a pool or client.
 */
export async function applySchema(db: PgQueryable): Promise<void> {
  // A multi-statement simple query runs as one implicit transaction, so the
  // xact lock is held until the whole schema is applied.
  await db.query(`SELECT pg_advisory_xact_lock(${SCHEMA_LOCK_KEY});\n${SCHEMA_SQL}`);
}

type Row = Record<string, unknown>;

const toDate = (v: unknown): Date => (v instanceof Date ? v : new Date(String(v)));
const toDateOrNull = (v: unknown): Date | null =>
  v === null || v === undefined ? null : toDate(v);
const toIntOrNull = (v: unknown): number | null =>
  v === null || v === undefined ? null : Number(v);
const json = (v: unknown): string => JSON.stringify(v === undefined ? null : v);

function sessionFromRow(r: Row): Session {
  return {
    id: String(r.id),
    tenantId: String(r.tenant_id),
    subjectId: r.subject_id === null ? null : String(r.subject_id),
    status: r.status as SessionStatus,
    inputs: (r.inputs ?? {}) as Record<string, unknown>,
    plan: (r.plan ?? []) as PlanStep[],
    version: Number(r.version),
    createdAt: toDate(r.created_at),
    updatedAt: toDate(r.updated_at),
  };
}

function runFromRow(r: Row): Run {
  return {
    id: String(r.id),
    sessionId: String(r.session_id),
    tenantId: String(r.tenant_id),
    subjectId: r.subject_id === null ? null : String(r.subject_id),
    worker: String(r.worker),
    stepId: String(r.step_id),
    status: r.status as RunStatus,
    inputs: (r.inputs ?? {}) as Record<string, unknown>,
    output: r.output ?? null,
    error: r.error === null ? null : String(r.error),
    idempotencyKey: String(r.idempotency_key),
    attempt: Number(r.attempt),
    deferCount: Number(r.defer_count),
    deferredAt: toDateOrNull(r.deferred_at),
    createdAt: toDate(r.created_at),
    startedAt: toDateOrNull(r.started_at),
    finishedAt: toDateOrNull(r.finished_at),
    costUsdCents: toIntOrNull(r.cost_usd_cents),
    costTokens: toIntOrNull(r.cost_tokens),
  };
}

const PATCH_COLUMNS: Record<keyof RunPatch, { column: string; cast?: string; encode?: boolean }> = {
  status: { column: "status" },
  output: { column: "output", cast: "jsonb", encode: true },
  error: { column: "error" },
  attempt: { column: "attempt" },
  deferCount: { column: "defer_count" },
  deferredAt: { column: "deferred_at" },
  startedAt: { column: "started_at" },
  finishedAt: { column: "finished_at" },
  costUsdCents: { column: "cost_usd_cents" },
  costTokens: { column: "cost_tokens" },
};

/**
 * A `Store` on PostgreSQL. Apply `SCHEMA_SQL` first.
 *
 * - Run deduplication uses the `UNIQUE (tenant_id, idempotency_key)`
 *   constraint with `INSERT ... ON CONFLICT DO NOTHING`.
 * - Plan writes use `UPDATE ... WHERE version = $expected`.
 * - Session locks use `pg_try_advisory_lock(namespace, hash(sessionId))`
 *   on a dedicated pooled client, released in `finally`. If the process
 *   dies, Postgres releases the lock with the connection.
 */
export class PostgresStore implements Store {
  private readonly pool: PgPoolLike;
  private readonly generateId: (kind: "session" | "run") => string;
  private readonly ownsPool: boolean;

  /** @param options - the pool and an optional id generator. */
  constructor(options: PostgresStoreOptions);
  /** @internal */
  constructor(options: PostgresStoreOptions, ownsPool: boolean);
  constructor(options: PostgresStoreOptions, ownsPool = false) {
    this.pool = options.pool;
    this.generateId = options.generateId ?? (() => randomUUID());
    this.ownsPool = ownsPool;
  }

  /**
   * Creates a store with its own `pg.Pool`, loading `pg` on demand.
   * `close()` ends that pool.
   *
   * @param connectionString - a Postgres URL.
   * @param options.applySchema - run `SCHEMA_SQL` before returning. Default false.
   */
  static async connect(
    connectionString: string,
    options: { applySchema?: boolean; max?: number } = {},
  ): Promise<PostgresStore> {
    const pg = (await import("pg")) as unknown as {
      default?: { Pool: new (cfg: object) => PgPoolLike };
      Pool?: new (cfg: object) => PgPoolLike;
    };
    const Pool = pg.Pool ?? pg.default?.Pool;
    if (!Pool) throw new Error("durastep/postgres: could not load pg.Pool");
    const pool = new Pool({ connectionString, max: options.max ?? 10 });
    if (options.applySchema) await applySchema(pool);
    return new PostgresStore({ pool }, true);
  }

  /** Ends the pool if this store created it (via `connect`). */
  async close(): Promise<void> {
    if (this.ownsPool) await this.pool.end?.();
  }

  async createSession(input: NewSession): Promise<Session> {
    const { rows } = await this.pool.query(
      `INSERT INTO durastep_sessions (id, tenant_id, subject_id, status, inputs, plan, version)
       VALUES ($1, $2, $3, 'PENDING', $4::jsonb, $5::jsonb, 0)
       RETURNING *`,
      [
        this.generateId("session"),
        input.tenantId,
        input.subjectId,
        json(input.inputs),
        json(input.plan),
      ],
    );
    return sessionFromRow(rows[0] as Row);
  }

  async getSession(id: string): Promise<Session | null> {
    const { rows } = await this.pool.query("SELECT * FROM durastep_sessions WHERE id = $1", [id]);
    return rows[0] ? sessionFromRow(rows[0] as Row) : null;
  }

  async updatePlan(
    id: string,
    plan: PlanStep[],
    expectedVersion: number,
    status: SessionStatus,
  ): Promise<boolean> {
    const { rowCount } = await this.pool.query(
      `UPDATE durastep_sessions
          SET plan = $2::jsonb, status = $3, version = version + 1, updated_at = now()
        WHERE id = $1 AND version = $4`,
      [id, json(plan), status, expectedVersion],
    );
    return rowCount === 1;
  }

  async withSessionLock<T>(id: string, fn: () => Promise<T>): Promise<T | Locked> {
    const client = await this.pool.connect();
    const key = sessionLockKey(id);
    let acquired = false;
    try {
      const { rows } = await client.query(
        "SELECT pg_try_advisory_lock($1::int4, $2::int4) AS got",
        [SESSION_LOCK_NAMESPACE, key],
      );
      acquired = (rows[0] as { got?: boolean } | undefined)?.got === true;
      if (!acquired) return { locked: true };
      return await fn();
    } finally {
      if (acquired) {
        try {
          await client.query("SELECT pg_advisory_unlock($1::int4, $2::int4)", [
            SESSION_LOCK_NAMESPACE,
            key,
          ]);
          client.release();
        } catch (err) {
          // Destroy the connection rather than return it with a lock held.
          client.release(err instanceof Error ? err : true);
        }
      } else {
        client.release();
      }
    }
  }

  async createRunIfAbsent(input: NewRun): Promise<{ run: Run; created: boolean }> {
    const inserted = await this.pool.query(
      `INSERT INTO durastep_runs
         (id, session_id, tenant_id, subject_id, worker, step_id, status, inputs, idempotency_key)
       VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', $7::jsonb, $8)
       ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
       RETURNING *`,
      [
        this.generateId("run"),
        input.sessionId,
        input.tenantId,
        input.subjectId,
        input.worker,
        input.stepId,
        json(input.inputs),
        input.idempotencyKey,
      ],
    );
    if (inserted.rows[0]) return { run: runFromRow(inserted.rows[0] as Row), created: true };
    const existing = await this.pool.query(
      "SELECT * FROM durastep_runs WHERE tenant_id = $1 AND idempotency_key = $2",
      [input.tenantId, input.idempotencyKey],
    );
    if (!existing.rows[0]) {
      throw new Error("durastep/postgres: idempotency conflict but no existing run was found");
    }
    return { run: runFromRow(existing.rows[0] as Row), created: false };
  }

  async getRun(id: string): Promise<Run | null> {
    const { rows } = await this.pool.query("SELECT * FROM durastep_runs WHERE id = $1", [id]);
    return rows[0] ? runFromRow(rows[0] as Row) : null;
  }

  updateRun(id: string, patch: RunPatch): Promise<Run>;
  updateRun(id: string, patch: RunPatch, guard: RunGuard): Promise<Run | null>;
  async updateRun(id: string, patch: RunPatch, guard?: RunGuard): Promise<Run | null> {
    const sets: string[] = [];
    const values: unknown[] = [id];
    for (const [key, value] of Object.entries(patch)) {
      const spec = PATCH_COLUMNS[key as keyof RunPatch];
      if (!spec || value === undefined) continue;
      values.push(spec.encode ? json(value) : value);
      sets.push(`${spec.column} = $${values.length}${spec.cast ? `::${spec.cast}` : ""}`);
    }
    let where = "id = $1";
    if (guard) {
      values.push([...guard.ifStatus]);
      where += ` AND status = ANY($${values.length}::text[])`;
    }
    const sql =
      sets.length > 0
        ? `UPDATE durastep_runs SET ${sets.join(", ")} WHERE ${where} RETURNING *`
        : `SELECT * FROM durastep_runs WHERE ${where}`;
    const { rows } = await this.pool.query(sql, values);
    if (rows[0]) return runFromRow(rows[0] as Row);
    if (guard) return null;
    throw new Error(`PostgresStore.updateRun: run ${id} not found`);
  }

  async listRuns(filter: RunFilter): Promise<Run[]> {
    const where: string[] = [];
    const values: unknown[] = [];
    const add = (clause: (n: number) => string, value: unknown) => {
      values.push(value);
      where.push(clause(values.length));
    };
    if (filter.sessionId !== undefined) add((n) => `session_id = $${n}`, filter.sessionId);
    if (filter.tenantId !== undefined) add((n) => `tenant_id = $${n}`, filter.tenantId);
    if (filter.status !== undefined) add((n) => `status = ANY($${n}::text[])`, [...filter.status]);
    if (filter.ids !== undefined) add((n) => `id = ANY($${n}::text[])`, [...filter.ids]);
    let sql = "SELECT * FROM durastep_runs";
    if (where.length > 0) sql += ` WHERE ${where.join(" AND ")}`;
    sql += " ORDER BY created_at, id";
    if (filter.limit !== undefined) {
      values.push(filter.limit);
      sql += ` LIMIT $${values.length}`;
    }
    const { rows } = await this.pool.query(sql, values);
    return rows.map((r) => runFromRow(r as Row));
  }

  async listSessions(filter: SessionFilter): Promise<Session[]> {
    const where: string[] = [];
    const values: unknown[] = [];
    const add = (clause: (n: number) => string, value: unknown) => {
      values.push(value);
      where.push(clause(values.length));
    };
    if (filter.tenantId !== undefined) add((n) => `tenant_id = $${n}`, filter.tenantId);
    if (filter.status !== undefined) add((n) => `status = ANY($${n}::text[])`, [...filter.status]);
    if (filter.updatedBefore !== undefined) add((n) => `updated_at < $${n}`, filter.updatedBefore);
    let sql = "SELECT * FROM durastep_sessions";
    if (where.length > 0) sql += ` WHERE ${where.join(" AND ")}`;
    sql += " ORDER BY updated_at, id";
    if (filter.limit !== undefined) {
      values.push(filter.limit);
      sql += ` LIMIT $${values.length}`;
    }
    const { rows } = await this.pool.query(sql, values);
    return rows.map((r) => sessionFromRow(r as Row));
  }

  async incrementCounter(key: string, by: number): Promise<number> {
    const { rows } = await this.pool.query(
      `INSERT INTO durastep_counters (key, value) VALUES ($1, $2)
       ON CONFLICT (key) DO UPDATE
         SET value = durastep_counters.value + EXCLUDED.value, updated_at = now()
       RETURNING value`,
      [key, by],
    );
    return Number((rows[0] as { value: unknown }).value);
  }

  async getCounter(key: string): Promise<number> {
    const { rows } = await this.pool.query("SELECT value FROM durastep_counters WHERE key = $1", [
      key,
    ]);
    return rows[0] ? Number((rows[0] as { value: unknown }).value) : 0;
  }
}

export * from "../../queue/postgres/index";
