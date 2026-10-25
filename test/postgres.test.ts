import pg from "pg";
import { afterAll, describe, expect, expectTypeOf, it } from "vitest";
import { chain, registry } from "../examples/basic/pipeline";
import { createRuntime, InlineDispatcher } from "../src/index";
import {
  applySchema,
  type PgPoolLike,
  PostgresStore,
  SCHEMA_SQL,
  sessionLockKey,
} from "../src/store/postgres/index";
import { runStoreContract } from "./store-contract";

const url = process.env.DATABASE_URL;

describe("durastep/postgres without a database", () => {
  it("ships a schema with the three tables and the idempotency constraint", () => {
    for (const table of ["durastep_sessions", "durastep_runs", "durastep_counters"]) {
      expect(SCHEMA_SQL).toContain(`CREATE TABLE IF NOT EXISTS ${table}`);
    }
    expect(SCHEMA_SQL).toMatch(/UNIQUE \(tenant_id, idempotency_key\)/);
  });

  it("hashes session ids into stable signed 32-bit lock keys", () => {
    const key = sessionLockKey("3f0c5a52-1b7e-4b8e-9d55-2f6f0d1f9a10");
    expect(Number.isInteger(key)).toBe(true);
    expect(key).toBeGreaterThanOrEqual(-(2 ** 31));
    expect(key).toBeLessThan(2 ** 31);
    expect(sessionLockKey("3f0c5a52-1b7e-4b8e-9d55-2f6f0d1f9a10")).toBe(key);
    expect(sessionLockKey("another-session")).not.toBe(key);
  });

  it("accepts a pg.Pool as its pool type", () => {
    expectTypeOf<pg.Pool>().toMatchTypeOf<PgPoolLike>();
  });

  it("adds the status guard to guarded run updates", async () => {
    const calls: Array<{ text: string; values: unknown[] | undefined }> = [];
    const fake: PgPoolLike = {
      async query(text, values) {
        calls.push({ text, values });
        return { rows: [], rowCount: 0 };
      },
      async connect() {
        throw new Error("unused");
      },
    };
    const store = new PostgresStore({ pool: fake });
    expect(await store.updateRun("r1", { status: "FAILED" }, { ifStatus: ["RUNNING"] })).toBeNull();
    expect(calls[0]?.text).toMatch(/WHERE id = \$1 AND status = ANY\(\$3::text\[\]\)/);
    expect(calls[0]?.values).toEqual(["r1", "FAILED", ["RUNNING"]]);
  });
});

const pool = url ? new pg.Pool({ connectionString: url, max: 10 }) : undefined;
let schemaReady: Promise<void> | undefined;

runStoreContract(
  "PostgresStore",
  async () => {
    if (!pool) throw new Error("DATABASE_URL not set");
    schemaReady ??= applySchema(pool);
    await schemaReady;
    return new PostgresStore({ pool });
  },
  { skip: !url },
);

afterAll(async () => {
  await pool?.end();
});

describe.skipIf(!url)("PostgresStore end to end", () => {
  it("runs the example chain to COMPLETED", async () => {
    if (!pool) return;
    schemaReady ??= applySchema(pool);
    await schemaReady;
    const runtime = createRuntime({
      store: new PostgresStore({ pool }),
      dispatcher: new InlineDispatcher({ attempts: 3 }),
      registry,
    });
    const session = await runtime.startSession(chain, {
      tenantId: `pg-e2e-${Date.now()}`,
      inputs: { url: "https://example.com" },
    });
    expect(session.status).toBe("COMPLETED");
    expect(session.plan.find((s) => s.stepId === "enrich")?.status).toBe("SKIPPED");
  });
});
