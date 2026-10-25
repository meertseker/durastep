import { afterAll, beforeEach, describe, expect, it } from "vitest";
import type { PlanStep } from "../src/core/types";
import type { NewRun, Store } from "../src/store/types";
import { isLocked } from "../src/store/types";

const plan: PlanStep[] = [
  { stepId: "a", worker: "w", dependsOn: [], status: "PENDING" },
  { stepId: "b", worker: "w", dependsOn: ["a"], status: "PENDING", optional: true },
];

/**
 * Behaviour every Store implementation must provide. Shared by the
 * MemoryStore and PostgresStore suites.
 */
export function runStoreContract(
  name: string,
  makeStore: () => Promise<Store>,
  options: { skip?: boolean; teardown?: () => Promise<void> } = {},
): void {
  describe.skipIf(options.skip === true)(`${name} store contract`, () => {
    let store: Store;
    let tenantId: string;
    let n = 0;

    beforeEach(async () => {
      store = await makeStore();
      n += 1;
      tenantId = `tenant-${Date.now()}-${n}`;
    });

    if (options.teardown) afterAll(options.teardown);

    const newRun = (sessionId: string, key: string, overrides: Partial<NewRun> = {}): NewRun => ({
      sessionId,
      tenantId,
      subjectId: "subject-1",
      worker: "w",
      stepId: "a",
      inputs: { x: 1 },
      idempotencyKey: key,
      ...overrides,
    });

    it("creates sessions PENDING at version 0 and returns copies", async () => {
      const s = await store.createSession({ tenantId, subjectId: null, inputs: { q: 1 }, plan });
      expect(s).toMatchObject({ tenantId, subjectId: null, status: "PENDING", version: 0 });
      s.plan[0]!.status = "FAILED";
      const again = await store.getSession(s.id);
      expect(again?.plan[0]?.status).toBe("PENDING");
      expect(again?.inputs).toEqual({ q: 1 });
      expect(await store.getSession("00000000-0000-0000-0000-000000000000")).toBeNull();
    });

    it("updates the plan only at the expected version", async () => {
      const s = await store.createSession({ tenantId, subjectId: null, inputs: {}, plan });
      const next = plan.map((p) => ({ ...p, status: "RUNNING" as const }));
      expect(await store.updatePlan(s.id, next, 0, "EXECUTING")).toBe(true);
      expect(await store.updatePlan(s.id, next, 0, "FAILED")).toBe(false);
      const loaded = await store.getSession(s.id);
      expect(loaded).toMatchObject({ version: 1, status: "EXECUTING" });
      expect(loaded?.plan.every((p) => p.status === "RUNNING")).toBe(true);
    });

    it("deduplicates runs on (tenantId, idempotencyKey)", async () => {
      const s = await store.createSession({ tenantId, subjectId: null, inputs: {}, plan });
      const first = await store.createRunIfAbsent(newRun(s.id, "key-1"));
      const second = await store.createRunIfAbsent(newRun(s.id, "key-1", { inputs: { x: 2 } }));
      expect(first.created).toBe(true);
      expect(second.created).toBe(false);
      expect(second.run.id).toBe(first.run.id);
      expect(second.run.inputs).toEqual({ x: 1 });
      const otherTenant = await store.createRunIfAbsent(
        newRun(s.id, "key-1", { tenantId: `${tenantId}-other` }),
      );
      expect(otherTenant.created).toBe(true);
    });

    it("creates exactly one run under concurrent inserts of the same key", async () => {
      const s = await store.createSession({ tenantId, subjectId: null, inputs: {}, plan });
      const results = await Promise.all(
        Array.from({ length: 10 }, () => store.createRunIfAbsent(newRun(s.id, "race"))),
      );
      expect(results.filter((r) => r.created)).toHaveLength(1);
      expect(new Set(results.map((r) => r.run.id)).size).toBe(1);
    });

    it("initialises runs PENDING with zero attempts", async () => {
      const s = await store.createSession({ tenantId, subjectId: null, inputs: {}, plan });
      const { run } = await store.createRunIfAbsent(newRun(s.id, "init"));
      expect(run).toMatchObject({
        status: "PENDING",
        attempt: 0,
        deferCount: 0,
        output: null,
        error: null,
        startedAt: null,
        finishedAt: null,
      });
      expect(run.createdAt).toBeInstanceOf(Date);
    });

    it("patches runs and honours status guards", async () => {
      const s = await store.createSession({ tenantId, subjectId: null, inputs: {}, plan });
      const { run } = await store.createRunIfAbsent(newRun(s.id, "patch"));
      const started = new Date();
      const updated = await store.updateRun(run.id, {
        status: "RUNNING",
        startedAt: started,
        attempt: 1,
      });
      expect(updated).toMatchObject({ status: "RUNNING", attempt: 1 });
      expect(updated.startedAt?.getTime()).toBe(started.getTime());

      const guarded = await store.updateRun(
        run.id,
        { status: "FAILED" },
        { ifStatus: ["PENDING"] },
      );
      expect(guarded).toBeNull();
      expect((await store.getRun(run.id))?.status).toBe("RUNNING");

      const done = await store.updateRun(
        run.id,
        { status: "SUCCEEDED", output: { ok: true }, costUsdCents: 3 },
        { ifStatus: ["RUNNING"] },
      );
      expect(done).toMatchObject({ status: "SUCCEEDED", output: { ok: true }, costUsdCents: 3 });
    });

    it("throws when patching a missing run without a guard", async () => {
      await expect(
        store.updateRun("00000000-0000-0000-0000-000000000000", { status: "FAILED" }),
      ).rejects.toThrow();
    });

    it("lets one holder take the session lock at a time and releases it on error", async () => {
      const s = await store.createSession({ tenantId, subjectId: null, inputs: {}, plan });
      let release!: () => void;
      const gate = new Promise<void>((r) => {
        release = r;
      });
      const holder = store.withSessionLock(s.id, async () => {
        await gate;
        return "held";
      });
      // Give the holder time to acquire (Postgres needs a round trip).
      await new Promise((r) => setTimeout(r, 50));
      const contender = await store.withSessionLock(s.id, async () => "contender");
      expect(isLocked(contender)).toBe(true);
      release();
      expect(await holder).toBe("held");

      await expect(
        store.withSessionLock(s.id, async () => {
          throw new Error("boom");
        }),
      ).rejects.toThrow("boom");
      expect(await store.withSessionLock(s.id, async () => "free again")).toBe("free again");
    });

    it("lists runs by session and status", async () => {
      const s1 = await store.createSession({ tenantId, subjectId: null, inputs: {}, plan });
      const s2 = await store.createSession({ tenantId, subjectId: null, inputs: {}, plan });
      const a = await store.createRunIfAbsent(newRun(s1.id, "l1"));
      await store.createRunIfAbsent(newRun(s1.id, "l2", { stepId: "b" }));
      await store.createRunIfAbsent(newRun(s2.id, "l3"));
      await store.updateRun(a.run.id, { status: "RUNNING" });
      expect(await store.listRuns({ sessionId: s1.id })).toHaveLength(2);
      const running = await store.listRuns({ sessionId: s1.id, status: ["RUNNING"] });
      expect(running.map((r) => r.id)).toEqual([a.run.id]);
      expect(await store.listRuns({ tenantId, ids: [a.run.id] })).toHaveLength(1);
    });

    it("lists sessions by status and staleness", async () => {
      const s = await store.createSession({ tenantId, subjectId: null, inputs: {}, plan });
      await store.updatePlan(s.id, plan, 0, "EXECUTING");
      const future = new Date(Date.now() + 60_000);
      const past = new Date(Date.now() - 60_000);
      const stale = await store.listSessions({
        tenantId,
        status: ["EXECUTING"],
        updatedBefore: future,
      });
      expect(stale.map((x) => x.id)).toEqual([s.id]);
      expect(
        await store.listSessions({ tenantId, status: ["EXECUTING"], updatedBefore: past }),
      ).toEqual([]);
      expect(await store.listSessions({ tenantId, status: ["COMPLETED"] })).toEqual([]);
    });

    it("increments counters atomically", async () => {
      const key = `${tenantId}:count`;
      expect(await store.getCounter(key)).toBe(0);
      await Promise.all(Array.from({ length: 5 }, () => store.incrementCounter(key, 2)));
      expect(await store.getCounter(key)).toBe(10);
      expect(await store.incrementCounter(key, 1)).toBe(11);
    });
  });
}
