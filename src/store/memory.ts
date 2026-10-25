import { randomUUID } from "node:crypto";
import type { PlanStep, Run, Session, SessionStatus } from "../core/types";
import type {
  Locked,
  NewRun,
  NewSession,
  RunFilter,
  RunGuard,
  RunPatch,
  SessionFilter,
  Store,
} from "./types";

/** Options for `MemoryStore`. */
export interface MemoryStoreOptions {
  /** Id generator. Defaults to `crypto.randomUUID`. */
  generateId?: (kind: "session" | "run") => string;
  /** Clock. Defaults to `() => new Date()`. */
  now?: () => Date;
}

const clone = <T>(value: T): T => structuredClone(value);

/**
 * An in-process `Store` backed by Maps. Every read and write deep-copies,
 * so callers cannot mutate stored state by accident (as with a database).
 * The unique-key insert and the version check are synchronous inside a
 * single tick, so they are atomic with respect to concurrent async callers.
 *
 * Use it for tests, examples and local development. State is lost when the
 * process exits.
 */
export class MemoryStore implements Store {
  private readonly sessions = new Map<string, Session>();
  private readonly runs = new Map<string, Run>();
  private readonly runByKey = new Map<string, string>();
  private readonly counters = new Map<string, number>();
  private readonly locks = new Set<string>();
  private readonly generateId: (kind: "session" | "run") => string;
  private readonly now: () => Date;

  /** @param options - id generator and clock overrides. */
  constructor(options: MemoryStoreOptions = {}) {
    this.generateId = options.generateId ?? (() => randomUUID());
    this.now = options.now ?? (() => new Date());
  }

  async createSession(input: NewSession): Promise<Session> {
    const now = this.now();
    const session: Session = {
      id: this.generateId("session"),
      tenantId: input.tenantId,
      subjectId: input.subjectId,
      status: "PENDING",
      inputs: clone(input.inputs),
      plan: clone(input.plan),
      version: 0,
      createdAt: now,
      updatedAt: now,
    };
    this.sessions.set(session.id, session);
    return clone(session);
  }

  async getSession(id: string): Promise<Session | null> {
    const session = this.sessions.get(id);
    return session ? clone(session) : null;
  }

  async updatePlan(
    id: string,
    plan: PlanStep[],
    expectedVersion: number,
    status: SessionStatus,
  ): Promise<boolean> {
    const session = this.sessions.get(id);
    if (!session || session.version !== expectedVersion) return false;
    session.plan = clone(plan);
    session.status = status;
    session.version += 1;
    session.updatedAt = this.now();
    return true;
  }

  async withSessionLock<T>(id: string, fn: () => Promise<T>): Promise<T | Locked> {
    if (this.locks.has(id)) return { locked: true };
    this.locks.add(id);
    try {
      return await fn();
    } finally {
      this.locks.delete(id);
    }
  }

  async createRunIfAbsent(input: NewRun): Promise<{ run: Run; created: boolean }> {
    const indexKey = `${input.tenantId}\u0000${input.idempotencyKey}`;
    const existingId = this.runByKey.get(indexKey);
    const existing = existingId ? this.runs.get(existingId) : undefined;
    if (existing) return { run: clone(existing), created: false };
    const run: Run = {
      id: this.generateId("run"),
      sessionId: input.sessionId,
      tenantId: input.tenantId,
      subjectId: input.subjectId,
      worker: input.worker,
      stepId: input.stepId,
      status: "PENDING",
      inputs: clone(input.inputs),
      output: null,
      error: null,
      idempotencyKey: input.idempotencyKey,
      attempt: 0,
      deferCount: 0,
      deferredAt: null,
      createdAt: this.now(),
      startedAt: null,
      finishedAt: null,
      costUsdCents: null,
      costTokens: null,
    };
    this.runs.set(run.id, run);
    this.runByKey.set(indexKey, run.id);
    return { run: clone(run), created: true };
  }

  async getRun(id: string): Promise<Run | null> {
    const run = this.runs.get(id);
    return run ? clone(run) : null;
  }

  updateRun(id: string, patch: RunPatch): Promise<Run>;
  updateRun(id: string, patch: RunPatch, guard: RunGuard): Promise<Run | null>;
  async updateRun(id: string, patch: RunPatch, guard?: RunGuard): Promise<Run | null> {
    const run = this.runs.get(id);
    if (!run) {
      if (guard) return null;
      throw new Error(`MemoryStore.updateRun: run ${id} not found`);
    }
    if (guard && !guard.ifStatus.includes(run.status)) return null;
    Object.assign(run, clone(patch));
    return clone(run);
  }

  async listRuns(filter: RunFilter): Promise<Run[]> {
    const ids = filter.ids ? new Set(filter.ids) : undefined;
    const out = [...this.runs.values()]
      .filter(
        (r) =>
          (filter.sessionId === undefined || r.sessionId === filter.sessionId) &&
          (filter.tenantId === undefined || r.tenantId === filter.tenantId) &&
          (filter.status === undefined || filter.status.includes(r.status)) &&
          (ids === undefined || ids.has(r.id)),
      )
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());
    return clone(filter.limit === undefined ? out : out.slice(0, filter.limit));
  }

  async listSessions(filter: SessionFilter): Promise<Session[]> {
    const before = filter.updatedBefore?.getTime();
    const out = [...this.sessions.values()]
      .filter(
        (s) =>
          (filter.tenantId === undefined || s.tenantId === filter.tenantId) &&
          (filter.status === undefined || filter.status.includes(s.status)) &&
          (before === undefined || s.updatedAt.getTime() < before),
      )
      .sort((a, b) => a.updatedAt.getTime() - b.updatedAt.getTime());
    return clone(filter.limit === undefined ? out : out.slice(0, filter.limit));
  }

  async incrementCounter(key: string, by: number): Promise<number> {
    const next = (this.counters.get(key) ?? 0) + by;
    this.counters.set(key, next);
    return next;
  }

  async getCounter(key: string): Promise<number> {
    return this.counters.get(key) ?? 0;
  }
}
