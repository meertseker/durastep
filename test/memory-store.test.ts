import { describe, expect, it } from "vitest";
import { MemoryStore } from "../src/store/memory";
import { runStoreContract } from "./store-contract";

runStoreContract("MemoryStore", async () => new MemoryStore());

describe("MemoryStore options", () => {
  it("uses the supplied id generator and clock", async () => {
    let n = 0;
    const fixed = new Date("2026-01-01T00:00:00Z");
    const store = new MemoryStore({ generateId: (kind) => `${kind}-${++n}`, now: () => fixed });
    const s = await store.createSession({ tenantId: "t", subjectId: null, inputs: {}, plan: [] });
    expect(s.id).toBe("session-1");
    expect(s.createdAt).toEqual(fixed);
  });
});
