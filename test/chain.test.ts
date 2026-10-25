import { describe, expect, expectTypeOf, it } from "vitest";
import { defineChain, validateChain } from "../src/core/chain";
import { ChainValidationError } from "../src/core/errors";
import { createRegistry, defineWorker } from "../src/core/registry";
import type { ChainStep } from "../src/core/types";

const fetchPage = defineWorker({
  kind: "fetch-page",
  async run(_ctx, input: { url: string; depth?: number }) {
    return { html: `<p>${input.url}</p>` };
  },
});
const analyse = defineWorker({
  kind: "analyse",
  async run(_ctx, input: { focus: "tone" | "facts" }) {
    return { score: input.focus.length };
  },
});
const registry = createRegistry([fetchPage, analyse]);

const step = (stepId: string, worker: string, dependsOn: string[] = []): ChainStep => ({
  stepId,
  worker,
  dependsOn,
});

describe("defineWorker", () => {
  it("rejects an empty kind and non-positive timings", () => {
    expect(() => defineWorker({ kind: "", run: async () => 1 })).toThrow(/kind/);
    expect(() => defineWorker({ kind: "x", deadlineMs: 0, run: async () => 1 })).toThrow(
      /deadlineMs/,
    );
  });

  it("infers the kind literal and the input type", () => {
    expectTypeOf(fetchPage.kind).toEqualTypeOf<"fetch-page">();
    expectTypeOf(fetchPage.run).parameter(1).toEqualTypeOf<{ url: string; depth?: number }>();
  });
});

describe("createRegistry", () => {
  it("looks up workers by kind and lists kinds in order", () => {
    expect(registry.kinds()).toEqual(["fetch-page", "analyse"]);
    expect(registry.get("analyse")).toBe(registry.workers[1]);
    expect(registry.get("missing")).toBeUndefined();
    expect(registry.has("fetch-page")).toBe(true);
  });

  it("types get() by kind", () => {
    expectTypeOf(registry.get("analyse").kind).toEqualTypeOf<"analyse">();
  });

  it("rejects duplicate kinds", () => {
    expect(() => createRegistry([fetchPage, fetchPage])).toThrow(/duplicate worker kind/);
  });
});

describe("defineChain", () => {
  it("normalises worker definitions to kinds and defaults dependsOn", () => {
    const chain = defineChain([{ stepId: "a", worker: fetchPage, inputs: { url: "u" } }]);
    expect(chain).toEqual([
      { stepId: "a", worker: "fetch-page", dependsOn: [], inputs: { url: "u" } },
    ]);
    expect(Object.isFrozen(chain)).toBe(true);
  });

  it("validates immediately when given a registry", () => {
    expect(() =>
      defineChain(registry, [{ stepId: "a", worker: "fetch-page", dependsOn: ["b"] }]),
    ).toThrow(ChainValidationError);
  });

  it("type-checks worker kinds and inputs against the registry", () => {
    defineChain(registry, [
      { stepId: "a", worker: "fetch-page", inputs: { url: "https://example.com" } },
      { stepId: "b", worker: "analyse", dependsOn: ["a"], inputs: { focus: "tone" } },
    ]);
    // @ts-expect-error unknown worker kind is a compile error
    expect(() => defineChain(registry, [{ stepId: "a", worker: "nope" }])).toThrow();
    // @ts-expect-error wrong input type for the worker is a compile error
    defineChain(registry, [{ stepId: "a", worker: "analyse", inputs: { focus: "price" } }]);
  });
});

describe("validateChain", () => {
  it("accepts a fan-out / fan-in DAG", () => {
    const chain = [
      step("fetch", "fetch-page"),
      step("a", "analyse", ["fetch"]),
      step("b", "analyse", ["fetch"]),
      step("join", "analyse", ["a", "b"]),
    ];
    expect(() => validateChain(chain, registry)).not.toThrow();
  });

  it("rejects an empty chain", () => {
    expect(() => validateChain([], registry)).toThrow(/at least one step/);
  });

  it("rejects duplicate step ids", () => {
    expect(() => validateChain([step("a", "analyse"), step("a", "analyse")], registry)).toThrow(
      'Duplicate stepId "a"',
    );
  });

  it("rejects unknown dependsOn references", () => {
    expect(() => validateChain([step("a", "analyse", ["ghost"])], registry)).toThrow(
      'Step "a" depends on unknown step "ghost"',
    );
  });

  it("rejects unknown workers", () => {
    expect(() => validateChain([step("a", "translate")], registry)).toThrow(
      'Step "a" uses unknown worker "translate"',
    );
  });

  it("rejects cycles and names the path", () => {
    const chain = [
      step("a", "analyse", ["c"]),
      step("b", "analyse", ["a"]),
      step("c", "analyse", ["b"]),
    ];
    expect(() => validateChain(chain, registry)).toThrow(/Cycle detected: a -> c -> b -> a/);
  });

  it("rejects a step that depends on itself", () => {
    expect(() => validateChain([step("a", "analyse", ["a"])], registry)).toThrow(/Cycle detected/);
  });

  it("rejects malformed steps", () => {
    const missingId = { worker: "analyse", dependsOn: [] } as unknown as ChainStep;
    const badDeps = { stepId: "a", worker: "analyse", dependsOn: "b" } as unknown as ChainStep;
    expect(() => validateChain([missingId], registry)).toThrow(/missing stepId/);
    expect(() => validateChain([badDeps], registry)).toThrow(/dependsOn must be an array/);
  });
});
