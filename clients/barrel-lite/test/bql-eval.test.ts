import { describe, expect, it } from "vitest";
import {
  compareValues,
  evalCond,
  finalize,
  framesFor,
  getPath,
  termCompare,
  type Frame,
} from "../src/bql/eval.js";
import type { Cond, Plan, TaggedPath } from "../src/bql/lower.js";
import type { JsonObject } from "../src/json.js";

const base = (comps: TaggedPath["comps"]): TaggedPath => ({ tag: "b", comps });
const frame = (doc: JsonObject): Frame => ({ doc, elem: undefined });

describe("getPath", () => {
  it("resolves object keys and array indexes; else not found", () => {
    const v = { a: { b: [10, 20] }, n: null };
    expect(getPath(v, [{ key: "a" }, { key: "b" }, { index: 1 }])).toEqual({ ok: true, value: 20 });
    expect(getPath(v, [{ key: "n" }])).toEqual({ ok: true, value: null }); // stored null present
    expect(getPath(v, [{ key: "missing" }])).toEqual({ ok: false });
    expect(getPath(v, [{ key: "a" }, { index: 0 }])).toEqual({ ok: false }); // a is object not array
    expect(getPath({ a: [1] }, [{ key: "a" }, { index: 5 }])).toEqual({ ok: false });
  });
});

describe("compareValues", () => {
  it("exact == and =/=", () => {
    expect(compareValues(1, "==", 1)).toBe(true);
    expect(compareValues("a", "=/=", "b")).toBe(true);
  });
  it("ordered ops require same primitive kind", () => {
    expect(compareValues(2, ">", 1)).toBe(true);
    expect(compareValues("b", "<", "a")).toBe(false);
    expect(compareValues(1, "<", "a")).toBe(false); // cross-type -> false
    expect(compareValues(true, ">", false)).toBe(false); // non-num/str -> false
  });
});

describe("evalCond leaves", () => {
  const f = frame({ id: "x", kind: "fruit", tags: ["a", "b"], n: null });
  it("path exact, absent -> false", () => {
    expect(evalCond({ t: "path", p: base([{ key: "kind" }]), v: "fruit" }, f)).toBe(true);
    expect(evalCond({ t: "path", p: base([{ key: "nope" }]), v: "x" }, f)).toBe(false);
  });
  it("missing and exists", () => {
    expect(evalCond({ t: "missing", p: base([{ key: "nope" }]) }, f)).toBe(true);
    expect(evalCond({ t: "exists", p: base([{ key: "kind" }]) }, f)).toBe(true);
  });
  it("in, contains, prefix, regex", () => {
    expect(evalCond({ t: "in", p: base([{ key: "kind" }]), vs: ["fruit", "veg"] }, f)).toBe(true);
    expect(evalCond({ t: "contains", p: base([{ key: "tags" }]), v: "b" }, f)).toBe(true);
    expect(evalCond({ t: "prefix", p: base([{ key: "kind" }]), stem: "fru" }, f)).toBe(true);
    expect(evalCond({ t: "regex", p: base([{ key: "kind" }]), re: /^f.+t$/ }, f)).toBe(true);
  });
  it("and, or, not", () => {
    const c: Cond = {
      t: "and",
      cs: [
        { t: "exists", p: base([{ key: "kind" }]) },
        { t: "not", c: { t: "missing", p: base([{ key: "kind" }]) } },
      ],
    };
    expect(evalCond(c, f)).toBe(true);
  });
});

describe("termCompare", () => {
  it("orders number < bool/null < object < array < string", () => {
    expect(termCompare(1, "a")).toBeLessThan(0);
    expect(termCompare(null, "a")).toBeLessThan(0);
    expect(termCompare(1, null)).toBeLessThan(0);
    expect(termCompare(false, null)).toBeLessThan(0);
    expect(termCompare(null, true)).toBeLessThan(0);
    expect(termCompare("a", "b")).toBeLessThan(0);
    expect(termCompare(2, 1)).toBeGreaterThan(0);
  });
});

describe("framesFor and finalize", () => {
  it("unnest expands arrays, drops missing/non-array (inner join)", () => {
    expect(framesFor({ tags: [1, 2] }, { comps: [{ key: "tags" }], alias: "t" })).toHaveLength(2);
    expect(framesFor({}, { comps: [{ key: "tags" }], alias: "t" })).toHaveLength(0);
    expect(framesFor({ tags: 5 }, { comps: [{ key: "tags" }], alias: "t" })).toHaveLength(0);
  });

  it("orders, offsets, limits, and projects", () => {
    const frames: Frame[] = [
      { doc: { id: "a", n: 3 }, elem: undefined },
      { doc: { id: "b", n: 1 }, elem: undefined },
      { doc: { id: "c", n: 2 }, elem: undefined },
    ];
    const plan: Plan = {
      source: { kind: "collection", name: "db", loc: { line: 1, column: 1 } },
      subscribe: false,
      conds: [],
      order: { path: base([{ key: "n" }]), dir: "asc" },
      offset: 1,
      limit: 1,
      project: [{ path: base([{ key: "n" }]), name: "n" }],
      empty: false,
    };
    // sorted by n asc: b(1), c(2), a(3); offset 1 -> c,a; limit 1 -> c
    expect(finalize(frames, plan)).toEqual([{ id: "c", n: 2 }]);
  });

  it("star projection drops _rev and keeps id", () => {
    const plan: Plan = {
      source: { kind: "collection", name: "db", loc: { line: 1, column: 1 } },
      subscribe: false,
      conds: [],
      offset: 0,
      project: "star",
      empty: false,
    };
    const rows = finalize([{ doc: { id: "a", _rev: "1@x", v: 5 }, elem: undefined }], plan);
    expect(rows).toEqual([{ id: "a", v: 5 }]);
  });
});
