import { describe, expect, it } from "vitest";
import { parse } from "../src/bql/parser.js";
import { compilePlan, likePrefix, likeRegex, type Cond } from "../src/bql/lower.js";
import { BqlError } from "../src/bql/errors.js";
import { golden } from "./fixtures.js";

function plan(bql: string, params: Record<string, unknown> = {}) {
  return compilePlan(parse(bql), params as Record<string, string | number | boolean | null>);
}

describe("BQL lower: conditions", () => {
  it("lowers = to path, != to compare =/=", () => {
    expect(plan("SELECT * FROM db WHERE a = 1").conds[0]).toEqual({
      t: "path",
      p: { tag: "b", comps: [{ key: "a" }] },
      v: 1,
    });
    expect(plan("SELECT * FROM db WHERE a != 1").conds[0]).toMatchObject({
      t: "compare",
      op: "=/=",
    });
  });

  it("lowers IS NULL to (path=null OR missing), IS NOT NULL to compare =/= null", () => {
    const isNull = plan("SELECT * FROM db WHERE a IS NULL").conds[0] as Cond;
    expect(isNull.t).toBe("or");
    expect(plan("SELECT * FROM db WHERE a IS NOT NULL").conds[0]).toMatchObject({
      t: "compare",
      op: "=/=",
      v: null,
    });
  });

  it("lowers LIKE prefix vs regex", () => {
    expect(plan("SELECT * FROM db WHERE a LIKE 'foo%'").conds[0]).toMatchObject({
      t: "prefix",
      stem: "foo",
    });
    expect((plan("SELECT * FROM db WHERE a LIKE '%foo'").conds[0] as Cond).t).toBe("regex");
  });

  it("dualizes NOT and expands BETWEEN", () => {
    // NOT (a = 1) -> compare =/= (a != 1)
    expect(plan("SELECT * FROM db WHERE NOT (a = 1)").conds[0]).toMatchObject({
      t: "compare",
      op: "=/=",
    });
    // BETWEEN -> two top-level conjuncts >= and =<
    const bt = plan("SELECT * FROM db WHERE a BETWEEN 1 AND 2").conds;
    expect(bt).toHaveLength(2);
    expect(bt[0]).toMatchObject({ t: "compare", op: ">=", v: 1 });
    expect(bt[1]).toMatchObject({ t: "compare", op: "=<", v: 2 });
    // NOT IN -> guarded and[exists, not in]
    const notIn = plan("SELECT * FROM db WHERE a NOT IN (1, 2)").conds[0] as Cond;
    expect(notIn.t).toBe("and");
  });

  it("mirrors a literal-op-path predicate", () => {
    // 3 < a  ->  a > 3
    expect(plan("SELECT * FROM db WHERE 3 < a").conds[0]).toMatchObject({
      t: "compare",
      op: ">",
      v: 3,
    });
  });

  it("substitutes params", () => {
    expect(plan("SELECT * FROM db WHERE a = $x", { x: "v" }).conds[0]).toMatchObject({
      t: "path",
      v: "v",
    });
  });

  it("tags unnest paths and names projections", () => {
    const p = plan("SELECT t AS x FROM db, UNNEST(tags) AS t WHERE kind = 'y'");
    expect(p.unnest?.alias).toBe("t");
    expect(p.project).toEqual([{ path: { tag: "u", comps: [] }, name: "x" }]);
  });
});

describe("BQL like helpers", () => {
  it("prefix detection", () => {
    expect(likePrefix("abc%")).toEqual({ kind: "prefix", stem: "abc" });
    expect(likePrefix("a_c%")).toEqual({ kind: "regex" });
    expect(likePrefix("%abc")).toEqual({ kind: "regex" });
    expect(likePrefix("a")).toEqual({ kind: "regex" });
  });
  it("anchored regex", () => {
    expect(likeRegex("a%c").source).toBe("^a.*c$");
    expect(likeRegex("a_c").source).toBe("^a.c$");
    expect(likeRegex("a.c").source).toBe("^a\\.c$");
  });
});

describe("BQL lower: compile errors match the golden tags", () => {
  it("produces the same error tag (or ok) as the Erlang for every fixture", () => {
    for (const c of golden.bql_errors ?? []) {
      if (c.error === null) {
        expect(() => plan(c.query)).not.toThrow();
      } else {
        try {
          plan(c.query);
          throw new Error(`expected ${c.query} to fail with ${c.error}`);
        } catch (e) {
          expect(e).toBeInstanceOf(BqlError);
          expect((e as BqlError).tag).toBe(c.error);
        }
      }
    }
  });
});
