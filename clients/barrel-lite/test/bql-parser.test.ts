import { describe, expect, it } from "vitest";
import { tokenize } from "../src/bql/lexer.js";
import { parse } from "../src/bql/parser.js";
import { BqlError } from "../src/bql/errors.js";

describe("BQL lexer", () => {
  it("tokenizes keywords case-insensitively, idents case-sensitively", () => {
    const types = tokenize("Select Foo from db").map((t) => t.type);
    expect(types).toEqual(["select", "ident", "from", "ident", "eof"]);
    expect(tokenize("SELECT")[0]?.type).toBe("select");
  });

  it("reads strings, numbers, params, and operators", () => {
    const toks = tokenize("x = 'a''b' AND n >= -3.5 OR p = $q");
    const kinds = toks.map((t) => t.type);
    expect(kinds).toContain("string");
    expect(kinds).toContain(">=");
    expect(kinds).toContain("param");
    const str = toks.find((t) => t.type === "string");
    expect(str?.value).toBe("a'b"); // '' collapses
  });

  it("skips line comments", () => {
    expect(tokenize("SELECT -- comment\n * FROM db").map((t) => t.type)).toEqual([
      "select", "*", "from", "ident", "eof",
    ]);
  });
});

describe("BQL parser", () => {
  it("parses SELECT * FROM db", () => {
    const ast = parse("SELECT * FROM db");
    expect(ast.select).toBe("star");
    expect(ast.from.source).toMatchObject({ kind: "collection", name: "db" });
  });

  it("parses projections with AS and nested paths", () => {
    const ast = parse("SELECT a.b, c[0] AS first FROM db");
    expect(ast.select).not.toBe("star");
    const projs = ast.select as ReturnType<typeof parse>["select"] & object[];
    expect(Array.isArray(projs)).toBe(true);
    if (Array.isArray(projs)) {
      expect(projs[0]?.expr.comps).toEqual([{ key: "b" }]);
      expect(projs[1]?.as).toBe("first");
      expect(projs[1]?.expr.comps).toEqual([{ index: 0 }]);
    }
  });

  it("parses the WHERE expression layer with precedence", () => {
    const ast = parse("SELECT * FROM db WHERE a = 1 OR b = 2 AND c = 3");
    // OR at the top, AND nested on the right
    expect(ast.where?.kind).toBe("or");
    if (ast.where?.kind === "or") expect(ast.where.right.kind).toBe("and");
  });

  it("normalizes <> to != and builds negatives", () => {
    const ast = parse("SELECT * FROM db WHERE a <> -2");
    expect(ast.where).toMatchObject({ kind: "cmp", op: "!=" });
    if (ast.where?.kind === "cmp") {
      expect(ast.where.right).toMatchObject({ kind: "lit", value: -2 });
    }
  });

  it("parses IS NULL, IS NOT MISSING, IN, LIKE, BETWEEN, CONTAINS", () => {
    expect(parse("SELECT * FROM db WHERE a IS NULL").where?.kind).toBe("is_null");
    expect(parse("SELECT * FROM db WHERE a IS NOT MISSING").where).toMatchObject({
      kind: "is_missing",
      neg: true,
    });
    expect(parse("SELECT * FROM db WHERE a NOT IN (1, 2)").where).toMatchObject({
      kind: "in",
      neg: true,
    });
    expect(parse("SELECT * FROM db WHERE a LIKE 'x%'").where?.kind).toBe("like");
    expect(parse("SELECT * FROM db WHERE a BETWEEN 1 AND 2").where?.kind).toBe("between");
    expect(parse("SELECT * FROM db WHERE CONTAINS(tags, 'x')").where?.kind).toBe("contains");
  });

  it("parses a keyword after a dot as a path key", () => {
    const ast = parse("SELECT a.select.from FROM db");
    if (ast.select !== "star") {
      expect(ast.select[0]?.expr.comps).toEqual([{ key: "select" }, { key: "from" }]);
    }
  });

  it("parses UNNEST, ORDER BY, LIMIT, OFFSET, SUBSCRIBE", () => {
    const ast = parse(
      "SELECT t FROM db, UNNEST(tags) AS t WHERE k = 'x' ORDER BY n DESC LIMIT 5 OFFSET 2",
    );
    expect(ast.from.unnest?.alias).toBe("t");
    expect(ast.orderBy).toEqual([{ path: expect.anything(), dir: "desc" }]);
    expect(ast.limit).toBe(5);
    expect(ast.offset).toBe(2);
    const sub = parse("SELECT * FROM db SUBSCRIBE");
    expect(sub.subscribe).toBe(true);
  });

  it("parses a table function source with named args", () => {
    const ast = parse("SELECT * FROM vector_top_k('q', k => 5) AS v");
    expect(ast.from.source.kind).toBe("table_fn");
    if (ast.from.source.kind === "table_fn") {
      expect(ast.from.source.args).toHaveLength(2);
    }
  });

  it("throws BqlError on a syntax error", () => {
    expect(() => parse("SELCT bad")).toThrow(BqlError);
    expect(() => parse("SELECT * FROM")).toThrow(BqlError);
  });
});
