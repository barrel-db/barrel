import { describe, expect, it } from "vitest";
import { compile, runLocal } from "../src/bql/query.js";
import { BqlServerOnlyError } from "../src/bql/errors.js";
import { Database } from "../src/db.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { golden } from "./fixtures.js";
import type { JsonObject } from "../src/json.js";
import type { LitValue } from "../src/bql/lower.js";

describe("BQL local run matches the Erlang rows (golden fixtures)", () => {
  it("reproduces every fixture query's ordered rows", () => {
    for (const c of golden.bql_local_run ?? []) {
      const plan = compile(c.query, { params: c.params as Record<string, LitValue> });
      const rows = runLocal(plan, c.docs as JsonObject[]);
      expect(rows, `query: ${c.query}`).toEqual(c.rows);
    }
  });
});

describe("BQL server-only queries", () => {
  it("refuses table functions and SUBSCRIBE locally", () => {
    expect(() => runLocal(compile("SELECT * FROM vector_top_k('q', k => 5) AS v"), [])).toThrow(
      BqlServerOnlyError,
    );
    expect(() => runLocal(compile("SELECT * FROM db SUBSCRIBE"), [])).toThrow(BqlServerOnlyError);
  });
});

describe("Database.query over the local cache", () => {
  it("queries the synced documents", async () => {
    const db = await Database.open("q1", { storage: new MemoryAdapter() });
    await db.put({ id: "a", kind: "fruit", name: "apple", price: 3 });
    await db.put({ id: "b", kind: "fruit", name: "pear", price: 5 });
    await db.put({ id: "c", kind: "tool", name: "hammer", price: 12 });

    const rows = await db.query(
      "SELECT name FROM db WHERE kind = 'fruit' ORDER BY price DESC",
    );
    expect(rows).toEqual([{ id: "b", name: "pear" }, { id: "a", name: "apple" }]);

    const withParam = await db.query("SELECT name FROM db WHERE price = $p", {
      params: { p: 12 },
    });
    expect(withParam).toEqual([{ id: "c", name: "hammer" }]);
    await db.close();
  });

  it("propagates BqlServerOnlyError for a table function", async () => {
    const db = await Database.open("q2", { storage: new MemoryAdapter() });
    await expect(db.query("SELECT * FROM bm25_top_k('x') AS v")).rejects.toBeInstanceOf(
      BqlServerOnlyError,
    );
    await db.close();
  });
});
