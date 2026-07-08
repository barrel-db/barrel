import { beforeEach, describe, expect, it } from "vitest";
import { Database } from "../src/db.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { FakeServer } from "./fake-server.js";
import { cosine } from "../src/vectors/cosine.js";

const URL = "http://fake";
const DB = "sp_test";

describe("Database searchLocal", () => {
  let server: FakeServer;

  beforeEach(() => {
    server = new FakeServer();
  });

  async function openDb(name: string) {
    return Database.open(name, {
      remote: { url: URL, db: DB, fetch: server.fetch },
      storage: new MemoryAdapter(),
    });
  }

  it("ranks by descending cosine, matching a hand computation", async () => {
    const db = await openDb("s1");
    const vecs: Record<string, number[]> = {
      a: [1, 0, 0],
      b: [0.9, 0.1, 0],
      c: [0, 1, 0],
      d: [-1, 0, 0],
    };
    for (const [id, v] of Object.entries(vecs)) {
      await db.put({ id, kind: id === "d" ? "neg" : "pos" });
      server.seedEmbedding(id, v);
    }
    await db.syncEmbeddings();

    const query = [1, 0, 0];
    const hits = await db.searchLocal(query, { k: 3 });
    expect(hits.map((h) => h.id)).toEqual(["a", "b", "c"]);

    // scores equal cosine of the stored (f32) vector against the query
    const q = Float32Array.from(query);
    for (const h of hits) {
      const expected = cosine(q, Float32Array.from(vecs[h.id] as number[]));
      expect(h.score).toBeCloseTo(expected, 6);
    }
    // the doc body rides along
    expect(hits[0]?.doc).toMatchObject({ id: "a", kind: "pos" });
    await db.close();
  });

  it("an optional BQL filter narrows candidates before ranking", async () => {
    const db = await openDb("s2");
    await db.put({ id: "a", kind: "pos" });
    await db.put({ id: "d", kind: "neg" });
    server.seedEmbedding("a", [1, 0.2, 0]);
    server.seedEmbedding("d", [1, 0, 0]); // identical to the query, so closest
    await db.syncEmbeddings();

    // without a filter, d wins; with kind='pos' it is excluded
    const all = await db.searchLocal([1, 0, 0]);
    expect(all.map((h) => h.id)).toEqual(["d", "a"]);
    const filtered = await db.searchLocal([1, 0, 0], { filter: "kind = 'pos'" });
    expect(filtered.map((h) => h.id)).toEqual(["a"]);
    await db.close();
  });

  it("skips docs without a vector and honours k", async () => {
    const db = await openDb("s3");
    await db.put({ id: "a" });
    await db.put({ id: "b" }); // no embedding seeded
    server.seedEmbedding("a", [1, 0, 0]);
    await db.syncEmbeddings();
    const hits = await db.searchLocal([1, 0, 0], { k: 5 });
    expect(hits.map((h) => h.id)).toEqual(["a"]);
    await db.close();
  });

  it("delegates searchVector and searchText to the server", async () => {
    const db = await openDb("s4");
    server.seedEmbedding("a", [1, 0, 0]);
    server.seedEmbedding("b", [0, 1, 0]);
    const vhits = await db.searchVector([1, 0, 0], { k: 2 });
    expect(vhits[0]?.key).toBe("a");
    expect(typeof vhits[0]?.score).toBe("number");

    server.seedSearchText("d1", "the quick brown fox", { lang: "en" });
    server.seedSearchText("d2", "a slow green turtle");
    const thits = await db.searchText("quick", { k: 5 });
    expect(thits.map((h) => h.key)).toEqual(["d1"]);
    expect(thits[0]?.text).toContain("quick");
    expect(thits[0]?.metadata).toMatchObject({ lang: "en" });

    const hybrid = await db.searchText("turtle", { mode: "hybrid" });
    expect(hybrid.map((h) => h.key)).toEqual(["d2"]);
    await db.close();
  });
});
