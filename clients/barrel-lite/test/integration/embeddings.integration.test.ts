import { describe, expect, it } from "vitest";
import { Database } from "../../src/db.js";
import { MemoryAdapter } from "../../src/store/memory.js";
import { cosine } from "../../src/vectors/cosine.js";
import { BASE, TOKEN, createDb, restPut, uniqueDb } from "./helpers.js";

function open(name: string, db: string) {
  return Database.open(name, {
    remote: { url: BASE, db, token: TOKEN },
    storage: new MemoryAdapter(),
  });
}

describe("embedding pull + local vector search vs a real server", () => {
  it("pulls vectors bit-exact and ranks by cosine like a hand computation", async () => {
    const db = uniqueDb();
    await createDb(db);

    // seed docs whose _embedding lands in the server's per-doc emb column
    const vecs: Record<string, number[]> = {
      a: [1, 0, 0, 0],
      b: [0.8, 0.6, 0, 0],
      c: [0, 1, 0, 0],
      d: [-1, 0, 0, 0],
    };
    const kinds: Record<string, string> = { a: "pos", b: "pos", c: "pos", d: "neg" };
    for (const [id, v] of Object.entries(vecs)) {
      await restPut(db, id, { id, kind: kinds[id], _embedding: v });
    }

    const client = await open("emb-itest", db);
    await client.pull();
    const n = await client.syncEmbeddings();
    expect(n).toBe(4);

    // vectors round-trip bit-exact through the float32 LE base64 wire
    for (const [id, v] of Object.entries(vecs)) {
      const got = client["store"].getVector(id);
      expect(got).toBeDefined();
      expect(Array.from(got as Float32Array)).toEqual(Array.from(Float32Array.from(v)));
    }

    // local cosine top-k matches a hand computation over the f32 vectors
    const query = [1, 0, 0, 0];
    const hits = await client.searchLocal(query, { k: 4 });
    expect(hits.map((h) => h.id)).toEqual(["a", "b", "c", "d"]);
    const q = Float32Array.from(query);
    for (const h of hits) {
      const expected = cosine(q, Float32Array.from(vecs[h.id] as number[]));
      expect(h.score).toBeCloseTo(expected, 6);
    }

    // a BQL filter narrows candidates: exclude the negative doc
    const pos = await client.searchLocal(query, { filter: "kind = 'pos'" });
    expect(pos.map((h) => h.id)).not.toContain("d");

    // NB: the server /search/vector ANN store is a separate corpus bound
    // to the embedding model's dimension (not the per-doc emb column this
    // test seeds), so it is exercised by the fake-server unit test, not here.

    await client.close();
  });
});
