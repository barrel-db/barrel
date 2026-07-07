import { describe, expect, it } from "vitest";
import { Database } from "../src/db.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { FakeServer } from "./fake-server.js";

const URL = "http://fake";
const DB = "sp_test";

describe("Database.queryRemote", () => {
  it("parses the ndjson rows and meta from the server", async () => {
    const server = new FakeServer();
    server.seed("d1", { kind: "fruit", name: "apple" }, "srv0000000000000", 100);
    server.seed("d2", { kind: "tool", name: "hammer" }, "srv0000000000000", 200);
    const db = await Database.open("qr", {
      remote: { url: URL, db: DB, fetch: server.fetch },
      storage: new MemoryAdapter(),
    });
    const { rows, meta } = await db.queryRemote("SELECT name FROM db WHERE kind = 'fruit'");
    expect(rows).toEqual([{ id: "d1", name: "apple" }]);
    expect(meta).toEqual({ hasMore: false, count: 1 });
    await db.close();
  });

  it("local and remote produce the same rows for the same query", async () => {
    const server = new FakeServer();
    const db = await Database.open("qr2", {
      remote: { url: URL, db: DB, fetch: server.fetch },
      storage: new MemoryAdapter(),
    });
    await db.put({ id: "a", kind: "fruit", name: "apple", price: 3 });
    await db.put({ id: "b", kind: "fruit", name: "pear", price: 5 });
    await db.put({ id: "c", kind: "tool", name: "hammer", price: 12 });
    await db.sync(); // push local docs to the server

    const bql = "SELECT name FROM db WHERE kind = 'fruit' ORDER BY price DESC";
    const local = await db.query(bql);
    const remote = await db.queryRemote(bql);
    expect(remote.rows).toEqual(local);
    await db.close();
  });
});
