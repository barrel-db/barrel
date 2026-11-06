import { describe, expect, it } from "vitest";
import { Database } from "../../src/db.js";
import { MemoryAdapter } from "../../src/store/memory.js";
import { BASE, TOKEN, createDb, restPut, uniqueDb } from "./helpers.js";

function openDb(db: string) {
  return Database.open(db, {
    remote: { url: BASE, db, token: TOKEN },
    storage: new MemoryAdapter(),
  });
}

describe("conflicts with a current server", () => {
  it("keeps a local write that loses as a conflict, here and on the server", async () => {
    const db = uniqueDb();
    await createDb(db);
    await restPut(db, "note", { text: "seed" });
    const phone = await openDb(db);
    await phone.pull();

    // the phone edits offline while someone edits on the server
    await phone.put({ id: "note", text: "from phone" });
    const other = await openDb(db);
    await other.pull();
    await other.put({ id: "note", text: "from other" });
    await other.push();

    await phone.sync();
    await phone.pull();
    const texts = [
      (await phone.get("note"))?.["text"],
      ...(await phone.conflicts("note")).map((c) => c.body?.["text"]),
    ].sort();
    expect(texts).toEqual(["from other", "from phone"]);

    // another client sees the same two versions
    await other.pull();
    expect((await other.conflicts("note")).length).toBe(1);
    expect((await other.get("note"))?.["text"]).toBe((await phone.get("note"))?.["text"]);
    await phone.close();
    await other.close();
  });

  it("a resolution replaces both versions everywhere", async () => {
    const db = uniqueDb();
    await createDb(db);
    await restPut(db, "d", { v: 0 });
    const a = await openDb(db);
    const b = await openDb(db);
    await a.pull();
    await b.pull();
    await a.put({ id: "d", v: 1 });
    await b.put({ id: "d", v: 2 });
    await a.sync();
    await b.sync();
    await a.pull();
    expect((await a.conflicts("d")).length).toBe(1);

    await a.resolve("d", { v: 3 });
    await a.push();
    await b.pull();
    expect(await a.conflicts("d")).toEqual([]);
    expect(await b.conflicts("d")).toEqual([]);
    expect((await b.get("d"))?.["v"]).toBe(3);
    await a.close();
    await b.close();
  });

  it("sequential edits from two clients add no conflict", async () => {
    const db = uniqueDb();
    await createDb(db);
    const a = await openDb(db);
    const b = await openDb(db);
    await a.put({ id: "s", v: 1 });
    await a.push();
    await b.pull();
    await b.put({ id: "s", v: 2 });
    await b.push();
    await a.pull();
    expect(await a.conflicts("s")).toEqual([]);
    expect((await a.get("s"))?.["v"]).toBe(2);
    await a.close();
    await b.close();
  });
});
