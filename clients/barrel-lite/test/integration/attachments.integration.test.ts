import { describe, expect, it } from "vitest";
import { Database } from "../../src/db.js";
import { MemoryAdapter } from "../../src/store/memory.js";
import { sha256Digest } from "../../src/attachments/digest.js";
import { BASE, TOKEN, createDb, uniqueDb } from "./helpers.js";

function open(name: string, db: string) {
  return Database.open(name, {
    remote: { url: BASE, db, token: TOKEN },
    storage: new MemoryAdapter(),
  });
}

describe("attachment sync vs a real server", () => {
  it("round-trips a blob between two clients with matching digest", async () => {
    const db = uniqueDb();
    await createDb(db);
    const bytes = new TextEncoder().encode("integration attachment payload");
    const digest = await sha256Digest(bytes);

    const a = await open("att-a", db);
    await a.putAttachment("doc1", "file.bin", bytes, { contentType: "application/octet-stream" });
    await a.sync();
    await a.close();

    const b = await open("att-b", db);
    await b.sync();
    const got = await b.getAttachment("doc1", "file.bin");
    expect(got?.bytes).toEqual(bytes);
    expect(got?.info.digest).toBe(digest);
    await b.close();
  });

  it("propagates a delete", async () => {
    const db = uniqueDb();
    await createDb(db);
    const a = await open("att-a2", db);
    await a.putAttachment("d", "f", new TextEncoder().encode("bye"), {});
    await a.sync();

    const b = await open("att-b2", db);
    await b.sync();
    expect(await b.getAttachment("d", "f")).toBeDefined();

    await a.removeAttachment("d", "f");
    await a.sync();
    await b.sync();
    expect(await b.getAttachment("d", "f")).toBeUndefined();
    await a.close();
    await b.close();
  });
});
