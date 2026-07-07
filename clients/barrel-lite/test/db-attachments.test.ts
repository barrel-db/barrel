import { describe, expect, it } from "vitest";
import { Database } from "../src/db.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { FakeServer } from "./fake-server.js";
import { sha256Digest } from "../src/attachments/digest.js";

const URL = "http://fake";
const DB = "sp_test";

function open(name: string, server: FakeServer) {
  return Database.open(name, {
    remote: { url: URL, db: DB, fetch: server.fetch },
    storage: new MemoryAdapter(),
  });
}

describe("Database attachments", () => {
  it("round-trips a blob put -> push -> pull -> get with matching digest", async () => {
    const server = new FakeServer();
    const bytes = new TextEncoder().encode("hello attachment");
    const digest = await sha256Digest(bytes);

    const a = await open("a", server);
    const put = await a.putAttachment("doc1", "f.txt", bytes, { contentType: "text/plain" });
    expect(put.digest).toBe(digest);
    await a.sync(); // pushes the attachment
    await a.close();

    const b = await open("b", server);
    await b.sync(); // pulls it
    const got = await b.getAttachment("doc1", "f.txt");
    expect(got?.bytes).toEqual(bytes);
    expect(got?.info.digest).toBe(digest);
    expect(got?.info.contentType).toBe("text/plain");
    expect((await b.getAttachmentInfo("doc1", "f.txt"))?.length).toBe(bytes.length);
    await b.close();
  });

  it("propagates a delete", async () => {
    const server = new FakeServer();
    const a = await open("a", server);
    await a.putAttachment("d", "f", new TextEncoder().encode("x"), {});
    await a.sync();

    const b = await open("b", server);
    await b.sync();
    expect(await b.getAttachment("d", "f")).toBeDefined();

    await a.removeAttachment("d", "f");
    await a.sync();
    await b.sync();
    expect(await b.getAttachment("d", "f")).toBeUndefined();
    await a.close();
    await b.close();
  });

  it("a re-sync after push is a no-op (nothing left dirty)", async () => {
    const server = new FakeServer();
    const a = await open("a", server);
    await a.putAttachment("d", "f", new TextEncoder().encode("same"), {});
    await a.sync();
    // a second sync has no dirty attachments and must still succeed
    await expect(a.sync()).resolves.toBeDefined();
    const got = await a.getAttachment("d", "f");
    expect(new TextDecoder().decode(got?.bytes)).toBe("same");
    await a.close();
  });

  it("degrades when the server has no attachment feed (501)", async () => {
    const server = new FakeServer();
    server.attUnsupported = true;
    const b = await open("b", server);
    // sync must not throw even though att_changes answers 501
    await expect(b.sync()).resolves.toBeDefined();
    await b.close();
  });
});
