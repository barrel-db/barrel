import { describe, expect, it } from "vitest";
import { Database } from "../../src/db.js";
import { MemoryAdapter } from "../../src/store/memory.js";
import {
  BASE,
  TOKEN,
  createDb,
  grant,
  createSpace,
  restGet,
  restPut,
  uniqueDb,
} from "./helpers.js";

function openDb(db: string, token = TOKEN, storage?: MemoryAdapter) {
  return Database.open(db, {
    remote: { url: BASE, db, token },
    storage: storage ?? new MemoryAdapter(),
  });
}

describe("TS <-> Erlang convergence", () => {
  it("pushes local writes the server then serves, with matching _rev", async () => {
    const db = uniqueDb();
    await createDb(db);
    const client = await openDb(db);
    const { version } = await client.put({ id: "a", title: "hello" });
    await client.push();

    const server = await restGet(db, "a");
    expect(server).toMatchObject({ title: "hello" });
    // the server-computed _rev equals the client-minted version token
    expect(server?._rev).toBe(version);
    await client.close();
  });

  it("pulls server-authored docs into the local store", async () => {
    const db = uniqueDb();
    await createDb(db);
    await restPut(db, "srv", { from: "server" });
    const client = await openDb(db);
    const stats = await client.pull();
    expect(stats.applied).toBeGreaterThanOrEqual(1);
    expect(await client.get("srv")).toMatchObject({ from: "server" });
    await client.close();
  });

  it("round-trips a bidirectional sync", async () => {
    const db = uniqueDb();
    await createDb(db);
    const client = await openDb(db);
    await client.put({ id: "c1", n: 1 });
    await restPut(db, "s1", { n: 2 });
    await client.sync();
    expect(await restGet(db, "c1")).toMatchObject({ n: 1 });
    expect(await client.get("s1")).toMatchObject({ n: 2 });
    await client.close();
  });
});

describe("filtered pull", () => {
  it("pulls only documents matching a query filter", async () => {
    const db = uniqueDb();
    await createDb(db);
    await restPut(db, "keep1", { type: "keep", v: 1 });
    await restPut(db, "drop1", { type: "drop", v: 2 });
    const client = await openDb(db);
    await client.pull({
      filter: { query: { where: [["path", ["type"], "keep"]] } },
    });
    expect(await client.get("keep1")).toMatchObject({ v: 1 });
    expect(await client.get("drop1")).toBeUndefined();
    await client.close();
  });
});

describe("checkpoint resume across restarts", () => {
  it("resumes from the stored cursor on reopen", async () => {
    const db = uniqueDb();
    await createDb(db);
    await restPut(db, "d1", { n: 1 });
    const storage = new MemoryAdapter();

    const first = await openDb(db, TOKEN, storage);
    await first.pull();
    expect(await first.get("d1")).toMatchObject({ n: 1 });
    await first.close();

    // a new server doc appears; a reopened client resumes from the cursor
    await restPut(db, "d2", { n: 2 });
    const second = await openDb(db, TOKEN, storage);
    const stats = await second.pull();
    // only the new doc is scanned, proving the checkpoint persisted
    expect(stats.scanned).toBe(1);
    expect(await second.get("d2")).toMatchObject({ n: 2 });
    await second.close();
  });
});

describe("capability-token auth", () => {
  it("a read grant pulls but cannot push", async () => {
    const space = await createSpace("lite-read");
    const writeToken = await grant(space, ["write"]);
    const readToken = await grant(space, ["read"]);

    // seed a doc into the space with the write token
    const writer = await Database.open(space, {
      remote: { url: BASE, db: space, token: writeToken },
      storage: new MemoryAdapter(),
    });
    await writer.put({ id: "s", v: 1 });
    await writer.push();
    await writer.close();

    const reader = await Database.open(space, {
      remote: { url: BASE, db: space, token: readToken },
      storage: new MemoryAdapter(),
    });
    await reader.pull();
    expect(await reader.get("s")).toMatchObject({ v: 1 });

    // a read grant may not push
    await reader.put({ id: "s2", v: 2 });
    await expect(reader.push()).rejects.toMatchObject({ code: "forbidden" });
    await reader.close();
  });
});

describe("CORS", () => {
  it("exposes the x-barrel-hlc header to browser JS", async () => {
    const db = uniqueDb();
    await createDb(db);
    const r = await fetch(`${BASE}/db/${db}`, {
      headers: { authorization: `Bearer ${TOKEN}`, origin: "https://app.example" },
    });
    expect(r.headers.get("access-control-allow-origin")).toBe("*");
    expect(r.headers.get("access-control-expose-headers") ?? "").toContain(
      "x-barrel-hlc",
    );
  });

  it("answers a preflight without auth", async () => {
    const r = await fetch(`${BASE}/db/any/doc/x`, {
      method: "OPTIONS",
      headers: {
        origin: "https://app.example",
        "access-control-request-method": "PUT",
      },
    });
    expect(r.status).toBe(204);
    expect(r.headers.get("access-control-allow-origin")).toBe("*");
  });
});
