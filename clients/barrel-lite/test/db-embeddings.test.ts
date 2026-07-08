import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Database } from "../src/db.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { FakeServer } from "./fake-server.js";
import { BusHub, FakeLockManager } from "./tab-doubles.js";

const URL = "http://fake";
const DB = "sp_test";

async function tick(): Promise<void> {
  for (let i = 0; i < 4; i++) await Promise.resolve();
}

describe("Database syncEmbeddings", () => {
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

  it("pulls per-doc vectors for the docs it holds", async () => {
    const db = await openDb("e1");
    await db.put({ id: "a", v: 1 });
    await db.put({ id: "b", v: 2 });
    server.seedEmbedding("a", [0.1, -0.2, 0.3]);
    server.seedEmbedding("b", [0.4, 0.5, 0.6]);

    const fetched = await db.syncEmbeddings();
    expect(fetched).toBe(2);
    // a second pass fetches nothing new
    expect(await db.syncEmbeddings()).toBe(0);
    await db.close();
  });

  it("liveSync({vectors}) pulls embeddings within a poll", async () => {
    vi.useFakeTimers();
    try {
      const db = await openDb("e2");
      await db.put({ id: "a", v: 1 });
      server.seedEmbedding("a", [1, 2, 3]);

      const handle = db.liveSync({ vectors: true, minIntervalMs: 500 });
      await vi.advanceTimersByTimeAsync(0); // initial cycle pulls the vector
      // reach into the leader's store via a follower-free path: syncEmbeddings
      // now reports nothing new because the live cycle already pulled it
      const again = await db.syncEmbeddings();
      expect(again).toBe(0);

      handle.stop();
      await db.close();
    } finally {
      vi.useRealTimers();
    }
  });

  it("a follower proxies syncEmbeddings to the leader", async () => {
    const adapter = new MemoryAdapter();
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const common = {
      remote: { url: URL, db: DB, fetch: server.fetch },
      storage: adapter,
      multiTab: true,
    };
    const a = await Database.open("e3", { ...common, tabs: { locks, bus: hub.connect() } });
    const b = await Database.open("e3", { ...common, tabs: { locks, bus: hub.connect() } });
    await tick();
    expect(a.isLeader).toBe(true);
    expect(b.isLeader).toBe(false);

    await b.put({ id: "a", v: 1 }); // proxied to the leader's store
    server.seedEmbedding("a", [1, 2, 3]);

    const fetched = await b.syncEmbeddings(); // proxied to the leader
    expect(fetched).toBe(1);

    await a.close();
    await b.close();
  });
});
