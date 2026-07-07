import { describe, expect, it } from "vitest";
import { Database } from "../src/db.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { FakeServer } from "./fake-server.js";
import { BusHub, FakeLockManager } from "./tab-doubles.js";
import type { DocChange } from "../src/sync/status.js";

const URL = "http://fake";
const DB = "sp_test";

async function tick(): Promise<void> {
  for (let i = 0; i < 4; i++) await Promise.resolve();
}

describe("Database multi-tab", () => {
  it("elects one leader; the follower proxies reads and writes", async () => {
    const server = new FakeServer();
    const adapter = new MemoryAdapter();
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const common = {
      remote: { url: URL, db: DB, fetch: server.fetch },
      storage: adapter,
      multiTab: true,
    };
    const a = await Database.open("m", { ...common, tabs: { locks, bus: hub.connect() } });
    const b = await Database.open("m", { ...common, tabs: { locks, bus: hub.connect() } });
    await tick();

    expect(a.isLeader).toBe(true);
    expect(b.isLeader).toBe(false);

    // a leader-side listener sees changes the follower makes
    const seenByLeader: DocChange[] = [];
    a.onChange((c) => seenByLeader.push(c));

    // follower write is proxied to the leader's store
    const res = await b.put({ id: "x", v: 1 });
    expect(res.id).toBe("x");
    expect(await a.get("x")).toMatchObject({ v: 1 });
    // follower read is proxied back
    expect(await b.get("x")).toMatchObject({ v: 1 });
    // and the change fanned out to the leader's listener
    expect(seenByLeader.map((c) => c.id)).toContain("x");

    await a.close();
    await b.close();
  });

  it("promotes the follower on leader close and serves from the reloaded store", async () => {
    const server = new FakeServer();
    const adapter = new MemoryAdapter();
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const common = {
      remote: { url: URL, db: DB, fetch: server.fetch },
      storage: adapter,
      multiTab: true,
    };
    const a = await Database.open("m", { ...common, tabs: { locks, bus: hub.connect() } });
    const b = await Database.open("m", { ...common, tabs: { locks, bus: hub.connect() } });
    await tick();

    // leader writes and flushes to the shared area, then dies
    await a.put({ id: "y", v: 2 });
    await a.close();
    await tick();

    expect(b.isLeader).toBe(true);
    // b reloaded the store on promotion and now serves locally
    expect(await b.get("y")).toMatchObject({ v: 2 });
    await b.close();
  });
});
