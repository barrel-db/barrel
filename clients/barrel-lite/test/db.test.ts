import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Database } from "../src/db.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { FakeServer } from "./fake-server.js";
import type { DocChange, SyncStatus } from "../src/sync/status.js";

const URL = "http://fake";
const DB = "sp_test";

describe("Database local CRUD", () => {
  it("puts, gets, and removes locally without a remote", async () => {
    const db = await Database.open("d1");
    await db.put({ id: "a", title: "hello" });
    expect(await db.get("a")).toMatchObject({ id: "a", title: "hello" });
    await db.remove("a");
    expect(await db.get("a")).toBeUndefined();
    expect(await db.allDocs()).toHaveLength(0);
    await db.close();
  });

  it("emits local change events", async () => {
    const db = await Database.open("d2");
    const changes: DocChange[] = [];
    db.onChange((c) => changes.push(c));
    await db.put({ id: "a" });
    await db.remove("a");
    expect(changes).toEqual([
      { id: "a", deleted: false, source: "local" },
      { id: "a", deleted: true, source: "local" },
    ]);
    await db.close();
  });

  it("throws when syncing without a remote", async () => {
    const db = await Database.open("d3");
    await expect(db.push()).rejects.toThrow(/no remote/);
    await db.close();
  });
});

describe("Database sync against a fake server", () => {
  let server: FakeServer;

  beforeEach(() => {
    server = new FakeServer();
  });

  async function openDb(name: string, physClock?: () => number) {
    return Database.open(name, {
      remote: { url: URL, db: DB, fetch: server.fetch },
      storage: new MemoryAdapter(),
      ...(physClock ? { physClock } : {}),
    });
  }

  it("pushes local writes and pulls remote ones with change events", async () => {
    const db = await openDb("s1");
    const remoteChanges: DocChange[] = [];
    db.onChange((c) => {
      if (c.source === "remote") remoteChanges.push(c);
    });
    await db.put({ id: "local1", v: 1 });
    await db.push();
    server.seed("remote1", { v: 2 }, "srv0000000000000", 100);
    const stats = await db.pull();
    expect(stats.applied).toBe(1);
    expect(await db.get("remote1")).toMatchObject({ v: 2 });
    expect(remoteChanges).toEqual([
      { id: "remote1", deleted: false, source: "remote" },
    ]);
    await db.close();
  });

  it("emits syncing then idle status around a pull", async () => {
    const db = await openDb("s2");
    const states: SyncStatus[] = [];
    db.onStatus((s) => states.push(s));
    await db.pull();
    expect(states.map((s) => s.state)).toEqual(["syncing", "idle"]);
    await db.close();
  });
});

describe("Database live sync (fake timers)", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("polls, backs off when idle, and resets on change", async () => {
    const server = new FakeServer();
    const db = await Database.open("l1", {
      remote: { url: URL, db: DB, fetch: server.fetch },
      storage: new MemoryAdapter(),
    });
    const handle = db.liveSync({ minIntervalMs: 500, maxIntervalMs: 4000 });

    // initial run (scheduled at 0)
    await vi.advanceTimersByTimeAsync(0);
    // idle: next at 500, then 750, then 1125...
    await vi.advanceTimersByTimeAsync(500);
    await vi.advanceTimersByTimeAsync(750);

    // a remote change appears; the next poll applies it and resets interval
    server.seed("r1", { v: 1 }, "srv0000000000000", 100);
    await vi.advanceTimersByTimeAsync(1200);
    expect(await db.get("r1")).toMatchObject({ v: 1 });

    handle.stop();
    await db.close();
  });

  it("wakes on a local mutation and stops cleanly", async () => {
    const server = new FakeServer();
    const db = await Database.open("l2", {
      remote: { url: URL, db: DB, fetch: server.fetch },
      storage: new MemoryAdapter(),
    });
    const handle = db.liveSync({ minIntervalMs: 5000, wakeDebounceMs: 50 });
    await vi.advanceTimersByTimeAsync(0); // initial run

    await db.put({ id: "a", v: 1 });
    // the wake debounce (50ms) triggers a push well before the 5s interval
    await vi.advanceTimersByTimeAsync(60);
    const doc = await server.fetch(`${URL}/db/${DB}/_sync/doc/a`, { method: "GET" });
    expect(doc.status).toBe(200);

    handle.stop();
    // after stop, no further ticks run
    await vi.advanceTimersByTimeAsync(10_000);
    await db.close();
  });

  it("surfaces an error status and keeps running", async () => {
    let calls = 0;
    const failingFetch = async (url: string, init?: RequestInit): Promise<Response> => {
      calls++;
      if (calls <= 2) throw new Error("offline");
      return new Response(JSON.stringify({ changes: [], last_seq: "first" }), {
        status: 200,
      });
    };
    const db = await Database.open("l3", {
      remote: { url: URL, db: DB, fetch: failingFetch },
      storage: new MemoryAdapter(),
    });
    const states: SyncStatus[] = [];
    db.onStatus((s) => states.push(s));
    const handle = db.liveSync({ minIntervalMs: 500, errorMinMs: 1000 });
    await vi.advanceTimersByTimeAsync(0); // first tick: push throws
    expect(states.some((s) => s.state === "error")).toBe(true);
    handle.stop();
    await db.close();
  });
});
