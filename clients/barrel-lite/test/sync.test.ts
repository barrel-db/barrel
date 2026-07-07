import { beforeEach, describe, expect, it } from "vitest";
import { FakeServer } from "./fake-server.js";
import { SyncTransport } from "../src/wire/transport.js";
import { LocalStore } from "../src/store/localstore.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { encodeHlc, HlcClock } from "../src/codec/hlc.js";
import { base64Encode } from "../src/codec/base64.js";
import { pull } from "../src/sync/puller.js";
import { push } from "../src/sync/pusher.js";
import type { ConflictEvent } from "../src/sync/types.js";

const URL = "http://fake";
const DB = "sp_test";

interface Ctx {
  server: FakeServer;
  store: LocalStore;
  transport: SyncTransport;
  physMs: { value: number };
}

async function setup(physMs = 1000): Promise<Ctx> {
  const server = new FakeServer();
  const phys = { value: physMs };
  const clock = new HlcClock({ physClock: () => phys.value });
  const area = await new MemoryAdapter().open("barrel-lite/test");
  const store = await LocalStore.open(area, clock);
  const transport = new SyncTransport({
    url: URL,
    db: DB,
    clock,
    fetch: server.fetch,
  });
  return { server, store, transport, physMs: phys };
}

describe("pull", () => {
  let ctx: Ctx;
  beforeEach(async () => {
    ctx = await setup();
  });

  it("pulls a server-authored doc into an empty store", async () => {
    ctx.server.seed("a", { v: 1 }, "srv0000000000000", 100);
    const stats = await pull(ctx.transport, ctx.store, { url: URL, db: DB });
    expect(stats.applied).toBe(1);
    expect(ctx.store.getBody("a")).toEqual({ v: 1, id: "a" });
    // the applied record is clean (not dirty)
    expect(ctx.store.get("a")?.dirty).toBe(false);
  });

  it("pulls a tombstone", async () => {
    ctx.server.seed("a", null, "srv0000000000000", 100);
    await pull(ctx.transport, ctx.store, { url: URL, db: DB });
    expect(ctx.store.get("a")?.deleted).toBe(true);
    expect(ctx.store.getBody("a")).toBeUndefined();
  });

  it("skips a change already covered by the local vector", async () => {
    ctx.server.seed("a", { v: 1 }, "srv0000000000000", 100);
    await pull(ctx.transport, ctx.store, { url: URL, db: DB });
    // second pull resumes from the checkpoint and applies nothing
    const stats = await pull(ctx.transport, ctx.store, { url: URL, db: DB });
    expect(stats.applied).toBe(0);
  });

  it("resets to a full pull when the history floor moved past the cursor", async () => {
    ctx.server.seed("a", { v: 1 }, "srv0000000000000", 100);
    await pull(ctx.transport, ctx.store, { url: URL, db: DB });
    // server compacts: floor advances beyond our stored cursor
    ctx.server.historyFloor = base64Cursor(9_999_999);
    ctx.server.seed("b", { v: 2 }, "srv0000000000000", 200);
    const stats = await pull(ctx.transport, ctx.store, { url: URL, db: DB });
    // both docs re-scanned from first
    expect(stats.scanned).toBeGreaterThanOrEqual(2);
    expect(ctx.store.getBody("b")).toEqual({ v: 2, id: "b" });
  });
});

describe("push", () => {
  let ctx: Ctx;
  beforeEach(async () => {
    ctx = await setup();
  });

  it("pushes a local write and clears its dirty flag", async () => {
    ctx.store.put("a", { v: 1 });
    const stats = await push(ctx.transport, ctx.store);
    expect(stats.pushed).toBe(1);
    expect(ctx.store.get("a")?.dirty).toBe(false);
    // it is retrievable back from the server
    const doc = await ctx.transport.getDoc("a");
    expect(doc.doc).toMatchObject({ v: 1 });
  });

  it("skips records the server already has (diff resume)", async () => {
    ctx.store.put("a", { v: 1 });
    await push(ctx.transport, ctx.store);
    // mark dirty again without changing the version, then re-push
    const rec = ctx.store.get("a");
    if (rec) ctx.store.putRecord({ ...rec, dirty: true });
    const stats = await push(ctx.transport, ctx.store);
    expect(stats.pushed).toBe(0);
    expect(stats.skipped).toBe(1);
  });

  it("pushes a local tombstone", async () => {
    ctx.store.put("a", { v: 1 });
    await push(ctx.transport, ctx.store);
    ctx.store.remove("a");
    await push(ctx.transport, ctx.store);
    const doc = await ctx.transport.getDoc("a");
    expect(doc.deleted).toBe(true);
  });
});

describe("conflict resolution", () => {
  it("remote wins: replaces the local body and fires onConflict", async () => {
    const ctx = await setup(1000);
    // local dirty write with a LOW hlc (author sorts low), server has a
    // concurrent write with a HIGHER hlc so the remote wins LWW
    ctx.store.put("a", { who: "local" });
    ctx.server.seed("a", { who: "remote" }, "zzzzzzzzzzzzzzzz", 5_000_000);
    const conflicts: ConflictEvent[] = [];
    await pull(ctx.transport, ctx.store, {
      url: URL,
      db: DB,
      onConflict: (e) => conflicts.push(e),
    });
    expect(ctx.store.getBody("a")).toMatchObject({ who: "remote" });
    expect(ctx.store.get("a")?.dirty).toBe(false);
    expect(conflicts).toHaveLength(1);
    expect(conflicts[0]?.id).toBe("a");
  });

  it("local wins: keeps the local body and stays dirty", async () => {
    const ctx = await setup(10_000_000);
    // local write with a HIGH hlc (phys 10M), server concurrent write low
    ctx.store.put("a", { who: "local" });
    ctx.server.seed("a", { who: "remote" }, "aaaaaaaaaaaaaaaa", 100);
    await pull(ctx.transport, ctx.store, { url: URL, db: DB });
    expect(ctx.store.getBody("a")).toMatchObject({ who: "local" });
    expect(ctx.store.get("a")?.dirty).toBe(true);
  });

  it("converges: after push the server winner matches both sides", async () => {
    const ctx = await setup(10_000_000);
    ctx.store.put("a", { who: "local" });
    ctx.server.seed("a", { who: "remote" }, "aaaaaaaaaaaaaaaa", 100);
    // local wins on pull, stays dirty; push resolves the server to local
    await pull(ctx.transport, ctx.store, { url: URL, db: DB });
    await push(ctx.transport, ctx.store);
    const doc = await ctx.transport.getDoc("a");
    expect(doc.doc).toMatchObject({ who: "local" });
    // a final pull leaves the local side clean and converged
    await pull(ctx.transport, ctx.store, { url: URL, db: DB });
    expect(ctx.store.get("a")?.dirty).toBe(false);
  });

  it("push reports a conflict when the server keeps a different winner", async () => {
    const ctx = await setup(1000);
    // server already holds a higher-hlc version; our lower push loses
    ctx.server.seed("a", { who: "remote" }, "zzzzzzzzzzzzzzzz", 5_000_000);
    ctx.store.put("a", { who: "local" }); // low hlc, no knowledge of remote
    const conflicts: ConflictEvent[] = [];
    const stats = await push(ctx.transport, ctx.store, {
      onConflict: (e) => conflicts.push(e),
    });
    expect(stats.conflicts).toBe(1);
    expect(conflicts).toHaveLength(1);
    expect(ctx.store.get("a")?.dirty).toBe(false);
  });
});

function base64Cursor(seq: number): string {
  // matches the fake server's seq-as-HLC cursor encoding
  return base64Encode(encodeHlc({ wall: BigInt(seq), logical: 0 }), "standard");
}
