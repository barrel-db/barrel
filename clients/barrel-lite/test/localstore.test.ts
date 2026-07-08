import { describe, expect, it } from "vitest";
import { LocalStore } from "../src/store/localstore.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { HlcClock } from "../src/codec/hlc.js";
import { versionFromToken } from "../src/codec/version.js";
import { vvContains, vvDecode } from "../src/codec/vv.js";
import { base64Decode } from "../src/codec/base64.js";
import type { StorageArea } from "../src/store/types.js";

async function freshArea(): Promise<StorageArea> {
  return new MemoryAdapter().open("barrel-lite/test");
}

function clockAt(ms: number): HlcClock {
  return new HlcClock({ physClock: () => ms });
}

describe("local store", () => {
  it("mints a stable source_id and authors versions with it", async () => {
    const area = await freshArea();
    const store = await LocalStore.open(area, clockAt(1000));
    const sid = store.sourceId;
    expect(sid).toMatch(/^[0-9a-f]{16}$/);
    const rec = store.put("a", { v: 1 });
    expect(versionFromToken(rec.version).author).toBe(sid);
    expect(rec.dirty).toBe(true);
    expect(store.getBody("a")).toEqual({ v: 1 });
  });

  it("bumps the doc's own VV entry across updates", async () => {
    const area = await freshArea();
    const store = await LocalStore.open(area, clockAt(1000));
    const r1 = store.put("a", { v: 1 });
    const r2 = store.put("a", { v: 2 });
    const sid = store.sourceId;
    const v2 = versionFromToken(r2.version);
    const vv1 = vvDecode(base64Decode(r1.vv));
    const vv2 = vvDecode(base64Decode(r2.vv));
    // the new VV contains the new version, the old one does not
    expect(vvContains(vv2, v2)).toBe(true);
    expect(vvContains(vv1, v2)).toBe(false);
    expect([...vv2.keys()]).toEqual([sid]);
  });

  it("delete writes a dirty tombstone that hides the body", async () => {
    const area = await freshArea();
    const store = await LocalStore.open(area, clockAt(1000));
    store.put("a", { v: 1 });
    const tomb = store.remove("a");
    expect(tomb.deleted).toBe(true);
    expect(tomb.dirty).toBe(true);
    expect(store.getBody("a")).toBeUndefined();
    expect(store.allDocs()).toHaveLength(0);
    expect(store.allDocs({ includeDeleted: true })).toHaveLength(1);
  });

  it("orders dirty records by version HLC", async () => {
    const area = await freshArea();
    let ms = 1000;
    const store = await LocalStore.open(area, new HlcClock({ physClock: () => ms }));
    store.put("b", { n: 1 });
    ms = 1001;
    store.put("a", { n: 2 });
    ms = 1002;
    store.put("c", { n: 3 });
    expect(store.dirtyRecords().map((r) => r.id)).toEqual(["b", "a", "c"]);
  });

  it("reloads documents, source_id, and clock from the last flush", async () => {
    const area = await freshArea();
    const store = await LocalStore.open(area, clockAt(5000));
    const sid = store.sourceId;
    store.put("a", { v: 1 });
    store.put("b", { v: 2 });
    await store.flush();

    // a new store over the same area sees the flushed state
    const reopened = await LocalStore.open(area, clockAt(0));
    expect(reopened.sourceId).toBe(sid);
    expect(reopened.getBody("a")).toEqual({ v: 1 });
    expect(reopened.getBody("b")).toEqual({ v: 2 });
    // the clock resumed from the flushed value (>= 5000), not the new phys(0)
    const next = reopened.put("c", { v: 3 });
    expect(versionFromToken(next.version).hlc.wall).toBeGreaterThanOrEqual(5000n);
  });

  it("loses writes made after the last flush (crash window)", async () => {
    const area = await freshArea();
    const store = await LocalStore.open(area, clockAt(1000));
    store.put("a", { v: 1 });
    await store.flush();
    store.put("b", { v: 2 }); // never flushed
    const reopened = await LocalStore.open(area, clockAt(1000));
    expect(reopened.getBody("a")).toEqual({ v: 1 });
    expect(reopened.getBody("b")).toBeUndefined();
  });

  it("persists and reloads checkpoints", async () => {
    const area = await freshArea();
    const store = await LocalStore.open(area, clockAt(1000));
    store.setCheckpoint("key1", "CURSOR1");
    await store.flush();
    const reopened = await LocalStore.open(area, clockAt(1000));
    expect(reopened.getCheckpoint("key1")).toBe("CURSOR1");
    expect(reopened.getCheckpoint("absent")).toBeUndefined();
  });

  it("stores vectors, fixes the dimension, and rejects a wrong length", async () => {
    const area = await freshArea();
    const store = await LocalStore.open(area, clockAt(1000));
    expect(store.vectorDim()).toBeUndefined();
    expect(store.putVector("a", Float32Array.from([1, 2, 3]))).toBe(true);
    expect(store.vectorDim()).toBe(3);
    expect(store.putVector("b", Float32Array.from([4, 5, 6]))).toBe(true);
    // wrong dimension is rejected, index stays rectangular
    expect(store.putVector("c", Float32Array.from([1, 2]))).toBe(false);
    expect(store.getVector("c")).toBeUndefined();
    expect(store.vectorIds().sort()).toEqual(["a", "b"]);
    store.removeVector("a");
    expect(store.getVector("a")).toBeUndefined();
  });

  it("reloads vectors byte-exact across a reopen", async () => {
    const area = await freshArea();
    const store = await LocalStore.open(area, clockAt(1000));
    const v = Float32Array.from([0.1, -0.2, 0.3, 0.4]);
    store.putVector("a", v);
    await store.flush();

    const reopened = await LocalStore.open(area, clockAt(0));
    expect(reopened.vectorDim()).toBe(4);
    const back = reopened.getVector("a");
    expect(back).toBeDefined();
    // f32-exact round trip
    expect(Array.from(back as Float32Array)).toEqual(Array.from(v));
  });

  it("clearDirty only clears when the version still matches", async () => {
    const area = await freshArea();
    const store = await LocalStore.open(area, clockAt(1000));
    const rec = store.put("a", { v: 1 });
    store.clearDirty("a", "wrong@version");
    expect(store.get("a")?.dirty).toBe(true);
    store.clearDirty("a", rec.version);
    expect(store.get("a")?.dirty).toBe(false);
  });
});
