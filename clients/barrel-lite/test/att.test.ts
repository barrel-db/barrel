import { describe, expect, it } from "vitest";
import {
  attApply,
  getAttachmentInfoLocal,
  getAttachmentLocal,
  putAttachmentLocal,
  reachableDigests,
  removeAttachmentLocal,
} from "../src/attachments/att.js";
import { MemoryBlobStore } from "../src/store/blobstore.js";
import { LocalStore } from "../src/store/localstore.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { HlcClock, encodeHlc } from "../src/codec/hlc.js";
import { base64Encode } from "../src/codec/base64.js";
import type { AttRef } from "../src/store/types.js";

function origin(wall: number): string {
  return base64Encode(encodeHlc({ wall: BigInt(wall), logical: 0 }), "standard");
}

function ref(o: Partial<AttRef>): AttRef {
  return {
    id: "d",
    name: "f",
    digest: "sha256-b",
    length: 1,
    contentType: "text/plain",
    origin: origin(1000),
    op: "put",
    dirty: false,
    ...o,
  };
}

describe("attApply LWW", () => {
  it("accepts against no stored ref", () => {
    expect(attApply(origin(1), "sha256-a", undefined)).toBe(true);
  });
  it("newer origin wins, older loses", () => {
    const stored = ref({ origin: origin(1000), digest: "sha256-a" });
    expect(attApply(origin(2000), "sha256-a", stored)).toBe(true);
    expect(attApply(origin(500), "sha256-a", stored)).toBe(false);
  });
  it("equal origin ties on digest byte order", () => {
    const stored = ref({ origin: origin(1000), digest: "sha256-b" });
    expect(attApply(origin(1000), "sha256-c", stored)).toBe(true); // c > b
    expect(attApply(origin(1000), "sha256-a", stored)).toBe(false); // a < b
    expect(attApply(origin(1000), "sha256-b", stored)).toBe(false); // equal = redelivery
  });
});

describe("local attachment ops", () => {
  async function fresh() {
    const area = await new MemoryAdapter().open("barrel-lite/t");
    const store = await LocalStore.open(area, new HlcClock({ physClock: () => 1000 }));
    const blobs = new MemoryBlobStore();
    return { store, blobs, area };
  }

  it("puts, reads, and reports info", async () => {
    const { store, blobs } = await fresh();
    const bytes = new TextEncoder().encode("hi");
    const info = await putAttachmentLocal(store, blobs, "d1", "f", bytes, "text/plain");
    expect(info.digest).toMatch(/^sha256-[0-9a-f]{64}$/);
    const got = await getAttachmentLocal(store, blobs, "d1", "f");
    expect(got?.bytes).toEqual(bytes);
    expect(got?.info.contentType).toBe("text/plain");
    expect(getAttachmentInfoLocal(store, "d1", "f")?.digest).toBe(info.digest);
    // marked dirty for push
    expect(store.getAttRef("d1", "f")?.dirty).toBe(true);
  });

  it("delete hides the attachment", async () => {
    const { store, blobs } = await fresh();
    await putAttachmentLocal(store, blobs, "d1", "f", new TextEncoder().encode("x"), "text/plain");
    removeAttachmentLocal(store, "d1", "f");
    expect(await getAttachmentLocal(store, blobs, "d1", "f")).toBeUndefined();
    expect(getAttachmentInfoLocal(store, "d1", "f")).toBeUndefined();
    expect(store.getAttRef("d1", "f")?.op).toBe("delete");
  });

  it("reachableDigests skips tombstones", async () => {
    const { store, blobs } = await fresh();
    await putAttachmentLocal(store, blobs, "d1", "a", new TextEncoder().encode("a"), "text/plain");
    const keep = getAttachmentInfoLocal(store, "d1", "a")!.digest;
    await putAttachmentLocal(store, blobs, "d2", "b", new TextEncoder().encode("b"), "text/plain");
    removeAttachmentLocal(store, "d2", "b");
    const reachable = reachableDigests(store);
    expect(reachable.has(keep)).toBe(true);
    expect(reachable.size).toBe(1);
  });

  it("persists attrefs and att-checkpoint across reload", async () => {
    const { store, blobs, area } = await fresh();
    await putAttachmentLocal(store, blobs, "d1", "f", new TextEncoder().encode("z"), "text/plain");
    store.setAttCheckpoint("key1", "CURSOR");
    await store.flush();

    const reopened = await LocalStore.open(area, new HlcClock({ physClock: () => 0 }));
    expect(reopened.getAttRef("d1", "f")?.digest).toMatch(/^sha256-/);
    expect(reopened.getAttCheckpoint("key1")).toBe("CURSOR");
  });

  it("clearAttRefDirty only clears the matching origin", async () => {
    const { store, blobs } = await fresh();
    await putAttachmentLocal(store, blobs, "d1", "f", new TextEncoder().encode("z"), "text/plain");
    const o = store.getAttRef("d1", "f")!.origin;
    store.clearAttRefDirty("d1", "f", "wrong");
    expect(store.getAttRef("d1", "f")?.dirty).toBe(true);
    store.clearAttRefDirty("d1", "f", o);
    expect(store.getAttRef("d1", "f")?.dirty).toBe(false);
  });
});
