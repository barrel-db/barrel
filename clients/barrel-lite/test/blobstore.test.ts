import { describe, expect, it } from "vitest";
import { MemoryBlobStore, OpfsBlobStore, type BlobStore } from "../src/store/blobstore.js";
import { sha256Digest } from "../src/attachments/digest.js";
import { FakeDir } from "./fake-opfs.js";

describe("digest", () => {
  it("matches the server sha256- format for empty and known content", async () => {
    expect(await sha256Digest(new Uint8Array())).toBe(
      "sha256-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    );
    // "abc" -> known sha256
    expect(await sha256Digest(new TextEncoder().encode("abc"))).toBe(
      "sha256-ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    );
  });

  it("digests a subarray view correctly", async () => {
    const framed = new Uint8Array([9, 9, 97, 98, 99, 9]);
    expect(await sha256Digest(framed.subarray(2, 5))).toBe(
      await sha256Digest(new TextEncoder().encode("abc")),
    );
  });
});

function suites(name: string, make: () => Promise<BlobStore>): void {
  describe(`blob store (${name})`, () => {
    it("round-trips bytes by digest", async () => {
      const store = await make();
      const digest = "sha256-aa";
      const data = new Uint8Array([1, 2, 3, 4]);
      expect(await store.has(digest)).toBe(false);
      await store.put(digest, data);
      expect(await store.has(digest)).toBe(true);
      expect(await store.read(digest)).toEqual(data);
      expect(await store.read("sha256-missing")).toBeUndefined();
    });

    it("is immutable: a second put keeps the first content", async () => {
      const store = await make();
      await store.put("sha256-x", new Uint8Array([1]));
      await store.put("sha256-x", new Uint8Array([9, 9]));
      expect(await store.read("sha256-x")).toEqual(new Uint8Array([1]));
    });

    it("removes and lists", async () => {
      const store = await make();
      await store.put("sha256-a", new Uint8Array([1]));
      await store.put("sha256-b", new Uint8Array([2]));
      expect((await store.list()).sort()).toEqual(["sha256-a", "sha256-b"]);
      await store.remove("sha256-a");
      expect(await store.has("sha256-a")).toBe(false);
    });

    it("gc keeps only reachable digests", async () => {
      const store = await make();
      await store.put("sha256-keep", new Uint8Array([1]));
      await store.put("sha256-drop", new Uint8Array([2]));
      await store.gc(new Set(["sha256-keep"]));
      expect(await store.has("sha256-keep")).toBe(true);
      expect(await store.has("sha256-drop")).toBe(false);
    });
  });
}

suites("memory", async () => new MemoryBlobStore());
suites("opfs", async () => OpfsBlobStore.open("barrel-lite/db", { root: new FakeDir() }));
