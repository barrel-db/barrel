import { describe, expect, it } from "vitest";
import { OpfsAdapter, type DirectoryHandleLike, type FileHandleLike } from "../src/store/opfs.js";
import { LocalStore } from "../src/store/localstore.js";
import { HlcClock } from "../src/codec/hlc.js";

/** In-memory stand-in for an OPFS directory tree. */
class FakeDir implements DirectoryHandleLike {
  readonly files = new Map<string, Uint8Array>();
  readonly dirs = new Map<string, FakeDir>();

  async getDirectoryHandle(name: string, options?: { create?: boolean }): Promise<DirectoryHandleLike> {
    let d = this.dirs.get(name);
    if (!d) {
      if (!options?.create) throw new Error("NotFound");
      d = new FakeDir();
      this.dirs.set(name, d);
    }
    return d;
  }

  async getFileHandle(name: string, options?: { create?: boolean }): Promise<FileHandleLike> {
    if (!this.files.has(name)) {
      if (!options?.create) throw new Error("NotFound");
      this.files.set(name, new Uint8Array());
    }
    const files = this.files;
    return {
      async getFile() {
        const data = files.get(name) as Uint8Array;
        return {
          async arrayBuffer(): Promise<ArrayBuffer> {
            const out = new ArrayBuffer(data.byteLength);
            new Uint8Array(out).set(data);
            return out;
          },
        };
      },
      async createWritable() {
        let staged = new Uint8Array();
        return {
          async write(d: Uint8Array) {
            staged = d.slice();
          },
          async close() {
            files.set(name, staged); // atomic replace on close
          },
        };
      },
    };
  }

  async removeEntry(name: string): Promise<void> {
    if (!this.files.delete(name)) this.dirs.delete(name);
  }

  async *entries(): AsyncIterableIterator<[string, { kind: "file" | "directory" }]> {
    for (const name of this.files.keys()) yield [name, { kind: "file" }];
    for (const name of this.dirs.keys()) yield [name, { kind: "directory" }];
  }
}

describe("OPFS adapter", () => {
  it("nests namespace dirs and round-trips a blob", async () => {
    const root = new FakeDir();
    const area = await new OpfsAdapter({ root }).open("barrel-lite/mydb");
    const data = new Uint8Array([1, 2, 3, 4]);
    await area.write("docs.json", data);
    expect(await area.read("docs.json")).toEqual(data);
    // the tree nested barrel-lite/mydb
    const lite = root.dirs.get("barrel-lite");
    expect(lite?.dirs.get("mydb")).toBeDefined();
  });

  it("returns undefined for a missing file", async () => {
    const area = await new OpfsAdapter({ root: new FakeDir() }).open("ns");
    expect(await area.read("absent")).toBeUndefined();
  });

  it("replaces atomically and lists files", async () => {
    const area = await new OpfsAdapter({ root: new FakeDir() }).open("ns");
    await area.write("a", new Uint8Array([1]));
    await area.write("a", new Uint8Array([2, 2]));
    expect(await area.read("a")).toEqual(new Uint8Array([2, 2]));
    await area.write("b", new Uint8Array([9]));
    expect((await area.list()).sort()).toEqual(["a", "b"]);
    await area.remove("a");
    expect(await area.read("a")).toBeUndefined();
  });

  it("backs a LocalStore that reloads across reopen", async () => {
    const root = new FakeDir();
    const adapter = new OpfsAdapter({ root });
    const area1 = await adapter.open("barrel-lite/s");
    const store1 = await LocalStore.open(area1, new HlcClock({ physClock: () => 1000 }));
    store1.put("doc1", { v: 1 });
    await store1.flush();

    const area2 = await adapter.open("barrel-lite/s");
    const store2 = await LocalStore.open(area2, new HlcClock({ physClock: () => 0 }));
    expect(store2.getBody("doc1")).toEqual({ v: 1 });
    expect(store2.sourceId).toBe(store1.sourceId);
  });
});
