/** In-memory stand-in for an OPFS directory tree, for adapter tests. */
import type { DirectoryHandleLike, FileHandleLike } from "../src/store/opfs.js";

export class FakeDir implements DirectoryHandleLike {
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
            files.set(name, staged);
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
