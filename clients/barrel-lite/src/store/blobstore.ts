/**
 * Content-addressed blob store: one immutable entry per digest, so
 * identical content across documents dedupes and integrity is implicit.
 * The digest ("sha256-<hex>") is the key (and, for OPFS, the file name;
 * the characters are all filename-safe). Blobs live outside the
 * document snapshot so large bytes never ride the JSON flush.
 */
import type { DirectoryHandleLike, FileHandleLike } from "./opfs.js";
import { resolveDir } from "./opfs.js";

export interface BlobStore {
  has(digest: string): Promise<boolean>;
  /** Store bytes under a digest (a no-op if already present). */
  put(digest: string, data: Uint8Array): Promise<void>;
  /** Read the bytes for a digest, or undefined if absent. */
  read(digest: string): Promise<Uint8Array | undefined>;
  remove(digest: string): Promise<void>;
  /** Delete every blob whose digest is not in `reachable`. */
  gc(reachable: Set<string>): Promise<void>;
  list(): Promise<string[]>;
}

export class MemoryBlobStore implements BlobStore {
  private readonly blobs = new Map<string, Uint8Array>();

  async has(digest: string): Promise<boolean> {
    return this.blobs.has(digest);
  }
  async put(digest: string, data: Uint8Array): Promise<void> {
    if (!this.blobs.has(digest)) this.blobs.set(digest, data.slice());
  }
  async read(digest: string): Promise<Uint8Array | undefined> {
    const v = this.blobs.get(digest);
    return v ? v.slice() : undefined;
  }
  async remove(digest: string): Promise<void> {
    this.blobs.delete(digest);
  }
  async gc(reachable: Set<string>): Promise<void> {
    for (const d of [...this.blobs.keys()]) {
      if (!reachable.has(d)) this.blobs.delete(d);
    }
  }
  async list(): Promise<string[]> {
    return [...this.blobs.keys()];
  }
}

export interface OpfsBlobStoreOptions {
  root?: DirectoryHandleLike;
}

export class OpfsBlobStore implements BlobStore {
  private constructor(private readonly dir: DirectoryHandleLike) {}

  /** Open the blobs/ directory for a database namespace. */
  static async open(namespace: string, opts: OpfsBlobStoreOptions = {}): Promise<OpfsBlobStore> {
    const dir = await resolveDir(`${namespace}/blobs`, opts.root);
    return new OpfsBlobStore(dir);
  }

  async has(digest: string): Promise<boolean> {
    try {
      await this.dir.getFileHandle(digest);
      return true;
    } catch {
      return false;
    }
  }

  async put(digest: string, data: Uint8Array): Promise<void> {
    if (await this.has(digest)) return; // immutable, dedup
    const handle = await this.dir.getFileHandle(digest, { create: true });
    const writable = await handle.createWritable();
    await writable.write(data);
    await writable.close();
  }

  async read(digest: string): Promise<Uint8Array | undefined> {
    let handle: FileHandleLike;
    try {
      handle = await this.dir.getFileHandle(digest);
    } catch {
      return undefined;
    }
    const file = await handle.getFile();
    return new Uint8Array(await file.arrayBuffer());
  }

  async remove(digest: string): Promise<void> {
    try {
      await this.dir.removeEntry(digest);
    } catch {
      // already gone
    }
  }

  async gc(reachable: Set<string>): Promise<void> {
    for (const digest of await this.list()) {
      if (!reachable.has(digest)) await this.remove(digest);
    }
  }

  async list(): Promise<string[]> {
    const names: string[] = [];
    for await (const [name, handle] of this.dir.entries()) {
      if (handle.kind === "file") names.push(name);
    }
    return names;
  }
}
