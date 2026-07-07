/**
 * Origin Private File System storage adapter (browser). Async
 * main-thread OPFS: createWritable replaces a file atomically on
 * close, which gives the store's snapshot-per-flush model crash
 * consistency without a dedicated worker. Web Locks already guarantees
 * a single writer per origin, so no cross-tab file contention. A
 * worker + createSyncAccessHandle adapter can slot in behind the same
 * StorageAdapter interface later if throughput demands it.
 */
import type { StorageAdapter, StorageArea } from "./types.js";

/** The directory-handle surface this adapter uses (subset of OPFS). */
export interface DirectoryHandleLike {
  getDirectoryHandle(
    name: string,
    options?: { create?: boolean },
  ): Promise<DirectoryHandleLike>;
  getFileHandle(
    name: string,
    options?: { create?: boolean },
  ): Promise<FileHandleLike>;
  removeEntry(name: string): Promise<void>;
  entries(): AsyncIterableIterator<[string, { kind: "file" | "directory" }]>;
}

export interface FileHandleLike {
  getFile(): Promise<{ arrayBuffer(): Promise<ArrayBuffer> }>;
  createWritable(): Promise<{
    write(data: Uint8Array): Promise<void>;
    close(): Promise<void>;
  }>;
}

export interface OpfsAdapterOptions {
  /** Override the OPFS root (tests); defaults to the origin's OPFS. */
  root?: DirectoryHandleLike;
}

export async function opfsRoot(): Promise<DirectoryHandleLike> {
  const storage = (navigator as unknown as {
    storage: { getDirectory(): Promise<DirectoryHandleLike> };
  }).storage;
  return storage.getDirectory();
}

/** Resolve (creating) a slash-separated namespace under a root handle. */
export async function resolveDir(
  namespace: string,
  root?: DirectoryHandleLike,
): Promise<DirectoryHandleLike> {
  let dir = root ?? (await opfsRoot());
  for (const segment of namespace.split("/").filter(Boolean)) {
    dir = await dir.getDirectoryHandle(segment, { create: true });
  }
  return dir;
}

export class OpfsAdapter implements StorageAdapter {
  constructor(private readonly opts: OpfsAdapterOptions = {}) {}

  async open(namespace: string): Promise<StorageArea> {
    const dir = await resolveDir(namespace, this.opts.root);
    return new OpfsArea(dir);
  }
}

class OpfsArea implements StorageArea {
  constructor(private readonly dir: DirectoryHandleLike) {}

  async read(name: string): Promise<Uint8Array | undefined> {
    let handle: FileHandleLike;
    try {
      handle = await this.dir.getFileHandle(name);
    } catch {
      return undefined; // NotFoundError
    }
    const file = await handle.getFile();
    return new Uint8Array(await file.arrayBuffer());
  }

  async write(name: string, data: Uint8Array): Promise<void> {
    const handle = await this.dir.getFileHandle(name, { create: true });
    const writable = await handle.createWritable();
    await writable.write(data);
    await writable.close(); // atomic replace
  }

  async remove(name: string): Promise<void> {
    try {
      await this.dir.removeEntry(name);
    } catch {
      // already gone
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
