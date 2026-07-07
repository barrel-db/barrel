/**
 * In-memory storage adapter: the default outside a browser (Node
 * agents, tests). Blobs live in a Map; write replaces atomically by
 * construction. Not durable, which is exactly the contract a cache
 * client offers anyway.
 */
import type { StorageAdapter, StorageArea } from "./types.js";

class MemoryArea implements StorageArea {
  private readonly blobs = new Map<string, Uint8Array>();

  async read(name: string): Promise<Uint8Array | undefined> {
    const v = this.blobs.get(name);
    return v ? v.slice() : undefined;
  }

  async write(name: string, data: Uint8Array): Promise<void> {
    this.blobs.set(name, data.slice());
  }

  async remove(name: string): Promise<void> {
    this.blobs.delete(name);
  }

  async list(): Promise<string[]> {
    return [...this.blobs.keys()];
  }
}

export class MemoryAdapter implements StorageAdapter {
  private readonly areas = new Map<string, MemoryArea>();

  async open(namespace: string): Promise<StorageArea> {
    let area = this.areas.get(namespace);
    if (!area) {
      area = new MemoryArea();
      this.areas.set(namespace, area);
    }
    return area;
  }
}
