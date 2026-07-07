/**
 * Storage abstraction and record shapes for the local cache.
 *
 * A StorageArea is a flat namespace of named binary blobs with atomic
 * replace on write. The memory adapter backs Node and tests; the OPFS
 * adapter (browser) plugs in behind the same interface.
 */
import type { JsonObject } from "../json.js";

export interface StorageAdapter {
  /** Open (creating if needed) a named area, e.g. "barrel-lite/<db>". */
  open(namespace: string): Promise<StorageArea>;
}

export interface StorageArea {
  read(name: string): Promise<Uint8Array | undefined>;
  /** Write replaces the whole blob atomically. */
  write(name: string, data: Uint8Array): Promise<void>;
  remove(name: string): Promise<void>;
  list(): Promise<string[]>;
}

/**
 * One cached document. body is null for a tombstone. version/vv are the
 * wire forms (token, standard-base64 VV). dirty marks a local mutation
 * awaiting push; the dirty set is the mutation queue.
 */
export interface DocRecord {
  id: string;
  body: JsonObject | null;
  version: string;
  vv: string;
  deleted: boolean;
  dirty: boolean;
}

/** Per-database metadata, persisted alongside the documents. */
export interface DbMeta {
  /** Minted once, persisted forever: the author of every local write. */
  sourceId: string;
  /** Last HLC, snapshotted at flush so the clock resumes after reload. */
  clock: { wall: string; logical: number };
  /** Pull cursors keyed by replication identity. */
  checkpoints: Record<string, string>;
}
