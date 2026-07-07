/**
 * barrel-lite: a TypeScript protocol client for the barrel database.
 *
 * Offline-first local store with HLC-stamped mutations, pushed and
 * pulled over the barrel_server sync wire (/db/:db/_sync/*). Not a
 * WASM port of the engine: the client re-implements the wire codecs
 * (HLC, version, version vector) byte-exactly and keeps local data as
 * a cache; sync is the durability story.
 */

export const VERSION = "0.1.0";

export { Database } from "./db.js";
export type {
  OpenOptions,
  RemoteOptions,
  TabsOptions,
  SyncOptions,
  LiveOptions,
} from "./db.js";

export { BroadcastChannelBus, LeaderChangedError } from "./tabs/channel.js";
export type { Broadcaster } from "./tabs/channel.js";
export type { LockManager } from "./tabs/leader.js";

export { MemoryAdapter } from "./store/memory.js";
export type {
  StorageAdapter,
  StorageArea,
  DocRecord,
  AttRef,
} from "./store/types.js";
export type { AttInfo } from "./attachments/att.js";
export type { BlobStore } from "./store/blobstore.js";

export type { SyncFilter, WireCond } from "./wire/filters.js";
export { SyncError } from "./wire/errors.js";
export type { SyncErrorCode } from "./wire/errors.js";
export type { LiveHandle } from "./sync/syncer.js";
export type { DocChange, SyncStatus, SyncState } from "./sync/status.js";
export type {
  ConflictEvent,
  OnConflict,
  PullStats,
  PushStats,
} from "./sync/types.js";
export { detectEnv } from "./env.js";
export type { Env } from "./env.js";
