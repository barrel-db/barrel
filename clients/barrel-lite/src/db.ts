/**
 * The public Database facade: a local document cache that syncs over
 * the barrel wire. Local reads and writes hit the in-memory store;
 * push/pull move documents; liveSync keeps them converging by adaptive
 * polling. onChange/onStatus fan out local and pulled changes.
 *
 * Step 9a is single-instance (always leader); the multi-tab leader
 * election wraps this in the next step.
 */
import { HlcClock } from "./codec/hlc.js";
import type { JsonObject } from "./json.js";
import { MemoryAdapter } from "./store/memory.js";
import { LocalStore } from "./store/localstore.js";
import type { DocRecord, StorageAdapter } from "./store/types.js";
import { SyncTransport, type FetchLike } from "./wire/transport.js";
import type { SyncFilter } from "./wire/filters.js";
import { pull } from "./sync/puller.js";
import { push } from "./sync/pusher.js";
import { LiveSync, type LiveHandle, type LiveTuning } from "./sync/syncer.js";
import type { DocChange, SyncStatus } from "./sync/status.js";
import type { OnConflict, PullStats, PushStats } from "./sync/types.js";

export interface RemoteOptions {
  url: string;
  db: string;
  token?: string;
  fetch?: FetchLike;
}

export interface OpenOptions {
  remote?: RemoteOptions;
  storage?: StorageAdapter;
  onConflict?: OnConflict;
  /** Injected physical clock (tests); defaults to Date.now. */
  physClock?: () => number;
  /** Debounced local-write flush interval; default 200 ms. */
  flushDebounceMs?: number;
}

export interface SyncOptions {
  filter?: SyncFilter;
}

export type LiveOptions = SyncOptions & LiveTuning;

const FLUSH_DEBOUNCE_MS = 200;

type Listener<T> = (event: T) => void;

export class Database {
  private live: LiveSync | undefined;
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly changeListeners = new Set<Listener<DocChange>>();
  private readonly statusListeners = new Set<Listener<SyncStatus>>();
  private closed = false;

  private constructor(
    readonly name: string,
    private readonly store: LocalStore,
    private readonly clock: HlcClock,
    private readonly transport: SyncTransport | undefined,
    private readonly remote: RemoteOptions | undefined,
    private readonly onConflict: OnConflict | undefined,
    private readonly flushDebounceMs: number,
  ) {}

  static async open(name: string, opts: OpenOptions = {}): Promise<Database> {
    const clockOpts = opts.physClock ? { physClock: opts.physClock } : {};
    const clock = new HlcClock(clockOpts);
    const adapter = opts.storage ?? new MemoryAdapter();
    const area = await adapter.open(`barrel-lite/${name}`);
    const store = await LocalStore.open(area, clock);
    let transport: SyncTransport | undefined;
    if (opts.remote) {
      const t: {
        url: string;
        db: string;
        clock: HlcClock;
        token?: string;
        fetch?: FetchLike;
      } = { url: opts.remote.url, db: opts.remote.db, clock };
      if (opts.remote.token !== undefined) t.token = opts.remote.token;
      if (opts.remote.fetch !== undefined) t.fetch = opts.remote.fetch;
      transport = new SyncTransport(t);
    }
    return new Database(
      name,
      store,
      clock,
      transport,
      opts.remote,
      opts.onConflict,
      opts.flushDebounceMs ?? FLUSH_DEBOUNCE_MS,
    );
  }

  get sourceId(): string {
    return this.store.sourceId;
  }

  /** Always true in single-instance mode; the leader layer overrides. */
  get isLeader(): boolean {
    return true;
  }

  //==================================================================
  // Local CRUD
  //==================================================================

  async get(id: string): Promise<JsonObject | undefined> {
    return this.store.getBody(id);
  }

  async put(doc: JsonObject & { id: string }): Promise<{ id: string; version: string }> {
    const { id, ...body } = doc;
    const rec = this.store.put(id, { ...body, id });
    this.emitChange({ id, deleted: false, source: "local" });
    this.afterMutation();
    return { id, version: rec.version };
  }

  async remove(id: string): Promise<{ id: string; version: string }> {
    const rec = this.store.remove(id);
    this.emitChange({ id, deleted: true, source: "local" });
    this.afterMutation();
    return { id, version: rec.version };
  }

  async allDocs(opts: { includeDeleted?: boolean } = {}): Promise<DocRecord[]> {
    return this.store.allDocs(opts);
  }

  //==================================================================
  // Sync
  //==================================================================

  async push(): Promise<PushStats> {
    const t = this.requireRemote();
    await this.flushNow();
    this.emitStatus({ state: "syncing" });
    const conflictOpt = this.onConflict ? { onConflict: this.onConflict } : {};
    const stats = await push(t, this.store, conflictOpt);
    this.emitStatus({ state: "idle" });
    return stats;
  }

  async pull(opts: SyncOptions = {}): Promise<PullStats> {
    const t = this.requireRemote();
    const remote = this.remote as RemoteOptions;
    this.emitStatus({ state: "syncing" });
    const stats = await pull(t, this.store, {
      url: remote.url,
      db: remote.db,
      ...(opts.filter ? { filter: opts.filter } : {}),
      ...(this.onConflict ? { onConflict: this.onConflict } : {}),
      onApply: (id, outcome, deleted) => {
        if (outcome !== "skip" && outcome !== "local_wins") {
          this.emitChange({ id, deleted, source: "remote" });
        }
      },
    });
    this.emitStatus({ state: "idle" });
    return stats;
  }

  async sync(opts: SyncOptions = {}): Promise<{ push: PushStats; pull: PullStats }> {
    const pushStats = await this.push();
    const pullStats = await this.pull(opts);
    return { push: pushStats, pull: pullStats };
  }

  liveSync(opts: LiveOptions = {}): LiveHandle {
    this.requireRemote();
    if (this.live) this.live.stop();
    const syncOpts: SyncOptions = opts.filter ? { filter: opts.filter } : {};
    const live = new LiveSync(
      async () => {
        const pushStats = await this.push();
        const pullStats = await this.pull(syncOpts);
        return { changed: pushStats.pushed > 0 || pullStats.applied > 0 };
      },
      opts,
      (s) => this.emitStatus(s),
    );
    this.live = live;
    live.start();
    return live;
  }

  //==================================================================
  // Events
  //==================================================================

  onChange(fn: Listener<DocChange>): () => void {
    this.changeListeners.add(fn);
    return () => this.changeListeners.delete(fn);
  }

  onStatus(fn: Listener<SyncStatus>): () => void {
    this.statusListeners.add(fn);
    return () => this.statusListeners.delete(fn);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (this.live) this.live.stop();
    await this.flushNow();
  }

  //==================================================================
  // Internals
  //==================================================================

  private requireRemote(): SyncTransport {
    if (!this.transport) {
      throw new Error("no remote configured: open with a `remote` option");
    }
    return this.transport;
  }

  private afterMutation(): void {
    this.scheduleFlush();
    if (this.live) this.live.wake();
  }

  private scheduleFlush(): void {
    if (this.flushTimer !== undefined) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined;
      void this.store.flush();
    }, this.flushDebounceMs);
  }

  private async flushNow(): Promise<void> {
    if (this.flushTimer !== undefined) {
      clearTimeout(this.flushTimer);
      this.flushTimer = undefined;
    }
    if (this.store.dirtyForFlush) await this.store.flush();
  }

  private emitChange(change: DocChange): void {
    for (const fn of this.changeListeners) fn(change);
  }

  private emitStatus(status: SyncStatus): void {
    for (const fn of this.statusListeners) fn(status);
  }
}
