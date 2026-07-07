/**
 * The public Database facade: a local document cache that syncs over
 * the barrel wire. Local reads and writes hit the in-memory store;
 * push/pull move documents; liveSync keeps them converging by adaptive
 * polling. onChange/onStatus fan out local and pulled changes.
 *
 * Multi-tab: when enabled, one tab per origin holds a Web Lock and
 * owns the store and sync; other tabs proxy their operations to it as
 * RPC and receive change/status fan-out, so UI code is identical on
 * either side. The leader drives sync; a promoted follower reloads the
 * store and takes over.
 */
import { HlcClock } from "./codec/hlc.js";
import type { JsonObject, JsonValue } from "./json.js";
import { MemoryAdapter } from "./store/memory.js";
import { OpfsAdapter } from "./store/opfs.js";
import { LocalStore } from "./store/localstore.js";
import type { DocRecord, StorageAdapter, StorageArea } from "./store/types.js";
import { SyncTransport, type FetchLike } from "./wire/transport.js";
import type { SyncFilter } from "./wire/filters.js";
import { pull } from "./sync/puller.js";
import { push } from "./sync/pusher.js";
import { LiveSync, type LiveHandle, type LiveTuning } from "./sync/syncer.js";
import { openChangesStream, type ChangesStreamHandle } from "./sync/changes-stream.js";
import type { DocChange, SyncStatus } from "./sync/status.js";
import type { OnConflict, PullStats, PushStats } from "./sync/types.js";
import { detectEnv } from "./env.js";
import { TabCoordinator } from "./tabs/coordinator.js";
import type { LockManager } from "./tabs/leader.js";
import { BroadcastChannelBus, type Broadcaster } from "./tabs/channel.js";

export interface RemoteOptions {
  url: string;
  db: string;
  token?: string;
  fetch?: FetchLike;
}

export interface TabsOptions {
  locks: LockManager;
  bus: Broadcaster;
}

export interface OpenOptions {
  remote?: RemoteOptions;
  storage?: StorageAdapter;
  onConflict?: OnConflict;
  /** Enable single-writer-per-origin leader election. */
  multiTab?: boolean;
  /** Injected Web Locks / BroadcastChannel doubles (tests). */
  tabs?: TabsOptions;
  /** Injected physical clock (tests); defaults to Date.now. */
  physClock?: () => number;
  /** Debounced local-write flush interval; default 200 ms. */
  flushDebounceMs?: number;
}

export interface SyncOptions {
  filter?: SyncFilter;
}

export type LiveOptions = SyncOptions &
  LiveTuning & {
    /** Hold a continuous SSE changes stream instead of polling alone. */
    continuous?: boolean;
  };

const STREAM_REOPEN_MIN_MS = 1_000;
const STREAM_REOPEN_MAX_MS = 60_000;

const FLUSH_DEBOUNCE_MS = 200;

type Listener<T> = (event: T) => void;

/** Resolve tab coordination transports: injected doubles, or the
 * browser's Web Locks + BroadcastChannel when both are present. */
function resolveTabs(
  name: string,
  injected: TabsOptions | undefined,
  env: { hasWebLocks: boolean; hasBroadcastChannel: boolean },
): TabsOptions | undefined {
  if (injected) return injected;
  if (env.hasWebLocks && env.hasBroadcastChannel) {
    return {
      locks: (navigator as unknown as { locks: LockManager }).locks,
      bus: new BroadcastChannelBus(`barrel-lite:db:${name}`),
    };
  }
  return undefined;
}

export class Database {
  private store: LocalStore;
  private live: LiveSync | undefined;
  private flushTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly changeListeners = new Set<Listener<DocChange>>();
  private readonly statusListeners = new Set<Listener<SyncStatus>>();
  private closed = false;
  // continuous SSE changes stream (leader-only, additive over the poller)
  private continuous = false;
  private changesStream: ChangesStreamHandle | undefined;
  private changesCursor: string | undefined;
  private streamReopenTimer: ReturnType<typeof setTimeout> | undefined;
  private streamBackoffMs = STREAM_REOPEN_MIN_MS;

  private constructor(
    readonly name: string,
    store: LocalStore,
    private readonly area: StorageArea,
    private readonly clock: HlcClock,
    private readonly transport: SyncTransport | undefined,
    private readonly remote: RemoteOptions | undefined,
    private readonly onConflict: OnConflict | undefined,
    private readonly flushDebounceMs: number,
    private readonly coordinator: TabCoordinator | undefined,
  ) {
    this.store = store;
    if (coordinator) {
      coordinator.onRemoteChange((c) => this.fire(c));
      coordinator.onRemoteStatus((s) => this.fireStatus(s));
      coordinator.onPromote(() => void this.onPromoted());
      coordinator.start();
    }
  }

  static async open(name: string, opts: OpenOptions = {}): Promise<Database> {
    const clockOpts = opts.physClock ? { physClock: opts.physClock } : {};
    const clock = new HlcClock(clockOpts);
    const env = detectEnv();
    const adapter =
      opts.storage ?? (env.hasOpfs ? new OpfsAdapter() : new MemoryAdapter());
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
    let coordinator: TabCoordinator | undefined;
    if (opts.multiTab) {
      const tabs = resolveTabs(name, opts.tabs, env);
      if (tabs) {
        coordinator = new TabCoordinator({
          locks: tabs.locks,
          bus: tabs.bus,
          lockName: `barrel-lite:db:${name}`,
        });
      }
    }
    return new Database(
      name,
      store,
      area,
      clock,
      transport,
      opts.remote,
      opts.onConflict,
      opts.flushDebounceMs ?? FLUSH_DEBOUNCE_MS,
      coordinator,
    );
  }

  get sourceId(): string {
    return this.store.sourceId;
  }

  get isLeader(): boolean {
    return this.coordinator ? this.coordinator.isLeader : true;
  }

  //==================================================================
  // Local CRUD (leader-local or proxied to the leader)
  //==================================================================

  async get(id: string): Promise<JsonObject | undefined> {
    if (this.isFollower()) {
      const r = await this.callLeader("get", [id]);
      return r === null ? undefined : (r as JsonObject);
    }
    return this.store.getBody(id);
  }

  async put(doc: JsonObject & { id: string }): Promise<{ id: string; version: string }> {
    if (this.isFollower()) {
      return (await this.callLeader("put", [doc])) as unknown as {
        id: string;
        version: string;
      };
    }
    const { id, ...body } = doc;
    const rec = this.store.put(id, { ...body, id });
    this.announce({ id, deleted: false, source: "local" });
    this.afterMutation();
    return { id, version: rec.version };
  }

  async remove(id: string): Promise<{ id: string; version: string }> {
    if (this.isFollower()) {
      return (await this.callLeader("remove", [id])) as unknown as {
        id: string;
        version: string;
      };
    }
    const rec = this.store.remove(id);
    this.announce({ id, deleted: true, source: "local" });
    this.afterMutation();
    return { id, version: rec.version };
  }

  async allDocs(opts: { includeDeleted?: boolean } = {}): Promise<DocRecord[]> {
    if (this.isFollower()) {
      return (await this.callLeader("allDocs", [opts as JsonValue])) as unknown as DocRecord[];
    }
    return this.store.allDocs(opts);
  }

  //==================================================================
  // Sync (leader-only work; a follower proxies the request)
  //==================================================================

  async push(): Promise<PushStats> {
    if (this.isFollower()) {
      return (await this.callLeader("push", [])) as unknown as PushStats;
    }
    const t = this.requireRemote();
    await this.flushNow();
    this.announceStatus({ state: "syncing" });
    const conflictOpt = this.onConflict ? { onConflict: this.onConflict } : {};
    const stats = await push(t, this.store, conflictOpt);
    this.announceStatus({ state: "idle" });
    return stats;
  }

  async pull(opts: SyncOptions = {}): Promise<PullStats> {
    if (this.isFollower()) {
      return (await this.callLeader("pull", [opts as JsonValue])) as unknown as PullStats;
    }
    const t = this.requireRemote();
    const remote = this.remote as RemoteOptions;
    this.announceStatus({ state: "syncing" });
    const stats = await pull(t, this.store, {
      url: remote.url,
      db: remote.db,
      ...(opts.filter ? { filter: opts.filter } : {}),
      ...(this.onConflict ? { onConflict: this.onConflict } : {}),
      onApply: (id, outcome, deleted) => {
        if (outcome !== "skip" && outcome !== "local_wins") {
          this.announce({ id, deleted, source: "remote" });
        }
      },
    });
    this.announceStatus({ state: "idle" });
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
    this.continuous = opts.continuous === true;
    const syncOpts: SyncOptions = opts.filter ? { filter: opts.filter } : {};
    const live = new LiveSync(
      async () => {
        // only the leader drives sync; a follower's loop idles until
        // promotion, then seamlessly starts doing the work
        if (this.isFollower()) return { changed: false };
        const pushStats = await this.push();
        const pullStats = await this.pull(syncOpts);
        return { changed: pushStats.pushed > 0 || pullStats.applied > 0 };
      },
      opts,
      (s) => this.fireStatus(s),
    );
    this.live = live;
    live.start();
    // the SSE stream is a wake signal on top of the poller (the floor)
    if (this.continuous && !this.isFollower()) this.openChangesStream();
    return {
      stop: () => {
        this.continuous = false;
        this.stopChangesStream();
        live.stop();
      },
    };
  }

  //==================================================================
  // Continuous changes stream (leader-only, additive over the poller)
  //==================================================================

  private openChangesStream(): void {
    if (this.closed || !this.continuous || this.isFollower() || this.changesStream) {
      return;
    }
    const remote = this.remote as RemoteOptions;
    const streamOpts: Parameters<typeof openChangesStream>[0] = {
      url: remote.url,
      db: remote.db,
      onChange: () => this.live?.wake(),
      onError: (e) => this.onStreamError(e),
      onOpen: () => {
        this.streamBackoffMs = STREAM_REOPEN_MIN_MS;
      },
    };
    if (remote.token !== undefined) streamOpts.token = remote.token;
    if (remote.fetch !== undefined) streamOpts.fetch = remote.fetch;
    if (this.changesCursor !== undefined) streamOpts.since = this.changesCursor;
    this.changesStream = openChangesStream(streamOpts);
  }

  private onStreamError(_e: Error): void {
    this.changesCursor = this.changesStream?.cursor() ?? this.changesCursor;
    this.changesStream = undefined;
    if (this.closed || !this.continuous || this.isFollower()) return;
    // the poller keeps converging; reopen the stream on a backoff
    const delay = this.streamBackoffMs;
    this.streamBackoffMs = Math.min(this.streamBackoffMs * 2, STREAM_REOPEN_MAX_MS);
    this.streamReopenTimer = setTimeout(() => {
      this.streamReopenTimer = undefined;
      this.openChangesStream();
    }, delay);
  }

  private stopChangesStream(): void {
    if (this.streamReopenTimer !== undefined) {
      clearTimeout(this.streamReopenTimer);
      this.streamReopenTimer = undefined;
    }
    if (this.changesStream) {
      this.changesCursor = this.changesStream.cursor() ?? this.changesCursor;
      this.changesStream.close();
      this.changesStream = undefined;
    }
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
    this.continuous = false;
    this.stopChangesStream();
    if (this.live) this.live.stop();
    await this.flushNow();
    if (this.coordinator) this.coordinator.stop();
  }

  //==================================================================
  // Internals
  //==================================================================

  private isFollower(): boolean {
    return this.coordinator !== undefined && !this.coordinator.isLeader;
  }

  private callLeader(method: string, args: JsonValue[]): Promise<JsonValue> {
    return (this.coordinator as TabCoordinator).call(method, args);
  }

  /** On promotion, reload the store (pick up the old leader's flushes)
   * and start answering follower RPCs. */
  private async onPromoted(): Promise<void> {
    if (this.closed) return;
    this.store = await LocalStore.open(this.area, this.clock);
    (this.coordinator as TabCoordinator).serve((m, a) => this.dispatch(m, a));
    // a promoted leader takes over the continuous stream too
    if (this.continuous) this.openChangesStream();
  }

  private async dispatch(method: string, args: JsonValue[]): Promise<JsonValue> {
    switch (method) {
      case "get":
        return (await this.get(args[0] as string)) ?? null;
      case "put":
        return (await this.put(args[0] as JsonObject & { id: string })) as unknown as JsonValue;
      case "remove":
        return (await this.remove(args[0] as string)) as unknown as JsonValue;
      case "allDocs":
        return (await this.allDocs((args[0] as { includeDeleted?: boolean }) ?? {})) as unknown as JsonValue;
      case "push":
        return (await this.push()) as unknown as JsonValue;
      case "pull":
        return (await this.pull((args[0] as SyncOptions) ?? {})) as unknown as JsonValue;
      case "sync":
        return (await this.sync((args[0] as SyncOptions) ?? {})) as unknown as JsonValue;
      default:
        throw new Error(`unknown method ${method}`);
    }
  }

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

  /** Emit a change locally and (as leader) fan it out to followers. */
  private announce(change: DocChange): void {
    this.fire(change);
    if (this.coordinator) this.coordinator.broadcastChange(change);
  }

  private announceStatus(status: SyncStatus): void {
    this.fireStatus(status);
    if (this.coordinator) this.coordinator.broadcastStatus(status);
  }

  private fire(change: DocChange): void {
    for (const fn of this.changeListeners) fn(change);
  }

  private fireStatus(status: SyncStatus): void {
    for (const fn of this.statusListeners) fn(status);
  }
}
