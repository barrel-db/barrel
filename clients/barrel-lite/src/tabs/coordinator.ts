/**
 * Ties leader election to the message bus. One tab is the leader
 * (holds the lock, owns the store and sync); followers proxy their
 * operations to it as RPC and receive change/status fan-out. On
 * promotion the new leader rejects its own in-flight follower calls
 * (they can be retried against itself).
 */
import type { DocChange, SyncStatus } from "../sync/status.js";
import type { JsonValue } from "../json.js";
import { LeaderLock, type LockManager } from "./leader.js";
import {
  LeaderChangedError,
  TabRpc,
  type Broadcaster,
  type TabMessage,
} from "./channel.js";

export interface CoordinatorOptions {
  locks: LockManager;
  bus: Broadcaster;
  lockName: string;
  rpcTimeoutMs?: number;
  genId?: () => string;
}

type Handler = (method: string, args: JsonValue[]) => Promise<JsonValue>;

export class TabCoordinator {
  private readonly lock: LeaderLock;
  private readonly rpc: TabRpc;
  private readonly bus: Broadcaster;
  private promoted = false;
  private readonly promoteListeners = new Set<() => void>();
  private readonly leaderChangeListeners = new Set<() => void>();
  private readonly changeListeners = new Set<(c: DocChange) => void>();
  private readonly statusListeners = new Set<(s: SyncStatus) => void>();
  private readonly unsub: () => void;

  constructor(opts: CoordinatorOptions) {
    this.bus = opts.bus;
    this.lock = new LeaderLock(opts.locks, opts.lockName);
    const rpcOpts: { timeoutMs?: number; genId?: () => string } = {};
    if (opts.rpcTimeoutMs !== undefined) rpcOpts.timeoutMs = opts.rpcTimeoutMs;
    if (opts.genId !== undefined) rpcOpts.genId = opts.genId;
    this.rpc = new TabRpc(this.bus, rpcOpts);
    this.unsub = this.bus.subscribe((msg) => this.onMessage(msg));
  }

  /** Begin campaigning for leadership (does not block on winning). */
  start(): void {
    void this.lock.campaign(() => this.onPromoted());
  }

  get isLeader(): boolean {
    return this.promoted;
  }

  onPromote(fn: () => void): () => void {
    this.promoteListeners.add(fn);
    return () => this.promoteListeners.delete(fn);
  }

  /** Fires when another tab announces itself as leader. */
  onLeaderChange(fn: () => void): () => void {
    this.leaderChangeListeners.add(fn);
    return () => this.leaderChangeListeners.delete(fn);
  }

  /** Leader: handle follower RPC calls. */
  serve(handler: Handler): void {
    this.rpc.serve(handler);
  }

  /** Follower: dispatch an operation to the leader. */
  call(method: string, args: JsonValue[]): Promise<JsonValue> {
    return this.rpc.call(method, args);
  }

  broadcastChange(change: DocChange): void {
    this.bus.post({ type: "change", change });
  }

  broadcastStatus(status: SyncStatus): void {
    this.bus.post({ type: "status", status });
  }

  onRemoteChange(fn: (c: DocChange) => void): () => void {
    this.changeListeners.add(fn);
    return () => this.changeListeners.delete(fn);
  }

  onRemoteStatus(fn: (s: SyncStatus) => void): () => void {
    this.statusListeners.add(fn);
    return () => this.statusListeners.delete(fn);
  }

  stop(): void {
    this.lock.stop();
    this.rpc.close();
    this.unsub();
    this.bus.close();
  }

  private onPromoted(): void {
    this.promoted = true;
    // we are the leader now: nobody else will answer our old calls
    this.rpc.rejectAll(new LeaderChangedError("promoted to leader"));
    this.bus.post({ type: "leader-hello" });
    for (const fn of this.promoteListeners) fn();
  }

  private onMessage(msg: TabMessage): void {
    if (msg.type === "change") {
      for (const fn of this.changeListeners) fn(msg.change);
    } else if (msg.type === "status") {
      for (const fn of this.statusListeners) fn(msg.status);
    } else if (msg.type === "leader-hello") {
      for (const fn of this.leaderChangeListeners) fn();
    }
  }
}
