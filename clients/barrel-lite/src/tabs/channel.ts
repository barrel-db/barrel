/**
 * Cross-tab message bus and follower→leader RPC over a Broadcaster
 * (BroadcastChannel in the browser, a double in tests). Followers
 * serialize their store operations to the leader as RPC; the leader
 * fans change and status events out to every tab so UI code is
 * identical on either side.
 */
import type { DocChange, SyncStatus } from "../sync/status.js";
import type { JsonValue } from "../json.js";

export type TabMessage =
  | { type: "change"; change: DocChange }
  | { type: "status"; status: SyncStatus }
  | { type: "leader-hello" }
  | { type: "rpc"; reqId: string; method: string; args: JsonValue[] }
  | { type: "rpc-reply"; reqId: string; ok: true; result: JsonValue }
  | { type: "rpc-reply"; reqId: string; ok: false; error: string };

export interface Broadcaster {
  post(msg: TabMessage): void;
  subscribe(fn: (msg: TabMessage) => void): () => void;
  close(): void;
}

/** Raised when the leader changes while an RPC is in flight (retriable). */
export class LeaderChangedError extends Error {
  constructor(message = "leader changed") {
    super(message);
    this.name = "LeaderChangedError";
  }
}

/** Broadcaster backed by a real BroadcastChannel. */
export class BroadcastChannelBus implements Broadcaster {
  private readonly channel: BroadcastChannel;

  constructor(name: string) {
    this.channel = new BroadcastChannel(name);
  }

  post(msg: TabMessage): void {
    this.channel.postMessage(msg);
  }

  subscribe(fn: (msg: TabMessage) => void): () => void {
    const handler = (e: MessageEvent): void => fn(e.data as TabMessage);
    this.channel.addEventListener("message", handler);
    return () => this.channel.removeEventListener("message", handler);
  }

  close(): void {
    this.channel.close();
  }
}

export interface TabRpcOptions {
  timeoutMs?: number;
  genId?: () => string;
}

interface Pending {
  resolve: (v: JsonValue) => void;
  reject: (e: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

const DEFAULT_TIMEOUT = 10_000;

/** Follower-side caller and leader-side server over one Broadcaster. */
export class TabRpc {
  private readonly pending = new Map<string, Pending>();
  private readonly timeoutMs: number;
  private readonly genId: () => string;
  private seq = 0;
  private handler: ((method: string, args: JsonValue[]) => Promise<JsonValue>) | undefined;
  private readonly unsubscribe: () => void;

  constructor(
    private readonly bus: Broadcaster,
    opts: TabRpcOptions = {},
  ) {
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT;
    this.genId = opts.genId ?? (() => `rpc-${++this.seq}`);
    this.unsubscribe = bus.subscribe((msg) => this.onMessage(msg));
  }

  /** Register the leader's handler for incoming follower calls. */
  serve(handler: (method: string, args: JsonValue[]) => Promise<JsonValue>): void {
    this.handler = handler;
  }

  /** Follower call: dispatch to the leader and await its reply. */
  call(method: string, args: JsonValue[]): Promise<JsonValue> {
    const reqId = this.genId();
    return new Promise<JsonValue>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(reqId);
        reject(new LeaderChangedError("rpc timed out (no leader answered)"));
      }, this.timeoutMs);
      this.pending.set(reqId, { resolve, reject, timer });
      this.bus.post({ type: "rpc", reqId, method, args });
    });
  }

  /** Reject every in-flight call (e.g. on promotion to leader). */
  rejectAll(err: Error): void {
    for (const [, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(err);
    }
    this.pending.clear();
  }

  close(): void {
    this.rejectAll(new LeaderChangedError("closed"));
    this.unsubscribe();
  }

  private onMessage(msg: TabMessage): void {
    if (msg.type === "rpc") {
      void this.dispatch(msg);
    } else if (msg.type === "rpc-reply") {
      const p = this.pending.get(msg.reqId);
      if (!p) return;
      clearTimeout(p.timer);
      this.pending.delete(msg.reqId);
      if (msg.ok) p.resolve(msg.result);
      else p.reject(new Error(msg.error));
    }
  }

  private async dispatch(msg: { reqId: string; method: string; args: JsonValue[] }): Promise<void> {
    if (!this.handler) return; // not the leader; ignore
    try {
      const result = await this.handler(msg.method, msg.args);
      this.bus.post({ type: "rpc-reply", reqId: msg.reqId, ok: true, result });
    } catch (e) {
      this.bus.post({
        type: "rpc-reply",
        reqId: msg.reqId,
        ok: false,
        error: e instanceof Error ? e.message : String(e),
      });
    }
  }
}
