/**
 * In-process doubles for the Web Locks API and BroadcastChannel, so
 * the multi-tab layer runs under vitest without a browser.
 */
import type { LockManager } from "../src/tabs/leader.js";
import type { Broadcaster, TabMessage } from "../src/tabs/channel.js";

interface Waiter {
  callback: () => Promise<void>;
  resolveOuter: () => void;
  signal?: AbortSignal;
}

/** A single-holder exclusive lock with a FIFO wait queue. */
export class FakeLockManager implements LockManager {
  private readonly queues = new Map<string, Waiter[]>();
  private readonly held = new Set<string>();

  request(
    name: string,
    options: { mode: "exclusive"; signal?: AbortSignal },
    callback: () => Promise<void>,
  ): Promise<void> {
    return new Promise<void>((resolveOuter) => {
      if (options.signal?.aborted) {
        resolveOuter();
        return;
      }
      const waiter: Waiter = { callback, resolveOuter };
      if (options.signal) waiter.signal = options.signal;
      const q = this.queues.get(name) ?? [];
      q.push(waiter);
      this.queues.set(name, q);
      this.pump(name);
    });
  }

  private pump(name: string): void {
    if (this.held.has(name)) return;
    const q = this.queues.get(name) ?? [];
    // drop aborted waiters
    while (q.length > 0 && q[0]?.signal?.aborted) {
      q.shift()?.resolveOuter();
    }
    const waiter = q[0];
    if (!waiter) return;
    this.held.add(name);
    // hold until the callback's promise resolves, then promote the next
    void Promise.resolve(waiter.callback()).then(() => {
      q.shift();
      this.held.delete(name);
      waiter.resolveOuter();
      this.pump(name);
    });
  }
}

/** A hub connecting FakeBus instances; delivery excludes the sender. */
export class BusHub {
  private readonly buses: FakeBus[] = [];

  connect(): FakeBus {
    const bus = new FakeBus(this);
    this.buses.push(bus);
    return bus;
  }

  deliver(from: FakeBus, msg: TabMessage): void {
    for (const bus of this.buses) {
      if (bus !== from && !bus.closed) bus.emit(msg);
    }
  }
}

export class FakeBus implements Broadcaster {
  closed = false;
  private readonly subs = new Set<(m: TabMessage) => void>();

  constructor(private readonly hub: BusHub) {}

  post(msg: TabMessage): void {
    this.hub.deliver(this, msg);
  }

  subscribe(fn: (msg: TabMessage) => void): () => void {
    this.subs.add(fn);
    return () => this.subs.delete(fn);
  }

  emit(msg: TabMessage): void {
    for (const fn of this.subs) fn(msg);
  }

  close(): void {
    this.closed = true;
    this.subs.clear();
  }
}
