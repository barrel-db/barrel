import { describe, expect, it } from "vitest";
import { TabCoordinator } from "../src/tabs/coordinator.js";
import { LeaderChangedError } from "../src/tabs/channel.js";
import { BusHub, FakeLockManager } from "./tab-doubles.js";
import type { JsonValue } from "../src/json.js";

const LOCK = "barrel-lite:db:test";

function makeCoordinator(hub: BusHub, locks: FakeLockManager, seq: { n: number }) {
  return new TabCoordinator({
    locks,
    bus: hub.connect(),
    lockName: LOCK,
    genId: () => `rpc-${++seq.n}`,
  });
}

async function tick(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

describe("leader election", () => {
  it("promotes the first campaigner and queues the rest", async () => {
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const seq = { n: 0 };
    const a = makeCoordinator(hub, locks, seq);
    const b = makeCoordinator(hub, locks, seq);
    a.start();
    b.start();
    await tick();
    expect(a.isLeader).toBe(true);
    expect(b.isLeader).toBe(false);
    a.stop();
    b.stop();
  });

  it("promotes the next tab when the leader resigns", async () => {
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const seq = { n: 0 };
    const a = makeCoordinator(hub, locks, seq);
    const b = makeCoordinator(hub, locks, seq);
    a.start();
    b.start();
    await tick();
    a.stop(); // resign
    await tick();
    expect(b.isLeader).toBe(true);
    b.stop();
  });

  it("announces itself with a leader-hello followers hear", async () => {
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const seq = { n: 0 };
    const a = makeCoordinator(hub, locks, seq);
    const b = makeCoordinator(hub, locks, seq);
    let helloSeen = false;
    b.onLeaderChange(() => {
      helloSeen = true;
    });
    a.start(); // posts leader-hello on promotion
    await tick();
    expect(helloSeen).toBe(true);
    a.stop();
    b.stop();
  });
});

describe("follower RPC", () => {
  it("round-trips a call to the leader's handler", async () => {
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const seq = { n: 0 };
    const leader = makeCoordinator(hub, locks, seq);
    const follower = makeCoordinator(hub, locks, seq);
    leader.start();
    follower.start();
    await tick();
    leader.serve(async (method, args): Promise<JsonValue> => {
      if (method === "echo") return args[0] ?? null;
      throw new Error(`unknown method ${method}`);
    });
    await expect(follower.call("echo", ["hi"])).resolves.toBe("hi");
    leader.stop();
    follower.stop();
  });

  it("propagates a handler error to the caller", async () => {
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const seq = { n: 0 };
    const leader = makeCoordinator(hub, locks, seq);
    const follower = makeCoordinator(hub, locks, seq);
    leader.start();
    follower.start();
    await tick();
    leader.serve(async () => {
      throw new Error("boom");
    });
    await expect(follower.call("x", [])).rejects.toThrow("boom");
    leader.stop();
    follower.stop();
  });

  it("rejects an in-flight call with LeaderChangedError on promotion", async () => {
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const seq = { n: 0 };
    const leader = makeCoordinator(hub, locks, seq);
    const follower = makeCoordinator(hub, locks, seq);
    leader.start();
    follower.start();
    await tick();
    // leader never serves this method, so the call stays pending
    const pending = follower.call("stuck", []);
    // the leader dies; the follower is promoted and rejects its own call
    leader.stop();
    await tick();
    expect(follower.isLeader).toBe(true);
    await expect(pending).rejects.toBeInstanceOf(LeaderChangedError);
    follower.stop();
  });

  it("fans change and status events out to other tabs", async () => {
    const hub = new BusHub();
    const locks = new FakeLockManager();
    const seq = { n: 0 };
    const leader = makeCoordinator(hub, locks, seq);
    const follower = makeCoordinator(hub, locks, seq);
    leader.start();
    follower.start();
    await tick();
    const changes: string[] = [];
    follower.onRemoteChange((c) => changes.push(c.id));
    leader.broadcastChange({ id: "doc1", deleted: false, source: "local" });
    expect(changes).toEqual(["doc1"]);
    leader.stop();
    follower.stop();
  });
});
