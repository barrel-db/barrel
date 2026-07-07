import { describe, expect, it } from "vitest";
import { detectEnv } from "../src/env.js";
import { Database } from "../src/db.js";
import { MemoryAdapter } from "../src/store/memory.js";

describe("detectEnv", () => {
  it("reports booleans and no browser storage/locks under Node", () => {
    const env = detectEnv();
    expect(typeof env.hasWebLocks).toBe("boolean");
    expect(typeof env.hasBroadcastChannel).toBe("boolean");
    expect(typeof env.hasOpfs).toBe("boolean");
    // Node has BroadcastChannel but neither Web Locks nor OPFS
    expect(env.hasWebLocks).toBe(false);
    expect(env.hasOpfs).toBe(false);
  });

  it("stays single-instance when multiTab is asked for without Web Locks", async () => {
    // no injected tabs + Node (no navigator.locks) => no coordinator
    const db = await Database.open("env1", {
      storage: new MemoryAdapter(),
      multiTab: true,
    });
    expect(db.isLeader).toBe(true); // single instance, always leader
    await db.put({ id: "a", v: 1 });
    expect(await db.get("a")).toMatchObject({ v: 1 });
    await db.close();
  });
});
