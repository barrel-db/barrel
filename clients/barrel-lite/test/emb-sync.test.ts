import { beforeEach, describe, expect, it } from "vitest";
import { pullEmbeddings } from "../src/vectors/emb-sync.js";
import { SyncTransport } from "../src/wire/transport.js";
import { LocalStore } from "../src/store/localstore.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { HlcClock } from "../src/codec/hlc.js";
import { FakeServer } from "./fake-server.js";

const URL = "http://fake";
const DB = "sp_test";

describe("pullEmbeddings", () => {
  let server: FakeServer;
  let store: LocalStore;
  let transport: SyncTransport;

  beforeEach(async () => {
    server = new FakeServer();
    const clock = new HlcClock({ physClock: () => 1000 });
    const area = await new MemoryAdapter().open("barrel-lite/t");
    store = await LocalStore.open(area, clock);
    transport = new SyncTransport({ url: URL, db: DB, clock, fetch: server.fetch });
  });

  it("fetches vectors for held docs, byte-exact", async () => {
    store.put("a", { id: "a", v: 1 });
    store.put("b", { id: "b", v: 2 });
    const va = [0.1, -0.2, 0.3];
    const vb = [0.4, 0.5, 0.6];
    server.seedEmbedding("a", va);
    server.seedEmbedding("b", vb);

    const { fetched } = await pullEmbeddings(transport, store);
    expect(fetched).toBe(2);
    expect(Array.from(store.getVector("a") as Float32Array)).toEqual(
      Array.from(Float32Array.from(va)),
    );
    expect(store.vectorDim()).toBe(3);
  });

  it("a second pull fetches nothing new (already have the vectors)", async () => {
    store.put("a", { id: "a" });
    server.seedEmbedding("a", [1, 2, 3]);
    await pullEmbeddings(transport, store);
    const { fetched } = await pullEmbeddings(transport, store);
    expect(fetched).toBe(0);
  });

  it("refetchAll re-reads even docs that already have a vector", async () => {
    store.put("a", { id: "a" });
    server.seedEmbedding("a", [1, 2, 3]);
    await pullEmbeddings(transport, store);
    const { fetched } = await pullEmbeddings(transport, store, { refetchAll: true });
    expect(fetched).toBe(1);
  });

  it("skips docs the server has no vector for", async () => {
    store.put("a", { id: "a" });
    store.put("b", { id: "b" });
    server.seedEmbedding("a", [1, 2, 3]); // b has none
    const { fetched } = await pullEmbeddings(transport, store);
    expect(fetched).toBe(1);
    expect(store.getVector("b")).toBeUndefined();
  });

  it("GCs vectors for removed docs", async () => {
    store.put("a", { id: "a" });
    server.seedEmbedding("a", [1, 2, 3]);
    await pullEmbeddings(transport, store);
    expect(store.getVector("a")).toBeDefined();
    store.remove("a"); // tombstone
    await pullEmbeddings(transport, store);
    expect(store.getVector("a")).toBeUndefined();
  });
});
