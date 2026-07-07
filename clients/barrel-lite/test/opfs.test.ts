import { describe, expect, it } from "vitest";
import { OpfsAdapter } from "../src/store/opfs.js";
import { LocalStore } from "../src/store/localstore.js";
import { HlcClock } from "../src/codec/hlc.js";
import { FakeDir } from "./fake-opfs.js";

describe("OPFS adapter", () => {
  it("nests namespace dirs and round-trips a blob", async () => {
    const root = new FakeDir();
    const area = await new OpfsAdapter({ root }).open("barrel-lite/mydb");
    const data = new Uint8Array([1, 2, 3, 4]);
    await area.write("docs.json", data);
    expect(await area.read("docs.json")).toEqual(data);
    // the tree nested barrel-lite/mydb
    const lite = root.dirs.get("barrel-lite");
    expect(lite?.dirs.get("mydb")).toBeDefined();
  });

  it("returns undefined for a missing file", async () => {
    const area = await new OpfsAdapter({ root: new FakeDir() }).open("ns");
    expect(await area.read("absent")).toBeUndefined();
  });

  it("replaces atomically and lists files", async () => {
    const area = await new OpfsAdapter({ root: new FakeDir() }).open("ns");
    await area.write("a", new Uint8Array([1]));
    await area.write("a", new Uint8Array([2, 2]));
    expect(await area.read("a")).toEqual(new Uint8Array([2, 2]));
    await area.write("b", new Uint8Array([9]));
    expect((await area.list()).sort()).toEqual(["a", "b"]);
    await area.remove("a");
    expect(await area.read("a")).toBeUndefined();
  });

  it("backs a LocalStore that reloads across reopen", async () => {
    const root = new FakeDir();
    const adapter = new OpfsAdapter({ root });
    const area1 = await adapter.open("barrel-lite/s");
    const store1 = await LocalStore.open(area1, new HlcClock({ physClock: () => 1000 }));
    store1.put("doc1", { v: 1 });
    await store1.flush();

    const area2 = await adapter.open("barrel-lite/s");
    const store2 = await LocalStore.open(area2, new HlcClock({ physClock: () => 0 }));
    expect(store2.getBody("doc1")).toEqual({ v: 1 });
    expect(store2.sourceId).toBe(store1.sourceId);
  });
});
