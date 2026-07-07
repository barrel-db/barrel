import { beforeEach, describe, expect, it } from "vitest";
import { SyncTransport } from "../src/wire/transport.js";
import { HlcClock, encodeHlc } from "../src/codec/hlc.js";
import { base64Encode } from "../src/codec/base64.js";
import { sha256Digest } from "../src/attachments/digest.js";
import { FakeServer } from "./fake-server.js";

const URL = "http://fake";
const DB = "sp_test";

function origin(wall: number): string {
  return base64Encode(encodeHlc({ wall: BigInt(wall), logical: 0 }), "standard");
}

describe("attachment transport", () => {
  let server: FakeServer;
  let t: SyncTransport;

  beforeEach(() => {
    server = new FakeServer();
    t = new SyncTransport({
      url: URL,
      db: DB,
      clock: new HlcClock({ physClock: () => 0 }),
      fetch: server.fetch,
    });
  });

  it("puts a blob, gets it back with headers, and diffs it as have", async () => {
    const bytes = new TextEncoder().encode("hello blob");
    const digest = await sha256Digest(bytes);
    const res = await t.putAtt("d1", "f.txt", bytes, {
      contentType: "text/plain",
      digest,
      origin: origin(1000),
    });
    expect(res).toBe("written");

    const blob = await t.getAtt("d1", "f.txt");
    expect(blob?.bytes).toEqual(bytes);
    expect(blob?.digest).toBe(digest);
    expect(blob?.contentType).toBe("text/plain");
    expect(blob?.length).toBe(bytes.length);

    const diff = await t.attDiff([{ id: "d1", name: "f.txt", digest }]);
    expect(diff).toEqual([{ id: "d1", name: "f.txt", status: "have" }]);
    const missing = await t.attDiff([{ id: "d1", name: "f.txt", digest: "sha256-00" }]);
    expect(missing[0]?.status).toBe("missing");
  });

  it("returns undefined for a missing blob", async () => {
    expect(await t.getAtt("nope", "x")).toBeUndefined();
  });

  it("maps a digest mismatch and an oversized blob", async () => {
    const bytes = new TextEncoder().encode("data");
    // wrong digest header
    expect(
      await t.putAtt("d1", "f", bytes, {
        contentType: "application/octet-stream",
        digest: "sha256-deadbeef",
        origin: origin(1000),
      }),
    ).toBe("digest_mismatch");
    // oversized
    server.attMaxLength = 2;
    expect(
      await t.putAtt("d1", "big", bytes, {
        contentType: "application/octet-stream",
        digest: await sha256Digest(bytes),
        origin: origin(1000),
      }),
    ).toBe("too_large");
  });

  it("ignores an older-origin write (LWW)", async () => {
    const v2 = new TextEncoder().encode("v2");
    await t.putAtt("d1", "f", v2, {
      contentType: "text/plain",
      digest: await sha256Digest(v2),
      origin: origin(2000),
    });
    const v1 = new TextEncoder().encode("v1");
    const res = await t.putAtt("d1", "f", v1, {
      contentType: "text/plain",
      digest: await sha256Digest(v1),
      origin: origin(1000), // older
    });
    expect(res).toBe("ignored");
    const blob = await t.getAtt("d1", "f");
    expect(blob?.bytes).toEqual(v2);
  });

  it("streams the attachment feed and deletes", async () => {
    await server.seedAtt("d1", "a", new TextEncoder().encode("aaa"), "text/plain", 100);
    await server.seedAtt("d2", "b", new TextEncoder().encode("bb"), "text/plain", 200);
    const feed = await t.attChanges("first", 100);
    expect(feed.unsupported).toBe(false);
    expect(feed.changes.map((r) => r.id).sort()).toEqual(["d1", "d2"]);
    expect(feed.changes[0]?.op).toBe("put");

    await t.deleteAtt("d1", "a", origin(3000));
    expect(await t.getAtt("d1", "a")).toBeUndefined();
  });

  it("reports 501 as unsupported", async () => {
    server.attUnsupported = true;
    const feed = await t.attChanges("first", 100);
    expect(feed.unsupported).toBe(true);
    expect(feed.changes).toEqual([]);
  });
});
