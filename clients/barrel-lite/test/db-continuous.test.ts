import { describe, expect, it, vi } from "vitest";
import { Database } from "../src/db.js";
import { MemoryAdapter } from "../src/store/memory.js";
import { FakeServer } from "./fake-server.js";
import type { FetchLike } from "../src/wire/transport.js";
import { encodeHlc } from "../src/codec/hlc.js";
import { base64Encode } from "../src/codec/base64.js";

const URL = "http://fake";
const DB = "sp_test";

function cursor(wall: number): string {
  return base64Encode(encodeHlc({ wall: BigInt(wall), logical: 0 }), "urlsafe");
}

/** A controllable SSE stream a test can push frames into. */
class SseChannel {
  private controller: ReadableStreamDefaultController<Uint8Array> | undefined;
  private readonly enc = new TextEncoder();
  stream(): ReadableStream<Uint8Array> {
    return new ReadableStream<Uint8Array>({
      start: (c) => {
        this.controller = c;
      },
    });
  }
  emit(frame: string): void {
    this.controller?.enqueue(this.enc.encode(frame));
  }
  close(): void {
    this.controller?.close();
  }
}

/** Route /changes to the SSE channel; everything else to the fake server. */
function combined(server: FakeServer, sse: SseChannel): FetchLike {
  return async (url, init) => {
    if (url.includes("/changes?feed=continuous")) {
      return new Response(sse.stream(), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      });
    }
    return server.fetch(url, init);
  };
}

describe("continuous liveSync", () => {
  it("an SSE change wakes the poller and pulls the doc", async () => {
    const server = new FakeServer();
    const sse = new SseChannel();
    const db = await Database.open("c1", {
      remote: { url: URL, db: DB, fetch: combined(server, sse) },
      storage: new MemoryAdapter(),
    });
    // huge poll interval so only the stream can drive the pull in time
    db.liveSync({ continuous: true, minIntervalMs: 100_000, wakeDebounceMs: 10 });

    // let the initial poller tick settle (nothing to pull yet)
    await vi.waitFor(() => expect(db.get("nope")).resolves.toBeUndefined());

    // a doc appears on the server; the SSE change wakes a pull for it
    server.seed("r1", { v: 1 }, "srv0000000000000", 100);
    sse.emit(`data: ${JSON.stringify({ id: "r1", hlc: cursor(100), rev: "x" })}\n\n`);

    await vi.waitFor(
      async () => expect(await db.get("r1")).toMatchObject({ v: 1 }),
      { timeout: 3000 },
    );
    await db.close();
  });

  it("stop() halts the stream so later frames do not wake a pull", async () => {
    const server = new FakeServer();
    const sse = new SseChannel();
    const db = await Database.open("c2", {
      remote: { url: URL, db: DB, fetch: combined(server, sse) },
      storage: new MemoryAdapter(),
    });
    const handle = db.liveSync({ continuous: true, minIntervalMs: 100_000, wakeDebounceMs: 10 });
    await vi.waitFor(() => expect(db.get("nope")).resolves.toBeUndefined());
    handle.stop();

    // a change after stop must not be pulled (poll interval is huge)
    server.seed("r2", { v: 2 }, "srv0000000000000", 200);
    sse.emit(`data: ${JSON.stringify({ id: "r2", hlc: cursor(200), rev: "x" })}\n\n`);
    await new Promise((r) => setTimeout(r, 100));
    expect(await db.get("r2")).toBeUndefined();
    await db.close();
  });
});
