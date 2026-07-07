import { describe, expect, it, vi } from "vitest";
import { openChangesStream, type ChangeRow } from "../src/sync/changes-stream.js";
import type { FetchLike } from "../src/wire/transport.js";
import { encodeHlc } from "../src/codec/hlc.js";
import { base64Encode } from "../src/codec/base64.js";

function cursor(wall: number): string {
  return base64Encode(encodeHlc({ wall: BigInt(wall), logical: 0 }), "urlsafe");
}

function streamFetch(
  chunks: string[],
  opts: { status?: number; captureUrl?: (u: string) => void } = {},
): FetchLike {
  const enc = new TextEncoder();
  return async (url, _init) => {
    opts.captureUrl?.(url);
    const status = opts.status ?? 200;
    if (status !== 200) {
      return new Response("err", { status });
    }
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const c of chunks) controller.enqueue(enc.encode(c));
        controller.close();
      },
    });
    return new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } });
  };
}

describe("openChangesStream", () => {
  it("delivers changes and tracks the highest cursor", async () => {
    const c1 = cursor(1000);
    const c2 = cursor(2000);
    const changes: ChangeRow[] = [];
    const errors: Error[] = [];
    const handle = openChangesStream({
      url: "http://host",
      db: "sp_x",
      fetch: streamFetch([
        `data: ${JSON.stringify({ id: "a", hlc: c1, rev: "1@x" })}\n\n`,
        `data: ${JSON.stringify({ id: "b", hlc: c2, rev: "2@x", deleted: true })}\n\n`,
      ]),
      onChange: (c) => changes.push(c),
      onError: (e) => errors.push(e),
    });
    await vi.waitFor(() => expect(changes.length).toBe(2));
    expect(changes[0]).toEqual({ id: "a", hlc: c1, rev: "1@x", deleted: false });
    expect(changes[1]?.deleted).toBe(true);
    expect(handle.cursor()).toBe(c2);
    handle.close();
  });

  it("builds the continuous url with the since cursor and bearer", async () => {
    let seen = "";
    const handle = openChangesStream({
      url: "http://host/",
      db: "sp_x",
      token: "bsp_t",
      since: cursor(500),
      fetch: streamFetch([], { captureUrl: (u) => (seen = u) }),
      onChange: () => {},
      onError: () => {},
    });
    await vi.waitFor(() => expect(seen).not.toBe(""));
    expect(seen).toContain("/db/sp_x/changes?feed=continuous&since=");
    handle.close();
  });

  it("skips ping frames", async () => {
    const changes: ChangeRow[] = [];
    const handle = openChangesStream({
      url: "http://host",
      db: "sp_x",
      fetch: streamFetch([
        "event: ping\ndata: {}\n\n",
        `data: ${JSON.stringify({ id: "a", hlc: cursor(1000), rev: "1@x" })}\n\n`,
      ]),
      onChange: (c) => changes.push(c),
      onError: () => {},
    });
    await vi.waitFor(() => expect(changes.length).toBe(1));
    expect(changes[0]?.id).toBe("a");
    handle.close();
  });

  it("surfaces a server error event", async () => {
    const errors: Error[] = [];
    const handle = openChangesStream({
      url: "http://host",
      db: "sp_x",
      fetch: streamFetch(['event: error\ndata: {"error":"boom"}\n\n']),
      onChange: () => {},
      onError: (e) => errors.push(e),
    });
    await vi.waitFor(() => expect(errors.length).toBe(1));
    expect(errors[0]?.message).toBe("boom");
    handle.close();
  });

  it("reports an http error", async () => {
    const errors: Error[] = [];
    const handle = openChangesStream({
      url: "http://host",
      db: "sp_x",
      fetch: streamFetch([], { status: 503 }),
      onChange: () => {},
      onError: (e) => errors.push(e),
    });
    await vi.waitFor(() => expect(errors.length).toBe(1));
    expect(errors[0]?.message).toContain("503");
    handle.close();
  });

  it("reports the stream closing when not closed by the caller", async () => {
    const errors: Error[] = [];
    openChangesStream({
      url: "http://host",
      db: "sp_x",
      fetch: streamFetch([`data: ${JSON.stringify({ id: "a", hlc: cursor(1) })}\n\n`]),
      onChange: () => {},
      onError: (e) => errors.push(e),
    });
    await vi.waitFor(() => expect(errors.some((e) => /closed/.test(e.message))).toBe(true));
  });
});
