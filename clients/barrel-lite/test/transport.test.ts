import { describe, expect, it } from "vitest";
import { SyncTransport, type FetchLike } from "../src/wire/transport.js";
import { SyncError } from "../src/wire/errors.js";
import { HlcClock, encodeHlc } from "../src/codec/hlc.js";
import { base64Encode } from "../src/codec/base64.js";

interface Recorded {
  url: string;
  method: string;
  headers: Record<string, string>;
  body?: unknown;
}

function mockFetch(
  handler: (req: Recorded) => { status: number; body?: unknown; headers?: Record<string, string> },
): { fetch: FetchLike; calls: Recorded[] } {
  const calls: Recorded[] = [];
  const fetchImpl: FetchLike = async (url, init) => {
    const headers: Record<string, string> = {};
    const h = (init?.headers ?? {}) as Record<string, string>;
    for (const [k, v] of Object.entries(h)) headers[k.toLowerCase()] = v;
    const rec: Recorded = {
      url,
      method: init?.method ?? "GET",
      headers,
      body: init?.body ? JSON.parse(init.body as string) : undefined,
    };
    calls.push(rec);
    const r = handler(rec);
    const respHeaders = new Headers(r.headers ?? {});
    const text = r.body === undefined ? "" : JSON.stringify(r.body);
    return new Response(text, { status: r.status, headers: respHeaders });
  };
  return { fetch: fetchImpl, calls };
}

const HLC_HDR = base64Encode(
  encodeHlc({ wall: 1700000000000n, logical: 3 }),
  "standard",
);

function makeTransport(fetchImpl: FetchLike, token?: string) {
  const opts = {
    url: "http://host:8080/",
    db: "sp_abc",
    clock: new HlcClock({ physClock: () => 0 }),
    fetch: fetchImpl,
    ...(token !== undefined ? { token } : {}),
  };
  return new SyncTransport(opts);
}

describe("transport request shapes", () => {
  it("info parses floors and hits the right URL", async () => {
    const { fetch, calls } = mockFetch(() => ({
      status: 200,
      body: { db: "sp_abc", history_floor: "AAAA", att_floor: "BBBB" },
      headers: { "x-barrel-hlc": HLC_HDR },
    }));
    const t = makeTransport(fetch);
    const info = await t.info();
    expect(info).toEqual({ db: "sp_abc", historyFloor: "AAAA", attFloor: "BBBB" });
    expect(calls[0]?.url).toBe("http://host:8080/db/sp_abc/_sync/info");
    expect(calls[0]?.method).toBe("GET");
    expect(calls[0]?.headers["x-barrel-hlc"]).toBeDefined();
  });

  it("changes sends since/limit/filter and parses rows", async () => {
    const { fetch, calls } = mockFetch(() => ({
      status: 200,
      body: {
        changes: [
          {
            id: "a",
            hlc: "AAAA",
            rev: "00@x",
            changes: [{ rev: "00@x" }],
            num_conflicts: 0,
            deleted: true,
          },
        ],
        last_seq: "CURSOR",
      },
    }));
    const t = makeTransport(fetch);
    const page = await t.changes("first", {
      limit: 50,
      filter: { channel: "mobile", paths: ["users/#"] },
    });
    expect(calls[0]?.body).toEqual({
      since: "first",
      limit: 50,
      filter: { channel: "mobile", paths: ["users/#"] },
    });
    expect(page.lastSeq).toBe("CURSOR");
    expect(page.changes[0]).toEqual({
      id: "a",
      hlc: "AAAA",
      rev: "00@x",
      changes: [{ rev: "00@x" }],
      numConflicts: 0,
      deleted: true,
    });
  });

  it("diff maps have/missing", async () => {
    const { fetch, calls } = mockFetch(() => ({
      status: 200,
      body: { diff: { a: "missing", b: "have" } },
    }));
    const t = makeTransport(fetch);
    const diff = await t.diff({ a: "00@x", b: "01@x" });
    expect(calls[0]?.body).toEqual({ versions: { a: "00@x", b: "01@x" } });
    expect(diff).toEqual({ a: "missing", b: "have" });
  });

  it("putVersion sends the four fields and returns the winner", async () => {
    const { fetch, calls } = mockFetch(() => ({
      status: 200,
      body: { id: "a", winner: "05@y" },
    }));
    const t = makeTransport(fetch);
    const res = await t.putVersion("a", {
      doc: { v: 1 },
      version: "05@y",
      vv: "AAEC",
    });
    expect(calls[0]?.method).toBe("PUT");
    expect(calls[0]?.url).toBe("http://host:8080/db/sp_abc/_sync/doc/a");
    expect(calls[0]?.body).toEqual({
      doc: { v: 1 },
      version: "05@y",
      vv: "AAEC",
      deleted: false,
    });
    expect(res).toEqual({ id: "a", winner: "05@y" });
  });

  it("getLocal returns undefined on 404", async () => {
    const { fetch } = mockFetch(() => ({ status: 404, body: { error: "not_found" } }));
    const t = makeTransport(fetch);
    expect(await t.getLocal("cp")).toBeUndefined();
  });

  it("encodes the doc id path segment", async () => {
    const { fetch, calls } = mockFetch(() => ({
      status: 200,
      body: { doc: { id: "users/1" }, version: "00@x", vv: "AAA=", deleted: false },
    }));
    const t = makeTransport(fetch);
    await t.getDoc("users/1");
    expect(calls[0]?.url).toBe("http://host:8080/db/sp_abc/_sync/doc/users%2F1");
  });
});

describe("transport auth and clock", () => {
  it("attaches the bearer token", async () => {
    const { fetch, calls } = mockFetch(() => ({ status: 200, body: { db: "sp_abc" } }));
    const t = makeTransport(fetch, "bsp_token");
    await t.info();
    expect(calls[0]?.headers["authorization"]).toBe("Bearer bsp_token");
  });

  it("folds the response x-barrel-hlc into the clock", async () => {
    const { fetch } = mockFetch(() => ({
      status: 200,
      body: { db: "sp_abc" },
      headers: { "x-barrel-hlc": HLC_HDR },
    }));
    const clock = new HlcClock({ physClock: () => 0 });
    const t = new SyncTransport({
      url: "http://host:8080",
      db: "sp_abc",
      clock,
      fetch,
    });
    await t.info();
    // clock advanced to at least the folded remote wall time
    expect(clock.peek().wall).toBe(1700000000000n);
  });

  it("syncHlc raises clock_skew on 409", async () => {
    const { fetch } = mockFetch(() => ({ status: 409, body: { error: "clock_skew" } }));
    const t = makeTransport(fetch);
    await expect(t.syncHlc()).rejects.toMatchObject({ code: "clock_skew" });
  });

  it("maps 401 and 403 to typed errors", async () => {
    const un = mockFetch(() => ({ status: 401, body: { error: "unauthorized" } }));
    const fb = mockFetch(() => ({ status: 403, body: { error: "forbidden" } }));
    await expect(makeTransport(un.fetch).info()).rejects.toMatchObject({
      code: "unauthorized",
    });
    await expect(makeTransport(fb.fetch).putVersion("a", {
      doc: {},
      version: "00@x",
      vv: "AAA=",
    })).rejects.toMatchObject({ code: "forbidden" });
  });

  it("wraps a fetch failure as a network error", async () => {
    const fetchImpl: FetchLike = async () => {
      throw new Error("offline");
    };
    const t = makeTransport(fetchImpl);
    await expect(t.info()).rejects.toBeInstanceOf(SyncError);
    await expect(t.info()).rejects.toMatchObject({ code: "network" });
  });
});
