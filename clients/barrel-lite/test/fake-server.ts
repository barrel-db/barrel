/**
 * An in-memory implementation of the barrel _sync wire, reproducing
 * put_version last-write-wins with the real codecs, so the sync engine
 * can be driven through every conflict path offline and deterministically.
 * Exposed as a FetchLike the real SyncTransport can point at.
 */
import { base64Decode, base64Encode } from "../src/codec/base64.js";
import {
  compareHlc,
  decodeHlc,
  encodeHlc,
  HlcClock,
  type Hlc,
} from "../src/codec/hlc.js";
import {
  compareVersion,
  versionFromToken,
  versionToToken,
  type Version,
} from "../src/codec/version.js";
import {
  vvBump,
  vvCompare,
  vvContains,
  vvDecode,
  vvEncode,
  vvMerge,
  vvNew,
  type VV,
} from "../src/codec/vv.js";
import type { JsonObject } from "../src/json.js";
import type { FetchLike } from "../src/wire/transport.js";
import { sha256Digest } from "../src/attachments/digest.js";

interface Stored {
  body: JsonObject | null;
  version: string; // token
  vv: VV;
  deleted: boolean;
  seq: number;
}

interface StoredAtt {
  op: "put" | "delete";
  origin: string; // std-b64 HLC
  digest: string;
  length: number;
  contentType: string;
  bytes: Uint8Array;
  seq: number;
}

function seqHlc(seq: number): Hlc {
  return { wall: BigInt(seq), logical: 0 };
}

const ATT_SEP = " ";

function attKey(id: string, name: string): string {
  return `${id} ${name}`;
}

/** The server-side LWW guard (barrel_att_feed:check). */
function attCheck(incomingOrigin: string, incomingDigest: string, stored: StoredAtt): boolean {
  const c = compareHlc(decodeHlc(base64Decode(incomingOrigin)), decodeHlc(base64Decode(stored.origin)));
  if (c < 0) return false; // older loses
  if (c > 0) return true; // newer wins
  return incomingDigest > stored.digest; // equal origin: digest tie-break
}

async function bodyBytes(body: unknown): Promise<Uint8Array> {
  if (body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  if (typeof Blob !== "undefined" && body instanceof Blob) {
    return new Uint8Array(await body.arrayBuffer());
  }
  if (typeof body === "string") return new TextEncoder().encode(body);
  return new Uint8Array();
}

export class FakeServer {
  private readonly docs = new Map<string, Stored>();
  private readonly atts = new Map<string, StoredAtt>();
  private seq = 0;
  private attSeq = 0;
  private readonly clock = new HlcClock({ physClock: () => 1_700_000_000_000 });
  historyFloor: string | undefined;
  attFloor: string | undefined;
  /** When true, the attachment feed answers 501 (backend unsupported). */
  attUnsupported = false;
  /** When set, PUT _sync/att answers 413 (oversized). */
  attMaxLength: number | undefined;

  /** Seed a document authored by some other replica (for pull tests). */
  seed(id: string, body: JsonObject | null, author: string, wall: number): void {
    const version: Version = { hlc: { wall: BigInt(wall), logical: 0 }, author };
    const vv = vvBump(vvNew(), version);
    this.seq++;
    this.docs.set(id, {
      body,
      version: versionToToken(version),
      vv,
      deleted: body === null,
      seq: this.seq,
    });
  }

  get fetch(): FetchLike {
    return async (url, init) => {
      const method = init?.method ?? "GET";
      const u = new URL(url);
      const tail = u.pathname.split("/_sync/")[1] ?? "";
      // attachment endpoints are binary-aware and handled first
      if (method === "GET" && tail.startsWith("att_changes")) {
        return this.jsonResponse(this.attChanges(u.searchParams));
      }
      if (method === "POST" && tail === "att_diff") {
        return this.jsonResponse(this.attDiff(this.jsonBody(init)));
      }
      if (tail.startsWith("att/")) {
        return this.attBlob(method, tail.slice(4), init);
      }
      // JSON endpoints
      const body = init?.body ? (JSON.parse(init.body as string) as JsonObject) : undefined;
      return this.jsonResponse(this.route(method, u.pathname, body));
    };
  }

  private jsonResponse(r: { status: number; json?: unknown }): Response {
    const headers = new Headers({
      "content-type": "application/json",
      "x-barrel-hlc": base64Encode(encodeHlc(this.clock.now()), "standard"),
    });
    const text = r.json === undefined ? "" : JSON.stringify(r.json);
    return new Response(text, { status: r.status, headers });
  }

  private jsonBody(init: RequestInit | undefined): JsonObject {
    return init?.body ? (JSON.parse(init.body as string) as JsonObject) : {};
  }

  private route(
    method: string,
    path: string,
    body: JsonObject | undefined,
  ): { status: number; json?: unknown } {
    const tail = path.split("/_sync/")[1] ?? "";
    if (method === "GET" && tail === "info") return this.info();
    if (method === "POST" && tail === "hlc") return { status: 200, json: body };
    if (method === "POST" && tail === "changes") return this.changes(body ?? {});
    if (method === "POST" && tail === "diff") return this.diff(body ?? {});
    if (method === "GET" && tail.startsWith("doc/")) {
      return this.getDoc(decodeURIComponent(tail.slice(4)));
    }
    if (method === "PUT" && tail.startsWith("doc/")) {
      return this.putVersion(decodeURIComponent(tail.slice(4)), body ?? {});
    }
    return { status: 404, json: { error: "not_found" } };
  }

  private info(): { status: number; json: unknown } {
    const json: JsonObject = { db: "fake" };
    if (this.historyFloor) json["history_floor"] = this.historyFloor;
    if (this.attFloor) json["att_floor"] = this.attFloor;
    return { status: 200, json };
  }

  //==================================================================
  // Attachment endpoints
  //==================================================================

  private attChanges(params: URLSearchParams): { status: number; json?: unknown } {
    if (this.attUnsupported) {
      return { status: 501, json: { error: "att_sync_unsupported" } };
    }
    const since = params.get("since") ?? "first";
    const limit = Number(params.get("limit") ?? "100");
    const sinceHlc: Hlc =
      since === "first" ? { wall: 0n, logical: 0 } : decodeHlc(base64Decode(since));
    const rows = [...this.atts.entries()]
      .filter(([, a]) => compareHlc(seqHlc(a.seq), sinceHlc) > 0)
      .sort((a, b) => a[1].seq - b[1].seq)
      .slice(0, limit);
    const changes = rows.map(([key, a]) => {
      const [id, name] = key.split(ATT_SEP);
      return {
        seq: base64Encode(encodeHlc(seqHlc(a.seq)), "standard"),
        origin: a.origin,
        op: a.op,
        id,
        name,
        digest: a.digest,
        length: a.length,
        content_type: a.contentType,
      };
    });
    const last =
      rows.length > 0
        ? base64Encode(encodeHlc(seqHlc((rows[rows.length - 1] as [string, StoredAtt])[1].seq)), "standard")
        : since;
    return { status: 200, json: { changes, last_seq: last } };
  }

  private attDiff(body: JsonObject): { status: number; json?: unknown } {
    const list = Array.isArray(body["attachments"]) ? body["attachments"] : [];
    const diff = list.map((e) => {
      const o = e as JsonObject;
      const stored = this.atts.get(attKey(String(o["id"]), String(o["name"])));
      const status =
        stored && stored.op === "put" && stored.digest === o["digest"] ? "have" : "missing";
      return { id: o["id"], name: o["name"], status };
    });
    return { status: 200, json: { diff } };
  }

  private async attBlob(method: string, tail: string, init: RequestInit | undefined): Promise<Response> {
    const [rawId, rawName] = tail.split("/");
    const id = decodeURIComponent(rawId ?? "");
    const name = decodeURIComponent(rawName ?? "");
    const headers = new Headers(init?.headers as HeadersInit);
    if (method === "GET") return this.attGet(id, name);
    if (method === "PUT") return this.jsonResponse(await this.attPut(id, name, headers, init));
    if (method === "DELETE") return this.jsonResponse(this.attDelete(id, name, headers));
    return this.jsonResponse({ status: 404, json: { error: "not_found" } });
  }

  private attGet(id: string, name: string): Response {
    const a = this.atts.get(attKey(id, name));
    if (!a || a.op === "delete") {
      return this.jsonResponse({ status: 404, json: { error: "not_found" } });
    }
    return new Response(a.bytes as BodyInit, {
      status: 200,
      headers: new Headers({
        "content-type": a.contentType,
        "x-barrel-digest": a.digest,
        "x-barrel-att-length": String(a.length),
        "x-barrel-hlc": base64Encode(encodeHlc(this.clock.now()), "standard"),
      }),
    });
  }

  private async attPut(
    id: string,
    name: string,
    headers: Headers,
    init: RequestInit | undefined,
  ): Promise<{ status: number; json?: unknown }> {
    const origin = headers.get("x-barrel-att-origin");
    if (!origin) return { status: 400, json: { error: "bad_origin" } };
    const expected = headers.get("x-barrel-digest") ?? "";
    const bytes = await bodyBytes(init?.body);
    if (this.attMaxLength !== undefined && bytes.length > this.attMaxLength) {
      return { status: 413, json: { error: "too_large" } };
    }
    const key = attKey(id, name);
    const existing = this.atts.get(key);
    if (existing && !attCheck(origin, expected, existing)) {
      return { status: 200, json: { ok: "ignored" } };
    }
    const actual = await sha256Digest(bytes);
    if (actual !== expected) return { status: 422, json: { error: "digest_mismatch" } };
    this.attSeq++;
    this.atts.set(key, {
      op: "put",
      origin,
      digest: expected,
      length: bytes.length,
      contentType: headers.get("content-type") ?? "application/octet-stream",
      bytes,
      seq: this.attSeq,
    });
    return { status: 200, json: { ok: true } };
  }

  private attDelete(id: string, name: string, headers: Headers): { status: number; json?: unknown } {
    const origin = headers.get("x-barrel-att-origin");
    if (!origin) return { status: 400, json: { error: "bad_origin" } };
    const key = attKey(id, name);
    const existing = this.atts.get(key);
    if (!existing || attCheck(origin, "", existing)) {
      this.attSeq++;
      this.atts.set(key, {
        op: "delete",
        origin,
        digest: "",
        length: 0,
        contentType: "",
        bytes: new Uint8Array(),
        seq: this.attSeq,
      });
    }
    return { status: 200, json: { ok: true } };
  }

  /** Seed an attachment authored by another replica (for pull tests). */
  async seedAtt(
    id: string,
    name: string,
    bytes: Uint8Array,
    contentType: string,
    originWall: number,
  ): Promise<void> {
    const origin = base64Encode(encodeHlc({ wall: BigInt(originWall), logical: 0 }), "standard");
    this.attSeq++;
    this.atts.set(attKey(id, name), {
      op: "put",
      origin,
      digest: await sha256Digest(bytes),
      length: bytes.length,
      contentType,
      bytes,
      seq: this.attSeq,
    });
  }

  private changes(body: JsonObject): { status: number; json: unknown } {
    const since = typeof body["since"] === "string" ? body["since"] : "first";
    const limit = typeof body["limit"] === "number" ? body["limit"] : 100;
    const sinceHlc: Hlc =
      since === "first" ? { wall: 0n, logical: 0 } : decodeHlc(base64Decode(since));
    const rows = [...this.docs.entries()]
      .filter(([, d]) => compareHlc(seqHlc(d.seq), sinceHlc) > 0)
      .sort((a, b) => a[1].seq - b[1].seq)
      .slice(0, limit);
    const changes = rows.map(([id, d]) => ({
      id,
      hlc: base64Encode(encodeHlc(seqHlc(d.seq)), "standard"),
      rev: d.version,
      changes: [{ rev: d.version }],
      num_conflicts: 0,
      deleted: d.deleted,
    }));
    const last =
      rows.length > 0
        ? base64Encode(encodeHlc(seqHlc((rows[rows.length - 1] as [string, Stored])[1].seq)), "standard")
        : since;
    return { status: 200, json: { changes, last_seq: last } };
  }

  private diff(body: JsonObject): { status: number; json: unknown } {
    const versions = (body["versions"] ?? {}) as Record<string, string>;
    const diff: Record<string, string> = {};
    for (const [id, token] of Object.entries(versions)) {
      const stored = this.docs.get(id);
      const version = versionFromToken(token);
      diff[id] = stored && vvContains(stored.vv, version) ? "have" : "missing";
    }
    return { status: 200, json: { diff } };
  }

  private getDoc(id: string): { status: number; json?: unknown } {
    const stored = this.docs.get(id);
    if (!stored) return { status: 404, json: { error: "not_found" } };
    return {
      status: 200,
      json: {
        doc: { ...(stored.body ?? {}), id },
        version: stored.version,
        vv: base64Encode(vvEncode(stored.vv), "standard"),
        deleted: stored.deleted,
      },
    };
  }

  private putVersion(id: string, body: JsonObject): { status: number; json: unknown } {
    const token = String(body["version"]);
    const version = versionFromToken(token);
    const remoteVv = vvDecode(base64Decode(String(body["vv"])));
    const deleted = body["deleted"] === true;
    const doc = deleted ? null : ((body["doc"] ?? {}) as JsonObject);
    const existing = this.docs.get(id);

    if (!existing) {
      this.store(id, doc, token, vvBump(remoteVv, version), deleted);
      return { status: 200, json: { id, winner: token } };
    }
    if (vvContains(existing.vv, version)) {
      return { status: 200, json: { id, winner: existing.version } }; // no-op
    }
    const merged = vvMerge(existing.vv, vvBump(remoteVv, version));
    if (vvCompare(remoteVv, existing.vv) === "dominates") {
      this.store(id, doc, token, merged, deleted);
      return { status: 200, json: { id, winner: token } };
    }
    // concurrent: last-write-wins
    const localVersion = versionFromToken(existing.version);
    if (compareVersion(localVersion, version) < 0) {
      this.store(id, doc, token, merged, deleted); // remote wins
      return { status: 200, json: { id, winner: token } };
    }
    // local wins; record the sibling by merging the vector
    this.store(id, existing.body, existing.version, merged, existing.deleted);
    return { status: 200, json: { id, winner: existing.version } };
  }

  private store(
    id: string,
    body: JsonObject | null,
    version: string,
    vv: VV,
    deleted: boolean,
  ): void {
    this.seq++;
    this.docs.set(id, { body, version, vv, deleted, seq: this.seq });
  }
}
