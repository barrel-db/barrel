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

interface Stored {
  body: JsonObject | null;
  version: string; // token
  vv: VV;
  deleted: boolean;
  seq: number;
}

function seqHlc(seq: number): Hlc {
  return { wall: BigInt(seq), logical: 0 };
}

export class FakeServer {
  private readonly docs = new Map<string, Stored>();
  private seq = 0;
  private readonly clock = new HlcClock({ physClock: () => 1_700_000_000_000 });
  historyFloor: string | undefined;

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
      const path = new URL(url).pathname;
      const body = init?.body ? (JSON.parse(init.body as string) as JsonObject) : undefined;
      const { status, json } = this.route(method, path, body);
      const headers = new Headers({
        "content-type": "application/json",
        "x-barrel-hlc": base64Encode(encodeHlc(this.clock.now()), "standard"),
      });
      const text = json === undefined ? "" : JSON.stringify(json);
      return new Response(text, { status, headers });
    };
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
    return { status: 200, json };
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
