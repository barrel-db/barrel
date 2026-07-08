/**
 * The barrel sync wire (/db/:db/_sync/*) as a fetch client. JSON
 * bodies, standard base64 for HLCs and version vectors, version tokens
 * verbatim. Every request sends the client clock in x-barrel-hlc and
 * folds the response header back (passive coupling: a fast client
 * clock would otherwise win every last-write-wins forever).
 *
 * Checkpoints are kept client-side by the sync engine, so the local
 * endpoints are provided for completeness but not used by a pull-only
 * client (they need the write right).
 */
import { base64Decode, base64Encode } from "../codec/base64.js";
import { base64ToFloat32 } from "../codec/float32.js";
import { decodeHlc, encodeHlc, HlcClock } from "../codec/hlc.js";
import type { JsonObject, JsonValue } from "../json.js";
import { SyncError } from "./errors.js";
import { filterToWire, type SyncFilter } from "./filters.js";

export type FetchLike = (
  input: string,
  init?: RequestInit,
) => Promise<Response>;

export interface TransportOptions {
  /** Server base, e.g. "http://host:8080" (no trailing slash needed). */
  url: string;
  /** Database or space id. */
  db: string;
  /** Bearer token (global or bsp_ capability); omitted on an open server. */
  token?: string;
  /** Shared HLC clock; the transport folds x-barrel-hlc into it. */
  clock: HlcClock;
  /** Injected fetch (tests, Node agents); defaults to global fetch. */
  fetch?: FetchLike;
}

export interface SyncInfo {
  db: string;
  historyFloor?: string; // opaque standard-base64 cursor
  attFloor?: string;
}

export interface ChangeRow {
  id: string;
  hlc: string; // standard-base64 HLC
  rev: string; // version token
  changes: { rev: string }[];
  numConflicts: number;
  deleted: boolean;
}

export interface ChangesPage {
  changes: ChangeRow[];
  lastSeq: string; // "first" | standard-base64 cursor
}

export interface DocForRep {
  doc: JsonObject;
  version: string; // token
  vv: string; // standard-base64 encoded version vector
  deleted: boolean;
}

export interface PutVersionInput {
  doc: JsonObject;
  version: string; // token
  vv: string; // standard-base64 encoded version vector
  deleted?: boolean;
}

export interface PutVersionResult {
  id: string;
  winner: string; // token
}

export interface ChangesQuery {
  limit?: number;
  filter?: SyncFilter;
}

const SENTINEL_FIRST = "first";

export interface QueryMeta {
  hasMore: boolean;
  count?: number;
  continuation?: string;
}

export class SyncTransport {
  private readonly base: string;
  private readonly dbRoot: string;
  private readonly token?: string;
  private readonly clock: HlcClock;
  private readonly fetchImpl: FetchLike;

  constructor(opts: TransportOptions) {
    const root = opts.url.replace(/\/+$/, "");
    this.dbRoot = `${root}/db/${encodeURIComponent(opts.db)}`;
    this.base = `${this.dbRoot}/_sync`;
    if (opts.token !== undefined) this.token = opts.token;
    this.clock = opts.clock;
    this.fetchImpl = opts.fetch ?? ((i, init) => fetch(i, init));
  }

  async info(): Promise<SyncInfo> {
    const { json } = await this.request("GET", "/info");
    const obj = asObject(json);
    const info: SyncInfo = { db: String(obj["db"]) };
    if (typeof obj["history_floor"] === "string") {
      info.historyFloor = obj["history_floor"];
    }
    if (typeof obj["att_floor"] === "string") {
      info.attFloor = obj["att_floor"];
    }
    return info;
  }

  /** Handshake the clock; throws SyncError("clock_skew") on a 409. */
  async syncHlc(): Promise<void> {
    const hlc = base64Encode(encodeHlc(this.clock.peek()), "standard");
    const { status, json } = await this.send("POST", "/hlc", { hlc });
    if (status === 409) {
      throw new SyncError("clock_skew", "remote clock too far ahead", 409);
    }
    if (status < 200 || status >= 300) this.raise(status, json);
  }

  async changes(since: string, query: ChangesQuery = {}): Promise<ChangesPage> {
    const body: JsonObject = { since: since || SENTINEL_FIRST };
    if (query.limit !== undefined) body["limit"] = query.limit;
    if (query.filter !== undefined) {
      body["filter"] = filterToWire(query.filter);
    }
    const { json } = await this.request("POST", "/changes", body);
    const obj = asObject(json);
    const rows = Array.isArray(obj["changes"]) ? obj["changes"] : [];
    return {
      changes: rows.map((r) => toChangeRow(asObject(r))),
      lastSeq: String(obj["last_seq"] ?? SENTINEL_FIRST),
    };
  }

  /** Batch have/missing test; keys map to "have" or "missing". */
  async diff(
    versions: Record<string, string>,
  ): Promise<Record<string, "have" | "missing">> {
    const { json } = await this.request("POST", "/diff", { versions });
    const obj = asObject(asObject(json)["diff"]);
    const out: Record<string, "have" | "missing"> = {};
    for (const [id, v] of Object.entries(obj)) {
      out[id] = v === "missing" ? "missing" : "have";
    }
    return out;
  }

  async getDoc(id: string): Promise<DocForRep> {
    const { json } = await this.request("GET", `/doc/${encodeURIComponent(id)}`);
    const obj = asObject(json);
    return {
      doc: asObject(obj["doc"]),
      version: String(obj["version"]),
      vv: String(obj["vv"]),
      deleted: obj["deleted"] === true,
    };
  }

  async putVersion(
    id: string,
    input: PutVersionInput,
  ): Promise<PutVersionResult> {
    const body: JsonObject = {
      doc: input.doc,
      version: input.version,
      vv: input.vv,
      deleted: input.deleted === true,
    };
    const { json } = await this.request(
      "PUT",
      `/doc/${encodeURIComponent(id)}`,
      body,
    );
    const obj = asObject(json);
    return { id: String(obj["id"]), winner: String(obj["winner"]) };
  }

  /** Read a checkpoint local doc; undefined when absent (404). */
  async getLocal(id: string): Promise<JsonObject | undefined> {
    const { status, json } = await this.send(
      "GET",
      `/local/${encodeURIComponent(id)}`,
    );
    if (status === 404) return undefined;
    if (status < 200 || status >= 300) this.raise(status, json);
    return asObject(json);
  }

  async putLocal(id: string, doc: JsonObject): Promise<void> {
    await this.request("PUT", `/local/${encodeURIComponent(id)}`, doc);
  }

  async deleteLocal(id: string): Promise<void> {
    await this.request("DELETE", `/local/${encodeURIComponent(id)}`);
  }

  /** The current clock (for status/debug). */
  clockState(): HlcClock {
    return this.clock;
  }

  //==================================================================
  // Embeddings (POST /db/:db/_bulk_get with include_embedding)
  //==================================================================

  /** Fetch per-doc vectors for a batch of ids. Ids without a stored
   * vector are absent from the result map. */
  async bulkGetEmbeddings(
    ids: string[],
  ): Promise<Map<string, { vector: Float32Array; dim: number; source: string }>> {
    const out = new Map<string, { vector: Float32Array; dim: number; source: string }>();
    if (ids.length === 0) return out;
    const { json } = await this.dbRootRequest("POST", "/_bulk_get", {
      ids,
      include_embedding: true,
    });
    const results = Array.isArray(asObject(json)["results"])
      ? (asObject(json)["results"] as JsonValue[])
      : [];
    for (const r of results) {
      const doc = asObject(r);
      const id = typeof doc["id"] === "string" ? doc["id"] : undefined;
      const emb = doc["_embedding"];
      if (id === undefined || typeof emb !== "object" || emb === null || Array.isArray(emb)) {
        continue;
      }
      const e = emb as JsonObject;
      if (typeof e["vector"] !== "string" || typeof e["dim"] !== "number") continue;
      const vector = base64ToFloat32(e["vector"]);
      if (vector.length !== e["dim"]) continue;
      out.set(id, {
        vector,
        dim: e["dim"],
        source: typeof e["source"] === "string" ? e["source"] : "client",
      });
    }
    return out;
  }

  //==================================================================
  // Query delegation (POST /db/:db/query, ndjson response)
  //==================================================================

  async queryRemote(
    bql: string,
    opts: { params?: Record<string, JsonValue>; continuation?: string } = {},
  ): Promise<{ rows: JsonObject[]; meta: QueryMeta }> {
    const body: JsonObject = { query: bql };
    if (opts.params !== undefined) body["params"] = opts.params;
    if (opts.continuation !== undefined) body["continuation"] = opts.continuation;
    const headers: Record<string, string> = {
      "x-barrel-hlc": base64Encode(encodeHlc(this.clock.peek()), "standard"),
      "content-type": "application/json",
    };
    if (this.token !== undefined) headers["authorization"] = `Bearer ${this.token}`;
    let resp: Response;
    try {
      resp = await this.fetchImpl(`${this.dbRoot}/query`, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
      });
    } catch (e) {
      throw new SyncError("network", `request failed: ${String(e)}`);
    }
    this.foldClock(resp.headers.get("x-barrel-hlc"));
    const text = await resp.text();
    if (resp.status < 200 || resp.status >= 300) {
      let err: JsonValue = null;
      try {
        err = JSON.parse(text) as JsonValue;
      } catch {
        err = null;
      }
      this.raise(resp.status, err);
    }
    return parseNdjson(text);
  }

  //==================================================================
  // Attachment wire (streamed; separate feed + LWW on origin HLC)
  //==================================================================

  /** The attachment feed since a cursor (exclusive). 501 => unsupported. */
  async attChanges(
    since: string,
    limit: number,
  ): Promise<{ changes: AttRow[]; lastSeq: string; unsupported: boolean }> {
    const q = `?since=${encodeURIComponent(since || SENTINEL_FIRST)}&limit=${limit}`;
    const resp = await this.rawFetch("GET", `/att_changes${q}`, {});
    if (resp.status === 501) {
      return { changes: [], lastSeq: since || SENTINEL_FIRST, unsupported: true };
    }
    const json = await readJson(resp);
    if (resp.status < 200 || resp.status >= 300) this.raise(resp.status, json);
    const obj = asObject(json);
    const rows = Array.isArray(obj["changes"]) ? obj["changes"] : [];
    return {
      changes: rows.map((r) => toAttRow(asObject(r))),
      lastSeq: String(obj["last_seq"] ?? SENTINEL_FIRST),
      unsupported: false,
    };
  }

  /** Digest diff: which (id,name) the server already has. */
  async attDiff(
    entries: { id: string; name: string; digest: string }[],
  ): Promise<{ id: string; name: string; status: "have" | "missing" }[]> {
    const { json } = await this.request("POST", "/att_diff", {
      attachments: entries as unknown as JsonValue,
    });
    const rows = Array.isArray(asObject(json)["diff"])
      ? (asObject(json)["diff"] as JsonValue[])
      : [];
    return rows.map((r) => {
      const o = asObject(r);
      return {
        id: String(o["id"]),
        name: String(o["name"]),
        status: o["status"] === "have" ? "have" : "missing",
      };
    });
  }

  /** Download a blob; undefined on 404. Bytes are NOT digest-verified here. */
  async getAtt(id: string, name: string): Promise<AttBlob | undefined> {
    const resp = await this.rawFetch("GET", attPath(id, name), {});
    if (resp.status === 404) return undefined;
    if (resp.status < 200 || resp.status >= 300) {
      this.raise(resp.status, await readJson(resp));
    }
    const bytes = new Uint8Array(await resp.arrayBuffer());
    const lengthHeader = resp.headers.get("x-barrel-att-length");
    return {
      bytes,
      digest: resp.headers.get("x-barrel-digest") ?? "",
      length: lengthHeader ? Number(lengthHeader) : bytes.length,
      contentType: resp.headers.get("content-type") ?? "application/octet-stream",
    };
  }

  /** Upload a blob (LWW on origin). 400 bad_origin throws; others map. */
  async putAtt(
    id: string,
    name: string,
    body: Uint8Array | Blob,
    meta: { contentType: string; digest: string; origin: string },
  ): Promise<PutAttResult> {
    const resp = await this.rawFetch("PUT", attPath(id, name), {
      headers: {
        "content-type": meta.contentType,
        "x-barrel-digest": meta.digest,
        "x-barrel-att-origin": meta.origin,
      },
      body,
    });
    if (resp.status === 200) {
      const json = await readJson(resp);
      const ok = asObject(json)["ok"];
      return ok === "ignored" ? "ignored" : "written";
    }
    if (resp.status === 422) return "digest_mismatch";
    if (resp.status === 413) return "too_large";
    this.raise(resp.status, await readJson(resp));
  }

  async deleteAtt(id: string, name: string, origin: string): Promise<void> {
    const resp = await this.rawFetch("DELETE", attPath(id, name), {
      headers: { "x-barrel-att-origin": origin },
    });
    if (resp.status < 200 || resp.status >= 300) {
      this.raise(resp.status, await readJson(resp));
    }
  }

  //==================================================================
  // Internals
  //==================================================================

  /** A raw request that streams a binary body/response but still sets
   * and folds the x-barrel-hlc clock header (attachments are not JSON). */
  private async rawFetch(
    method: string,
    path: string,
    opts: { headers?: Record<string, string>; body?: Uint8Array | Blob },
  ): Promise<Response> {
    const headers: Record<string, string> = {
      "x-barrel-hlc": base64Encode(encodeHlc(this.clock.peek()), "standard"),
      ...(opts.headers ?? {}),
    };
    if (this.token !== undefined) {
      headers["authorization"] = `Bearer ${this.token}`;
    }
    const init: RequestInit = { method, headers };
    if (opts.body !== undefined) init.body = opts.body as BodyInit;
    let resp: Response;
    try {
      resp = await this.fetchImpl(this.base + path, init);
    } catch (e) {
      throw new SyncError("network", `request failed: ${String(e)}`);
    }
    this.foldClock(resp.headers.get("x-barrel-hlc"));
    return resp;
  }

  private async request(
    method: string,
    path: string,
    body?: JsonValue,
  ): Promise<{ status: number; json: JsonValue }> {
    const res = await this.send(method, path, body);
    if (res.status < 200 || res.status >= 300) this.raise(res.status, res.json);
    return res;
  }

  /** A JSON request against the db root (not the _sync base). */
  private async dbRootRequest(
    method: string,
    path: string,
    body?: JsonValue,
  ): Promise<{ status: number; json: JsonValue }> {
    const headers: Record<string, string> = {
      "x-barrel-hlc": base64Encode(encodeHlc(this.clock.peek()), "standard"),
    };
    if (this.token !== undefined) headers["authorization"] = `Bearer ${this.token}`;
    const init: RequestInit = { method, headers };
    if (body !== undefined) {
      headers["content-type"] = "application/json";
      init.body = JSON.stringify(body);
    }
    let resp: Response;
    try {
      resp = await this.fetchImpl(this.dbRoot + path, init);
    } catch (e) {
      throw new SyncError("network", `request failed: ${String(e)}`);
    }
    this.foldClock(resp.headers.get("x-barrel-hlc"));
    const json = await readJson(resp);
    if (resp.status < 200 || resp.status >= 300) this.raise(resp.status, json);
    return { status: resp.status, json };
  }

  private async send(
    method: string,
    path: string,
    body?: JsonValue,
  ): Promise<{ status: number; json: JsonValue }> {
    const headers: Record<string, string> = {
      "x-barrel-hlc": base64Encode(encodeHlc(this.clock.peek()), "standard"),
    };
    if (this.token !== undefined) {
      headers["authorization"] = `Bearer ${this.token}`;
    }
    const init: RequestInit = { method, headers };
    if (body !== undefined) {
      headers["content-type"] = "application/json";
      init.body = JSON.stringify(body);
    }
    let resp: Response;
    try {
      resp = await this.fetchImpl(this.base + path, init);
    } catch (e) {
      throw new SyncError("network", `request failed: ${String(e)}`);
    }
    this.foldClock(resp.headers.get("x-barrel-hlc"));
    const text = await resp.text();
    let json: JsonValue = null;
    if (text.length > 0) {
      try {
        json = JSON.parse(text) as JsonValue;
      } catch {
        json = null;
      }
    }
    return { status: resp.status, json };
  }

  private foldClock(header: string | null): void {
    if (!header) return;
    try {
      this.clock.update(decodeHlc(base64Decode(header)));
    } catch {
      // swallow skew / parse errors, matching maybe_sync_from_header
    }
  }

  private raise(status: number, json: JsonValue): never {
    const err =
      typeof json === "object" && json !== null && "error" in json
        ? String((json as JsonObject)["error"])
        : `http ${status}`;
    if (status === 401) throw new SyncError("unauthorized", err, status);
    if (status === 403) throw new SyncError("forbidden", err, status);
    if (status === 404) throw new SyncError("not_found", err, status);
    if (status >= 500) throw new SyncError("server_error", err, status);
    throw new SyncError("bad_request", err, status);
  }
}

export interface AttRow {
  seq: string;
  origin: string;
  op: "put" | "delete";
  id: string;
  name: string;
  digest: string;
  length: number;
  contentType: string;
}

export interface AttBlob {
  bytes: Uint8Array;
  digest: string;
  length: number;
  contentType: string;
}

export type PutAttResult = "written" | "ignored" | "digest_mismatch" | "too_large";

function attPath(id: string, name: string): string {
  return `/att/${encodeURIComponent(id)}/${encodeURIComponent(name)}`;
}

function toAttRow(o: JsonObject): AttRow {
  return {
    seq: String(o["seq"]),
    origin: String(o["origin"]),
    op: o["op"] === "delete" ? "delete" : "put",
    id: String(o["id"]),
    name: String(o["name"]),
    digest: typeof o["digest"] === "string" ? o["digest"] : "",
    length: typeof o["length"] === "number" ? o["length"] : 0,
    contentType: typeof o["content_type"] === "string" ? o["content_type"] : "",
  };
}

function parseNdjson(text: string): { rows: JsonObject[]; meta: QueryMeta } {
  const rows: JsonObject[] = [];
  let meta: QueryMeta = { hasMore: false };
  for (const line of text.split("\n")) {
    if (line.trim().length === 0) continue;
    let obj: JsonObject;
    try {
      obj = JSON.parse(line) as JsonObject;
    } catch {
      continue;
    }
    if (obj["row"] !== undefined) {
      rows.push(obj["row"] as JsonObject);
    } else if (obj["meta"] !== undefined) {
      const m = obj["meta"] as JsonObject;
      meta = { hasMore: m["has_more"] === true };
      if (typeof m["count"] === "number") meta.count = m["count"];
      if (typeof m["continuation"] === "string") meta.continuation = m["continuation"];
    }
  }
  return { rows, meta };
}

async function readJson(resp: Response): Promise<JsonValue> {
  const text = await resp.text();
  if (text.length === 0) return null;
  try {
    return JSON.parse(text) as JsonValue;
  } catch {
    return null;
  }
}

function asObject(v: JsonValue | undefined): JsonObject {
  if (typeof v === "object" && v !== null && !Array.isArray(v)) {
    return v as JsonObject;
  }
  throw new SyncError("bad_request", "expected a JSON object in response");
}

function toChangeRow(obj: JsonObject): ChangeRow {
  const changes = Array.isArray(obj["changes"]) ? obj["changes"] : [];
  return {
    id: String(obj["id"]),
    hlc: String(obj["hlc"]),
    rev: String(obj["rev"]),
    changes: changes.map((c) => ({ rev: String(asObject(c)["rev"]) })),
    numConflicts: typeof obj["num_conflicts"] === "number"
      ? obj["num_conflicts"]
      : 0,
    deleted: obj["deleted"] === true,
  };
}
