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

export class SyncTransport {
  private readonly base: string;
  private readonly token?: string;
  private readonly clock: HlcClock;
  private readonly fetchImpl: FetchLike;

  constructor(opts: TransportOptions) {
    const root = opts.url.replace(/\/+$/, "");
    this.base = `${root}/db/${encodeURIComponent(opts.db)}/_sync`;
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
  // Internals
  //==================================================================

  private async request(
    method: string,
    path: string,
    body?: JsonValue,
  ): Promise<{ status: number; json: JsonValue }> {
    const res = await this.send(method, path, body);
    if (res.status < 200 || res.status >= 300) this.raise(res.status, res.json);
    return res;
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
