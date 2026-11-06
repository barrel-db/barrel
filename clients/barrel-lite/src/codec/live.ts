/**
 * A document's replicated state: a byte-exact port of barrel_live (the
 * multi-value register of the causal CRDT framework). `live` holds the
 * current versions; `obs` (per-writer high-water marks, raised only by
 * that writer's own writes) and `cloud` (exact tokens) record every
 * version the state has seen. Merging keeps a version both sides hold
 * or that the other side never saw, so concurrent writes all survive
 * and a replaced version never comes back.
 *
 * Binary form (barrel_live:encode_state/1), version 1:
 *   <<1, N:32, [Lp(Token), Deleted:8, Lp(VV)]*N, Ctx>>
 *   Ctx = <<1, N:32, [Lp(Writer), Hlc:12]*N, N:32, [Lp(Token)]*N,
 *           N:32, [Lp(Token), N:32, [Lp(Token)]*N, Lp(Result)]*N>>
 * where Lp(B) = <<Len:16, B>> and a token is <<Hlc:12, Writer>>.
 */
import { compareBytes, utf8Decode, utf8Encode } from "./bytes.js";
import { compareHlc, decodeHlc, encodeHlc, type Hlc } from "./hlc.js";
import { compareVersion, versionFromToken, versionToToken, type Version } from "./version.js";
import { vvBump, vvDecode, vvEncode, vvMerge, vvNew, type VV } from "./vv.js";

const VSN = 1;

export interface LiveInfo {
  readonly version: Version;
  readonly deleted: boolean;
  readonly vv: VV;
  /** An automatic resolution: the original writes it covers. */
  readonly base?: readonly Version[];
  readonly result?: Uint8Array;
}

export interface LiveState {
  /** Keyed by version token. */
  readonly live: ReadonlyMap<string, LiveInfo>;
  readonly obs: ReadonlyMap<string, Hlc>;
  readonly cloud: ReadonlySet<string>;
}

export function liveNew(): LiveState {
  return { live: new Map(), obs: new Map(), cloud: new Set() };
}

/** Has this state seen the version? */
export function liveSeen(s: LiveState, v: Version): boolean {
  if (s.cloud.has(versionToToken(v))) return true;
  const high = s.obs.get(v.author);
  return high !== undefined && compareHlc(v.hlc, high) !== 1;
}

/** The join: commutative, associative, idempotent. */
export function liveMerge(a: LiveState, b: LiveState): LiveState {
  const live = new Map<string, LiveInfo>();
  for (const [t, ia] of a.live) {
    const ib = b.live.get(t);
    if (ib) live.set(t, mergeInfo(ia, ib));
    else if (!liveSeen(b, ia.version)) live.set(t, ia);
  }
  for (const [t, ib] of b.live) {
    if (!a.live.has(t) && !liveSeen(a, ib.version)) live.set(t, ib);
  }
  const obs = new Map(a.obs);
  for (const [w, h] of b.obs) {
    const cur = obs.get(w);
    if (cur === undefined || compareHlc(h, cur) === 1) obs.set(w, h);
  }
  return { live, obs, cloud: new Set([...a.cloud, ...b.cloud]) };
}

function mergeInfo(a: LiveInfo, b: LiveInfo): LiveInfo {
  const vv = vvMerge(a.vv, b.vv);
  if (a.base && b.base) {
    const seen = new Map<string, Version>();
    for (const v of [...a.base, ...b.base]) seen.set(versionToToken(v), v);
    return { ...a, vv, base: [...seen.values()].sort(compareVersion) };
  }
  if (!a.base && b.base) return { ...a, vv, base: b.base, result: b.result };
  return { ...a, vv };
}

/** Current versions, lowest token first. */
export function liveTokens(s: LiveState): Version[] {
  return [...s.live.values()].map((i) => i.version).sort(compareVersion);
}

export interface LiveView {
  winner?: Version;
  conflicts: Version[];
  deleted: boolean;
}

/** Winner = highest token; the other live versions are conflicts. */
export function liveView(s: LiveState): LiveView {
  const sorted = liveTokens(s);
  if (sorted.length === 0) return { conflicts: [], deleted: true };
  const winner = sorted[sorted.length - 1]!;
  return {
    winner,
    conflicts: sorted.slice(0, -1),
    deleted: s.live.get(versionToToken(winner))!.deleted,
  };
}

/**
 * What a write replaces: an observed set takes precedence; over a
 * delete, every delete (checking rev if given); otherwise rev must be
 * the winner. Returns "conflict" when the write is not allowed.
 */
export function liveReplaces(
  s: LiveState,
  rev: string | undefined,
  observed: readonly string[] | undefined,
): string[] | "conflict" {
  if (observed) return observed.filter((t) => s.live.has(t));
  if (s.live.size === 0) return [];
  const view = liveView(s);
  const winner = versionToToken(view.winner!);
  if (view.deleted) {
    if (rev !== undefined && rev !== winner) return "conflict";
    return [...s.live.values()].filter((i) => i.deleted).map((i) => versionToToken(i.version));
  }
  return rev === winner ? [winner] : "conflict";
}

/** A local write replacing `replaces`. */
export function liveAuthor(
  s: LiveState,
  v: Version,
  deleted: boolean,
  replaces: readonly string[],
): LiveState {
  for (const i of s.live.values()) {
    if (compareVersion(i.version, v) !== -1) {
      throw new Error("a new version must sort above every live version");
    }
  }
  let vv = vvNew();
  const live = new Map(s.live);
  for (const t of replaces) {
    const i = live.get(t);
    if (i) {
      vv = vvMerge(vv, i.vv);
      live.delete(t);
    }
  }
  vv = vvBump(vv, v);
  live.set(versionToToken(v), { version: v, deleted, vv });
  const obs = new Map(s.obs);
  obs.set(v.author, v.hlc);
  return { live, obs, cloud: s.cloud };
}

//====================================================================
// Encoding
//====================================================================

function encodeVersion(v: Version): Uint8Array {
  return concat([encodeHlc(v.hlc), utf8Encode(v.author)]);
}

function decodeVersion(b: Uint8Array): Version {
  return { hlc: decodeHlc(b.subarray(0, 12)), author: utf8Decode(b.subarray(12)) };
}

class Writer {
  private parts: Uint8Array[] = [];
  u8(n: number): void {
    this.parts.push(Uint8Array.of(n));
  }
  u32(n: number): void {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setUint32(0, n);
    this.parts.push(b);
  }
  lp(b: Uint8Array): void {
    const len = new Uint8Array(2);
    new DataView(len.buffer).setUint16(0, b.length);
    this.parts.push(len, b);
  }
  raw(b: Uint8Array): void {
    this.parts.push(b);
  }
  bytes(): Uint8Array {
    return concat(this.parts);
  }
}

class Reader {
  private off = 0;
  private readonly view: DataView;
  constructor(private readonly b: Uint8Array) {
    this.view = new DataView(b.buffer, b.byteOffset, b.byteLength);
  }
  u8(): number {
    return this.view.getUint8(this.off++);
  }
  u32(): number {
    const n = this.view.getUint32(this.off);
    this.off += 4;
    return n;
  }
  lp(): Uint8Array {
    const n = this.view.getUint16(this.off);
    this.off += 2;
    const out = this.b.subarray(this.off, this.off + n);
    this.off += n;
    return out;
  }
  raw(n: number): Uint8Array {
    const out = this.b.subarray(this.off, this.off + n);
    this.off += n;
    return out;
  }
  done(): boolean {
    return this.off === this.b.length;
  }
}

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

function byToken(a: LiveInfo, b: LiveInfo): number {
  return compareVersion(a.version, b.version);
}

/** The state in barrel_live's binary form. */
export function encodeState(s: LiveState): Uint8Array {
  const w = new Writer();
  w.u8(VSN);
  const infos = [...s.live.values()].sort(byToken);
  w.u32(infos.length);
  for (const i of infos) {
    w.lp(encodeVersion(i.version));
    w.u8(i.deleted ? 1 : 0);
    w.lp(vvEncode(i.vv));
  }
  w.u8(VSN);
  const obs = [...s.obs].sort((a, b) => compareBytes(utf8Encode(a[0]), utf8Encode(b[0])));
  w.u32(obs.length);
  for (const [writer, hlc] of obs) {
    w.lp(utf8Encode(writer));
    w.raw(encodeHlc(hlc));
  }
  const cloud = [...s.cloud].map(versionFromToken).sort(compareVersion);
  w.u32(cloud.length);
  for (const v of cloud) w.lp(encodeVersion(v));
  const autos = infos.filter((i) => i.base && i.result);
  w.u32(autos.length);
  for (const i of autos) {
    w.lp(encodeVersion(i.version));
    w.u32(i.base!.length);
    for (const b of i.base!) w.lp(encodeVersion(b));
    w.lp(i.result!);
  }
  return w.bytes();
}

export function decodeState(bytes: Uint8Array): LiveState {
  const r = new Reader(bytes);
  if (r.u8() !== VSN) throw new Error("unknown state version");
  const live = new Map<string, LiveInfo>();
  for (let n = r.u32(); n > 0; n--) {
    const version = decodeVersion(r.lp());
    const deleted = r.u8() === 1;
    const vv = vvDecode(r.lp());
    live.set(versionToToken(version), { version, deleted, vv });
  }
  if (r.u8() !== VSN) throw new Error("unknown context version");
  const obs = new Map<string, Hlc>();
  for (let n = r.u32(); n > 0; n--) {
    const writer = utf8Decode(r.lp());
    obs.set(writer, decodeHlc(r.raw(12)));
  }
  const cloud = new Set<string>();
  for (let n = r.u32(); n > 0; n--) cloud.add(versionToToken(decodeVersion(r.lp())));
  for (let n = r.u32(); n > 0; n--) {
    const t = versionToToken(decodeVersion(r.lp()));
    const base: Version[] = [];
    for (let m = r.u32(); m > 0; m--) base.push(decodeVersion(r.lp()));
    const result = r.lp();
    const i = live.get(t);
    if (i) live.set(t, { ...i, base, result });
  }
  if (!r.done()) throw new Error("trailing bytes in state");
  return { live, obs, cloud };
}
