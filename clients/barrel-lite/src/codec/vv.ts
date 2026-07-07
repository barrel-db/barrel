/**
 * Version vectors: a byte-exact port of barrel_vv. A VV maps an author
 * (source_id) to the highest HLC seen from it. Comparing two vectors
 * decides how a replicated write relates: dominates (fast-forward),
 * dominated/eq (already covered), or concurrent (a conflict resolved
 * by last-write-wins).
 *
 * The binary encoding is deterministic: `<Count:16, [NodeLen:8, Node,
 * Hlc:12]...>` with entries sorted by raw byte order of the node id,
 * standard-base64'd on the wire.
 */
import { compareBytes, utf8Decode, utf8Encode } from "./bytes.js";
import { compareHlc, decodeHlc, encodeHlc, type Hlc } from "./hlc.js";
import type { Version } from "./version.js";

export type VV = Map<string, Hlc>;

export type VvRelation = "eq" | "dominates" | "dominated" | "concurrent";

/** The empty vector. */
export function vvNew(): VV {
  return new Map();
}

/** Record a version, monotonically (never goes backward). */
export function vvBump(vv: VV, v: Version): VV {
  const existing = vv.get(v.author);
  if (existing === undefined || compareHlc(v.hlc, existing) === 1) {
    const next = new Map(vv);
    next.set(v.author, v.hlc);
    return next;
  }
  return vv;
}

/** Pointwise maximum of two vectors. */
export function vvMerge(a: VV, b: VV): VV {
  let acc = a;
  for (const [node, hlc] of b) {
    acc = vvBump(acc, { hlc, author: node });
  }
  return acc;
}

/** Does the vector cover a version (same or newer HLC from its node)? */
export function vvContains(vv: VV, v: Version): boolean {
  const seen = vv.get(v.author);
  if (seen === undefined) return false;
  return compareHlc(seen, v.hlc) !== -1;
}

function entryCompare(a: Hlc | undefined, b: Hlc | undefined): -1 | 0 | 1 {
  if (a === undefined && b === undefined) return 0;
  if (a === undefined) return -1; // a missing entry is lower
  if (b === undefined) return 1;
  return compareHlc(a, b);
}

/** Relate two vectors. Order-independent, matching barrel_vv:compare. */
export function vvCompare(a: VV, b: VV): VvRelation {
  const nodes = new Set<string>([...a.keys(), ...b.keys()]);
  let acc: "eq" | "dominates" | "dominated" = "eq";
  for (const node of nodes) {
    const cmp = entryCompare(a.get(node), b.get(node));
    if (cmp === 0) continue;
    if (acc === "eq" && cmp > 0) acc = "dominates";
    else if (acc === "eq" && cmp < 0) acc = "dominated";
    else if (acc === "dominates" && cmp > 0) {
      // stays dominates
    } else if (acc === "dominated" && cmp < 0) {
      // stays dominated
    } else {
      return "concurrent";
    }
  }
  return acc;
}

/** Deterministic binary encoding (entries sorted by node byte order). */
export function vvEncode(vv: VV): Uint8Array {
  const entries = [...vv.entries()].sort((x, y) =>
    compareBytes(utf8Encode(x[0]), utf8Encode(y[0])),
  );
  const nodeBytes = entries.map(([node]) => utf8Encode(node));
  let total = 2;
  for (const nb of nodeBytes) {
    if (nb.length > 255) throw new Error("node id exceeds 255 bytes");
    total += 1 + nb.length + 12;
  }
  const out = new Uint8Array(total);
  new DataView(out.buffer).setUint16(0, entries.length, false);
  let off = 2;
  for (let i = 0; i < entries.length; i++) {
    const nb = nodeBytes[i] as Uint8Array;
    out[off++] = nb.length;
    out.set(nb, off);
    off += nb.length;
    out.set(encodeHlc((entries[i] as [string, Hlc])[1]), off);
    off += 12;
  }
  return out;
}

/** Decode a vector; the input must be exactly one encoded vector. */
export function vvDecode(bytes: Uint8Array): VV {
  const [vv, rest] = vvDecodePrefix(bytes);
  if (rest.length !== 0) {
    throw new Error("trailing bytes after version vector");
  }
  return vv;
}

/** Decode a vector from the head, returning it and the remaining bytes. */
export function vvDecodePrefix(bytes: Uint8Array): [VV, Uint8Array] {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint16(0, false);
  let off = 2;
  const vv: VV = new Map();
  for (let i = 0; i < count; i++) {
    const nodeLen = bytes[off] as number;
    off += 1;
    const node = utf8Decode(bytes.subarray(off, off + nodeLen));
    off += nodeLen;
    vv.set(node, decodeHlc(bytes.subarray(off, off + 12)));
    off += 12;
  }
  return [vv, bytes.subarray(off)];
}
