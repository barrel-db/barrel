/**
 * Document versions: a byte-exact port of barrel_version. A version is
 * an HLC plus the author id (the source_id of the database that wrote
 * it). The wire/API token is `<24 hex of the HLC>@<author>`; the fixed
 * hex width makes lexicographic token order equal causal order.
 *
 * The winner among sibling versions is the maximum under
 * compareVersion (HLC first, author byte-order tie-break), a
 * commutative rule so every replica agrees.
 */
import { compareBytes, fromHex, toHex, utf8Encode } from "./bytes.js";
import { compareHlc, decodeHlc, encodeHlc, type Hlc } from "./hlc.js";

export interface Version {
  readonly hlc: Hlc;
  readonly author: string;
}

const HLC_HEX_LEN = 24; // 12 bytes, hex encoded

/** API token form: `<hex(hlc)>@<author>`. */
export function versionToToken(v: Version): string {
  return `${toHex(encodeHlc(v.hlc))}@${v.author}`;
}

/** Parse a token back to a version. Throws on a malformed token. */
export function versionFromToken(token: string): Version {
  if (token.length < HLC_HEX_LEN + 2 || token[HLC_HEX_LEN] !== "@") {
    throw new Error(`invalid version token: ${token}`);
  }
  const hex = token.slice(0, HLC_HEX_LEN);
  const author = token.slice(HLC_HEX_LEN + 1);
  if (author.length === 0) {
    throw new Error("version token has empty author");
  }
  return { hlc: decodeHlc(fromHex(hex)), author };
}

/** Total order: HLC first, then byte-wise author. Returns -1, 0, 1. */
export function compareVersion(a: Version, b: Version): -1 | 0 | 1 {
  const h = compareHlc(a.hlc, b.hlc);
  if (h !== 0) return h;
  return compareBytes(utf8Encode(a.author), utf8Encode(b.author)) as -1 | 0 | 1;
}

/** The deterministic winner (max under compareVersion). */
export function versionWinner(a: Version, b: Version): Version {
  return compareVersion(a, b) < 0 ? b : a;
}
