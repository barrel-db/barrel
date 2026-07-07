/**
 * Base64 with both alphabets, hand-rolled so encoding is identical on
 * every runtime (no atob/btoa Latin-1 traps, no Node Buffer).
 *
 * Alphabet rules on the barrel wire:
 *   - the sync surface (/db/:db/_sync/*) uses STANDARD base64 with
 *     padding for every HLC, cursor, and version vector.
 *   - the plain /db/:db/changes feed uses URL-SAFE base64.
 * The client keeps _sync cursors opaque, so it never has to convert
 * between the two; both are provided for completeness.
 */

const STD = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const URL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

export type Alphabet = "standard" | "urlsafe";

function encodeTable(alphabet: Alphabet): string {
  return alphabet === "urlsafe" ? URL : STD;
}

function decodeTable(alphabet: Alphabet): Int16Array {
  const src = encodeTable(alphabet);
  const table = new Int16Array(128).fill(-1);
  for (let i = 0; i < src.length; i++) {
    table[src.charCodeAt(i)] = i;
  }
  return table;
}

const DECODE_STD = decodeTable("standard");
const DECODE_URL = decodeTable("urlsafe");

/** Encode bytes to base64. Standard alphabet emits `=` padding. */
export function base64Encode(
  bytes: Uint8Array,
  alphabet: Alphabet = "standard",
): string {
  const t = encodeTable(alphabet);
  let out = "";
  let i = 0;
  for (; i + 2 < bytes.length; i += 3) {
    const n =
      ((bytes[i] as number) << 16) |
      ((bytes[i + 1] as number) << 8) |
      (bytes[i + 2] as number);
    out += t[(n >> 18) & 63];
    out += t[(n >> 12) & 63];
    out += t[(n >> 6) & 63];
    out += t[n & 63];
  }
  const rem = bytes.length - i;
  if (rem === 1) {
    const n = (bytes[i] as number) << 16;
    out += t[(n >> 18) & 63];
    out += t[(n >> 12) & 63];
    if (alphabet === "standard") out += "==";
  } else if (rem === 2) {
    const n = ((bytes[i] as number) << 16) | ((bytes[i + 1] as number) << 8);
    out += t[(n >> 18) & 63];
    out += t[(n >> 12) & 63];
    out += t[(n >> 6) & 63];
    if (alphabet === "standard") out += "=";
  }
  return out;
}

/**
 * Decode base64 to bytes. Accepts both alphabets (the `-_` and `+/`
 * characters map to the same values) and tolerates missing padding, so
 * a value produced by either encoder round-trips.
 */
export function base64Decode(input: string): Uint8Array {
  const clean = input.replace(/=+$/, "");
  const out = new Uint8Array(Math.floor((clean.length * 3) / 4));
  let acc = 0;
  let bits = 0;
  let o = 0;
  for (let i = 0; i < clean.length; i++) {
    const c = clean.charCodeAt(i);
    let v = -1;
    if (c < 128) {
      const s = DECODE_STD[c] as number;
      v = s !== -1 ? s : (DECODE_URL[c] as number);
    }
    if (v === -1) {
      throw new Error(`invalid base64 character at offset ${i}`);
    }
    acc = (acc << 6) | v;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out[o++] = (acc >> bits) & 0xff;
    }
  }
  return out.subarray(0, o);
}
