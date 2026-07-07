/**
 * Byte-level helpers shared by the codecs. Everything here is
 * platform-neutral: Uint8Array, no Buffer, no atob.
 */

/**
 * Lexicographic (unsigned byte-wise) comparison of two byte arrays,
 * matching Erlang binary ordering: a shorter array that is a prefix of
 * a longer one sorts first. Returns -1, 0, or 1.
 *
 * This is the ordering barrel uses for version-vector node ids
 * (barrel_vv:encode sorts entries by binary key order) and for the
 * author tie-break in barrel_version:compare.
 */
export function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a[i] as number;
    const y = b[i] as number;
    if (x < y) return -1;
    if (x > y) return 1;
  }
  if (a.length < b.length) return -1;
  if (a.length > b.length) return 1;
  return 0;
}

const HEX = "0123456789abcdef";

/** Lowercase hex of a byte array (no separators). */
export function toHex(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i] as number;
    out += HEX[b >> 4];
    out += HEX[b & 0x0f];
  }
  return out;
}

/** Parse a hex string (any case, even length) into bytes. */
export function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) {
    throw new Error(`odd-length hex string: ${hex.length}`);
  }
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    const byte = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    if (Number.isNaN(byte)) {
      throw new Error(`invalid hex at offset ${i * 2}`);
    }
    out[i] = byte;
  }
  return out;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** UTF-8 encode a string to bytes. */
export function utf8Encode(s: string): Uint8Array {
  return encoder.encode(s);
}

/** UTF-8 decode bytes to a string. */
export function utf8Decode(bytes: Uint8Array): string {
  return decoder.decode(bytes);
}

/**
 * A source_id: 8 random bytes as 16 lowercase hex characters. Mirrors
 * barrel_db_server:ensure_source_id/2. The client mints this once and
 * persists it forever; it is the author component of every version the
 * client authors.
 */
export function randomSourceId(): string {
  const bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  return toHex(bytes);
}
