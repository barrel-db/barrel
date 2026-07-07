/**
 * Content digest matching the server: the literal ASCII prefix
 * "sha256-" followed by the lowercase hex of the SHA-256 of the bytes
 * (barrel_att:compute_digest). Browsers and Node 20+ compute it via
 * crypto.subtle.digest.
 */
import { toHex } from "../codec/bytes.js";

export async function sha256Digest(bytes: Uint8Array): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", toArrayBuffer(bytes));
  return "sha256-" + toHex(new Uint8Array(buf));
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  // a fresh, exact-length ArrayBuffer (bytes may be a subarray view)
  const out = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(out).set(bytes);
  return out;
}
