/**
 * Float32 vector codec matching the server's embedding storage:
 * 32-bit IEEE-754 little-endian, packed contiguously (barrel_doc:
 * encode_embedding = << <<F:32/float-little>> ... >>). DataView's
 * setFloat32(..., true) rounds a JS double to single precision exactly
 * as the Erlang codec does, so a number[] and a Float32Array encode
 * identically and round-trip byte-exact.
 */
import { base64Decode, base64Encode } from "./base64.js";

export function encodeFloat32LE(vec: Float32Array | number[]): Uint8Array {
  const n = vec.length;
  const out = new Uint8Array(n * 4);
  const view = new DataView(out.buffer);
  for (let i = 0; i < n; i++) {
    view.setFloat32(i * 4, vec[i] as number, true);
  }
  return out;
}

export function decodeFloat32LE(bytes: Uint8Array): Float32Array {
  if (bytes.length % 4 !== 0) {
    throw new Error(`float32 blob length must be a multiple of 4, got ${bytes.length}`);
  }
  const n = bytes.length / 4;
  const out = new Float32Array(n);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let i = 0; i < n; i++) {
    out[i] = view.getFloat32(i * 4, true);
  }
  return out;
}

/** Standard-base64 of the float32 LE blob (the wire form). */
export function float32ToBase64(vec: Float32Array | number[]): string {
  return base64Encode(encodeFloat32LE(vec), "standard");
}

export function base64ToFloat32(b64: string): Float32Array {
  return decodeFloat32LE(base64Decode(b64));
}
