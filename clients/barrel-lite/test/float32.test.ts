import { describe, expect, it } from "vitest";
import {
  base64ToFloat32,
  decodeFloat32LE,
  encodeFloat32LE,
  float32ToBase64,
} from "../src/codec/float32.js";
import { golden } from "./fixtures.js";

describe("float32 codec", () => {
  it("matches the Erlang base64 for every fixture vector", () => {
    for (const c of golden.vector_embedding ?? []) {
      expect(float32ToBase64(c.vector)).toBe(c.base64);
      expect(c.vector.length).toBe(c.dim);
    }
  });

  it("round-trips base64 -> float32 within single precision", () => {
    for (const c of golden.vector_embedding ?? []) {
      const back = base64ToFloat32(c.base64);
      expect(back.length).toBe(c.dim);
      // the fixture values are the f32-rounded doubles; encoding again is exact
      expect(float32ToBase64(back)).toBe(c.base64);
    }
  });

  it("encodes a Float32Array and a number[] identically", () => {
    const nums = [0.1, -0.2, 0.3];
    expect(float32ToBase64(nums)).toBe(float32ToBase64(Float32Array.from(nums)));
  });

  it("rejects a blob whose length is not a multiple of 4", () => {
    expect(() => decodeFloat32LE(new Uint8Array(5))).toThrow();
  });

  it("round-trips through encode/decode", () => {
    const v = Float32Array.from([1.5, -2.25, 0, 100]);
    expect(decodeFloat32LE(encodeFloat32LE(v))).toEqual(v);
  });
});
