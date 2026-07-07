import { describe, expect, it } from "vitest";
import { base64Decode, base64Encode } from "../src/codec/base64.js";
import { encodeHlc } from "../src/codec/hlc.js";
import { golden } from "./fixtures.js";
import { fromHex } from "../src/codec/bytes.js";

describe("base64", () => {
  it("standard alphabet matches btoa-equivalent known vectors", () => {
    // "hello" -> aGVsbG8=  ; "hi" -> aGk= ; "" -> ""
    const enc = (s: string) =>
      base64Encode(new Uint8Array([...s].map((c) => c.charCodeAt(0))));
    expect(enc("hello")).toBe("aGVsbG8=");
    expect(enc("hi")).toBe("aGk=");
    expect(enc("")).toBe("");
    expect(enc("any carnal pleasure.")).toBe("YW55IGNhcm5hbCBwbGVhc3VyZS4=");
  });

  it("urlsafe alphabet drops padding and uses -_", () => {
    const bytes = new Uint8Array([0xfb, 0xff, 0xbf]);
    expect(base64Encode(bytes, "standard")).toBe("+/+/");
    expect(base64Encode(bytes, "urlsafe")).toBe("-_-_");
    const oneLeft = new Uint8Array([0xff]);
    expect(base64Encode(oneLeft, "standard")).toBe("/w==");
    expect(base64Encode(oneLeft, "urlsafe")).toBe("_w");
  });

  it("decodes both alphabets and tolerates missing padding", () => {
    expect(base64Decode("+/+/")).toEqual(new Uint8Array([0xfb, 0xff, 0xbf]));
    expect(base64Decode("-_-_")).toEqual(new Uint8Array([0xfb, 0xff, 0xbf]));
    expect(base64Decode("aGk")).toEqual(base64Decode("aGk="));
  });

  it("round-trips every fixture HLC through standard base64", () => {
    // The 12-byte HLC never needs padding (multiple of 3), the case
    // the sync wire relies on.
    for (const c of golden.hlc_encode) {
      const bytes = encodeHlc({ wall: BigInt(c.wall), logical: c.logical });
      const b64 = base64Encode(bytes, "standard");
      expect(b64).not.toContain("=");
      expect(base64Decode(b64)).toEqual(fromHex(c.hex));
    }
  });

  it("rejects invalid characters", () => {
    expect(() => base64Decode("!!!!")).toThrow();
  });
});
