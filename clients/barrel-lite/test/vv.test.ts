import { describe, expect, it } from "vitest";
import {
  vvBump,
  vvCompare,
  vvContains,
  vvDecode,
  vvEncode,
  vvMerge,
  vvNew,
  type VV,
} from "../src/codec/vv.js";
import type { Version } from "../src/codec/version.js";
import { toHex } from "../src/codec/bytes.js";
import { golden, type VvEntry } from "./fixtures.js";

function vvOf(entries: VvEntry[]): VV {
  let vv = vvNew();
  for (const e of entries) {
    const v: Version = {
      hlc: { wall: BigInt(e.wall), logical: e.logical },
      author: e.node,
    };
    vv = vvBump(vv, v);
  }
  return vv;
}

describe("vv encode", () => {
  it("matches the Erlang encoding (sorted by node byte order)", () => {
    for (const c of golden.vv_encode ?? []) {
      expect(toHex(vvEncode(vvOf(c.entries)))).toBe(c.hex);
    }
  });

  it("round-trips encode then decode", () => {
    for (const c of golden.vv_encode ?? []) {
      const vv = vvOf(c.entries);
      const back = vvDecode(vvEncode(vv));
      expect(back.size).toBe(vv.size);
      for (const [node, hlc] of vv) {
        expect(back.get(node)).toEqual(hlc);
      }
    }
  });

  it("rejects trailing bytes", () => {
    const encoded = vvEncode(vvOf([{ node: "a", wall: "1", logical: 0 }]));
    const padded = new Uint8Array(encoded.length + 1);
    padded.set(encoded);
    expect(() => vvDecode(padded)).toThrow();
  });
});

describe("vv relate", () => {
  it("matches the Erlang relation for every pair", () => {
    for (const c of golden.vv_relate ?? []) {
      expect(vvCompare(vvOf(c.a), vvOf(c.b))).toBe(c.result);
    }
  });
});

describe("vv contains", () => {
  it("matches the Erlang have-test for every version", () => {
    for (const c of golden.vv_contains ?? []) {
      const v: Version = {
        hlc: { wall: BigInt(c.wall), logical: c.logical },
        author: c.node,
      };
      expect(vvContains(vvOf(c.vv), v)).toBe(c.result);
    }
  });
});

describe("vv bump and merge", () => {
  it("bump never regresses a node's HLC", () => {
    const author = "n1";
    let vv = vvNew();
    vv = vvBump(vv, { hlc: { wall: 1000n, logical: 0 }, author });
    const higher = vvBump(vv, { hlc: { wall: 2000n, logical: 0 }, author });
    expect(higher.get(author)).toEqual({ wall: 2000n, logical: 0 });
    const lower = vvBump(higher, { hlc: { wall: 500n, logical: 0 }, author });
    expect(lower.get(author)).toEqual({ wall: 2000n, logical: 0 });
    expect(lower).toBe(higher); // unchanged returns the same reference
  });

  it("merge is the pointwise maximum", () => {
    const a = vvOf([
      { node: "x", wall: "1000", logical: 0 },
      { node: "y", wall: "1000", logical: 0 },
    ]);
    const b = vvOf([
      { node: "y", wall: "2000", logical: 0 },
      { node: "z", wall: "500", logical: 0 },
    ]);
    const m = vvMerge(a, b);
    expect(m.get("x")).toEqual({ wall: 1000n, logical: 0 });
    expect(m.get("y")).toEqual({ wall: 2000n, logical: 0 });
    expect(m.get("z")).toEqual({ wall: 500n, logical: 0 });
    // inputs untouched
    expect(a.get("y")).toEqual({ wall: 1000n, logical: 0 });
  });
});
