import { describe, expect, it } from "vitest";
import {
  compareVersion,
  versionFromToken,
  versionToToken,
  versionWinner,
  type Version,
} from "../src/codec/version.js";
import { golden } from "./fixtures.js";

function vOf(x: { wall: string; logical: number; author: string }): Version {
  return { hlc: { wall: BigInt(x.wall), logical: x.logical }, author: x.author };
}

describe("version tokens", () => {
  it("mints the Erlang token for every fixture", () => {
    for (const c of golden.version_tokens ?? []) {
      expect(versionToToken(vOf(c))).toBe(c.token);
    }
  });

  it("round-trips token -> version -> token", () => {
    for (const c of golden.version_tokens ?? []) {
      const v = versionFromToken(c.token);
      expect(v.author).toBe(c.author);
      expect(v.hlc.wall).toBe(BigInt(c.wall));
      expect(v.hlc.logical).toBe(c.logical);
      expect(versionToToken(v)).toBe(c.token);
    }
  });

  it("rejects malformed tokens", () => {
    expect(() => versionFromToken("nope")).toThrow();
    expect(() => versionFromToken("000000000000000000000000@")).toThrow();
    expect(() => versionFromToken("00000000000000000000000@a")).toThrow();
  });
});

describe("version compare", () => {
  it("matches the Erlang compare and winner for every pair", () => {
    const map = { lt: -1, eq: 0, gt: 1 } as const;
    for (const c of golden.version_compare ?? []) {
      const a = vOf(c.a);
      const b = vOf(c.b);
      expect(compareVersion(a, b)).toBe(map[c.result]);
      const w = versionWinner(a, b);
      expect(w.author).toBe(c.winner.author);
      expect(w.hlc.wall).toBe(BigInt(c.winner.wall));
      expect(w.hlc.logical).toBe(c.winner.logical);
    }
  });
});
