import { describe, expect, it } from "vitest";
import {
  ClockSkewError,
  compareHlc,
  decodeHlc,
  encodeHlc,
  HlcClock,
  type Hlc,
  MAX_LOGICAL,
} from "../src/codec/hlc.js";
import { toHex } from "../src/codec/bytes.js";
import { golden, type TsJson } from "./fixtures.js";

function hlcOf(ts: TsJson): Hlc {
  return { wall: BigInt(ts.wall), logical: ts.logical };
}

describe("hlc encode", () => {
  it("matches the Erlang encoding for every fixture", () => {
    for (const c of golden.hlc_encode) {
      const bytes = encodeHlc({ wall: BigInt(c.wall), logical: c.logical });
      expect(toHex(bytes)).toBe(c.hex);
      expect(bytes.length).toBe(12);
    }
  });

  it("round-trips encode then decode", () => {
    for (const c of golden.hlc_encode) {
      const hlc = { wall: BigInt(c.wall), logical: c.logical };
      expect(decodeHlc(encodeHlc(hlc))).toEqual(hlc);
    }
  });

  it("decodes from a subarray view", () => {
    const framed = new Uint8Array(20);
    const inner = encodeHlc({ wall: 1234n, logical: 7 });
    framed.set(inner, 5);
    expect(decodeHlc(framed.subarray(5, 17))).toEqual({
      wall: 1234n,
      logical: 7,
    });
  });

  it("rejects the wrong length", () => {
    expect(() => decodeHlc(new Uint8Array(11))).toThrow();
  });
});

describe("hlc compare", () => {
  it("matches the Erlang result for every pair", () => {
    const map = { lt: -1, eq: 0, gt: 1 } as const;
    for (const c of golden.hlc_compare) {
      expect(compareHlc(hlcOf(c.a), hlcOf(c.b))).toBe(map[c.result]);
    }
  });
});

describe("hlc clock traces", () => {
  it("replays every Erlang trace step for step", () => {
    for (const trace of golden.hlc_traces) {
      // A queue of physical values the injected clock returns in order,
      // one per op, exactly as the escript set the manual clock.
      const physQueue = trace.steps.map((s) => Number(s.phys));
      let idx = 0;
      const clock = new HlcClock({
        maxOffset: trace.maxoffset,
        physClock: () => physQueue[idx] as number,
      });
      for (const step of trace.steps) {
        if (step.op === "now") {
          const got = clock.now();
          expectState(got, step.result);
        } else {
          const remote = hlcOf(step.remote as TsJson);
          if (step.result === "timeahead") {
            const before = clock.peek();
            expect(() => clock.update(remote)).toThrow(ClockSkewError);
            expect(clock.peek()).toEqual(before);
          } else {
            expectState(clock.update(remote), step.result);
          }
        }
        idx++;
      }
    }
  });
});

describe("hlc logical overflow", () => {
  it("throws when now() would exceed MAX_LOGICAL", () => {
    expect(golden.max_logical).toBe(MAX_LOGICAL);
    const clock = new HlcClock({
      physClock: () => 0, // wall(1000) always >= phys(0), so now() ticks logical
      initial: { wall: 1000n, logical: MAX_LOGICAL },
    });
    expect(() => clock.now()).toThrow(/logical_overflow/);
  });
});

function expectState(got: Hlc, want: TsJson | "timeahead"): void {
  if (want === "timeahead") throw new Error("unexpected timeahead");
  expect(got.wall).toBe(BigInt(want.wall));
  expect(got.logical).toBe(want.logical);
}
