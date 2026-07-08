import { describe, expect, it } from "vitest";
import { cosine, dot, norm } from "../src/vectors/cosine.js";

const f = (xs: number[]) => Float32Array.from(xs);

describe("cosine", () => {
  it("dot and norm", () => {
    expect(dot(f([1, 2, 3]), f([4, 5, 6]))).toBe(32);
    expect(norm(f([3, 4]))).toBe(5);
  });

  it("identical direction is 1, opposite is -1, orthogonal is 0", () => {
    expect(cosine(f([1, 0]), f([2, 0]))).toBeCloseTo(1, 6); // not unit, still 1
    expect(cosine(f([1, 0]), f([-1, 0]))).toBeCloseTo(-1, 6);
    expect(cosine(f([1, 0]), f([0, 1]))).toBeCloseTo(0, 6);
  });

  it("zero norm yields 0", () => {
    expect(cosine(f([0, 0]), f([1, 1]))).toBe(0);
  });

  it("ranks by descending similarity (hand-checked)", () => {
    const q = f([1, 0, 0]);
    const docs: [string, Float32Array][] = [
      ["a", f([2, 0, 0])], // cosine 1
      ["b", f([1, 1, 0])], // cosine ~0.707
      ["c", f([0, 1, 0])], // cosine 0
      ["d", f([-1, 0, 0])], // cosine -1
    ];
    const ranked = docs
      .map(([id, v]) => ({ id, score: cosine(q, v) }))
      .sort((x, y) => y.score - x.score);
    expect(ranked.map((r) => r.id)).toEqual(["a", "b", "c", "d"]);
    expect(ranked[0]?.score).toBeCloseTo(1, 6);
    expect(ranked[1]?.score).toBeCloseTo(Math.SQRT1_2, 6);
  });
});
