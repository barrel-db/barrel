import { describe, expect, it } from "vitest";
import { VERSION } from "../src/index.js";

describe("barrel-lite", () => {
  it("exports a semver version", () => {
    expect(VERSION).toMatch(/^\d+\.\d+\.\d+$/);
  });
});
