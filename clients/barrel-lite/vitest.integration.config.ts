import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/integration/**/*.test.ts"],
    globalSetup: ["test/integration/global-setup.ts"],
    // the server boot can take a few seconds on a cold build
    hookTimeout: 120_000,
    testTimeout: 30_000,
  },
});
