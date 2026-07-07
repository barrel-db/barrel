import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    // integration (needs a running barrel_server) and browser (Playwright)
    // suites run through their own entry points
    exclude: ["**/node_modules/**", "test/integration/**", "test/browser/**"],
  },
});
