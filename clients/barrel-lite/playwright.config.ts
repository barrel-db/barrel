import { defineConfig, devices } from "@playwright/test";

const STATIC_PORT = process.env.STATIC_PORT ?? "5173";
const BARREL_PORT = process.env.BARREL_PORT ?? "18080";

export default defineConfig({
  testDir: "./test/browser",
  timeout: 60_000,
  fullyParallel: false,
  workers: 1,
  use: {
    baseURL: `http://127.0.0.1:${STATIC_PORT}`,
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: [
    {
      // compile the server profile, then boot it headless
      command: `bash -c 'cd ../.. && rebar3 as server compile >/dev/null && bash clients/barrel-lite/scripts/start-server.sh'`,
      url: `http://127.0.0.1:${BARREL_PORT}/health`,
      reuseExistingServer: !process.env.CI,
      timeout: 180_000,
      env: { BARREL_PORT },
    },
    {
      command: "node test/browser/static-server.mjs",
      url: `http://127.0.0.1:${STATIC_PORT}/test/browser/harness.html`,
      reuseExistingServer: !process.env.CI,
      timeout: 30_000,
      env: { STATIC_PORT },
    },
  ],
});
