/**
 * Boots a real barrel_server for the integration suite and tears it
 * down after. Skipped when BARREL_URL points at an already-running
 * server. Requires rebar3 + Erlang on PATH (the CI image has both).
 */
import { type ChildProcess, execSync, spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, "../../../..");
const PORT = process.env.BARREL_PORT ?? "18080";
const TOKEN = process.env.BARREL_TOKEN ?? "itest-token";
const BASE = `http://127.0.0.1:${PORT}`;

let child: ChildProcess | undefined;

export async function setup(): Promise<void> {
  if (process.env.BARREL_URL) return; // external server provided

  execSync("rebar3 as server compile", { cwd: ROOT, stdio: "inherit" });

  const script = resolve(HERE, "../../scripts/start-server.sh");
  child = spawn("bash", [script], {
    cwd: ROOT,
    env: { ...process.env, BARREL_PORT: PORT, BARREL_TOKEN: TOKEN },
    stdio: ["ignore", "inherit", "inherit"],
  });

  await waitForHealth();
  process.env.BARREL_URL = BASE;
  process.env.BARREL_TOKEN = TOKEN;
}

export async function teardown(): Promise<void> {
  if (child && child.pid !== undefined) {
    child.kill("SIGTERM");
  }
}

async function waitForHealth(): Promise<void> {
  const deadline = Date.now() + 90_000;
  for (;;) {
    try {
      const r = await fetch(`${BASE}/health`);
      if (r.ok) return;
    } catch {
      // not up yet
    }
    if (Date.now() > deadline) {
      throw new Error("barrel_server did not become healthy in time");
    }
    await new Promise((r) => setTimeout(r, 500));
  }
}
