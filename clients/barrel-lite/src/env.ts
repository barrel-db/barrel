/**
 * Runtime capability detection. The isomorphic core runs in Node and
 * the browser; the multi-tab and OPFS layers are gated on the APIs the
 * host actually provides.
 */
export interface Env {
  hasWebLocks: boolean;
  hasBroadcastChannel: boolean;
  hasOpfs: boolean;
}

export function detectEnv(): Env {
  const nav: unknown =
    typeof navigator !== "undefined" ? navigator : undefined;
  const hasWebLocks =
    nav !== undefined && typeof (nav as { locks?: unknown }).locks === "object";
  const hasBroadcastChannel = typeof BroadcastChannel !== "undefined";
  const hasOpfs =
    nav !== undefined &&
    typeof (nav as { storage?: { getDirectory?: unknown } }).storage
      ?.getDirectory === "function";
  return { hasWebLocks, hasBroadcastChannel, hasOpfs };
}
