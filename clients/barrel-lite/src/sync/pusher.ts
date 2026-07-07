/**
 * The push loop: send each dirty record the server does not already
 * have. A diff round trip (batched) skips versions the server holds
 * (cheap resume after a crashed push); each remaining record is PUT as
 * a version. If the server picks a different winner, the local record
 * is cleared and onConflict fires (the winning body arrives on the
 * next pull).
 */
import type { LocalStore } from "../store/localstore.js";
import type { DocRecord } from "../store/types.js";
import type { SyncTransport } from "../wire/transport.js";
import type { OnConflict, PushStats } from "./types.js";

const DIFF_BATCH = 100;

export interface PushOptions {
  onConflict?: OnConflict;
}

export async function push(
  transport: SyncTransport,
  store: LocalStore,
  opts: PushOptions = {},
): Promise<PushStats> {
  const dirty = store.dirtyRecords();
  const stats: PushStats = { pushed: 0, skipped: 0, conflicts: 0 };
  if (dirty.length === 0) return stats;

  for (let i = 0; i < dirty.length; i += DIFF_BATCH) {
    const batch = dirty.slice(i, i + DIFF_BATCH);
    const versions: Record<string, string> = {};
    for (const rec of batch) versions[rec.id] = rec.version;
    const diff = await transport.diff(versions);

    for (const rec of batch) {
      if (diff[rec.id] === "have") {
        store.clearDirty(rec.id, rec.version);
        stats.skipped++;
        continue;
      }
      await pushOne(transport, store, rec, opts, stats);
    }
  }

  await store.flush();
  return stats;
}

async function pushOne(
  transport: SyncTransport,
  store: LocalStore,
  rec: DocRecord,
  opts: PushOptions,
  stats: PushStats,
): Promise<void> {
  const result = await transport.putVersion(rec.id, {
    doc: rec.body ?? {},
    version: rec.version,
    vv: rec.vv,
    deleted: rec.deleted,
  });
  store.clearDirty(rec.id, rec.version);
  stats.pushed++;
  if (result.winner !== rec.version) {
    stats.conflicts++;
    if (opts.onConflict) {
      opts.onConflict({ id: rec.id, losing: rec, winner: result.winner });
    }
  }
}
