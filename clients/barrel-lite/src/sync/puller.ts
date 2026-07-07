/**
 * The pull loop: fetch changes since the stored cursor, fetch each
 * document the local vector does not already cover, apply it, and
 * advance the checkpoint. If the server's history floor has moved past
 * our cursor (the retained log was compacted), reset to a full pull.
 * Documents and the checkpoint persist in the same flush.
 */
import { base64Decode } from "../codec/base64.js";
import { compareHlc, decodeHlc } from "../codec/hlc.js";
import { versionFromToken } from "../codec/version.js";
import { vvContains, vvDecode } from "../codec/vv.js";
import type { LocalStore } from "../store/localstore.js";
import type { SyncFilter } from "../wire/filters.js";
import type { SyncTransport } from "../wire/transport.js";
import { applyRemote, type ApplyOutcome } from "./apply.js";
import { checkpointKey } from "./checkpoint.js";
import type { OnConflict, PullStats } from "./types.js";

const DEFAULT_LIMIT = 100;
const FIRST = "first";

export interface PullOptions {
  url: string;
  db: string;
  filter?: SyncFilter;
  limit?: number;
  onConflict?: OnConflict;
  /** Called for each applied document (for change fan-out). */
  onApply?: (id: string, outcome: ApplyOutcome, deleted: boolean) => void;
}

export async function pull(
  transport: SyncTransport,
  store: LocalStore,
  opts: PullOptions,
): Promise<PullStats> {
  const key = checkpointKey(opts.url, opts.db, opts.filter);
  const limit = opts.limit ?? DEFAULT_LIMIT;

  let since = store.getCheckpoint(key) ?? FIRST;
  since = await resetIfBelowFloor(transport, store, key, since);

  const stats: PullStats = { scanned: 0, applied: 0 };
  for (;;) {
    const query = opts.filter ? { limit, filter: opts.filter } : { limit };
    const page = await transport.changes(since, query);
    if (page.changes.length === 0) break;

    for (const row of page.changes) {
      stats.scanned++;
      const local = store.get(row.id);
      const remoteVersion = versionFromToken(row.rev);
      if (local && vvContains(vvDecode(base64Decode(local.vv)), remoteVersion)) {
        continue; // already covered locally
      }
      const doc = await transport.getDoc(row.id);
      const outcome = applyRemote(store, row.id, doc, opts.onConflict);
      opts.onApply?.(row.id, outcome, doc.deleted);
      stats.applied++;
    }

    since = page.lastSeq;
    store.setCheckpoint(key, since);
    await store.flush();

    if (page.changes.length < limit) break;
  }
  return stats;
}

/** Reset the cursor to "first" if the server's history floor is newer. */
async function resetIfBelowFloor(
  transport: SyncTransport,
  store: LocalStore,
  key: string,
  since: string,
): Promise<string> {
  if (since === FIRST) return since;
  const info = await transport.info();
  if (!info.historyFloor) return since;
  try {
    const sinceHlc = decodeHlc(base64Decode(since));
    const floorHlc = decodeHlc(base64Decode(info.historyFloor));
    if (compareHlc(sinceHlc, floorHlc) < 0) {
      store.setCheckpoint(key, FIRST);
      return FIRST;
    }
  } catch {
    // an undecodable cursor is safest reset to a full pull
    store.setCheckpoint(key, FIRST);
    return FIRST;
  }
  return since;
}
