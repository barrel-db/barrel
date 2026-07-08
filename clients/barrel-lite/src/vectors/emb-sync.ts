/**
 * Embedding pull: fetch per-doc vectors for the documents the store
 * holds, via _bulk_get with include_embedding. A dedicated pass (not
 * riding the doc feed) because the async indexer writes a computed
 * vector with no rev bump, so the changes feed never re-emits it;
 * refetchAll re-reads vectors for docs that already have one to pick
 * those up. Vectors for gone/tombstoned docs are dropped.
 */
import type { LocalStore } from "../store/localstore.js";
import type { SyncTransport } from "../wire/transport.js";

const BATCH = 100;

export async function pullEmbeddings(
  transport: SyncTransport,
  store: LocalStore,
  opts: { batch?: number; refetchAll?: boolean } = {},
): Promise<{ fetched: number }> {
  const batchSize = opts.batch ?? BATCH;

  // live (non-deleted) doc ids, and the set that still lacks a vector
  const liveIds = new Set<string>();
  for (const rec of store.allDocs()) {
    if (rec.body !== null) liveIds.add(rec.id);
  }
  // GC: drop vectors whose doc is gone or a tombstone
  for (const id of store.vectorIds()) {
    if (!liveIds.has(id)) store.removeVector(id);
  }

  const want = opts.refetchAll
    ? [...liveIds]
    : [...liveIds].filter((id) => store.getVector(id) === undefined);

  let fetched = 0;
  for (let i = 0; i < want.length; i += batchSize) {
    const ids = want.slice(i, i + batchSize);
    const vectors = await transport.bulkGetEmbeddings(ids);
    for (const [id, { vector }] of vectors) {
      if (store.putVector(id, vector)) fetched++;
    }
    await store.flush();
  }
  return { fetched };
}
