/**
 * Applying a pulled document to the local store, mirroring
 * barrel_db_server:do_put_version: idempotent skip, fast-forward on
 * dominance, and last-write-wins on a concurrent conflict. Losers are
 * not kept as local siblings (the server retains them); onConflict is
 * the hook for apps that must not silently drop a local edit.
 */
import { base64Decode, base64Encode } from "../codec/base64.js";
import {
  compareVersion,
  versionFromToken,
  type Version,
} from "../codec/version.js";
import {
  vvContains,
  vvCompare,
  vvDecode,
  vvEncode,
  vvMerge,
  type VV,
} from "../codec/vv.js";
import { recordBodies, recordFromState, recordState, type LocalStore } from "../store/localstore.js";
import type { DocRecord } from "../store/types.js";
import type { DocForRep, DocState } from "../wire/transport.js";
import { decodeState, encodeState, liveMerge } from "../codec/live.js";
import type { JsonObject } from "../json.js";
import type { OnConflict } from "./types.js";

export type ApplyOutcome =
  | "created"
  | "skip"
  | "fast_forward"
  | "remote_wins"
  | "local_wins";

export function applyRemote(
  store: LocalStore,
  id: string,
  remote: DocForRep,
  onConflict?: OnConflict,
): ApplyOutcome {
  const remoteVersion = versionFromToken(remote.version);
  const remoteVv = vvDecode(base64Decode(remote.vv));
  const local = store.get(id);

  if (!local) {
    store.putRecord(cleanRecord(id, remote, remoteVv));
    return "created";
  }

  const localVv = vvDecode(base64Decode(local.vv));
  if (vvContains(localVv, remoteVersion)) {
    return "skip"; // we already cover this version
  }

  const relation = vvCompare(remoteVv, localVv);
  const merged = vvMerge(localVv, remoteVv);

  if (relation === "dominates") {
    store.putRecord(cleanRecord(id, remote, merged));
    return "fast_forward";
  }
  if (relation === "eq" || relation === "dominated") {
    return "skip"; // our vector already covers the remote
  }

  // concurrent: deterministic last-write-wins
  const localVersion = versionFromToken(local.version);
  if (compareVersion(localVersion, remoteVersion) < 0) {
    // remote wins: replace, keeping the merged vector
    if (local.dirty && onConflict) {
      onConflict({ id, losing: local, winner: remote.version });
    }
    store.putRecord(cleanRecord(id, remote, merged));
    return "remote_wins";
  }
  // local wins: keep our body/version, record that we have seen the
  // remote (merged vv); a dirty local stays dirty so the push resolves
  // server-side to the same winner.
  store.putRecord({
    ...local,
    vv: base64Encode(vvEncode(merged), "standard"),
  });
  return "local_wins";
}

function cleanRecord(id: string, remote: DocForRep, vv: VV): DocRecord {
  return {
    id,
    body: remote.deleted ? null : remote.doc,
    version: remote.version,
    vv: base64Encode(vvEncode(vv), "standard"),
    deleted: remote.deleted,
    dirty: false,
  };
}

export type { Version };

//====================================================================
// Whole document states (servers that report full_state)
//====================================================================

export type StateOutcome = "created" | "merged" | "skip";

/**
 * Merge a document state pulled from the server into the local record.
 * Concurrent versions are all kept: a local write that loses stays as a
 * conflict, readable from the record, and onConflict tells the app.
 */
export function applyState(
  store: LocalStore,
  id: string,
  remote: DocState,
  onConflict?: OnConflict,
): StateOutcome {
  const local = store.get(id);
  const remoteState = decodeState(remote.state);
  const merged = local ? liveMerge(recordState(local), remoteState) : remoteState;
  const encoded = base64Encode(encodeState(merged), "standard");
  if (local && local.state === encoded) return "skip";
  const bodies = recordBodies(local);
  for (const [t, body] of Object.entries(remote.bodies)) {
    const info = merged.live.get(t);
    if (info) bodies.set(t, { deleted: info.deleted, body: info.deleted ? null : body });
  }
  const rec = recordFromState(id, merged, bodies, local?.dirty ?? false);
  if (local && local.dirty && rec.version !== local.version && onConflict) {
    // our write is no longer the winner; it stays as a conflict
    if (merged.live.has(local.version)) {
      onConflict({ id, losing: local, winner: rec.version });
    }
  }
  store.putRecord(rec);
  return local ? "merged" : "created";
}

/** The state and live bodies of a record, for a push. */
export function recordDocState(rec: DocRecord): DocState {
  const state = recordState(rec);
  const bodies: Record<string, JsonObject> = {};
  for (const [t, b] of recordBodies(rec)) {
    if (state.live.has(t)) bodies[t] = b.body ?? {};
  }
  return { state: encodeState(state), bodies };
}
