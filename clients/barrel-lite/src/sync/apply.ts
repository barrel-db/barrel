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
import type { LocalStore } from "../store/localstore.js";
import type { DocRecord } from "../store/types.js";
import type { DocForRep } from "../wire/transport.js";
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
