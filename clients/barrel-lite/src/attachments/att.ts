/**
 * Local attachment operations and the last-write-wins guard, a port of
 * barrel_att_feed:check. Attachments are content-addressed by digest in
 * a blob store and indexed by (docId, name) in the local attref index;
 * origin is the LWW key (a standard-base64 HLC). There are no version
 * vectors: older origin loses, newer wins, equal origin ties on digest
 * byte order (equal digest is a redelivery, so it loses too).
 */
import { base64Decode } from "../codec/base64.js";
import { compareHlc, decodeHlc } from "../codec/hlc.js";
import type { BlobStore } from "../store/blobstore.js";
import type { LocalStore } from "../store/localstore.js";
import type { AttRef } from "../store/types.js";
import { sha256Digest } from "./digest.js";

export interface AttInfo {
  digest: string;
  length: number;
  contentType: string;
}

/** Should an incoming attachment write replace the stored one? */
export function attApply(
  incomingOrigin: string,
  incomingDigest: string,
  stored: AttRef | undefined,
): boolean {
  if (!stored) return true;
  const c = compareHlc(decodeHlc(base64Decode(incomingOrigin)), decodeHlc(base64Decode(stored.origin)));
  if (c < 0) return false; // older loses
  if (c > 0) return true; // newer wins
  return incomingDigest > stored.digest; // equal origin: digest tie-break
}

/** Store an attachment locally, minting a fresh origin, dirty for push. */
export async function putAttachmentLocal(
  store: LocalStore,
  blobs: BlobStore,
  id: string,
  name: string,
  bytes: Uint8Array,
  contentType: string,
): Promise<AttInfo> {
  const digest = await sha256Digest(bytes);
  await blobs.put(digest, bytes);
  const origin = store.mintOrigin();
  store.putAttRef({
    id,
    name,
    digest,
    length: bytes.length,
    contentType,
    origin,
    op: "put",
    dirty: true,
  });
  return { digest, length: bytes.length, contentType };
}

/** Read an attachment's bytes and info; undefined if missing or deleted. */
export async function getAttachmentLocal(
  store: LocalStore,
  blobs: BlobStore,
  id: string,
  name: string,
): Promise<{ bytes: Uint8Array; info: AttInfo } | undefined> {
  const ref = store.getAttRef(id, name);
  if (!ref || ref.op === "delete") return undefined;
  const bytes = await blobs.read(ref.digest);
  if (!bytes) return undefined;
  return {
    bytes,
    info: { digest: ref.digest, length: ref.length, contentType: ref.contentType },
  };
}

export function getAttachmentInfoLocal(
  store: LocalStore,
  id: string,
  name: string,
): AttInfo | undefined {
  const ref = store.getAttRef(id, name);
  if (!ref || ref.op === "delete") return undefined;
  return { digest: ref.digest, length: ref.length, contentType: ref.contentType };
}

/** Tombstone an attachment locally, minting a fresh origin, dirty for push. */
export function removeAttachmentLocal(
  store: LocalStore,
  id: string,
  name: string,
): void {
  store.putAttRef({
    id,
    name,
    digest: "",
    length: 0,
    contentType: "",
    origin: store.mintOrigin(),
    op: "delete",
    dirty: true,
  });
}

/** Digests still referenced by a live (non-tombstone) attref (for GC). */
export function reachableDigests(store: LocalStore): Set<string> {
  const set = new Set<string>();
  for (const ref of store.allAttRefs()) {
    if (ref.op === "put" && ref.digest) set.add(ref.digest);
  }
  return set;
}
