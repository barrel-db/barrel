/**
 * The local document cache: an in-memory record set over a StorageArea,
 * with the client's source_id and HLC clock persisted so authorship
 * and monotonicity survive a reload.
 *
 * Local writes mint a version {clock.now(), sourceId} and bump the
 * doc's own VV entry, so pull-time conflict resolution needs only the
 * stored VV (no rebase base). Persistence is a single snapshot per db;
 * flush() is explicit so the timing policy lives in the facade. Data
 * unflushed at a crash is lost, which is the cache contract: sync is
 * the durability story.
 */
import { base64Decode, base64Encode } from "../codec/base64.js";
import { randomSourceId, utf8Decode, utf8Encode } from "../codec/bytes.js";
import { compareHlc, encodeHlc, type HlcClock } from "../codec/hlc.js";
import { versionFromToken, versionToToken } from "../codec/version.js";
import { vvBump, vvDecode, vvEncode, vvNew } from "../codec/vv.js";
import { base64ToFloat32, float32ToBase64 } from "../codec/float32.js";
import type { JsonObject } from "../json.js";
import type { AttRef, DbMeta, DocRecord, StorageArea } from "./types.js";

const DOCS_BLOB = "docs.json";
const META_BLOB = "meta.json";
const ATTREFS_BLOB = "attrefs.json";
const VECTORS_BLOB = "vectors.json";
const SNAPSHOT_VERSION = 1;

interface DocsSnapshot {
  v: number;
  docs: DocRecord[];
}

interface AttRefsSnapshot {
  v: number;
  attrefs: AttRef[];
}

interface VectorsSnapshot {
  v: number;
  dim: number;
  vectors: { id: string; vec: string }[];
}

function attRefKey(id: string, name: string): string {
  return `${encodeURIComponent(id)}/${encodeURIComponent(name)}`;
}

export class LocalStore {
  private readonly area: StorageArea;
  private readonly clock: HlcClock;
  private readonly docs = new Map<string, DocRecord>();
  private readonly attrefs = new Map<string, AttRef>();
  private readonly vectors = new Map<string, Float32Array>();
  private vectorDimValue: number | undefined;
  private meta: DbMeta;
  private needsFlush = false;

  private constructor(area: StorageArea, clock: HlcClock, meta: DbMeta) {
    this.area = area;
    this.clock = clock;
    this.meta = meta;
  }

  /**
   * Open a store over an area, seeding a fresh source_id if none is
   * persisted and resuming the clock from the last flushed value.
   */
  static async open(area: StorageArea, clock: HlcClock): Promise<LocalStore> {
    const metaBytes = await area.read(META_BLOB);
    let meta: DbMeta;
    if (metaBytes) {
      meta = JSON.parse(utf8Decode(metaBytes)) as DbMeta;
      // resume monotonicity: the clock must not regress below our own
      // last authored write
      clock.update({ wall: BigInt(meta.clock.wall), logical: meta.clock.logical });
    } else {
      meta = { sourceId: randomSourceId(), clock: { wall: "0", logical: 0 }, checkpoints: {} };
    }
    const store = new LocalStore(area, clock, meta);
    const docsBytes = await area.read(DOCS_BLOB);
    if (docsBytes) {
      const snap = JSON.parse(utf8Decode(docsBytes)) as DocsSnapshot;
      for (const rec of snap.docs) store.docs.set(rec.id, rec);
    }
    const attrefsBytes = await area.read(ATTREFS_BLOB);
    if (attrefsBytes) {
      const snap = JSON.parse(utf8Decode(attrefsBytes)) as AttRefsSnapshot;
      for (const ref of snap.attrefs) {
        store.attrefs.set(attRefKey(ref.id, ref.name), ref);
      }
    }
    const vectorsBytes = await area.read(VECTORS_BLOB);
    if (vectorsBytes) {
      const snap = JSON.parse(utf8Decode(vectorsBytes)) as VectorsSnapshot;
      store.vectorDimValue = snap.dim;
      for (const { id, vec } of snap.vectors) {
        store.vectors.set(id, base64ToFloat32(vec));
      }
    }
    return store;
  }

  /** A fresh standard-base64 origin HLC (attachment LWW key), advancing
   * the shared clock so it stays monotonic with document writes. */
  mintOrigin(): string {
    return base64Encode(encodeHlc(this.clock.now()), "standard");
  }

  get sourceId(): string {
    return this.meta.sourceId;
  }

  /** The record for an id (including tombstones), or undefined. */
  get(id: string): DocRecord | undefined {
    return this.docs.get(id);
  }

  /** The live body for an id; undefined when missing or deleted. */
  getBody(id: string): JsonObject | undefined {
    const rec = this.docs.get(id);
    if (!rec || rec.deleted || rec.body === null) return undefined;
    return rec.body;
  }

  /** All records; live only unless includeDeleted is set. */
  allDocs(opts: { includeDeleted?: boolean } = {}): DocRecord[] {
    const out: DocRecord[] = [];
    for (const rec of this.docs.values()) {
      if (rec.deleted && !opts.includeDeleted) continue;
      out.push(rec);
    }
    return out;
  }

  /** Write a document locally, minting a new version authored by us. */
  put(id: string, body: JsonObject): DocRecord {
    return this.author(id, body, false);
  }

  /** Delete a document locally (a dirty tombstone to push). */
  remove(id: string): DocRecord {
    return this.author(id, null, true);
  }

  private author(id: string, body: JsonObject | null, deleted: boolean): DocRecord {
    const existing = this.docs.get(id);
    const baseVv = existing ? vvDecode(base64Decode(existing.vv)) : vvNew();
    const hlc = this.clock.now();
    const version = { hlc, author: this.meta.sourceId };
    const vv = vvBump(baseVv, version);
    const rec: DocRecord = {
      id,
      body,
      version: versionToToken(version),
      vv: base64Encode(vvEncode(vv), "standard"),
      deleted,
      dirty: true,
    };
    this.docs.set(id, rec);
    this.needsFlush = true;
    return rec;
  }

  /** Dirty records ordered by their version HLC (push order). */
  dirtyRecords(): DocRecord[] {
    return this.allDocsRaw()
      .filter((r) => r.dirty)
      .sort((a, b) =>
        compareHlc(versionFromToken(a.version).hlc, versionFromToken(b.version).hlc),
      );
  }

  /** Overwrite a record wholesale (used by the sync apply path). */
  putRecord(rec: DocRecord): void {
    this.docs.set(rec.id, rec);
    this.needsFlush = true;
  }

  /** Clear the dirty flag for a record if it still holds `version`. */
  clearDirty(id: string, version: string): void {
    const rec = this.docs.get(id);
    if (rec && rec.dirty && rec.version === version) {
      this.docs.set(id, { ...rec, dirty: false });
      this.needsFlush = true;
    }
  }

  getCheckpoint(key: string): string | undefined {
    return this.meta.checkpoints[key];
  }

  setCheckpoint(key: string, cursor: string): void {
    this.meta = {
      ...this.meta,
      checkpoints: { ...this.meta.checkpoints, [key]: cursor },
    };
    this.needsFlush = true;
  }

  //==================================================================
  // Attachment index
  //==================================================================

  getAttRef(id: string, name: string): AttRef | undefined {
    return this.attrefs.get(attRefKey(id, name));
  }

  allAttRefs(): AttRef[] {
    return [...this.attrefs.values()];
  }

  dirtyAttRefs(): AttRef[] {
    return this.allAttRefs().filter((r) => r.dirty);
  }

  putAttRef(ref: AttRef): void {
    this.attrefs.set(attRefKey(ref.id, ref.name), ref);
    this.needsFlush = true;
  }

  clearAttRefDirty(id: string, name: string, origin: string): void {
    const ref = this.attrefs.get(attRefKey(id, name));
    if (ref && ref.dirty && ref.origin === origin) {
      this.attrefs.set(attRefKey(id, name), { ...ref, dirty: false });
      this.needsFlush = true;
    }
  }

  getAttCheckpoint(key: string): string | undefined {
    return this.meta.attCheckpoints?.[key];
  }

  setAttCheckpoint(key: string, cursor: string): void {
    this.meta = {
      ...this.meta,
      attCheckpoints: { ...(this.meta.attCheckpoints ?? {}), [key]: cursor },
    };
    this.needsFlush = true;
  }

  //==================================================================
  // Vector index (per-doc embeddings, single dimension)
  //==================================================================

  vectorDim(): number | undefined {
    return this.vectorDimValue;
  }

  getVector(id: string): Float32Array | undefined {
    return this.vectors.get(id);
  }

  vectorIds(): string[] {
    return [...this.vectors.keys()];
  }

  allVectors(): IterableIterator<[string, Float32Array]> {
    return this.vectors.entries();
  }

  /** Store a doc's vector. The first put fixes the dimension; a
   * wrong-length vector is rejected so the index stays rectangular.
   * Returns true if stored. */
  putVector(id: string, vec: Float32Array): boolean {
    if (this.vectorDimValue === undefined) {
      this.vectorDimValue = vec.length;
    } else if (vec.length !== this.vectorDimValue) {
      return false;
    }
    this.vectors.set(id, vec);
    this.needsFlush = true;
    return true;
  }

  removeVector(id: string): void {
    if (this.vectors.delete(id)) this.needsFlush = true;
  }

  /** True if there are unpersisted changes since the last flush. */
  get dirtyForFlush(): boolean {
    return this.needsFlush;
  }

  /** Persist the document set and metadata (clock snapshot included). */
  async flush(): Promise<void> {
    const hlc = this.clock.peek();
    this.meta = {
      ...this.meta,
      clock: { wall: hlc.wall.toString(), logical: hlc.logical },
    };
    const snap: DocsSnapshot = { v: SNAPSHOT_VERSION, docs: this.allDocsRaw() };
    await this.area.write(DOCS_BLOB, utf8Encode(JSON.stringify(snap)));
    await this.area.write(META_BLOB, utf8Encode(JSON.stringify(this.meta)));
    const attSnap: AttRefsSnapshot = { v: SNAPSHOT_VERSION, attrefs: this.allAttRefs() };
    await this.area.write(ATTREFS_BLOB, utf8Encode(JSON.stringify(attSnap)));
    const vecSnap: VectorsSnapshot = {
      v: SNAPSHOT_VERSION,
      dim: this.vectorDimValue ?? 0,
      vectors: [...this.vectors.entries()].map(([id, vec]) => ({
        id,
        vec: float32ToBase64(vec),
      })),
    };
    await this.area.write(VECTORS_BLOB, utf8Encode(JSON.stringify(vecSnap)));
    this.needsFlush = false;
  }

  private allDocsRaw(): DocRecord[] {
    return [...this.docs.values()];
  }
}
