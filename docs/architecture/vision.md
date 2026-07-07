# Vision: the database for agents

This note defines what the `barrel` application becomes: not a wrapper over
`barrel_docdb` and `barrel_vectordb`, but the layer that makes them one
database, positioned for agent workloads. Read it before working on any layer
feature; the decision log at the end records what is settled and why.

## Positioning

Turso's agent story reduces to five ideas: database-as-file multi-tenancy,
copy-on-write branching, MVCC with commit-time conflict detection, asymmetric
sync with an authoritative remote, and an audit-log packaging (AgentFS).
The agent-infrastructure field splits into a memory layer (Letta, Mem0, Zep:
extraction, consolidation, decay) and a substrate (Neon, Turso, Tiger,
Cloudflare: cheap isolated DBs, branches, time travel, hybrid search, MCP).
Nobody owns both. Barrel does: this umbrella is the substrate, `barrel_memory`
is the memory layer.

Barrel's wedges, which the incumbents cannot copy:

- In-VM embedding for BEAM applications: no network hop, no serialization.
- True edge and multi-master sync with causal (HLC) conflict resolution;
  Turso retreated from edge replicas and uses last-push-wins.
- Branch merge. Turso and Neon branches are one-way forks; a CouchDB-heritage
  engine gets merge from its replication machinery.
- Provenance at the database level ("what did the agent know when").
- Open source and self-hosted end to end, EU-friendly.

## The record

A barrel record is one unit: document body, blobs (attachments), vectors, and
provenance (HLC, actor, source, session). One write path owns it.

- Write = document + embed-pending marker in a single docdb WriteBatch
  (atomic). Vector indexing is driven from the changes feed with exactly-once
  semantics; a re-driven consumer heals missing vectors after a crash.
- Per-database embedding policy: which fields embed, which model, sync or
  async. Sync mode gives read-your-write search; async is the default.
- `barrel_vectordb` stores only vectors and ANN indexes. Text and metadata are
  read through the `barrel_vectordb_docstore` seam pointed at the record's
  database (the seam's direction inverts: the document is primary, the vector
  is derived).
- Endgame (later phase): vector column families move into the record store's
  RocksDB so vector and document commit in the same batch with no marker.

## Query: BQL, a PartiQL dialect

One query surface over documents, vectors, and full-text.

- PartiQL semantics: MISSING vs NULL, path expressions, UNNEST. Adopt the
  spec, do not invent semantics.
- leex/yecc parser (riak_ql is the architectural template). The planner
  compiles onto the existing machinery: automatic path indexing, roaring64
  postings, cardinality-based index selection, snapshot cursors.
- Vector and FTS enter SQL as table functions first: `vector_top_k(...)`,
  `bm25_top_k(...)` (the libSQL pattern, no planner magic). Hybrid = RRF.
  An `ORDER BY distance LIMIT k` planner rewrite can come later.
- Live queries: the same query + SUBSCRIBE, backed by the existing query
  subscriptions; a live query is an Erlang process pushing deltas.
- v1 scope: SELECT, WHERE, ORDER BY, LIMIT, paths, UNNEST, table functions.
  Not v1: joins, GROUP BY, SQL-92 completeness.

## Sync

Server-authoritative for light clients, multi-master between servers.

- HLC version vectors replace rev-tree semantics (the Couchbase Lite 4.0
  move). Conflict default: last-write-wins by HLC, which respects causality.
  Losing versions are superseded, not deleted, and stay queryable in history.
  A merge hook allows custom resolution.
- Asymmetric protocol: push logical HLC-stamped mutations, pull a compacted
  change log.
- Partial sync is a write-time index (channels derived from the existing path
  posting lists), never a read-time filter over the global feed.
- Blobs sync out-of-band, content-addressed. Vectors sync quantized; ANN
  indexes are never shipped, they rebuild locally.
- A per-database retention window bounds tombstones, the PITR window, and the
  merge window in one knob.

## Timeline: branches and point-in-time recovery

Branching, PITR, and merge are one subsystem built on two primitives:
RocksDB checkpoints (fork mechanics: hardlinked SSTs, near-instant) and a
retained HLC change log (semantics). The changes feed must retain history
within the retention window; today it keeps only the latest change per doc.

- Branch = fork at now: checkpoint both stores, record `{parent, fork_hlc}`.
- PITR = fork at a past T: nearest checkpoint before T, replay the log to T.
  Restore creates a new database (Turso parity), and it can be merged back.
- Merge = one-shot replication of branch changes since `fork_hlc` into the
  parent through the normal sync conflict machinery, with a policy of
  `lww | fail_on_conflict | Fun`. Merge is sync; no separate engine.
- v1 lineage is linear parent-child. Branch DAGs and cross-lineage merges are
  explicitly later.

## Agent layer

- Database-per-agent: open/create cheap enough for hundreds of ephemeral
  databases per task; one supervised process per database, binary names end
  to end (no atom leak), and a lifecycle manager (`barrel_dbs`) with idle
  close and LRU eviction.
- Spaces: shared context containers with capability tokens; context is shared
  by reference, not copied. A space IS a barrel database; a capability is a
  hashed random token with read < write < admin rights, revocable locally.
- Sessions with TTL and handoffs are barrel primitives (mechanisms folded in
  from `barrel_memory`); a handoff is a shared space plus a capability whose
  possession is the right to accept. Sessions ride a general document-TTL
  primitive (`expires_at` + opt-in sweeper).
- MCP as a first-class surface: databases and live queries exposed as MCP
  resources with subscriptions (specced but unimplemented industry-wide),
  plus tools for query, write, branch, and merge, and the agent layer.
- Audit: provenance fields plus the retained change log answer "what did the
  agent know when" at the database level.
- SHIPPED (phase 8): see decision log row 7.

## Concurrency and encryption

- Point reads move off the per-database writer onto RocksDB snapshots
  (queries already bypass it; `get_doc` must too). Group commit in the
  writer. Document-level rev CAS stays as commit-time conflict detection.
  Write scale-out comes from many databases, which the agent model provides.
- Encryption at rest: a `barrel_keyprovider` behaviour with per-database
  keys (BYOK) resolved on the KEYSPACE (a branch opens with its parent's
  key), RocksDB EncryptedEnv for every store, and a sector cipher
  (offset-addressable AES-256-CTR + GCM envelopes for term files) for the
  mmap'd flat files EncryptedEnv cannot cover: BM25 disk and DiskANN.
  HNSW/FAISS state persists through the encrypted stores. Fail-closed
  opens via cleartext key-check markers. Per-database keys double as
  agent isolation. SHIPPED (phase 7).

## Browser: barrel-lite

A TypeScript protocol client, not a WASM port of the engine.

- OPFS storage with Web-Locks leader election (single writer per origin).
- HLC-stamped local mutations; push logical, pull the change log; local data
  is a cache (Safari evicts), sync is the durability story.
- Local BQL subset for the synced set; no ANN in the browser: brute-force
  WASM SIMD over the synced subset, optional transformers.js embeddings,
  offload heavy or global queries to the server.
- SHIPPED (phases 9a, 9b): see decision log row 8. Browser vector search is a
  later sub-phase (9c).

## barrel_memory

Stays a standalone product: the policy layer (LLM extraction, consolidation,
decay curves, importance scoring, MCP memory tools). Its mechanisms move down
into barrel: sessions/TTL, HLC mesh sync, per-key encryption, hybrid
retrieval configuration.

Migration path (staged, adapter-first):

1. barrel gains the mechanisms (record, spaces, sessions, sync, encryption).
2. `barrel_memory` swaps its bespoke RocksDB column families for barrel APIs
   behind its existing module boundaries.
3. Its mesh sync is replaced by barrel sync; memory records become documents
   in spaces with provenance; embeddings ride the record's embedding policy.
4. What remains in `barrel_memory` is policy and its MCP/HTTP surface.

## Non-goals

- No symmetric CRDT replication (the field's post-mortems are conclusive).
- No SQL-92 completeness before the document subset earns it.
- No WASM port of the Erlang engine.
- No embedded DuckDB/DataFusion query engine (cannot see barrel's indexes).

## Decision log

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| 1 | Sync core | HLC version vectors, not rev-trees | Rev-tree wounds (tombstones, arbitrary winners, metadata in read path); CBL 4.0 precedent. SHIPPED as phases 3+5: VV protocol + retained history (phase 3); network transport over HTTP (JSON + base64 wire, /db/:db/_sync/*), static bearer auth, write-time channels (0x1E feed) for partial sync, content-addressed attachment sync (feed in the blob store, LWW on origin HLC, streamed wire), event-driven continuous tasks (changes-stream wake, adaptive remote polling, error backoff). Quantized vector sync deferred to phase 9 |
| 2 | Doc+vector atomicity | Outbox marker in one WriteBatch, changes-feed indexing; storage unification later | Atomic now without blocking vectordb's standalone life. SHIPPED as record mode (phase 2): tagged outbox + supervised indexer + sync mode + read-through docstore |
| 3 | Query dialect | PartiQL semantics | JSON-first SQL semantics already specified; nothing to invent. SHIPPED as BQL v1 (phase 4): leex/yecc parser lowering onto the query engine, vector_top_k/bm25_top_k/hybrid_top_k table functions, UNNEST, SUBSCRIBE live queries, streaming REST endpoint |
| 4 | barrel_memory | Standalone policy product on barrel primitives | Memory layer is commoditizing; substrate is the durable value; migration staged |
| 5 | Branching | Timeline subsystem: checkpoint + retained HLC log; branch, PITR, merge-as-sync; linear lineage v1 | Merge is the leapfrog over Turso/Neon; PITR parity required. SHIPPED (phase 6): O(1) hard-link forks via rocksdb checkpoints + keyspace indirection (branch keeps the parent's name in keys, TIMELINE sidecar identity, fresh source_id); PITR rewind from the retained history log incl. conflict-window reconstruction; merge = incremental replication since fork through put_version (LWW/conflict_merger) with attachment phase; record-mode branches backfill their vector store from stored embeddings; REST /db/:db/_timeline/* |
| 6 | Encryption at rest | Per-database keys via barrel_keyprovider, keyspace-resolved; EncryptedEnv + sector cipher for flat files | Keys double as agent isolation; a branch shares its parent's key by keyspace. SHIPPED (phase 7): barrel_crypto app (GCM envelope, offset-addressable CTR, key-check tokens, env/custom providers); EncryptedEnv on docdb docs+attachments and every vectordb RocksDB; BM25 disk (static postings nonce rotated per compaction, embedded blockmax) and DiskANN (embedded graph sectors, static vectors/pq, sealed meta/pq_state); fail-closed open matrix everywhere; one `encryption` spec at barrel:open covers the whole logical db; server via open_opts |
| 7 | Agent layer | Spaces = databases + capability tokens; sessions/handoffs folded from barrel_memory; MCP inside barrel_server | Sharing by reference beats copying; possession-of-token beats caller identity for handoffs; one lifecycle manager serves REST, MCP, and spaces. SHIPPED (phase 8): binary db names + vectordb via-registry (atom leak closed); barrel_dbs lifecycle manager (idle close, LRU cap, pinning); provenance write option (actor/session/source, entity column + history TLV tail, local-only wire) with audit reads (history/doc_versions/version_body) and REST endpoints; doc TTL (expires_at, 0x1F expiry index, lazy reads + opt-in sweeper); apps/barrel_spaces (registry db, sp_ ids, per-space encryption), barrel_caps (bsp_ tokens, hash-only storage, rights ladder), sessions (sliding TTL, chronological messages, janitor), handoffs (CAS accept, complete revokes, chains); REST /spaces + /handoffs with capability bearers; MCP endpoint at /mcp (livery_mcp bridge, own auth provider for server + capability tokens, 26 tools, 3 resource templates, live-query bridge with debounced updated notifications, session GC, caps) |
| 8 | Browser client | barrel-lite: a TS protocol client in the umbrella (clients/barrel-lite), not a WASM port | The wire is plain JSON + base64, so the client re-implements the codecs and keeps local data as a cache. SHIPPED (phase 9a): byte-exact HLC/version/VV codecs (golden fixtures generated from the Erlang via an escript); local store over a StorageAdapter (memory + async main-thread OPFS) with a persisted source id and client-side checkpoints; one-shot and adaptive-polling live sync (apply mirrors do_put_version: skip/fast-forward/LWW with an onConflict hook); Web-Locks leader election with BroadcastChannel follower-proxy RPC (leader owns the store and sync); server enablers = CORS middleware (livery_cors wrapper) and capability tokens scoped to their space's /db routes (read = pull leg, write = push leg, fail-closed classifier); integration vs a real barrel_server and a Playwright chromium smoke. SHIPPED (phase 9b): a server-side continuous /changes SSE mode (subscribe push, ack backpressure, heartbeat) with a client stream that wakes the poller and degrades on drop; attachment sync (content-addressed OPFS blob store, sha256- digest, LWW on origin HLC, separate feed + checkpoint, put/get/remove/gc API); and a local BQL subset ported byte-for-byte (lexer + recursive-descent parser + lowering + evaluator) matching the server document subset, pinned by end-to-end golden fixtures generated from the Erlang, with queryRemote delegating vector/keyword/heavy queries to POST /db/:db/query. Browser vector sync + brute-force search (9c) is a later sub-phase |
