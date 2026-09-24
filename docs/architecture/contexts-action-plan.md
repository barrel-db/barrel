# Contexts: action plan

Status: plan (2026-09-24), updated with experiment results the same day:
see [section 12](#12-status-after-the-experiments) and
[contexts-experiments.md](contexts-experiments.md). This document turns the
[contexts proposal](contexts.md) and the external context analysis
(`barrel-contexts-analysis.md`, kept outside the repository) into a technical
design and a dependency-ordered backlog. Nothing described here as new is
implemented. Every statement about current behaviour was checked against the
checkout on 2026-09-24 and cites the file it comes from. No performance
number in this document is measured; every threshold is provisional.

Read [contexts.md](contexts.md) for the original motivation. Where the two
documents disagree, this one reflects the code as it is today;
[section 11](#11-corrections-to-contextsmd) lists the replacements.

## 1. Product definition

> Publish meaningful datasets once. Discover them, query them remotely, or
> bring the necessary data and indexes locally. Keep the working set small as
> the total collection grows.

A **context** is a named, owned, bounded dataset with a stable identity. It
describes meaning and scope. It is not a node address, not a physical
database name, and not a partition. **Partitions** (databases, index
segments, manifest parts) bound storage and execution cost. A context may
live in one database or be published as many artifacts; the agent chooses
contexts, the executor chooses databases, artifacts, and locations.

### Chosen direction

1. **Remote composition first.** Query one local and several remote contexts
   with one BQL statement over the existing HTTP query route, with bounded
   fanout, deadlines, per-source provenance, and visible missing coverage.
2. **Portability and local materialization second.** Save retrieved sets and
   import published snapshots into bounded local working sets that answer
   queries offline with explicit coverage.
3. **Index packages third, as a measured experiment.** Publish immutable
   `barrel_ngram` segments to object storage and cache whole segments
   locally. Range reads and vector packages wait for measurements.

This keeps the order the analysis recommends. One departure: the catalog is
reduced to explicit registration in the first milestone (no enumeration
sweep, no generated descriptions), because the first slice only needs to
resolve a context id to query locations. Semantic discovery moves to the
milestone after the slice proves useful.

### Assumptions

- The first workload is agent retrieval (lexical, vector, hybrid) plus simple
  filtered, ordered row queries. Structured analytics are not a target.
- Contexts are few per query (single digits). Hundreds of contexts per query
  is out of scope for the first two milestones.
- Publishers run their own `barrel_server` endpoints. No shared cluster,
  consensus, or placement service is assumed.
- Every member of a composition is a Barrel database or a Barrel-published
  artifact set. Heterogeneous engines are not supported.
- Erlang callers get the core API; REST and MCP are thin adapters in
  `barrel_server`.

### Non-goals

- Cross-context transactions, cross-context atomic writes, distributed
  large-large joins, global uniqueness.
- Automatic replication or download of a whole context because a remote query
  failed.
- Cluster membership, consensus, automatic rebalancing, or permanent shard
  ownership.
- Treating semantic discovery as proof that unselected contexts hold no
  answers.
- One remote access pattern for every index format. Lexical segments, ANN
  graphs, and documents are planned separately.
- Globally meaningful rank fusion across disjoint corpora by equal-weight RRF.
- Live (SUBSCRIBE) federation in the first release.

## 2. Verified gap analysis

### 2.1 What exists and can be reused

| Capability | Where | What it gives the plan |
|---|---|---|
| Compile once, run against any database | `barrel_bql:compile/1,2` (`apps/barrel_docdb/src/barrel_bql.erl:55-76`); `barrel_bql_query:run/3` matches the target from the Db handle (`apps/barrel/src/barrel_bql_query.erl:31`) | The coordinator can classify a statement once and run it on local members. The `FROM` name stays in the plan but is ignored at run time. |
| Remote BQL over HTTP | `POST /db/:db/query` (`apps/barrel_server/src/barrel_server_api.erl:128-129`); `query/1`, `query_decision`, `prepare_query/1` (`apps/barrel_server/src/barrel_server_http.erl:589-635`) | NDJSON rows then a final `meta` line. Compile errors are 400 before the stream starts. This is the remote execution wire for milestone 1. |
| Browser remote query | `SyncTransport.queryRemote` (`clients/barrel-lite/src/wire/transport.ts:284-318`), `Db.queryRemote` (`clients/barrel-lite/src/db.ts:293-302`) | Confirms the wire is usable from a client. It does no local/remote merge. |
| Table functions | `vector_top_k`, `bm25_top_k`, `hybrid_top_k` (`apps/barrel_docdb/src/barrel_bql_lower.erl:63`); execution with over-fetch when a WHERE exists (`barrel_bql_query.erl:103-132`) | Per-context retrieval legs for federated retrieval. |
| In-store hybrid fusion | `do_search_hybrid/4`, `rrf_merge/6` (`apps/barrel_vectordb/src/barrel_vectordb_server.erl:1657-1741`) | RRF with `rrf_k` 60 combines BM25 and vector legs inside one store. It is not a cross-corpus ranking. |
| Cross-encoder reranker | `barrel_rerank:rerank/3,4` (`apps/barrel_rerank/src/barrel_rerank.erl:77-150`) | A comparator for heterogeneous candidates. Nothing outside the app calls it. Python sidecar. |
| Persisted ANN graphs | `load_or_create_index/5`, `load_index_graph/5` (`barrel_vectordb_server.erl:1053-1188`); CHANGELOG 2.4.0 (`apps/barrel_vectordb/CHANGELOG.md:8-19`) | A graph that matches the vectors column family (write sequence, config fingerprint, CRC) loads without a rebuild; `stats/1` reports `index_origin`. Cold open cost is a measurement, not missing work. |
| Lifecycle manager | `barrel_dbs` (`apps/barrel/src/barrel_dbs.erl`): coalesced opens, idle close, LRU cap `dbs_max_open`, `pin/1` | Keeps local members open on demand. Pinning is a boolean, not a refcount (`:61`, `:276-286`). |
| O(1) branches, PITR, merge | `barrel_timeline` (`apps/barrel_docdb/src/barrel_timeline.erl`) | A derived dataset mechanism. Not partitioning. |
| RocksDB checkpoint of docs and attachments | `barrel_db_server:checkpoint_to/3` (`apps/barrel_docdb/src/barrel_db_server.erl:124-131`, `:472-483`) | Building block for export. Only the timeline calls it today. |
| Per-instance identity and last write HLC | `barrel_docdb:db_instance_id/1` (`apps/barrel_docdb/src/barrel_docdb.erl:2140-2163`); `barrel_changes:get_last_seq/2` (`apps/barrel_docdb/src/barrel_changes.erl:778-813`) | Ingredients for an observed-version tag. Neither reaches the HTTP query response. |
| Capabilities | `barrel_caps` (`apps/barrel_spaces/src/barrel_caps.erl`): one scope per grant, `read < write < admin`; server classification (`apps/barrel_server/src/barrel_server_auth.erl:231-307`) | Source-side enforcement for local and remote members. |
| Immutable lexical segments with local sharding | `barrel_ngram_segment:write/2` temp+rename (`apps/barrel_ngram/src/barrel_ngram_segment.erl:105-167`); per-shard manifest (`barrel_ngram_manifest.erl`); rendezvous sharding (`barrel_ngram_shards.erl:25-38`) | The first candidate for published index packages. |
| Object storage client | `livery_s3` 0.2.0 (range GET, conditional PUT `if_none_match`/`if_match`, prefix listing, multipart), used by `apps/barrel_att_s3` | A client for package publication. Only reachable in the `s3` profiles today. |

### 2.2 The six corrections the task asked to verify

| Claim | Verdict | Evidence |
|---|---|---|
| Remote HTTP BQL and barrel-lite `queryRemote` already exist. | **True.** Gaps: the response drops `last_seq` (`query_meta/1`, `barrel_server_http.erl:708-720`); a streamable plan (no ORDER BY, LIMIT, OFFSET, UNNEST) streams the whole result with no bound (`run_query/4`, `:689-706`); there is no deadline or row cap on the route; `parseNdjson` in barrel-lite skips in-band `{"error":...}` lines, so a mid-stream failure reads as a short success (`transport.ts:603-624`). | as cited |
| Compatible persisted HNSW/FAISS graphs load without rebuilding. | **True** since barrel_vectordb 2.4.0. DiskANN manages its own files. BM25 disk and DiskANN are flat files under `db_path`, outside RocksDB (`barrel_vectordb_server.erl:304`, `:1583-1584`). | CHANGELOG 2.4.0; `load_index_graph/5` |
| Ngram indexing already has immutable segments and local sharding. | **True**, with limits that matter for publication: no checksums anywhere in the segment format; no fsync on segment, manifest, or `corpus.meta` writes; no per-segment key or gram summary usable for pruning; a query opens segment paths after the snapshot with no lease, so a concurrent compaction can delete an input and the query fails with `enoent`; posting and key read errors are swallowed as empty (`barrel_ngram_query.erl:322,331`; `barrel_ngram_segment.erl:432-435`). ngram is not integrated in the `barrel` app or BQL; its only consumer is the MCP `ngram_search` tool (`apps/barrel_server/src/barrel_server_mcp_tools.erl:400-478`). | `apps/barrel_ngram/docs/design.md:20-21`, sources as cited |
| S3 attachments do not imply queryable remote indexes. | **True.** `barrel_att_s3_store` implements the attachment backend contract only (DocId/AttName keys, a local feed RocksDB, HLC metadata); it never uses range GETs (`apps/barrel_att_s3/src/barrel_att_s3_store.erl:672-678`). Its reusable parts are the `livery_s3` client, the conditional-write probe (`:291-306`), and the multipart GC with the MinIO prefix workaround (`barrel_att_s3_multipart_gc.erl:125-134`). Garage lacks the conditional writes it probes for (`apps/barrel_att_s3/docs/limitations.md:129-133`). | as cited |
| Filtered replication retains documents after they stop matching. | **True.** "A document that stops matching stops being sent; replicas keep their last copy (no target-side delete). A fresh replica never sees departed docs." (`docs/guides/synchronization.md:204-205`; `barrel_channel.erl:13-19`). A filtered replica is not an accurate continuously maintained subset. | as cited |
| Branching is not partitioning. | **True.** A branch is a hard-link checkpoint of the whole database with keyspace indirection to the parent (`barrel_timeline.erl:84-120`; `barrel_keyspace.erl:4-20`). A branch of a branch is rejected. A composed branch gets a fresh vector store (`barrel:branch/3`, `apps/barrel/src/barrel.erl:311-374`). Splitting needs a selection, an ownership rule, and a new manifest. | as cited |

### 2.3 Further facts that shape the design

- **Pagination does not cross the wire for ordered or bounded queries.**
  Continuations exist only for streamable collection plans
  (`barrel_bql_lower.erl:640-644`); materializing plans and table functions
  return `{error, {unsupported, continuation}}`
  (`apps/barrel_docdb/src/barrel_bql_exec.erl:49-52`,
  `barrel_bql_query.erl:98-99`). A continuation is a node-local ETS cursor on
  a RocksDB snapshot with a 60 s TTL (`barrel_query_cursor.erl:24,49`).
- **Ordering is term order with stable input-position ties, not id ties**
  (`barrel_bql_exec:sort_frames/2`, `:322-344`). Only one ORDER BY key is
  allowed (`barrel_bql.erl:134`). barrel-lite uses a different comparator
  (`clients/barrel-lite/src/bql/eval.ts:126-139`).
- **Scores.** `vector_top_k` returns `_score = 1 - distance` (cosine by
  default) and `_distance`; `bm25_top_k` returns a raw, corpus-dependent BM25
  score; `hybrid_top_k` returns an RRF score (`barrel_bql_query.erl:180-185`;
  `barrel_vectordb_server.erl:1544`). RRF ties come out of `sets:to_list` and
  are not ordered deterministically (`:1727-1741`).
- **No read version on retrieval results.** Table-function meta is
  `#{has_more => false, count}` only (`barrel_bql_query.erl:122`). The
  search leg and the document join do not share a snapshot. Collection meta
  carries `last_seq`, read from the live metadata key, so it can be ahead of
  the snapshot the rows came from.
- **Embedder identity is incomplete.** `barrel:embedder_info/1` returns
  provider module and name and dimension, not a model id
  (`apps/barrel_embed/src/barrel_embed.erl:179-191`). The record-mode policy
  holds the embedder config opaquely in a local doc `_barrel/embedding`
  (`barrel.erl:1003`, `:1329-1344`).
- **No durable database enumeration.** `barrel_dbs:list/0` and
  `barrel_docdb:list_dbs/0` return open databases only
  (`barrel_dbs.erl:287-288`; `barrel_docdb.erl:671-685`). `_barrel_system`
  stores the node id and `_dbdir:<Name>` only for databases created with an
  explicit `data_dir` (`barrel_docdb.erl:542-624`).
- **No close hook in `barrel_dbs`.** The proposal's refresher seam would
  have to be built.
- **`auth_context/1` scopes are already populated** with the rights names
  (`barrel_caps.erl:164-166`). Adding capability scopes means a new field or
  a changed meaning, not filling an empty slot.
- **A `read` capability is also a copy capability.** `read` classifies
  `query`, `find`, `search/*`, `_bulk_get`, and the pull legs of `_sync/*`
  (`barrel_server_auth.erl:231-307`). The source cannot grant query without
  copy today.
- **Stats are estimates of keys, not documents.** `document_count` is
  `rocksdb.estimate-num-keys` over the whole store, including index and feed
  keys (`apps/barrel_docdb/src/barrel_docdb_usage.erl:79-103`).
- **No export, import, backup, or restore exists.** The vector store has no
  checkpoint-to-directory (`barrel_vectordb:checkpoint/1` persists in place,
  `apps/barrel_vectordb/src/barrel_vectordb.erl:740-742`); its default
  `db_path` is `priv/barrel_vectordb_<name>` relative to the working directory
  and nothing records it (`:757-760`). A plain record-mode open does not
  backfill a fresh vector store (`barrel.erl:245-299`); only branches run
  `barrel_record_backfill:run/1`.
- **Import identity hazards.** The per-instance `source_id` is stored under
  the logical name; restoring a checkpoint under the same name elsewhere
  reuses it, and two live copies would author versions under one id
  (`barrel_db_server.erl:1232-1252`). Restoring under a new name keeps keys
  that embed the old name, which is what the keyspace indirection of
  branches exists to handle.
- **Dependency boundaries** (from each app's `rebar.config` and `app.src`):
  `barrel_docdb` depends on `barrel_crypto`; `barrel_vectordb` on
  `barrel_crypto`, `barrel_embed`; `barrel` on `barrel_crypto`,
  `barrel_docdb`, `barrel_vectordb`, `barrel_embed`; `barrel_spaces` on
  `barrel`; `barrel_ngram` on `barrel_docdb`; `barrel_att_s3` on
  `barrel_docdb` plus `livery_s3`; `barrel_server` on `barrel`,
  `barrel_spaces`, `barrel_ngram`. `hackney` is already a dependency of
  `barrel_docdb` and `barrel_vectordb`. `livery_s3` must stay out of the
  default build (root `rebar.config:94-95`, `:130-131`).

### 2.4 What is genuinely missing

| Missing primitive | Needed by |
|---|---|
| Context identity, card store, location resolution | M1 |
| An Erlang client for remote `POST /db/:db/query` with a deadline and in-band error handling | M1 |
| Observed version and a row cap and deadline on the HTTP query route | M1 |
| A federated executor with shape checks, bounded fanout, merge, provenance, coverage | M1 |
| REST and MCP adapters for the contract | M1 |
| Working sets, retrieved-set slices, import of published snapshots, offline coverage | M2 |
| Export of a quiesced composed database to a checksummed artifact set | M2 |
| A read-only open mode so a server can honestly serve a pinned generation | M2 |
| Query-without-copy permission at the source | later (decision D1, section 10) |
| Segment checksums, fsync, a fetch seam, and a published-corpus mode in `barrel_ngram` | M3 |
| Object-store publication, retention, and GC for packages | M3 |
| Embedding model identity, cross-context score merge, reranked merge | M1.5 |
| Catalog search, federation of catalogs, handoff of working sets, live federation | later |

## 3. Conceptual model and contracts

### 3.1 Identity rules

- **Context id**: `ctx_` plus 24 lowercase base32 characters (15 random
  bytes), minted once by the owner and never reused. It does not encode a
  node, a database name, or a display name. Barrel already mints ids this way
  (`barrel_spaces:new_id/1`).
- **Display name**: a mutable, human path such as `projects/gunicorn`. A
  catalog may resolve a name to an id; queries and manifests always carry
  the id.
- **Location**: where a context can be queried: `{endpoint, db}` for a
  remote Barrel server, `{local, db}` for this node. A location is not an
  identity; a context may have several.
- **Replica versus derived dataset**:
  - A **replica** serves the same context id. It is either a live replica
    (continuous replication, no pinned version) or an imported copy of one
    published generation. A replica may appear as another location on the
    same card.
  - A **derived dataset** gets a new context id and records
    `derived_from: {context, version, selection}`. Branches, retrieved-set
    slices, filtered subsets, and merged datasets are derived. A branch is
    never a replica, even when its content is identical at fork time.
- **Versions**: two kinds, never mixed silently.
  - `live`: a mutable database. The strongest statement Barrel can make is an
    **observation**: `{instance_id, last_seq}` read at or after execution. It
    is not a consistent snapshot and not proof of complete indexing.
  - `generation`: an integer, monotonic per context, naming one immutable
    published manifest. Only a generation is a pinned version.

### 3.2 Catalog card

Small, descriptive, advisory. No artifact inventory.

```json
{
  "type": "context_card",
  "format": 1,
  "id": "ctx_4q2m7c1xk9a3bz5d8f6h0j2n",
  "name": "projects/gunicorn",
  "title": "Gunicorn source",
  "description": "Source tree and commit history of gunicorn, one doc per file.",
  "description_source": "authored",
  "owner": "org:enki",
  "topics": ["python", "http", "wsgi"],
  "discoverable": "listed",
  "locations": [
    {"kind": "remote", "endpoint": "https://src.example.org",
     "db": "gunicorn_src", "serves": "live",
     "query": ["rows", "vector", "bm25", "hybrid"]},
    {"kind": "snapshot", "publication": "s3://barrel-pub/ctx_4q2m.../",
     "latest_generation": 42}
  ],
  "capabilities": {"query": "token", "copy": "token", "public": false},
  "embedding": {"fingerprint": "sha256:9c1e...", "dimensions": 384},
  "updated_at": "2026-09-24T10:00:00Z"
}
```

- `discoverable`: `listed | unlisted`. Unlisted cards resolve by id only.
- `capabilities` states what the source enforces; the card never carries
  credentials and never grants anything. It is advisory: the source decides.
- `embedding.fingerprint` is the hash defined in 3.4; it is present only when
  the source can report it (backlog item B11).
- Computed statistics (`doc_estimate`, `bytes`) are optional, labelled as
  estimates, and never used for correctness.

### 3.3 Snapshot manifest

One root per generation, pointing to parts. The root stays small no matter
how many artifacts a generation has.

```json
{
  "type": "snapshot_manifest",
  "format": 1,
  "context": "ctx_4q2m7c1xk9a3bz5d8f6h0j2n",
  "generation": 42,
  "parent_generation": 41,
  "published_at": "2026-09-24T10:05:00Z",
  "publisher": "node:src-1",
  "engine": {"barrel_docdb": "1.5.0", "barrel_vectordb": "2.4.0",
             "min_reader": {"barrel_docdb": "1.5"}},
  "sources": [
    {"db": "gunicorn_src", "instance_id": "a1b2c3d4e5f60718",
     "last_seq": "AZ3kQ2d1AAAB", "quiesced": true}
  ],
  "parts": [
    {"kind": "data", "ref": "parts/sha256-7f2a....json",
     "sha256": "7f2a...", "artifacts": 3, "bytes": 482113536},
    {"kind": "index", "index": "ngram-v4", "ref": "parts/sha256-01bd....json",
     "sha256": "01bd...", "artifacts": 64, "bytes": 912004096}
  ]
}
```

A part lists artifacts with checksums and pruning summaries:

```json
{
  "type": "manifest_part",
  "format": 1,
  "kind": "index",
  "index_dataset": "idx_ngram_gunicorn_v4",
  "artifacts": [
    {"path": "objects/sha256-3c9d....ngseg", "sha256": "3c9d...",
     "bytes": 14220288, "docs": 1000,
     "summary": {"gram_filter": {"kind": "bloom", "bits": 131072,
                                 "hashes": 5, "b64": "..."}}}
  ]
}
```

Growth rules:

- Artifacts are content-addressed (`objects/sha256-<hex>`), so a new
  generation reuses unchanged artifacts and unchanged parts by reference.
  Only changed parts and the new root are written.
- A part holds at most a fixed number of artifacts (initially 1000). Large
  contexts add parts, not a larger root.
- `quiesced: true` means the export ran with writes stopped (see B14). A
  generation made without quiescing is not publishable in M2.

### 3.4 Index dataset

Describes one index built over a known source state.

```json
{
  "type": "index_dataset",
  "format": 1,
  "id": "idx_ngram_gunicorn_v4",
  "context": "ctx_4q2m7c1xk9a3bz5d8f6h0j2n",
  "generation": 42,
  "index": {"engine": "barrel_ngram", "segment_format": 4,
            "manifest_format": 2, "postings": "roaring",
            "fields": ["body", "path"],
            "phase2_selector": {"radius": 3, "sample_rate": 4}},
  "source": {"data_part": "parts/sha256-7f2a....json",
             "instance_id": "a1b2c3d4e5f60718", "last_seq": "AZ3kQ2d1AAAB"},
  "coverage": {"kind": "exact", "docs_indexed": 64000, "docs_in_source": 64000,
               "unindexed": []},
  "locator": {"kind": "doc_id", "resolve": "data_part"}
}
```

For a vector index (M3 or later), `index` carries the fingerprint inputs:
provider, model id, model revision, dimensions, distance metric, text
preprocessing (fields, join, truncation), and quantization. The
**embedding fingerprint** is the sha256 of the canonical JSON of those
fields. Two vector result sets are score-comparable only when their
fingerprints are equal.

`coverage.kind` is `exact` (every source document at `source.last_seq` is
indexed, checked at publication by counting) or `partial` with an explicit
`unindexed` list or count. An index without a coverage statement is not
publishable.

### 3.5 Working set

A local record of what an agent is working with. It lives in a local
database (`_barrel_worksets`, one document per working set).

```json
{
  "type": "working_set",
  "id": "ws_k3v8r2m1q0p9s7t5",
  "owner": "session:9f2c",
  "created_at": "2026-09-24T11:00:00Z",
  "budget": {"bytes": 1073741824, "open_dbs": 8, "remote_parallel": 4,
             "transfer_bytes": 268435456, "deadline_ms": 10000},
  "members": [
    {"context": "ctx_4q2m...", "mode": "remote", "location": 0,
     "credential_ref": "src-example-read"},
    {"context": "ctx_7h1p...", "mode": "snapshot", "generation": 17,
     "local_db": "wsnap_7h1p_17", "coverage": "complete_generation"},
    {"context": "ctx_2d9x...", "mode": "retrieved_set",
     "local_db": "wslice_k3v8_2d9x",
     "derived": {"from": "ctx_2d9x...", "observed": {"instance_id": "0f1e...",
                 "last_seq": "AZ3k..."},
                 "query": "SELECT * FROM hybrid_top_k('h2 frames', k => 40) AS h",
                 "docs": 40},
     "coverage": "retrieved_set"}
  ],
  "usage": {"bytes": 38211072}
}
```

- `mode`: `local` (a local live database), `remote`, `snapshot` (an
  imported generation), `retrieved_set` (a frozen slice), later `cache`
  (index segments).
- `credential_ref` names a credential held in node configuration. Working
  sets, cards, and manifests never contain secrets.

### 3.6 Operations

Six verbs. The Erlang API is the contract; REST and MCP map to it one to one.

| Verb | Erlang (`apps/barrel`) | REST (`barrel_server`) | MCP tool |
|---|---|---|---|
| discover | `barrel_ctx:discover(Query, Opts)` | `GET /contexts?q=` | `context_discover` |
| inspect | `barrel_ctx:inspect(CtxId)` | `GET /contexts/:id` | `context_inspect` |
| attach | `barrel_ctx:attach(WsId, CtxId, Opts)` | `POST /worksets/:ws/members` | `context_attach` |
| query | `barrel_ctx:query(Request)` | `POST /contexts/_query` | `context_query` |
| materialize | `barrel_ctx:materialize(WsId, Request)` | `POST /worksets/:ws/_materialize` | `context_materialize` |
| detach | `barrel_ctx:detach(WsId, CtxId)` | `DELETE /worksets/:ws/members/:id` | `context_detach` |

Responsibilities:

- `apps/barrel`: identity, cards, working sets, the executor, the remote
  client, materialization. No capability checks (the `barrel` app cannot
  depend on `barrel_spaces`).
- `apps/barrel_server`: authentication, per-member authorization of local
  members through the existing classifier, request decoding, response
  encoding, MCP tool specs. The same request and response maps are encoded
  as JSON for REST and as tool results for MCP.
- Sources: enforce their own access. A card never authorizes.

Attach is logical: it adds a member to a working set. It does not open,
pin, or download anything. Opening happens per query through
`barrel_dbs:ensure/2`; pinning is only used for the duration of a query or
a materialization, with a refcount held by the executor, because
`barrel_dbs` pins are booleans.

### 3.7 Query request and response

Request, one local and two remote contexts:

```json
{
  "query": "SELECT h.id, h.path, h._score FROM hybrid_top_k('h2 frame parsing', k => 10) AS h",
  "contexts": ["ctx_local_docs", "ctx_4q2m...", "ctx_8w3n..."],
  "merge": "grouped",
  "deadline_ms": 5000,
  "per_context_timeout_ms": 4000,
  "max_parallel": 8
}
```

Response, all sources answered:

```json
{
  "execution": "succeeded",
  "merge": "grouped",
  "groups": [
    {"context": "ctx_local_docs", "rows": [{"id": "rfc7540.md#6.1", "path": "rfc/7540.md", "_score": 0.031}]},
    {"context": "ctx_4q2m...", "rows": [{"id": "gunicorn/http2/frames.py", "path": "gunicorn/http2/frames.py", "_score": 0.033}]},
    {"context": "ctx_8w3n...", "rows": []}
  ],
  "sources": [
    {"context": "ctx_local_docs", "location": {"kind": "local", "db": "docs_http"},
     "status": "ok", "rows": 10, "bound": "limit_reached",
     "retrieval": "approximate", "version": {"kind": "live",
     "observed": {"instance_id": "5d0c...", "last_seq": "AZ3k..."}},
     "elapsed_ms": 38},
    {"context": "ctx_4q2m...", "location": {"kind": "remote", "endpoint": "https://src.example.org", "db": "gunicorn_src"},
     "status": "ok", "rows": 10, "bound": "limit_reached",
     "retrieval": "approximate", "version": {"kind": "live",
     "observed": {"instance_id": "a1b2...", "last_seq": "AZ3m..."}},
     "elapsed_ms": 212},
    {"context": "ctx_8w3n...", "location": {"kind": "remote", "endpoint": "https://rfc.example.net", "db": "rfcs"},
     "status": "ok", "rows": 0, "bound": "exhausted",
     "retrieval": "approximate", "version": {"kind": "generation", "generation": 17},
     "elapsed_ms": 180}
  ],
  "coverage": {"requested": 3, "answered": 3, "failed": 0, "skipped": 0,
               "scope_origin": "explicit"}
}
```

Field meanings, so no single boolean has to carry them:

- `execution`: `succeeded` (every requested member answered),
  `partial` (at least one answered, at least one did not), `failed` (none
  answered, or the request was rejected).
- `sources[].status`: `ok | timeout | unreachable | unauthorized | error |
  skipped_budget | skipped_offline`.
- `sources[].bound`: `exhausted` (the source returned everything that
  matched the statement) or `limit_reached` (the source hit the LIMIT or
  k; more matches may exist).
- `sources[].retrieval`: `exact` for collection queries and `bm25_top_k`
  (exact top-k by that corpus's BM25), `approximate` for `vector_top_k` and
  `hybrid_top_k` (ANN).
- `sources[].version`: `live` with an observation, or a `generation`. A
  source is reported as `generation` only when it is an imported,
  read-only generation (see B15). The observation may be later than the
  state the rows came from; it is a freshness hint.
- `coverage.scope_origin`: `explicit` when the caller named the contexts,
  `discovered` when `discover` chose them. Discovery never implies that
  other contexts hold no answers.

The same request with one remote member timing out:

```json
{
  "execution": "partial",
  "merge": "grouped",
  "groups": [
    {"context": "ctx_local_docs", "rows": ["..."]},
    {"context": "ctx_4q2m...", "rows": ["..."]}
  ],
  "sources": [
    {"context": "ctx_local_docs", "status": "ok", "rows": 10, "bound": "limit_reached", "...": "..."},
    {"context": "ctx_4q2m...", "status": "ok", "rows": 10, "bound": "limit_reached", "...": "..."},
    {"context": "ctx_8w3n...", "status": "timeout", "rows": 0,
     "error": {"reason": "deadline", "after_ms": 4000,
               "rows_received_before_timeout": 0}}
  ],
  "coverage": {"requested": 3, "answered": 2, "failed": 1, "skipped": 0,
               "missing": ["ctx_8w3n..."], "scope_origin": "explicit"}
}
```

Rows received from a source before it timed out or before an in-band error
are discarded, and the count is reported. A partially streamed source is
never presented as answered.

### 3.8 Materialization request and response

Save selected results into the working set as a retrieved-set slice.

```json
{
  "from_query": {"query": "SELECT h.id FROM hybrid_top_k('h2 frame parsing', k => 40) AS h",
                 "contexts": ["ctx_4q2m..."]},
  "include": {"attachments": false, "embeddings": true},
  "max_bytes": 67108864
}
```

```json
{
  "working_set": "ws_k3v8r2m1q0p9s7t5",
  "slices": [
    {"context": "ctx_4q2m...", "local_db": "wslice_k3v8_4q2m",
     "docs": 40, "bytes": 1841152, "status": "complete",
     "derived": {"from": "ctx_4q2m...",
                 "observed": {"instance_id": "a1b2...", "last_seq": "AZ3m..."},
                 "selection": "query", "ids_hash": "sha256:51aa..."}}
  ],
  "usage": {"bytes": 39952224, "budget_bytes": 1073741824}
}
```

Rules: materialization is by explicit id list (the ids the query returned),
fetched with `POST /db/:db/_bulk_get` (with `include_embedding` when asked),
written into one local database per source context so ids never collide. A
slice is frozen: it has no refresh in M2. Exceeding `max_bytes` or the
working-set budget fails before writing (the size is known from the fetch)
and leaves nothing half-written: the slice database is created under a
temporary name and renamed into the working set only when complete.

### 3.9 Offline query with partial coverage

Working set: `ctx_7h1p` imported at generation 17 (`snapshot`),
`ctx_2d9x` as a 40-document `retrieved_set`, `ctx_4q2m` remote only. The
network is down.

```json
{
  "execution": "partial",
  "merge": "grouped",
  "groups": [
    {"context": "ctx_7h1p...", "rows": ["..."]},
    {"context": "ctx_2d9x...", "rows": ["..."]}
  ],
  "sources": [
    {"context": "ctx_7h1p...", "status": "ok", "bound": "exhausted",
     "retrieval": "exact", "version": {"kind": "generation", "generation": 17},
     "membership": "complete_generation"},
    {"context": "ctx_2d9x...", "status": "ok", "bound": "exhausted",
     "retrieval": "exact",
     "version": {"kind": "retrieved_set",
                 "observed": {"instance_id": "0f1e...", "last_seq": "AZ3k..."}},
     "membership": "retrieved_set",
     "note": "answers cover only the 40 saved documents, not the source context"},
    {"context": "ctx_4q2m...", "status": "skipped_offline",
     "error": {"reason": "no_local_copy"}}
  ],
  "coverage": {"requested": 3, "answered": 2, "failed": 0, "skipped": 1,
               "missing": ["ctx_4q2m..."], "scope_origin": "explicit"}
}
```

The executor never falls back to downloading `ctx_4q2m` because it is
unreachable.

## 4. Partial local data

Three different objects, three different promises.

| Object | Membership | Answers offline | Needs more when |
|---|---|---|---|
| Retrieved set (slice) | An explicit id list captured at one observation | Any supported query, but results cover only the saved documents. Reported as `membership: retrieved_set`. Never claims source completeness. | The question is about the context, not the saved documents: the executor queries the remote location if reachable, else reports `skipped_offline` for the context-level question. In M2 the caller chooses by naming the slice context or the source context. |
| Complete snapshot of a generation (or, later, of a declared predicate or partition) | Everything in one published generation, or everything matching a declared predicate at that generation | Any supported query, exactly, for that generation. Reported as `membership: complete_generation` (or `complete_predicate` with the predicate). | The query needs newer data than the generation, or rows outside the predicate: the executor reports the version it used; it does not merge live data in unless the caller also attached the live location. A query whose WHERE is not implied by the snapshot predicate is answered only for the predicate and marked `membership: predicate_overlap`. |
| Cache of index segments | None: a disposable copy of some artifacts of a generation | Only queries whose pruning selects segments that are all present and whose confirmation data is local. Otherwise the answer is incomplete and reported per missing segment. | A selected segment is absent: fetch it within the budget, or report `missing_segments`. Never an empty complete result. |

Start with immutable or explicitly bounded membership: slices are frozen,
snapshots are pinned generations, caches are keyed by generation. Predicate
snapshots come after generation snapshots.

**Continuous subsets are a separate plan (later, not M2).** Filtered
replication cannot maintain one today because departed documents stay on the
target. A maintained subset needs:

1. a membership contract per subset (predicate, channel, or id list) with
   its own checkpoint;
2. **leave events applied on the target**: the channel machinery already
   writes leave rows at the source (`barrel_channel.erl:13-19`); the target
   must delete (or tombstone locally without propagating back) documents
   whose last membership row is a leave;
3. deletion propagation bounded by retention: a subset checkpoint older than
   the source `history_floor` forces a full resync that also removes local
   documents absent from the resync (`barrel_rep.erl:402-420` restarts from
   `first` today but never removes);
4. a reported subset version (`{source instance_id, checkpoint seq}`) so
   queries can state freshness.

## 5. Execution, ranking, ordering

### 5.1 First-release query subset

The executor compiles the statement locally with `barrel_bql:compile/2` to
classify it, then accepts exactly these shapes and rejects the rest with
`{error, {unsupported_federated_query, Reason}}` (HTTP 400,
`"error": "unsupported_federated_query"`).

| Shape | Accepted form | Merge |
|---|---|---|
| Ordered rows | `SELECT ... FROM c [WHERE ...] ORDER BY <field> [ASC|DESC] LIMIT n`, with the ORDER BY field in the projection (or `SELECT *`), `n <= 1000` | `ordered` k-way merge |
| Unordered rows | `SELECT ... FROM c [WHERE ...] LIMIT n` | `grouped` only |
| Retrieval | `SELECT ... FROM vector_top_k|bm25_top_k|hybrid_top_k(...) AS h [WHERE ...] [LIMIT n]` | `grouped` by default; `rerank` or `score` under conditions (5.3) |

Rejected in the first release, with the reason in the error: `SUBSCRIBE`;
`OFFSET`; `UNNEST`; missing `LIMIT` on row queries (a streamable plan has no
bound over HTTP); an ORDER BY key not in the projection (the first release
does not rewrite queries); a `continuation` (none survives the wire for
these shapes); more contexts than the budget; `merge: score` without a
compatible fingerprint.

### 5.2 Limits, ordering, deduplication, pagination

- **Pushdown**: each member runs the statement unchanged, so it returns at
  most `n` rows (or `k`). For ordered rows this is sufficient: the global
  top `n` is contained in the union of each member's top `n` under the same
  comparator.
- **Comparator**: Erlang term order on the ORDER BY value, then context id,
  then document id. Members order ties by scan position, not id
  (`barrel_bql_exec.erl:322-344`), so when several rows tie at a member's
  `n`-th position, which of them the member returned depends on its scan
  order. The merge is deterministic given what members return; the response
  marks the member `bound: limit_reached` so the caller knows ties at the
  cut may exist.
- **Type mixing**: `1` and `1.0` compare as different terms; a context that
  stores the order field with mixed number types orders differently from
  one that does not. The executor does not coerce; this is documented.
- **Deduplication**: off. Rows from different contexts are distinct even
  with equal ids; every row carries `_ctx`. `dedup: by_id` is later work
  and only valid between a context and its replicas.
- **Pagination**: none in the first release. `bound: limit_reached` tells
  the caller to narrow the predicate or raise `n` within the cap. Keyset
  pagination (`WHERE field > last`) is possible when the order field has no
  ties, but BQL cannot express the `(field, id)` tiebreak (id conditions
  are not allowed inside OR, `barrel_bql_lower.erl`), so it waits for a
  planner change.

### 5.3 When scores are comparable

| Merge | Allowed when | Meaning |
|---|---|---|
| `grouped` | always | Rows per context in each context's own order. No cross-context ranking is claimed. |
| `score` | `vector_top_k` only, and every member reports the same embedding fingerprint and a cosine metric | Global order by `_score`. Chosen automatically when the condition holds (measured equal to a single brute-force index); otherwise the executor falls back to `grouped` and says why. |
| `rerank` | any retrieval shape, when `barrel_rerank` is running and a `text_field` is named | Candidates (each member's top `k`, at most `rerank_pool`) are scored by one cross-encoder; the global order is by that score. Documents a member did not return cannot be recovered. |
| `interleave` | any retrieval shape | Round-robin by member rank, labelled as presentation, not relevance. |

Never offered: raw BM25 scores across contexts (IDF and length
normalization depend on the corpus); RRF scores across contexts (with
disjoint corpora every member's first hit gets the same contribution);
vector scores across different fingerprints.

### 5.4 Pruning and ordered traversal

- **Exact pruning** skips a member or an artifact only when sound metadata
  proves it cannot contribute: a partition key range that excludes the
  predicate, a bloom filter that proves a required trigram is absent (no
  false negatives), a generation's declared predicate that contradicts the
  query. Exact pruning does not change `coverage`.
- **Heuristic selection** (catalog search, card topics, a relevance guess)
  changes what is queried. Anything skipped this way is reported as
  `skipped` with the reason, and `scope_origin` becomes `discovered`.
- **Ordered traversal** over key or time partitions (later, M3 and beyond):
  visit partitions in order of their range bounds; stop once `n` rows are
  collected and the next partition's lower bound (upper bound for DESC) is
  strictly worse than the `n`-th row. This is exact only when partition
  bounds are sound for the ORDER BY field and every visited partition
  returned `exhausted` or a full `n`. Semantic relevance has no such bound;
  stopping early on retrieval is always approximate and reported as such.

## 6. Resources, permissions, lifecycle

### 6.1 Budgets

Initial values are assumptions to measure, not capacity claims.

| Budget | Default | Enforced by | On exceed |
|---|---|---|---|
| Contexts per query | 8 | executor, before fanout | reject request |
| Concurrent remote requests per query | 8 | executor worker pool | queue within deadline |
| Concurrent remote requests per node | 32 | executor, shared counter | `skipped_budget` |
| Per-member timeout | 4000 ms | remote client (hackney `recv_timeout` plus a timer) | `timeout` |
| Query deadline | 5000 ms | executor | remaining members `timeout` |
| Rows per member | the statement's LIMIT or k, cap 1000 | shape check, server `max_rows` (B2) | reject |
| Bytes per remote response | 16 MiB | remote client | `error: response_too_large` |
| Working-set local bytes | 1 GiB | materialize, import | reject before writing |
| Transfer per materialize | 256 MiB | materialize | reject before writing |
| Open databases | `barrel_dbs` `dbs_max_open` | existing LRU | `too_many_open_dbs` becomes a member `error` |
| Segment cache bytes (M3) | 2 GiB | segment cache | evict unpinned, then `missing_segments` |

A timed-out remote request is abandoned by the client. The server keeps
folding until it finishes, because the route has no deadline today; B2 adds
a server-side `deadline_ms` checked between chunks.

### 6.2 Permissions

- Query and copy are separate permissions in the contract. The source
  enforces them; the card only advertises them.
- **Today a `read` capability permits copying**, because `_bulk_get` and the
  pull legs of `_sync` classify as read. In M1 and M2, `copy` on a card is
  advisory and materialization requires the same token as queries. A
  source-enforced query-only scope (a new grant field, not the
  already-populated `scopes`) is backlog item B20 and needs a user decision
  (D1, section 10).
- Local members are authorized in `barrel_server` with the existing
  classifier for `POST /db/:db/query` before the executor sees them. Erlang
  callers of `barrel_ctx:query/1` are trusted, as with `barrel:query/2`.
- Remote members use credentials from node configuration: `barrel` app env
  `ctx_credentials`, a map from endpoint (or from a named reference) to a
  bearer or capability token. A working-set member may name a reference
  with `credential_ref`; otherwise the endpoint entry is used.
  Signed-request support reuses `barrel_sync_sig` later.
- Manifests, cards, and working sets never hold credentials. Encrypted
  databases export as ciphertext; the key travels through the keyprovider.

### 6.3 Publication lifecycle (M2 and M3)

One publisher per context generation.

1. **Build**: export the quiesced database (B14) or seal the index (B22)
   into a local staging directory; compute sha256 per artifact.
2. **Upload artifacts**: content-addressed keys under
   `objects/sha256-<hex>`; an artifact already present is skipped. Uploads
   are idempotent, so a restart re-runs this step.
3. **Validate**: HEAD every artifact, compare size, and for the first
   implementation re-download and hash a sample (all artifacts in tests).
4. **Upload parts**, then **publish the root** at
   `generations/<n>.json` with `if_none_match: "*"` where the store supports
   conditional create (AWS, MinIO; Garage does not). Where it does not, the
   deployment must guarantee a single publisher; the publisher still checks
   for an existing root before writing and fails if one appears.
5. **Advance `latest`**: overwrite `latest.json` (a hint). Readers can
   always list `generations/` instead.

Recovery: a crash before step 4 leaves only unreferenced objects, which GC
removes after a grace period. A crash after step 4 leaves a complete
generation; step 5 is re-run.

Retention and GC:

- Keep every generation newer than `retain_generations` (default 3) and any
  generation younger than `retain_days` (default 7) after it was
  superseded. Readers that need longer copy the generation (import).
- GC marks artifacts reachable from retained roots and deletes unreachable
  objects older than `gc_grace` (default 24 h, so an in-progress publish is
  never collected). GC runs on the publisher only.
- Local caches (snapshots, segments) evict by LRU within their budget and
  never evict an artifact pinned by a running query.

## 7. Walkthrough of the first vertical slice (M1)

Setup: node L runs `barrel_server` with a local database `docs_http`
(RFC excerpts). Nodes R1 and R2 run `barrel_server` with `gunicorn_src`
(one document per source file, record mode) and `rfcs`. On L, three cards
are registered:

```erlang
{ok, _} = barrel_ctx_catalog:register(#{
    <<"id">> => <<"ctx_local_docs">>, <<"name">> => <<"docs/http">>,
    <<"locations">> => [#{<<"kind">> => <<"local">>,
                          <<"db">> => <<"docs_http">>}]}),
{ok, _} = barrel_ctx_catalog:register(#{
    <<"id">> => Gunicorn, <<"name">> => <<"projects/gunicorn">>,
    <<"locations">> => [#{<<"kind">> => <<"remote">>,
                          <<"endpoint">> => <<"https://r1:8080">>,
                          <<"db">> => <<"gunicorn_src">>}]}),
{ok, _} = barrel_ctx_catalog:register(#{
    <<"id">> => Rfcs, <<"name">> => <<"protocols/http">>,
    <<"locations">> => [#{<<"kind">> => <<"remote">>,
                          <<"endpoint">> => <<"https://r2:8080">>,
                          <<"db">> => <<"rfcs">>}]}).
```

1. The agent calls `context_query` over MCP with the hybrid statement of 3.7
   and the three ids.
2. `barrel_server` authorizes `ctx_local_docs` against the caller's token
   for `POST /db/docs_http/query`. The remote members are authorized by
   R1 and R2 with the tokens L holds for those endpoints in
   `ctx_credentials`.
3. The executor compiles the statement, classifies it as retrieval with
   `k = 10`, merge `grouped`, and starts three workers (limit 4).
4. The local worker ensures `docs_http` through `barrel_dbs`, pins it for the
   query, runs `barrel:'query'/3`, unpins, and records `instance_id` and
   `last_seq` from the meta (B1).
5. The remote workers post the statement to R1 and R2 with `max_rows 10` and
   `deadline_ms 4000` (B2), stream NDJSON, and keep rows only if a final
   `meta` line arrives.
6. The executor builds the response of 3.7: three groups, `sources` with
   status, bound, retrieval `approximate`, and observed versions, and
   `coverage` 3 of 3.

**Failure.** R2 is stopped. The R2 worker gets a connection refusal:
`status: unreachable`, `execution: partial`, `coverage.missing` lists the
RFC context, the other two groups are unchanged. If R2 instead stalls, the
worker times out at 4000 ms, the response returns before the 5000 ms
deadline, and R2's server finishes its fold in the background (bounded by
its own `deadline_ms` once B2 ships).

**Unsupported shape.** `SELECT * FROM c ORDER BY path` without LIMIT is
rejected with `unsupported_federated_query`, reason `limit_required`,
before any member is contacted.

**Offline.** Not part of M1. With no materialization, an unreachable remote
member is always `unreachable` or `timeout`; nothing is downloaded. M2 adds
working sets, slices, and imported generations (3.9).

**Acceptance gate for M1.** On the three-node setup, scripted:

- An ordered row query over three members equals the union oracle.
- A retrieval query returns grouped results with per-source provenance and
  observed versions.
- One member unreachable, one stalled, and one erroring mid-stream are
  each reported correctly, with no rows kept from failed members, and the
  response time stays within the deadline plus 10%.
- Every rejected shape in 5.1 returns its reason.
- No database is left pinned after the requests (`barrel_dbs` entries back
  to unpinned).
- No data is copied to L (disk usage of L's data directory unchanged apart
  from the catalog).

## 8. Backlog

Items are in dependency order within each milestone. "Spike" items produce a
measurement or a decision, not shippable code. Every item's validation runs
in the umbrella (`rebar3 compile`, `xref`, `dialyzer`, the named suites)
plus the server profile (`rebar3 as server ct`) when `barrel_server` is
touched.

### Milestone M1: remote composition (first useful release)

**B1. Observed version on query results.**
Behaviour: table-function results carry `last_seq`; collection and
table-function meta carry `instance_id`; `barrel:'query'/3` meta exposes
both.
Owner: `barrel` (`barrel_bql_query`), `barrel_docdb` (`barrel_bql_exec`,
`db_instance_id/1`).
Depends on: none.
Acceptance: every successful `barrel:'query'/3` returns
`#{instance_id, last_seq}` in meta; values are equal to
`barrel_docdb:db_instance_id/1` and `get_last_seq/2` read after the query
when no write happened in between.
Validation: new cases in the BQL suite for collection and each table
function; a concurrent-write case showing `last_seq` may be later than the
rows (documented, asserted as `>=`).

**B2. HTTP query route: meta, row cap, deadline, final status.**
Behaviour: the final NDJSON `meta` line carries `instance_id` and
`last_seq`; the request accepts `max_rows` (cap 1000) and `deadline_ms`;
the fold stops at either and reports `"bound": "limit_reached"` or
`"error": "deadline"` as the last line; a stream that ends without a `meta`
line is by definition failed.
Owner: `barrel_server` (`barrel_server_http:query_input/1`, `run_query/4`,
`query_meta/1`).
Depends on: B1.
Acceptance: a streamable query with `max_rows => 5` returns 5 rows and
`has_more: true`; a query exceeding `deadline_ms` ends with an error line
and no `meta`; existing clients that ignore the new fields keep working.
Validation: `rebar3 as server ct` HTTP suite cases; manual `curl` against a
seeded server.

**B3. barrel-lite surfaces in-band errors.**
Behaviour: `parseNdjson` throws on an `{"error": ...}` line and on a missing
final `meta` line.
Owner: `clients/barrel-lite` (`src/wire/transport.ts`).
Depends on: none (B2 makes the missing-meta rule meaningful).
Acceptance: an integration test against a server that errors mid-stream
rejects the promise with the server's error.
Validation: barrel-lite test suite and the existing real-server
integration job.

**B4. Context identity and local card store.**
Behaviour: `barrel_ctx_catalog` stores cards (3.2) as documents in a local
`_barrel_catalog` database; `register/1`, `get/1`, `list/1`,
`update/2`, `resolve_name/1`; card validation rejects credentials and
unknown location kinds. No enumeration sweep, no generated descriptions.
Owner: `barrel` (new `barrel_ctx_catalog`).
Depends on: none.
Acceptance: cards round-trip; ids are `ctx_` plus 24 base32 characters;
a card with a `token` field anywhere is rejected; a name resolves to exactly
one id or `{error, ambiguous}`.
Validation: new `barrel_ctx_catalog_SUITE`.

**B5. Remote query client.**
Behaviour: `barrel_ctx_remote:query(Location, Bql, Opts)` posts to
`/db/:db/query` with hackney, streams NDJSON, enforces the per-member
timeout and byte cap, treats an error line or a missing `meta` line as
failure, and returns rows plus meta. Credentials come from
`ctx_credentials` (6.2).
Owner: `barrel` (new `barrel_ctx_remote`; add `hackney` to `barrel`'s
`applications`, since it is called directly).
Depends on: B2 for version and deadline fields (works without them, with
`version: unknown`).
Acceptance: against a live server returns the same rows as a local
`barrel:'query'/3` on the same data; a closed port gives `unreachable`; a
server stalled past the timeout gives `timeout`; an error mid-stream gives
`error` with zero rows kept.
Validation: `rebar3 as server ct` suite that starts `barrel_server` on a
loopback port and queries it through the client; a stall handler for the
timeout case.

**B6. Federated executor.**
Behaviour: `barrel_ctx:query/1` implements 3.7 and 5.1-5.3 for `grouped`
and `ordered` merges: shape classification, budget checks, bounded
parallel fanout (local members through `barrel_dbs:ensure/2` and
`barrel:'query'/3`, remote through B5), per-member and global deadlines,
provenance (`_ctx` on every row), `sources` and `coverage` blocks.
Owner: `barrel` (new `barrel_ctx`, `barrel_ctx_query`).
Depends on: B4, B5.
Acceptance: a federated ordered query over three members returns the same
top `n` as running the statement over the union of their documents loaded
into one database (the oracle); every rejected shape returns
`unsupported_federated_query` with a reason; a member failing never fails
the request; a request never returns rows from a member reported as failed.
Validation: `barrel_ctx_query_SUITE` with the union oracle, generated
queries over seeded data (property-style, fixed seed), and fault injection
(closed port, stall, error mid-stream).

**B7. REST and MCP adapters, local authorization.**
Behaviour: `POST /contexts/_query`, `GET /contexts`, `GET /contexts/:id`,
`POST /contexts` (register, global auth only); MCP tools `context_query`,
`context_list`, `context_inspect`. Local members are authorized with the
existing route classifier as if the caller had called `POST /db/:db/query`;
an unauthorized member is `status: unauthorized`, not a request failure.
Owner: `barrel_server` (`barrel_server_api`, new
`barrel_server_contexts`, `barrel_server_mcp_tools`).
Depends on: B6.
Acceptance: a capability token for one space queries a composition of its
space and a public remote context; the other local member is reported
`unauthorized`; MCP and REST return the same maps.
Validation: `rebar3 as server ct` suites; an MCP tool call from the existing
MCP test harness.

**B8. Demo and acceptance gate for M1** (the section 7 walkthrough, scripted).
Owner: `docs/guides/contexts.md` (new guide) plus a CT suite that runs the
demo.
Depends on: B7.

**Spike S1. Cost of remote fanout.** Measure per-member latency, rows and
bytes per query, and server CPU for 1, 3, and 8 members with injected RTT
(0, 20, 100 ms) on loopback. Output: the default budgets of 6.1 confirmed or
revised.

### Milestone M1.5: comparable ranking

**B9. Embedding fingerprint.** `barrel:embedder_info/1` and `barrel:info/1`
report provider, model id, model revision when known, dimensions, distance
metric, and the record-mode preprocessing (fields, join), plus the
fingerprint (3.4). Owner: `barrel_embed` (providers report model),
`barrel`. Acceptance: two databases with the same policy report equal
fingerprints; changing the model changes it. Validation: unit tests per
provider with a stub config.

**B10. `score` merge for vector retrieval.** Depends on B9, B6. Acceptance:
rejected unless every member reports the same fingerprint and cosine;
otherwise equal to a single-database `vector_top_k` over the union when
ANN is exact at small sizes (brute-force oracle).

**B11. Fingerprint on cards and in the HTTP meta.** Depends on B9, B2.

**B12. `rerank` merge.** Optional runtime dispatch to `barrel_rerank` (a
`code:ensure_loaded` check, as docdb does for the S3 backend), `rerank_pool`
cap, `text_field` required. Depends on B6. Acceptance: when the sidecar is
absent, `rerank` is rejected, not silently downgraded. Validation: a suite
tagged to run only when the Python sidecar is available.

**Spike S2. Ranking quality across contexts.** On a labelled query set,
compare `grouped`, `interleave`, `score` (same fingerprint), and `rerank`
by recall@k and nDCG@k against a single-index baseline built over the union.
Output: the default merge for retrieval.

### Milestone M2: portability and local materialization

**B13. Working sets.** `barrel_ctx:attach/3`, `detach/2`, `get_ws/1`,
stored in `_barrel_worksets`; budgets (3.5, 6.1); executor accepts
`working_set` in place of `contexts`. Owner: `barrel` (new
`barrel_ctx_ws`). Depends on B6. Acceptance: attach opens nothing
(`barrel_dbs:list/0` unchanged); a query over a working set equals the same
query with its members listed.

**B14. Quiesced export of a composed database.** Close the database through
`barrel_dbs`, then copy the docdb directory, the attachment store (blob
backend only in M2), and the vector store `db_path` (resolved through
`barrel_vectordb_server:get_db_path/1` before close) into a staging
directory with a manifest (3.3) and sha256 per file. Owner: `barrel` (new
`barrel_ctx_export`). Depends on none; needs exclusive access (refuses if
the database is open in another owner). Acceptance: an export, imported
under another name, answers the same queries (docs, vector, BM25) with the
same results; the persisted ANN graph loads (`index_origin = loaded`).
Validation: export/import suite over record-mode and plain databases,
encrypted and not.

**Spike S3. Import identity.** Decide how an imported generation gets a
fresh `source_id` and a name different from the original while keys embed
the original name. Candidates: reuse the branch keyspace mechanism
(`TIMELINE` sidecar with `keyspace => original`, new source id), or a
rewrite during import. Output: the chosen mechanism and its restrictions
(a branch cannot be branched today).

**B15. Read-only open and import.** A `read_only => true` open option in
docdb and vectordb that refuses writes; `barrel_ctx_export:import/2` puts
an imported generation under `wsnap_<ctx>_<gen>`, verifies checksums, opens
it read only, and records `version: generation`. Owner: `barrel_docdb`,
`barrel_vectordb`, `barrel`. Depends on B14, S3. Acceptance: any write to
an imported generation fails; a server can serve it and report
`generation` honestly; an interrupted import resumes from the verified
files and never opens a partially copied directory.

**B16. Retrieved-set slices.** `barrel_ctx:materialize/2` (3.8) with
`_bulk_get` for remote members and `get_docs` for local ones, one database
per source context, temporary-name then rename, budget checks before
writing. Owner: `barrel` (`barrel_ctx_ws`, `barrel_ctx_remote`). Depends on
B13. Acceptance: a slice answers the same queries as the source restricted
to its ids; exceeding the budget writes nothing; provenance survives a
node restart.

**B17. Offline coverage.** The executor marks unreachable remote members
`skipped_offline` without trying when the node is configured offline, and
reports `membership` for slices and snapshots (3.9). Depends on B13-B16.

**B18. Publication to a directory, then to object storage.** A publisher
that follows 6.3 against a local directory "bucket" first, then S3 through
`livery_s3` in an optional app (not in the default build). Owner: new
optional app `barrel_publish` (depends on `barrel`; `livery_s3` only
there). Depends on B14. Acceptance: kill tests at each step of 6.3 leave
either the previous generation or the new one readable, never a mix; GC
never deletes an object referenced by a retained root.

**B19. REST and MCP for M2.** `context_attach`, `context_detach`,
`context_materialize`, working-set routes. Depends on B13, B16.

**Spike S4. Snapshot size and preparation cost.** Export and import time,
bytes, and cold-open time (with `index_origin`) for small, medium, and
large test corpora. Output: whether whole-database snapshots are an
acceptable package format for M2 users.

### Milestone M3: ngram index packages (experiment)

**B22. Segment integrity.** Add a sha256 of the segment body to the
manifest entry at freeze and merge; fsync segment, manifest, and
`corpus.meta` before rename (and the directory after); make posting and
key read errors propagate instead of reading as empty. Owner:
`barrel_ngram` (`barrel_ngram_segment`, `barrel_ngram_manifest`,
`barrel_ngram_query`, `barrel_ngram_corpus_config`). Depends on none.
Acceptance: a corrupted segment fails open with an explicit error; an
injected pread error fails the query. Validation: `barrel_ngram` suites plus
fault-injection cases.

**B23. Segment leases.** Queries hold a lease on the segment files of their
snapshot; compaction deletes an input only when no lease remains. Owner:
`barrel_ngram_shard`, `barrel_ngram_query`. Acceptance: a query running
during compaction never fails with `enoent`. This fixes a local race and is
a prerequisite for cache eviction.

**B24. Seal and export a corpus as a package.** Compact each shard to a
bounded number of segments, write the index-dataset descriptor (3.4) with
coverage computed against the source docdb at a quiesced `last_seq`, and a
per-segment gram bloom filter in the part. Owner: `barrel_ngram` (new
`barrel_ngram_package`). Depends on B22.

**B25. Fetch seam and published-corpus mode.** A `barrel_ngram_fetch`
behaviour (`fetch(Key, DestPath, Opts)`) with a directory implementation
in `barrel_ngram` and an S3 implementation in the optional publish app;
`barrel_ngram:open_published/2` opens a corpus from a manifest, prunes
segments with the bloom filters (exact pruning, 5.4), downloads the
selected whole segments into a bounded LRU cache, verifies sha256, and
reuses the existing local segment reader unchanged. Confirmation fetches
candidate documents from a local snapshot of the same generation or
through `_bulk_get` from a remote location of that generation. Owner:
`barrel_ngram`, `barrel_publish`. Depends on B23, B24, B18.
Acceptance: literal and regex results equal a full-scan baseline over the
same generation (exactness); a missing or corrupt segment yields
`missing_segments`, never an empty complete result; the cache never exceeds
its budget and never evicts a segment in use.

**B26. Package query surface.** Expose published-corpus search as a member
kind in the executor (retrieval `exact`, `version: generation`) and in the
MCP `ngram_search` tool. Depends on B25, B6.

**Spike S5. The M3 benchmark** (design in section 9). Output: the go
or no-go for range reads and for vector packages.

Vector packages, range reads, and finer caching are not scheduled. Each
needs a measured bottleneck from S5 first. A vector package design starts
from the fact that a persisted HNSW graph is random-access heavy; putting
it in a bucket unchanged is not a plan.

### Later, gated on decisions

- **B20. Query-without-copy scope** at the source (new grant field;
  `_bulk_get` and `_sync` pull denied; query, find, search allowed).
  Owner: `barrel_spaces`, `barrel_server`. Needs decision D1.
- **B21. Catalog discovery**: `discover` over cards (BQL LIKE and
  topic filters first; hybrid search when the catalog opens in record
  mode), cards replicated between nodes as regular documents.
- Continuous subsets (section 4), catalog federation, working sets in
  handoffs, live federation, pagination with a `(field, id)` keyset.

## 9. Benchmark design

Compare three configurations on the same data and queries.

| Config | What is measured |
|---|---|
| A. Full local import | The whole corpus imported as a snapshot (B14/B15) on the query node. |
| B. Remote execution | The corpus served by `barrel_server` on another host (or a netem-shaped loopback: 0, 20, 100 ms RTT), queried through the executor. |
| C. Cached index packages | The corpus published as ngram packages (B24) in a directory bucket with injected latency, then MinIO; documents from a local snapshot (C1) or remote `_bulk_get` (C2); cache budgets at 5%, 20%, 50% of the index size. |

**Corpus.** Source code and documentation of a few hundred repositories, one
document per file, partitioned into contexts by repository (hundreds of
contexts) and by release (a few large contexts). Record the corpus
generation, document count, text bytes, and index bytes; publish the
generator script so runs are repeatable.

**Queries.**

- Narrow: rare literals and regexes scoped to one or a few contexts;
  filtered ordered row queries on one context.
- Corpus-wide: common literals, broad regexes, and short literals (< 3
  bytes, brute force in ngram) across all contexts; hybrid retrieval across
  all contexts.
- Each query in a fixed list with its expected result from a full-scan
  oracle on config A.

**Metrics.**

| Metric | How |
|---|---|
| Exactness | Literal, regex, and row queries must match the oracle exactly in every config. Any difference is a failure, not a metric. |
| Retrieval quality | recall@10, nDCG@10 on a labelled subset, per merge mode, against a single index over the union. |
| Latency | cold (empty caches, closed databases) and warm (second run), p50 and p95 over 5 runs. |
| Transferred bytes | per query and cumulative over the query list, per config. |
| Local footprint | disk bytes after the run; peak RSS of the node. |
| Preparation cost | time and bytes to make the corpus queryable (import, publish, first cache fill). |
| Update visibility | time from a write at the source to the answer reflecting it: live (B), next generation (A, C). |
| Missing-source behaviour | one location down, one segment deleted from the bucket: the response must report it and never return an empty complete result. |

**Provisional thresholds.** These are starting points, to be revised after
the first run.

- Move from M1 to M2 when an agent task (scripted) completes with remote
  composition and needs repeated queries offline or over a slow link, and
  config B's p95 for narrow queries at 100 ms RTT exceeds config A's by more
  than 5x.
- Move from M2 to M3 when whole-database snapshots of large contexts take
  more than 10 minutes or more than 50% of the node's disk budget to
  prepare, while narrow queries touch less than 10% of the index.
- Keep M3 (and consider range reads) when config C at a 20% cache budget
  transfers less than 25% of the bytes of a full import over the query list
  and its warm p95 is within 2x of config A for narrow queries. If
  corpus-wide queries in C download most segments anyway, record that as
  the cost of partitioning, not a failure.
- Consider vector packages only if S2 and S5 show retrieval dominated by
  vector legs whose indexes cannot be imported within the M2 preparation
  threshold.

## 10. Unresolved decisions

Only decisions that need user input or measurements. Routine choices made
in this document are stated where they apply.

- **D1. Query-only permission.** Is a source-enforced "query but not copy"
  grant needed for the first external users? If yes, B20 moves into M1 and
  touches `barrel_spaces` and `barrel_server_auth`; if no, the card's
  `copy` field stays advisory until then.
- **D2. Benchmark corpus.** Which real corpus (and license) to use for S1,
  S2, S4, S5. The plan assumes public source code plus documentation.
- **D3. Object stores to support for publication.** AWS S3 and MinIO support
  conditional create; Garage does not. Supporting Garage means a
  single-publisher guarantee outside Barrel.
- **D4. Default retrieval merge.** `grouped` is the safe default. `rerank`
  needs the Python sidecar on the querying node. Decide after S2.
- **D5. Where cards live across nodes.** One catalog per node with explicit
  registration (M1), or a replicated shared catalog database (B21). The
  second needs a trust rule for who may write which card.
- **D6. Whole-database snapshots as the M2 package format.** Decide after
  S4 whether exports are acceptable or M2 must start with document-only
  packages plus local index rebuild.

## 11. Corrections to contexts.md

Recommended replacements, not applied in this task.

| contexts.md | Replace with |
|---|---|
| "What exists today": "`FROM` is an alias that the executor discards" | The `FROM` name stays in the compiled plan and is ignored at run time (`barrel_bql_query.erl:31`). |
| "the vector store rebuilds its HNSW index ... on every open ... the fast serialization path ... is not wired in" (and the same claim under "Scale, honestly" and in P5) | Since barrel_vectordb 2.4.0 a graph that matches the vectors column family loads directly; anything doubtful rebuilds; `stats/1` reports `index_origin`. Remove HNSW deserialization from P5. |
| "`remote` is deferred: barrel has no BQL-over-HTTP execution wire" and open question 1 | `POST /db/:db/query` streams BQL results as NDJSON and barrel-lite's `queryRemote` uses it. Remote mode is the first milestone; the wire needs a row cap, a deadline, an observed version, and client handling of in-band errors (B1-B3, B5). |
| "Hybrid search fuses ... with rank-based RRF ... Rank-based fusion is the only fusion that survives crossing databases" and "The default is therefore rank-based RRF across contexts" | RRF across disjoint corpora gives every context's first hit the same contribution; it is not a relevance ranking. Default to grouped results; offer score merge only for equal embedding fingerprints and rerank through `barrel_rerank` (5.3). |
| "`barrel_dbs` ... supports pinning. Pinning is exactly an attachment lease." | Pinning is a boolean per entry, lost on re-insertion; an attachment lease needs a refcount outside `barrel_dbs`. Attach should be logical and pin only during queries (3.6). |
| "the `auth_context/1` return already has a `scopes` field waiting for this" | `scopes` is already populated with the rights names; new scopes need a new field or a changed meaning. |
| Card id `ctx:<node_id>:<db_name>` | A random `ctx_` id independent of node and database; locations are a list on the card (3.1). |
| "one card per database"; "A context is one barrel database plus its catalog card" | A context is a dataset identity; it may be served by several locations and published as generations of many artifacts. |
| "the close-hook seam is shared with the placement layer" | `barrel_dbs` has no hooks today; the seam must be built if the refresher is kept. |
| `stats.doc_count` described as a RocksDB estimate of documents | `document_count` estimates all keys, including index and feed keys; label it as a key estimate or compute a document count. |
| "every result carries per-context read HLCs" and the consistency section | Results carry an observation `{instance_id, last_seq}` that may be later than the rows' snapshot; retrieval results carry none today. Only imported read-only generations are pinned versions. |
| `complete := boolean()` | `execution`, per-source `status` and `bound`, `retrieval`, `version`, and `coverage` (3.7). |
| Branch listed as a context kind and used as a splitting tool | A branch is a derived dataset with a new context id; it is not a partition. Splitting needs a selection, an ownership rule, and a new manifest. |
| Placement modes `replica` and `cached` via filtered replication | Filtered replication keeps departed documents; a maintained subset needs target-side leave handling and deletion propagation (section 4). |
| Export format with `epoch` and conditional-put manifests "shared with the placement layer" | No epoch or generation concept exists in docdb or vectordb. Exports use the generation manifests of 3.3 and the publication protocol of 6.3; the vector store has no `checkpoint_to` and BM25 disk and DiskANN files live outside RocksDB, so M2 exports a quiesced database. |
| Roadmap P0-P6 | Revised order below. |

Revised phase order:

| Phase | Delivers | Backlog |
|---|---|---|
| M1 | Remote composition over explicit contexts: card store, remote client, executor, REST/MCP, wire fixes | B1-B8, S1 |
| M1.5 | Comparable ranking: embedding fingerprint, score and rerank merges | B9-B12, S2 |
| M2 | Working sets, slices, quiesced export, read-only import, publication, offline coverage | B13-B19, S3, S4 |
| M3 | ngram packages: integrity, leases, sealing, fetch seam, published-corpus mode | B22-B26, S5 |
| Later | Query-only scope, catalog discovery and federation, continuous subsets, live federation, handoff of working sets, vector packages | B20, B21, section 4 |

Also correct `overview.md`, which omits `barrel_att_s3` and `barrel_ngram`,
places the timeline in `barrel` (it is in `barrel_docdb`), and calls S3
attachments planned; and `synchronization.md`, which says TLS serving is not
covered (`:286-287`) while documenting TLS listeners (`:138-164`), and that
barrel-lite does not use SSE (`:280`) while `barrel-lite.md:70-77` documents
it.

## 12. Status after the experiments

The directions were built and measured on local branches (2026-09-24);
results and numbers are in [contexts-experiments.md](contexts-experiments.md).
This section is the feature spec delta and the build path.

### Resolved decisions

- **D4**: `grouped` by default for retrieval; automatic `score` for
  `vector_top_k` when all members report the same fingerprint and cosine;
  `interleave` only with `relevance: false`; raw BM25, hybrid, and RRF
  scores never merged across contexts; `rerank` refused until a code-aware
  reranker is validated (the MiniLM cross-encoder lowered R@10 from 0.80 to
  0.73).
- **D6**: whole-database snapshots are the M2 package format (full corpus
  export or import in about 60-70 ms, ANN graphs load without rebuild).
- **Contract changes that ran**: unknown context id rejects the request
  (404 `unknown_context`); failed members report
  `rows_received_before_failure`; sources add `elapsed_ms`, `bytes`, and
  `membership`; `version.kind: unknown` for servers without B1/B2; a
  `merge_fallback` block when `score` is not possible; `working_set` is
  accepted in place of `contexts`; context queries open existing databases
  only (`must_exist`), so a mistyped card reports `db_not_found`.
- **Leases**: `barrel_dbs` gains counted, monitored leases (`lease/2`,
  `release/1`) separate from pins, and export holds (`hold/2`, `unhold/1`).

### Still open

D1 (query-only permission), D2 (a second corpus with multi-relevance
queries), D3 (object stores for publication), D5 (shared catalog). New:
hybrid retrieval across contexts needs shared BM25 statistics; card quality
for discovery.

### Build path

Fixes for `main` first, independent of contexts:

1. Disk BM25 persistence in `barrel_vectordb_bm25_disk`: merged (#47,
   barrel_vectordb 2.4.1).
2. ngram correctness (PR #49): query leases, error
   propagation, segment checksums (manifest v3), fsync. Needs a
   `barrel_ngram` minor release (corpora rebuild once).
3. Record-mode policy secrets: keep `api_key`-like values out of the
   persisted policy.
4. MCP resource template ordering in `barrel_mcp`.

Contexts feature, in this order (one PR each, stacked):

| PR | Backlog | Content |
|---|---|---|
| 1 | B15 lower layers, S3 | docdb and vectordb read-only opens, import keyspace, `db_exists`, test config without venv |
| 2 | B1, B2 | observed version, `max_rows`, `deadline_ms` on the query route |
| 3 | B9, B11 | embedding model id and fingerprint, fingerprint in query meta |
| 4 | B15, B6 | barrel read-only and stored-policy opens, `barrel_dbs` leases, holds, `must_exist` |
| 5 | B14, B15 | export and import, secret guard |
| 6 | B4, B5 | card store, unified remote client |
| 7 | B13, B16, B17 | working sets, slices, offline coverage |
| 8 | B6, B10 | executor with merges and working sets |
| 9 | B7, B19 | REST and MCP |
| 10 | B8, S1, S2 | demo, guide, benches |

PRs 2, 3, 6, 8, 9 (with the M1 parts of 4) are the first shippable feature:
remote composition over REST and MCP. PRs 1, 5, 7 add portability and
offline use, after the disk BM25 fix.

Before each PR is ready: split the track commits by file where they cross
layers, CHANGELOG entries and version bumps, API docs, connection reuse and
TLS options in the remote client (PR 6), a k-way merge for `ordered` (PR 8),
and a CI job for the demo (PR 10).

M3 continues only after a compact segment format (v5) for `barrel_ngram`;
the experimental package, publisher, and cache code is kept outside the
repository for that re-test.
