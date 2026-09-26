# Contexts: design

A context is a named, owned, bounded dataset (one Barrel database today)
that an agent discovers, attaches, and queries together with other
contexts, on this node or on other `barrel_server` nodes. This page
describes the design as it ships in barrel 1.10.0 and barrel_server
1.10.0: identities, the card, the snapshot manifest, working sets, the
operations, the query contract, execution and merge rules, budgets,
permissions, and how a database is published and imported. Read it before
you change anything under `barrel_ctx*` or the contexts routes; to use
contexts, read the [contexts guide](../guides/contexts.md).

## Model

Contexts are federation over composition, not transparent shards. The
agent chooses contexts; the executor chooses databases and locations, runs
one BQL statement on each, and says per source what answered, from which
state, and what is missing. Nothing is merged or ranked unless the merge is
sound for the statement.

| Piece | Module | Stored in or talks to |
|---|---|---|
| Catalog of context cards | `barrel_ctx_catalog` | the `_barrel_catalog` database |
| Federated executor | `barrel_ctx_query`, `barrel_ctx_shape`, `barrel_ctx_merge` | local databases and remote members |
| Remote client | `barrel_ctx_remote` | another node's `POST /db/:db/query` and `_bulk_get` |
| Working sets, slices, coverage | `barrel_ctx_ws`, `barrel_ctx_slice`, `barrel_ctx_coverage` | the `_barrel_worksets` database |
| Export and import | `barrel_ctx_export`, `barrel_ctx_manifest` | one directory per generation |
| Errors and summaries | `barrel_ctx_error`, `barrel_ctx_explain` | one error shape, a `summary` on every answer |
| Facade | `barrel_ctx` | the Erlang API |
| REST and MCP | `barrel_server_contexts`, `barrel_server_worksets`, `barrel_server_mcp_contexts` | `/contexts`, `/worksets`, the `context_*` tools |

## Identity

- **Context id**: `ctx_` plus 24 lowercase base32 characters (15 random
  bytes), minted once and never reused. It encodes no node, database or
  display name.
- **Name**: a mutable, human path such as `otp/sasl`. Every operation
  accepts a name where it accepts an id; an unknown name answers the
  closest names, a name shared by several cards answers
  `ambiguous_context`.
- **Location**: where the context can be queried: `local` (a database on
  this node), `remote` (`endpoint` and `db` on another server), or
  `snapshot` (a publication). A location is not an identity; a card may
  list several.
- **Versions**, never mixed:
  - `live`: a mutable database. The strongest statement is an observation,
    `{instance_id, last_seq}`, read after the query. It is a freshness
    hint, not a consistent snapshot.
  - `generation`: an integer, monotonic per context, naming one exported
    manifest. Only a generation is a pinned version.
- **Derived datasets** (retrieved-set slices) record where they come from:
  the source context, its observed version, the selection, and a hash of
  the ids.

## Card

A card is small, descriptive and advisory. It never holds credentials and
never grants anything; the source decides.

```json
{"id": "ctx_5loieq7q3hdaxluhwkklz5px", "name": "otp/sasl", "type": "context_card",
 "format": 1, "discoverable": "listed", "title": "OTP sasl",
 "topics": ["erlang", "release"],
 "locations": [{"kind": "remote", "endpoint": "http://127.0.0.1:18082", "db": "otp_sasl"}],
 "embedding": {"fingerprint": "sha256:...", "distance": "cosine", "dimensions": 768}}
```

- Validation: a name of 1 to 256 bytes, at least one location, `http` or
  `https` endpoints without userinfo, database names of `[A-Za-z0-9_-]`,
  at most 64 KiB. A key that names a secret (`token`, `password`,
  `api_key`, `authorization`, ...) at any depth, or a `bsp_` capability
  value, fails with `invalid_card`.
- `discoverable`: `listed` or `unlisted`. Unlisted cards resolve by id or
  name but stay out of lists and discovery.
- `embedding` is optional: what the source reports for vector results. A
  remote member's own report in its query meta wins over the card.
- Discovery is a filter, not a ranking: a card matches when every word of
  the query appears in its name, title, description or topics.

## Snapshot manifest

An export writes one directory per generation: the database files under
`docdb/` and `vectordb/`, content-addressed parts, and a root
`manifest.json`.

```json
{"type": "snapshot_manifest", "format": 1,
 "context": "ctx_v7i2tvh35bjlkqmjpm42dgex", "generation": 1, "parent_generation": null,
 "published_at": "2026-09-26T09:36:47Z", "publisher": "node:...",
 "engine": {"barrel": "1.10.0", "barrel_docdb": "1.7.0", "barrel_vectordb": "2.5.0"},
 "sources": [{"db": "otp_sasl", "keyspace": "otp_sasl", "instance_id": "1bd86765269c67f5",
              "last_seq": "AAABoN0S5DMAAAAP", "quiesced": true}],
 "layout": {"docdb": "docdb", "vectordb": "vectordb", "mode": "plain", "att_backend": "blob",
            "encrypted": false, "dimensions": 8, "vectors": 17, "vector_config_etf": "..."},
 "parts": [{"kind": "data", "ref": "parts/sha256-4b08....json", "sha256": "4b08...",
            "artifacts": 21, "bytes": 433203}]}
```

- A part lists at most 1000 artifacts, each with `path`, `sha256` and
  `bytes`, and is named by its own sha256. Large databases add parts, not
  a larger root.
- `layout.vector_config_etf` carries the portable vector config (dimension,
  index backend, BM25 backend) so the importer loads the persisted graph
  without a rebuild. It never carries keys, embedders or docstores.
- Encrypted databases export as ciphertext; the key travels through the
  key provider, never in the manifest.

## Working set

A working set is the list of contexts an agent works with, one document
per set in the local `_barrel_worksets` database.

| Member mode | What it is | Answers offline |
|---|---|---|
| `local` | a live database on this node | yes, as `live` |
| `remote` | a location on another server | no |
| `retrieved_set` | a frozen slice: the documents a query returned, copied into `wslice_<ws>_<ctx>` | yes, only for the saved documents |
| `snapshot` | an imported generation, served read only as `wsnap_<ctx>_<generation>` | yes, as that generation |

- Attach and detach are logical: nothing opens, pins or downloads.
- Budget per working set, overridable at creation: `bytes` (1 GiB of local
  copies), `contexts` (8), `transfer_bytes` (256 MiB per materialize),
  `remote_parallel` (4), `deadline_ms` (5000), `open_dbs` (the
  `barrel_dbs` `dbs_max_open`).
- A slice is fetched by explicit ids through `_bulk_get` (with
  `include_embedding` when asked), written under a temporary name, and
  renamed into the set only when complete; the size is checked against the
  budget before anything is written. It keeps a memory BM25 index rebuilt
  at open. A slice of a plain source holds documents, not vectors.
- Detaching a slice deletes it; deleting a working set deletes its slices.
  Imported generations are shared by name and stay.
- A member may name a `credential_ref`, a key of the node's
  `ctx_credentials`; the working set never holds the secret.

## Operations

The Erlang API in `barrel_ctx` is the contract; REST and MCP map to it one
to one. The [guide](../guides/contexts.md#the-same-operations-over-rest-and-erlang)
has the full table with the REST routes and MCP tools.

| Verb | Erlang | What it does |
|---|---|---|
| capabilities | `capabilities/0` | accepted shapes with examples, merges, limits, budgets, offline state |
| discover, list, inspect | `discover/2`, `list/1`, `inspect/1` | read cards |
| register | `register/1`, `update/2`, `unregister/1` | manage cards |
| query | `query/1` | one statement over contexts or a working set |
| attach, detach | `attach/3`, `detach/2` | change a working set's members |
| materialize | `materialize/2` | save what a query returned as slices |
| import | `import/3` | import an exported generation into a working set |
| offline | `offline/0`, `set_offline/1` | switch the node offline |

Errors are `{error, {Code, Details}}` in Erlang and
`{"error": Code, "message": ..., "hint": ..., "details": {...}}` on REST
and MCP, with one HTTP status per code (`barrel_ctx_error`).

## Query contract

A request names the statement and either `contexts` (ids or names) or a
`working_set`, plus optional `merge`, `offline`, `params`, `deadline_ms`,
`per_context_timeout_ms` and `max_parallel`. The answer carries:

- `execution`: `succeeded` (every member answered), `partial`, or `failed`
  (none answered; MCP marks the result as an error).
- `rows` (merged) or `groups` (one per context), each row tagged with
  `_ctx` and `_ctx_name`.
- `sources[]`, one per member: `status` (`ok`, `timeout`, `unreachable`,
  `unauthorized`, `error`, `skipped_budget`, `skipped_offline`), `rows`,
  `bound` (`limit_reached` or `exhausted`), `retrieval` (`exact` for rows
  and BM25, `approximate` for vector and hybrid), `membership`, `version`,
  `location`, `elapsed_ms`, and on failure an `error` with `reason`,
  `message`, `hint` and `rows_received_before_failure`.
- `coverage`: `requested`, `answered`, `failed`, `skipped`, `missing`, and
  `scope_origin` (`explicit`: the caller named the contexts or the working
  set).
- `summary`: one paragraph with the same facts for a reader.

Rows from a member that failed are never returned, even those received
before the failure.

| Membership | Promise | Version reported |
|---|---|---|
| `live` | the database now | `live` with `observed` |
| `complete_generation` | everything in one exported generation, exactly | `generation` |
| `retrieved_set` | only the saved documents, never the source context | `retrieved_set` with the source's `observed` at save time |

A member that cannot answer offline is `skipped_offline` with
`no_local_copy`. The executor never downloads a context because it is
unreachable.

## Execution

1. Classify the statement (`barrel_ctx_shape`):

   | Shape | Form | Merge |
   |---|---|---|
   | Ordered rows | `... ORDER BY <selected field> LIMIT n` | `ordered` |
   | Unordered rows | `... LIMIT n` | `grouped` |
   | Retrieval | `vector_top_k`, `bm25_top_k`, `hybrid_top_k` | `grouped`, or `score` (below) |

   Refused before any member is contacted: no `LIMIT` on a row query,
   `LIMIT` or `k` above 1000, `SUBSCRIBE`, `OFFSET`, `UNNEST`, a
   continuation, an `ORDER BY` field not selected, more than 8 contexts, a
   context listed twice.
2. Decide every member before any work (`barrel_ctx_coverage`): offline
   members that need the network are skipped, members whose local copy is
   missing fail.
3. Run. Each member runs the statement unchanged, so it returns at most
   `n` rows or `k` hits. A local member opens with `must_exist` (a mistyped
   card reports `db_not_found` instead of creating a database) and runs
   under a `barrel_dbs` lease, so idle close and eviction skip it. A remote
   member posts to the source's query route with `max_rows` and
   `deadline_ms`, streams NDJSON, and counts rows only when the final meta
   line arrives. Remote members run under the request deadline, the
   per-member timeout, `max_parallel`, and a node-wide slot pool; a member
   that finds the pool full is `skipped_budget`.
4. Merge and explain.

| Merge | Allowed when | Meaning |
|---|---|---|
| `ordered` | ordered rows | Rows sorted by the `ORDER BY` value, then context id, then row id, cut at `LIMIT`. |
| `grouped` | always | One group per context, each in its own order. No cross-context ranking claimed. Default for unordered rows and retrieval. |
| `score` | `vector_top_k`, and every member reports the same embedding fingerprint and cosine distance | Global order by `_score`. Chosen automatically when the condition holds; otherwise the answer falls back to `grouped` with `merge_fallback` saying why and at which context. Requested explicitly without the condition, it is refused with `scores_not_comparable`. |
| `interleave` | retrieval | Round-robin by member rank, answered with `relevance: false`. |
| `rrf`, `rerank` | never | Refused with `merge_not_supported`. |

Raw BM25, hybrid and RRF scores never merge across contexts: IDF and
length normalization depend on the corpus, and with disjoint corpora every
member's first hit gets the same RRF contribution. There is no
deduplication (rows from different contexts are distinct even with equal
ids) and no pagination: `bound: limit_reached` tells the caller to narrow
the predicate or raise the limit.

## Budgets

| Budget | Default (app env) | Cap | On exceed |
|---|---|---|---|
| Contexts per query | 8 (`ctx_max_contexts`) | | `too_many_contexts` |
| Rows per member | the statement's `LIMIT` or `k` | 1000 | `limit_too_large` |
| Query deadline | 5000 ms (`ctx_deadline_ms`) | 60000 ms | remaining members `timeout` |
| Per-member timeout | 4000 ms (`ctx_member_timeout_ms`) | the deadline | `timeout` |
| Parallel members per query | 8 (`ctx_max_parallel`) | 16 | queued within the deadline |
| Remote requests per node | 32 (`ctx_node_remote_max`) | | `skipped_budget` |
| Bytes per remote response | 16 MiB | | member `error`, `response_too_large` |
| Working-set local bytes | 1 GiB | | `over_budget` before writing |
| Transfer per materialize | 256 MiB | | `over_budget` before writing |

A working set's budget gives defaults a request may override. The source
bounds its own work too: its query route caps `max_rows` at 1000 and
`deadline_ms` at 300000 and stops a query past its deadline.

## Permissions

- The source enforces access; a card only describes it.
- In `barrel_server`, registering cards, working sets, imports and offline
  mode need a global principal. A capability token may read cards and run
  context queries; each local member is authorized as its own
  `POST /db/:db/query` on that database before the executor sees it.
  Erlang callers of `barrel_ctx:query/1` are trusted, as with
  `barrel:query/2`.
- Remote members use a bearer token from the `barrel` app env
  `ctx_credentials`, a map keyed by endpoint or by a `credential_ref`.
- A `read` right on a source also allows copying: materialize uses
  `_bulk_get`, which classifies as a read.

## Publication and import

A generation moves between nodes as a directory.

1. **Export** (`barrel_ctx_export:export/3`): take an exclusive hold on the
   database (`barrel_dbs:hold/2`; the database is closed during the copy),
   copy the docdb and vector store files, then write the parts and the
   root. A source written by an older version is opened once writable
   under the hold, so the copy opens read only. A record-mode policy that
   holds an `api_key`-like value is refused with `policy_holds_secret` and
   nothing is copied.
2. **Transfer** the directory by any means.
3. **Import** (`barrel_ctx_export:import/2`, or `barrel_ctx:import/3` into
   a working set): copy and verify every file against its sha256 into a
   partial directory, resume from files already verified, write an import
   sidecar (source keyspace, a fresh source id), and rename into place
   only when complete.
4. **Serve** read only: opening, querying and closing an import creates or
   rewrites no file (RocksDB `OpenForReadOnly`), so every file keeps its
   checksum and several nodes can serve one directory. A disk BM25 index
   is used as written; a memory one is rebuilt at open.
   `barrel_ctx_export:remove_import/1` deletes a local generation.

## Decisions

Measured on OTP's own source: 1078 modules in 33 applications, one
application per context, 217 known-item queries, `nomic-embed-text` (768
dimensions, cosine). The harnesses are `bench/ctx_remote` (fanout),
`bench/ctx_ranking` (ranking), `bench/rocksdb_readonly` (read-only opens)
and `scripts/bench_ctx_snapshot.escript` (export and import);
`scripts/contexts-demo.sh` runs the whole feature on three nodes.

**Ranking: grouped by default, score merge only with equal fingerprints.**

| Method over the 33 contexts | recall@10 |
|---|---|
| One exact vector index over the union (reference) | 0.802 |
| `score` merge of `vector_top_k` (same fingerprint) | 0.802 |
| `grouped`, the target's own group | 0.908 |
| Raw BM25 score sort | 0.558 |
| Cross-context RRF, or `interleave` in context order | 0.078 |
| Cross-encoder rerank (MiniLM) of a 100-candidate pool | 0.733 |

A score merge over equal fingerprints equals one brute-force index, so it
is chosen automatically. Rank fusion and interleave across disjoint
corpora are not relevance orders, and the general-purpose reranker made
code results worse, so `rerank` stays refused until a code-aware reranker
is validated.

**Whole-database snapshots are the package format.** Exporting or
importing the whole corpus takes about 50 to 70 ms, and ANN graphs load
without a rebuild, so document-only packages with a local index rebuild
were not needed.

**Read-only means no file written.** Serving a copy with the stores opened
read-write rewrote files at every open (714 KB, or 33 MB with a WAL to
replay), broke the manifest checksums, and kept a second node out of the
directory. With `OpenForReadOnly` nothing is written, a second reader
opens the same directory, and opens are 20 to 25% faster.

**Remote fanout is cheap.** Query latency is the network round trip plus a
few milliseconds, flat from 1 to 8 members.

## Not supported yet

- **Index packages**: publishing `barrel_ngram` segments to object storage
  and caching them locally waits for a compact segment format; the current
  one makes the index about 34 times the text.
- **Object-store publication**: generations move as directories; there is
  no upload, retention or GC of published generations.
- **Hybrid across contexts** needs shared BM25 statistics; until then
  hybrid and BM25 answers stay grouped.
- **Working-set ownership**: a working set records an `owner` label that
  nothing enforces; any global principal can read or delete any set.
- **Query-only permission**: a grant that allows querying a source but not
  copying from it.
- **Shared catalog**: cards live in one catalog per node, registered
  explicitly; there is no replicated catalog and no rule for who may write
  which card.
- **Remote client**: each call opens and closes its own connection; no
  connection reuse and no TLS options beyond the defaults.
- Continuous subsets (filtered replication that removes departed
  documents), predicate snapshots, pagination, deduplication across
  replicas, and live federation.
