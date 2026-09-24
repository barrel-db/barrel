# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [1.10.0] - 2026-09-25

### Added
- `barrel_ctx`: the contexts facade. `capabilities/0` (query shapes with examples, merges, limits, budgets, offline state), `discover/2`, `inspect/1`, `list/1`, `register/1`, `update/2`, `unregister/1`, `resolve/1` (names wherever ids are accepted, close matches on a typo), `query/1`, working sets (`create_ws/1`, `get_ws/1`, `list_ws/0`, `delete_ws/1`, `attach/3`, `detach/2`, `materialize/2`, `import/3`) and `offline/0`, `set_offline/1`.
- `barrel_ctx_query`: federated BQL over local, imported, sliced and remote contexts. Each member is decided before any work (offline members that need the network are skipped, missing local copies fail); local members run under a lease, remote ones under the request deadline and a parallelism bound. Every source reports its status, version and membership; answers are `succeeded`, `partial` or `failed`.
- Merges (`barrel_ctx_merge`, `barrel_ctx_shape`): `ordered` rows are merged by the statement's `ORDER BY` and cut at its `LIMIT`; retrieval is `grouped` by context by default; `score` merges `vector_top_k` hits only when every member reports the same embedding fingerprint and cosine distance, otherwise it falls back to `grouped` and says why; `interleave` answers with `relevance: false`; `rrf` and `rerank` are refused.
- `barrel_ctx_error`: one error catalog, `{error, {Code, Details}}` with a message, a hint and an HTTP status per code. `barrel_ctx_explain`: a `summary` on every answer, names on sources, groups, members and rows, a message and hint on every source that did not answer.

## [1.9.0] - 2026-09-25

### Added
- `barrel_ctx_ws`: working sets, the contexts an agent works with, stored one document each in `_barrel_worksets`, with byte and member budgets. Attach and detach are logical (nothing opens or downloads); `members/1` resolves each member's mode, local copy, generation and coverage for an executor; `open_member/1` opens a local copy; `import_snapshot/3` imports an exported generation into the set.
- `barrel_ctx_slice`: a retrieved-set slice, the documents (and their vectors) a query returned from a local or remote context, copied into a local read-only database with its provenance (source, observed version, ids hash). Remote documents come through `_bulk_get` under the remote client's limits; the slice counts against the working set's byte budget. A slice keeps a memory BM25 index rebuilt at open.
- `barrel_ctx_coverage`: what a member can answer offline (`live`, `complete_generation`, `retrieved_set`, `skipped_offline`) and how a query's sources summarize.

## [1.8.0] - 2026-09-25

### Added
- `barrel_ctx_catalog`: context cards stored in a local catalog database (`_barrel_catalog` by default). A card names a context and where it lives (local database, remote endpoint, imported copy), with title, description, topics and an optional `embedding` block. Cards are validated (size, name, locations) and refused when they carry a secret. `register/1`, `get/1`, `list/1`, `discover/2` (every word of the query matches name, title, description or topics), `update/2`, `unregister/1`, `resolve_name/1`.
- `barrel_ctx_remote`: client for another node's `POST /db/:db/query` (NDJSON stream), `_bulk_get` and a one-row observation of its version. Each call runs under a caller deadline, caps response bytes (16 MiB by default) and closes its connection on every path; remote calls share a node-wide slot pool (`ctx_node_remote_max`). Failures come back as one map (`status`, `reason`, `rows_received`, ...). Credentials resolve from an explicit option, a `credential_ref` looked up in `ctx_credentials`, or the endpoint entry.

### Changed
- Depends on `hackney ~> 4.4` (the remote client calls it directly).

## [1.7.0] - 2026-09-25

### Added
- `barrel_ctx_export:export/3`: quiesced export of a composed database to a directory. It holds the database (`barrel_dbs:hold/2`), copies the docdb and vector store files, and writes a manifest with a sha256 per file, the source identity (keyspace, instance id, last seq) and the portable vector config. Encrypted databases export as ciphertext. A record-mode policy that names a secret (`api_key`, `token`, `password`, ...) at any depth is refused with `{policy_holds_secret, Key}` and nothing is copied.
- `barrel_ctx_export:import/2`: copies and verifies every file into a partial directory, resumes from files already verified, writes an import sidecar (source keyspace, a fresh source id minted into the sidecar) and renames into place only when complete. `open/1`, `open_opts/1`, `import_info/1`, `list_imports/0`, `remove_import/1` serve and manage imported generations, always read only: opening, querying and closing an import creates or rewrites no file, so every file keeps the checksum of the manifest and several nodes can serve one directory. The import keeps the source's BM25 backend: a disk index is copied as written, a memory one is rebuilt at open.
- A source that an older version wrote (a column family added since, a disk BM25 index from before the durable format or with an interrupted compaction) fails a read-only open with `read_only_upgrade_needed`. Export then opens it once writable and closes it under the hold before copying, so the exported files open read only and match their checksums; a source opened read only by its owner is not upgraded and the export fails with the error.
- `barrel_ctx_manifest`: manifest read, write, scan and per-file verification.

## [1.6.0] - 2026-09-25

### Added
- `open/2` takes `read_only => true`: both stores open read only, every write answers `{error, read_only}`, record mode persists no policy and starts no indexer. The handle carries `read_only => true`. BM25 needs no refill: the vector store rebuilds a memory index from the documents at open, and a disk index is durable.
- `open/2` takes `embedding => stored`: record mode with the policy the database persisted (`{error, no_stored_policy}` on a plain database), for copies that carry their source's policy.
- `barrel_dbs:lease/2`, `release/1`, `leases/0`: counted, monitored holds that keep a database open against idle close and eviction until released or until the holder exits.
- `barrel_dbs:hold/2`, `unhold/1`: exclusive file access. A hold closes the database and refuses every `ensure` until `unhold`; it is refused on a pinned or leased database, one owned by another tag, or one open outside the manager.
- `barrel_dbs:lookup/1`: pinned flag, owner tag and the options a database was opened with.
- `barrel_dbs:ensure/2` takes `must_exist => true`: a cold open of a database that is not there answers `{error, not_found}` instead of creating it.

### Changed
- Idle close and eviction skip leased databases as well as pinned ones.
- Requires `barrel_vectordb ~> 2.5` (read-only stores).

## [1.5.0] - 2026-09-25

### Added
- `embedder_info/1` returns the embedder's identity: `provider`, `model`, `revision`, `dimensions`, `distance`, `preprocessing` (the policy's fields and join) and a `fingerprint` (`sha256:` of their canonical JSON, see `barrel_embed_fingerprint`). Two databases with the same fingerprint produce comparable vector scores. No fingerprint without a configured embedder.
- `info/1` carries the same identity under `embedder`.

### Changed
- Requires `barrel_embed ~> 2.5` and starts `crypto`.

## [1.4.0] - 2026-09-25

### Added
- `query/2,3` and `query_fold/5` results carry the observed version in their meta (`instance_id`, `last_seq`), for collection queries and table functions (`vector_top_k`, `bm25_top_k`, `hybrid_top_k`) alike.
- `barrel_bql_query:row_bound/1`: the most rows a compiled plan can return (its `LIMIT`, or `k` capped by `LIMIT`).

### Changed
- Requires `barrel_docdb ~> 1.7` (observed version).

## [1.3.1] - 2026-08-23

### Changed
- Requires `barrel_vectordb` 2.3 (the store server without gen_batch_server)
  and OTP 26 or later, so a dependency on `barrel` resolves to a build that is
  warning-free on OTP 29.

## [1.3.0] - 2026-08-23

### Added
- `embed/2`, `embed_batch/2` and `embedder_info/1` on the database handle:
  embed text with the database's own embedder (the policy's on a record-mode
  database, the vector store's on a plain one), so consumers no longer need to
  reach into the handle's `embed` field.

## [1.2.0] - 2026-08-09

### Added
- `put_attachment/5`: store a document attachment with options
  (`create_only`, `expected_etag`, `content_type`, ...).

## [1.1.0] - 2026-07-18

### Added
- `barrel:open` `store_supervised` option: parent the vector store to a
  supervisor instead of linking it to the caller, so a store opened on behalf of
  a long-lived owner outlives the process that opened it.

### Changed
- `barrel_dbs` opens databases in a short-lived worker off its message loop, so a
  cold or wedged open no longer blocks every other ensure/close/list call
  node-wide; concurrent opens of the same database coalesce onto one open, and
  close/destroy/branch/pin defer while their target is mid-open.

### Fixed
- The docdb-crash reopen path stops the surviving vector store instead of leaking
  its RocksDB handles.

## [1.0.1] - 2026-07-11

### Fixed
- Repoint the barrel_vectordb dependency to 2.1.2 (2.1.1 shipped without its barrel_embed requirement) and use `~>` pins so a sibling patch does not force a re-release here.

## [1.0.0] - 2026-07-10

First tagged release of the embeddable database. Composes `barrel_docdb`,
`barrel_vectordb`, and `barrel_crypto` under one id, adding record mode, the
timeline (branch/PITR/merge), and BQL. See the umbrella
[CHANGELOG](../../CHANGELOG.md) for the coordinated release notes.
