# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
