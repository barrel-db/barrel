# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
