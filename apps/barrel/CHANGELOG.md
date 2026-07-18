# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
