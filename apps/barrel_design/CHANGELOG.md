# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- Requires erlang_wasm 0.13.0 (OTP 29).

### Added
- Design manifests: named, versioned designs whose views select documents with a BQL predicate and build rows with a native projection or a Wasm map, totalled with `_sum` or `_count`. Manifests are checked before anything is stored; a view's signature ignores captions, so renaming a column keeps its index.
- Wasm maps under the `barrel_v1` ABI: a module is admitted only with the two Barrel imports, a bounded 32-bit memory and no start function, SIMD, atomics, exceptions or GC. Every document runs in a fresh instance with finite fuel, a wall-clock deadline, and limits on memory pages, call depth, host calls, field size and emitted rows; a failed call contributes no rows.
- `barrel_design:preview/3` runs a view over sample documents from inline WAT or a `.wasm` binary without installing it.
- A Wasm map can be installed as WAT text, with no toolchain: it is stored as its canonical encoding, so comments, layout and identifiers do not change its digest. `barrel_design:module/1` gives the digest and bytes a design names a module by.
- `install/3` stores a design and its module bytes with the database, as local documents that are backed up with it and never replicated, and starts building its views over the existing documents. Views then follow every write, including bulk, replicated and conflict-resolution writes and deletes, even when nobody reads them, and resume from a durable checkpoint after a restart. Installing a higher version keeps the rows of unchanged views and rebuilds the others in a new generation while the old one keeps serving.
- `query/3` reads a view by key, key range or leading key components, in pages, or totals it with `_sum` or `_count` per category. A read can wait for a given database write first. Expired documents leave rows and totals at once. A map that fails on a document removes that document's rows and is counted in `status/2` until the document is fixed.
- `refresh/2,3` catches a view up with the database's last write as of the call, shared between concurrent callers; a view already current answers at once. `rebuild/2` builds a view again from scratch.
- Views are a BQL source: `FROM view('design', 'view', group_level => 1) AS v`, with conditions on `v.key` narrowing the scan, through `barrel_docdb:query`, `barrel:query` and the HTTP and MCP query routes.
- A view of an encrypted database stores its rows encrypted with the database key. A database deleted and recreated under the same name, restored or branched starts from an empty index.
