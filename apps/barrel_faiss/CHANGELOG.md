# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-03-07

### Changed

- CI: Use Debian-based Erlang containers for GitHub Actions
- CI: Add xref, dialyzer, eunit, and common tests as separate steps
- Configure xref checks for library usage

## [0.2.0] - 2026-01-04

### Added

- Deletion support:
  - `remove_ids/2` - Remove vectors by ID (supports Flat, IVF indexes; not HNSW)
  - `add_with_ids/3` - Add vectors with explicit IDs (supports IVF indexes)

## [0.1.0] - 2026-01-04

### Added

- Initial release of barrel_faiss
- Core index operations:
  - `new/1,2` - Create flat indexes with L2 or inner product metric
  - `index_factory/2,3` - Create indexes using FAISS factory strings
  - `close/1` - Release index resources
- Index properties:
  - `dimension/1` - Get vector dimension
  - `is_trained/1` - Check training status
  - `ntotal/1` - Get vector count
- Vector operations:
  - `add/2` - Add vectors to index
  - `search/3` - K-nearest neighbor search
  - `train/2` - Train IVF indexes
- Serialization:
  - `serialize/1` - Serialize index to binary
  - `deserialize/1` - Restore index from binary
- File I/O:
  - `write_index/2` - Save index to file
  - `read_index/1` - Load index from file
- Supported index types via factory strings:
  - `Flat` - Exact brute-force search
  - `HNSW` - Hierarchical Navigable Small World graphs
  - `IVF` - Inverted file indexes
  - `PQ` - Product quantization
  - `SQ` - Scalar quantization
- Dirty scheduler support:
  - CPU-bound operations on dirty CPU schedulers
  - File I/O on dirty IO schedulers
- Platform support:
  - Linux (amd64, arm64)
  - macOS (Homebrew, MacPorts)
  - FreeBSD
- Documentation:
  - Getting started guide
  - K/V database integration guide
  - API reference via ex_doc

[0.2.1]: https://github.com/barrel-db/barrel_faiss/releases/tag/0.2.1
[0.2.0]: https://github.com/barrel-db/barrel_faiss/releases/tag/0.2.0
[0.1.0]: https://github.com/barrel-db/barrel_faiss/releases/tag/0.1.0
