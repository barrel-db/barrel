# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Design manifests: named, versioned designs whose views select documents with a BQL predicate and build rows with a native projection or a Wasm map, totalled with `_sum` or `_count`. Manifests are checked before anything is stored; a view's signature ignores captions, so renaming a column keeps its index.
- Wasm maps under the `barrel_v1` ABI: a module is admitted only with the two Barrel imports, a bounded 32-bit memory and no start function, SIMD, atomics, exceptions or GC. Every document runs in a fresh instance with finite fuel, a wall-clock deadline, and limits on memory pages, call depth, host calls, field size and emitted rows; a failed call contributes no rows.
- `barrel_design:preview/3` runs a view over sample documents from inline WAT or a `.wasm` binary without installing it.
