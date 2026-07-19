# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-07-19

### Added

- Incremental live indexing: a corpus subscribes to its database's changes
  feed (push mode) and keeps in sync in the background. Updates and deletes
  are reflected in results through the confirm pass, which re-fetches the
  current document and drops `not_found`.
- Multi-segment storage with a crash-safe manifest (`barrel_ngram_manifest`),
  written atomically (temp + rename); the manifest commit is the recovery
  point, and orphan segments from a crash before commit are cleared at start.
- Query fans across every live segment plus the unfrozen buffer.
- Recovery replays only the feed tail since the persisted HLC watermark.
- `barrel_ngram:refresh/1`: synchronous catch-up + freeze; `index/1` now
  delegates to it.

## [0.1.0] - 2026-07-18

### Added

- Dense trigram substring index over `barrel_docdb`. A corpus is bound to
  a database and built one-shot from the changes feed; queries turn a
  literal into its overlapping trigrams, intersect posting lists, and
  confirm each candidate against the real document text.
- Immutable segment format: a direct-addressed `u32` offset table over
  the 2^24 gram space, delta+varint posting lists, and an ordinal-to-key
  sidecar, read with `file:pread`.
- `barrel_ngram_selector` behaviour with a dense selector, the shared
  seam between the indexer and the query planner.
- Public API: `open/2`, `index/1`, `search/2,3`, `close/1`.
