# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-07-19

### Added

- Regex search (`barrel_ngram:regex/2,3`, PCRE syntax). A regex is turned
  into a mandatory-trigram boolean query (Russ Cox / Google Code Search)
  that is intersected and unioned over the posting lists, then each
  candidate is confirmed with a bounded `re:run`. The analysis is always
  sound (unsure constructs contribute no constraint), so results are
  exact. Trigram-accelerated on dense corpora; sparse corpora brute-force
  since their index holds only a sample of grams.
- `barrel_ngram_postings:union_all/1` and a `covers_all_grams/1` selector
  callback (the regex planner uses the trigram query only when the index
  is complete).

## [0.4.0] - 2026-07-19

### Added

- Sparse (content-defined) gram selector: a trigram is kept only when a
  hash of its local byte window passes a sampling test, shrinking the
  index. The query planner intersects only over grams whose window falls
  entirely inside the literal, and falls back to a brute-force scan for
  short literals, so results stay exact. Opt in per corpus with
  `selector => barrel_ngram_selector_sparse` and `selector_opts`.

### Changed

- The selector behaviour callbacks now take an options map
  (`select_grams/2`, `reliable_grams/2`), carrying per-selector tuning.

## [0.3.0] - 2026-07-19

### Added

- Compaction: an offloaded worker merges live segments into one,
  collapsing each key to its newest version by HLC and physically
  evicting superseded and deleted entries. Fires automatically when the
  segment count crosses a threshold (`compact_threshold`), and
  `barrel_ngram:compact/1` runs it synchronously.
- Deletes are recorded as segment tombstones so compaction can evict
  them; the segment sidecar (format v2) carries a per-ordinal change HLC
  and deleted flag.

### Changed

- Segment format bumped to v2. The manifest rename remains the sole
  commit point, so a crash mid-merge leaves an orphan segment that is
  cleaned up on the next open.

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
