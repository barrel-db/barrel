# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0] - 2026-08-15

### Changed

- **Breaking**: `close/1` now returns `ok | {error, term()}` instead of
  an unconditional `ok` -- `{error, busy}` on lock contention, or an
  error if a shard could not be confirmed stopped.
- **Breaking**: a corpus with on-disk segments/manifests but no
  `corpus.meta` (indexed before this release) is rejected on open with
  `{error, {legacy_corpus_requires_reindex, Corpus}}`. There is no
  migration path -- `db`/`shards` were never recoverable from what a
  pre-release corpus persisted -- reindex into a fresh `data_dir`.
- `open/2` validates every option up front (bounds, types, shape)
  before any side effect, including the corpus name itself: a name
  containing `/`, `\`, a NUL byte, or equal to `.`/`..` is now rejected
  outright. It was previously used unvalidated as a filesystem path
  component (security-relevant: closes a path-traversal risk).
- `open/2` and `close/1` for the same corpus are now serialized by a
  one-shot lifecycle coordinator, so a reopen of an already-live corpus
  is always reconciled against the running one instead of silently
  overwriting its metadata out from under it.
- `open/2` now persists a corpus-level `corpus.meta` (database, shard
  count, `phase2_selector_opts`, `fields`, `postings` codec), checked
  before any shard starts and committed only once every shard is up.
  A mismatched reopen is rejected with
  `{error, {config_mismatch, Field, Persisted, Requested}}` instead of
  silently reindexing, rebinding to a different database, or changing
  shard count and orphaning the old shard set.
- A corpus now detects its bound database being deleted and recreated
  under the same name, both at open time and continuously on every
  shard resubscribe. A mismatch stops the affected shard instead of
  silently reattaching and indexing under a stale watermark.

### Fixed

- `(?x)` extended-mode regex patterns are now interpreted for real
  (unescaped whitespace and `#`-comments stripped before analysis)
  instead of treating whitespace as a mandatory literal trigram, a
  false negative for every real match of an extended-mode pattern.
- The regex analyzer now fails closed (falls back to full-content
  confirmation) on lazy/possessive quantifiers, PCRE control verbs
  (`(*ACCEPT)`, `(*SKIP)`, ...), and unrecognized alphanumeric escapes
  (`\p{L}`, `\K`, `\R`, ...) instead of silently mis-parsing them as
  literal text, which could produce an unsound trigram query. POSIX
  class syntax (`[[:space:]]`, `[[.ch.]]`, `[[=e=]]`) is now scanned to
  its real closing bracket instead of stopping one character short.
- A positive `source`-verified match (literal or regex, buffer or
  segment) is now always re-confirmed against live `barrel_docdb`
  content before being returned, closing a false-positive window where
  a stale `source` could serve an outdated or deleted document's
  content as if current. A top-level `get_docs` error now propagates
  as a query error instead of a silent empty result; a per-document
  error other than `not_found` now propagates as
  `{confirm_failed, DocId, Reason}` instead of a silent non-match.
- `refresh/1` now drains up to a captured HLC target instead of
  re-querying its own moving watermark, so sustained concurrent writes
  can no longer make it recurse indefinitely. A drain error now
  propagates as `{error, {refresh_incomplete, Reason}}` instead of
  being silently discarded.
- Positional distance-checking (`match_starts/4`) now merge-joins the
  two offset lists instead of comparing every pair, closing an O(n*m)
  blowup on a document with many repeats of both grams.
- The compaction worker is now linked (and monitored) to its shard, so
  a shard stop or crash during compaction no longer leaves the worker
  running unsupervised with an orphaned temp segment.
- `gallop_intersect/2`'s arguments in `intersect_all/1`'s fold were
  reversed relative to its own documented contract (results were
  unaffected; only performance for a skewed pair of list sizes).
- A query against a corpus that was never opened, or is already
  closed, now returns `{error, corpus_not_open}` instead of crashing
  the caller.

## [0.8.0] - 2026-08-10

### Added

- Phase-2 (the sparse, content-defined positional index) now drives query
  results. A literal's reliable phase-2 grams are distance-checked to
  narrow candidates down to a specific byte position, not just a
  candidate document; with `open/2`'s new `source => {Module, InitArg}`
  option (a small byte-source behaviour, `barrel_ngram_source`), that
  candidate is confirmed by reading just the matched region instead of
  fetching the whole document. Without a `source`, phase-2 still narrows
  which documents get fetched, it just fetches them in full.
- Regex search gets the same treatment for a bounded subset of patterns: a
  clean AND-chain of literal runs with no alternation and no `^`/`$`/`\b`
  anchor or boundary picks its longest windowable literal run as an
  anchor and confirms with a windowed `re:run` instead of a full-document
  one. Everything else (unsupported constructs, alternation, anchors, an
  unbounded gap) still gets full-content confirmation, same as before --
  always exact either way.
- `case_sensitive => false` option on both `search/3` and `regex/3`. An
  ASCII literal or pattern narrows through phase-1's per-position
  case-variant expansion and verifies with `[caseless]`; a non-ASCII one
  skips narrowing entirely and verifies with `[caseless, unicode]`, with
  `{error, {invalid_literal_encoding, _}}` for a non-UTF-8 query and
  `{error, {invalid_document_encoding, DocId}}` if a candidate document
  turns out not to be valid UTF-8. Phase-2/windowing never applies here:
  its sampling is itself case-sensitive. A pattern with its own leading
  `(?i)` is caseless automatically, without the option.
- The regex analyzer gained a strict `unsupported` outcome for anything
  outside its supported subset -- lookarounds, backreferences, named
  groups, `\x{...}` escapes, `\Q...\E`, conditionals, a scoped or
  mid-pattern inline modifier -- so an unfamiliar construct falls back to
  full-content confirmation instead of risking a wrong (too-narrow)
  trigram query from being silently mis-parsed as literal text. It also
  now tracks per-literal-run prefix/suffix width bounds and a
  leading-`(?i)`/`(?s)`/`(?m)` flag, both needed for the windowing above.

### Changed

- **Breaking**: `open/2`'s `selector` option is retired -- every corpus now
  builds both a dense (phase-1, exhaustive) and a sparse (phase-2,
  content-defined, positional) index unconditionally. `selector_opts` is
  renamed `phase2_selector_opts` and now tunes phase-2 sampling
  specifically. `open/2` rejects `selector` outright with
  `{error, {unsupported_option, selector}}`.
- Segment format bumped to v4: posting blocks are now a self-delimiting
  composite of a phase-1 sub-block and an optional phase-2 (positional)
  sub-block, plus a new per-segment gram -> doc-count table. A pre-v4
  segment is rejected with a distinguishable
  `{unsupported_segment_version, _, _}` error rather than a generic one;
  there is no migration, reindex into a fresh corpus.
- Manifest format bumped to v2: it now persists and validates a corpus's
  `phase2_selector_opts`/`fields` across reopens, rejecting a mismatch
  with `{error, {config_mismatch, Field, Persisted, Requested}}` instead
  of silently reindexing under the new value. `open/2` also now eagerly
  validates every listed segment before returning, rather than surfacing
  a stale segment's error lazily on first query.

### Fixed

- Regex candidate gathering (`regex_segment_keys/2`) no longer silently
  swallows a segment-open error into an incomplete result; it now
  propagates, matching literal search's existing strict behavior.
- Windowed verification (literal and regex) could report overlapping
  matches that a plain left-to-right scan (`binary:matches/2`, `re:run`
  `global`) would not, when two distance-checked candidates genuinely
  overlapped (e.g. `"aaa"` at both offset 0 and 1 of `"aaaa"`). Spans are
  now reduced to that same non-overlapping set, matching every other
  verification path.
- A candidate whose sole reliable phase-2 gram also occurs many times
  elsewhere in the same document (repetitive content) no longer gets one
  windowed read per occurrence -- past 32 candidates in one document,
  verification falls back to a single full-content read instead. Found
  via profiling: a bounded regex query over a 100 KB repetitive document
  dropped from ~160ms to ~22ms.

## [0.7.1] - 2026-07-19

### Fixed

- Roaring NIF: guard the decode output allocation against a `size_t`
  multiply overflow, and validate a deserialized bitmap before any set op
  reads it (a corrupt or bit-rotted segment now fails cleanly).
- A manifest-save error during freeze no longer crashes the shard: the
  orphan segment is dropped and the buffer and watermark are kept (the tail
  replays). `manifest:save/1` tolerates an `ensure_dir` error.
- `refresh` drain stops when a full feed batch does not advance the
  watermark, so it cannot spin.
- `open/2` rolls back any shards it already started if a later shard fails,
  so a partial open leaves nothing orphaned; a shard is now a `transient`
  child, so an abnormal crash restarts it instead of leaving the corpus
  with no live shard.
- A query reads a shard's segments and buffer in one atomic call, so it
  never straddles a freeze.

## [0.7.0] - 2026-07-19

### Added

- Roaring-bitmap posting codec (opt in per corpus with `postings => roaring`).
  Posting lists are stored as roaring bitmaps and intersected with a native
  AND in a self-contained NIF (`barrel_ngram_roaring`, vendored CRoaring),
  removing the delta+varint decode cost for large dense corpora. On the
  intersection benchmark, twelve 50k-ordinal lists intersect in ~0.1 ms
  versus ~140 ms for varint. `varint` stays the default (it wins on small
  lists); results are identical either way (a differential oracle holds
  roaring byte-for-byte against varint).

### Changed

- Segment format bumped to v3 (a per-segment codec byte). This is
  barrel_ngram's first C: the roaring NIF is built by cmake at compile time.

### Added

- Task-oriented documentation under `docs/` (getting started, selectors,
  regex, sharding, operations, the MCP tool, and design), wired into
  ex_doc.

## [0.6.1] - 2026-07-19

### Added

- `barrel_ngram:is_open/1`, a cheap check of whether a corpus is open (used
  by callers that lazily open a corpus, such as the MCP tool).

## [0.6.0] - 2026-07-19

### Added

- Sharding: a corpus can be spread across N shards by rendezvous hashing
  on the document key (`open` option `shards => N`, default 1). Each shard
  indexes only its slice; queries fan out across shards and merge. Because
  a document is owned by exactly one shard, results are identical to a
  single-shard corpus. New self-contained `barrel_ngram_shards` module.

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
