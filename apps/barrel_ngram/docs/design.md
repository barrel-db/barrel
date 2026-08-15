# Design

This page explains how `barrel_ngram` works so you can reason about its behaviour,
storage, and cost. Read it when you are tuning a corpus, debugging a result, or deciding
whether the index fits your workload. It is inspired by GitHub Blackbird, stripped to the
core.

## The index in one paragraph

Every document is reduced to a byte string (its indexed text). That string is cut into
overlapping byte trigrams. For each trigram, the index stores the set of documents that
contain it (a posting list). A query is cut into the same trigrams; the documents that
contain the query are a subset of the documents that contain all of the query's trigrams,
so intersecting those posting lists yields a small candidate set. Trigram presence is
necessary but not sufficient, so each candidate is confirmed by fetching the current
document and running the real match. The confirm pass is what makes results exact.

## Segments

The index is a set of immutable segment files under the corpus directory, read with
`file:pread` (the OS/ZFS page cache does the caching; there is no mmap). A segment has:

- a header (magic, version, offsets, document count, an HLC watermark),
- a direct-addressed offset table: `u32` per trigram over the whole 2^24 gram space, so a
  trigram's posting list is one indexed read (mostly zeros, relies on filesystem
  compression),
- a postings region: one composite block per present trigram, `[Phase1Len:32][Phase1Block]
  [Phase2Len:32][Phase2Block]` -- each sub-block independently length-prefixed so the
  phase-1 (dense, delta+varint or roaring) bytes and the phase-2 (sparse, positional)
  bytes never bleed into each other; `Phase2Len =:= 0` means this trigram has no phase-2
  data,
- a gram -> doc-count table: a sorted `<<Gram:24, DocCount:32>>` array, one entry per
  distinct trigram with phase-2 data, so the query planner can rank a literal's candidate
  grams by rarity with a binary search instead of a full postings read,
- a sidecar: per ordinal, the document key, the change HLC, and a deleted flag.

Documents are addressed inside a segment by dense local *ordinals*; the sidecar maps an
ordinal back to its barrel document key for the confirm pass. A segment written by an
older or newer format than the reader expects fails to open with a distinguishable
`{unsupported_segment_version, Path, Got, Expected}` -- there is no migration, reindex
into a fresh corpus.

The manifest (the list of live segments plus the watermark) also persists the corpus's
own configuration -- `phase2_selector_opts` and `fields` -- and validates it on every
`open/2`: a reopen with a different value fails with
`{error, {config_mismatch, Field, Persisted, Requested}}` rather than silently reindexing
under the new value.

## Opening and closing a corpus

`open/2` and `close/1` for the same corpus name never interleave: a one-shot coordinator
serializes them, so a reopen of an already-live corpus is always reconciled against the
running one, not raced against it.

Alongside the per-shard manifest, a corpus persists a corpus-level `corpus.meta` --
`db`, the database's instance id, `shards`, `phase2_selector_opts`, `fields`, and
`postings` -- checked before any shard starts and committed only once every shard is up.
A reopen that disagrees with it, or with the corpus's own currently-running configuration,
fails with `{error, {config_mismatch, Field, Persisted, Requested}}` before anything is
touched, closing two gaps a per-shard-only check could not: rebinding a corpus to a
different (or recreated) database, and changing `shards` on a live corpus, which would
otherwise spawn an entirely disjoint set of shard directories and silently orphan the old
ones.

That database-instance id is also re-checked continuously, on every shard resubscribe, not
just at open. If the bound database is deleted and recreated under the same name while the
corpus stays open, the affected shard detects the mismatch on its next resubscribe and
stops itself instead of silently reattaching and indexing under a stale watermark.

A corpus indexed before corpus-level metadata existed (real segments on disk, no
`corpus.meta`) has no safe migration -- `db` and `shards` were never recoverable from what
it persisted -- so `open/2` rejects it with
`{error, {legacy_corpus_requires_reindex, Corpus}}`. Reindex it into a fresh `data_dir`.

`close/1` returns `ok | {error, term()}`: `{error, busy}` if another `open/2`/`close/1` for
the same corpus is already in flight and contention isn't resolved within the retry
window, or an error if a shard could not be confirmed stopped. It is otherwise idempotent
-- closing an already-closed or never-opened corpus is `ok`.

## The live lifecycle

The corpus tracks the database, so segments come and go:

- **Buffer.** Incoming changes are applied to an in-memory buffer keyed by document id (an
  update replaces, a delete becomes a tombstone).
- **Freeze.** When the buffer crosses a threshold (or on `refresh/1`), it is written to a
  new immutable segment, and the manifest (the list of live segments + the watermark) is
  committed by an atomic rename.
- **Query.** A search fans across every live segment plus the buffer.
- **Compaction.** When the segment count crosses a threshold (or on `compact/1`), a worker
  merges segments, collapsing each key to its newest version by HLC and physically
  evicting superseded and deleted entries.
- **Recovery.** On start the corpus reads the manifest and resubscribes from its
  watermark, so only the feed tail is replayed.

## Why the confirm pass matters

Because the confirm pass re-fetches the current document and runs the real match, the
index never has to be perfectly consistent to be correct. A stale entry left by an
update, a deleted document still present in an old segment, or a trigram false positive
are all dropped at confirm time. This is why updates and deletes need no separate liveness
bookkeeping for correctness, and why compaction can be approximate about eviction.

## Selectors

Which trigrams a document contributes is decided by a *selector*. Every corpus applies
both: the dense selector (phase-1, every trigram) and the sparse selector (phase-2, a
content-defined sample carrying byte offsets). A query always narrows through phase-1;
when a literal (or a regex's chosen literal run, see [regex](regex.md)) has phase-2 data
to spare, the planner narrows further with a distance-checked candidate match start
instead of just a candidate document. See [selectors](selectors.md).

## Cost

- A trigram lookup is one `pread` on the offset table plus one on the postings region.
- A query does trigram intersection then a batched multi-get for the confirm pass -- or,
  for a phase-2-narrowed candidate with a corpus `source` configured (`open/2`'s `source`
  option, a `{Module, InitArg}` pair implementing `barrel_ngram_source`), a `pread` of
  just the matched region instead of the whole document. Without a `source`, phase-2 still
  narrows which documents get fetched; it just fetches them in full.
- Storage is roughly the postings (proportional to text size) plus the offset table per
  segment; the phase-2 sample adds a smaller, sampled second set of postings alongside it,
  plus the doc-count table.

## Posting codecs and intersection performance

The default posting codec is delta+varint, intersected by galloping over decoded ordinal
lists. Measured over a 100k-document shard intersecting 12 lists
(`barrel_ngram_bench:run/0`): the galloping intersect is fast, but decoding the varint
blocks (materializing the ordinal lists) dominates for large lists, and it is inherent to
delta+varint (sequential, no random access). That only bites a large corpus with hot
trigrams (a common gram present in most documents); sharding keeps per-list sizes small in
normal use.

For that regime, open the corpus with `postings => roaring`. Posting lists are stored as
roaring bitmaps and intersected with a native AND in the NIF (`barrel_ngram_roaring`,
backed by vendored CRoaring), with no Erlang list materialization. Measured on the same
benchmark, roaring intersects twelve 50k-ordinal lists in about 0.1 ms versus 140 ms for
varint (a fourth of a millisecond even at 10k), at the cost of a fixed per-list overhead
that makes it slightly slower for tiny lists. So `varint` stays the default and `roaring`
is opt-in for large dense corpora. Both produce identical results (a differential oracle
holds `roaring` byte-for-byte against `varint`). The codec is a per-segment property, so a
corpus records it and reads it back at query and merge time.

## End-to-end query benchmark and profiling

`barrel_ngram_bench_search:run/0` measures a real `search`/`regex` call over a real corpus,
comparing no `source` (full-document fetch) against a windowed `source`, across document
sizes. `profile/0,1` runs one such call through `fprof` and prints the call graph, for
finding where a slow query actually spends its time. Neither runs as part of the test
suite; invoke them directly.

The comparison surfaced a real regression: a candidate whose only reliable phase-2 gram
also occurs many times elsewhere in the same document (repetitive content) used to get one
windowed read per occurrence, which for a 100 KB document with a common gram was an order
of magnitude slower than a single full-content read would have been. Verification now caps
the candidate count per document and falls back to one full read past the cap.
