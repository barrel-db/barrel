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

- a header (magic, offsets, document count, an HLC watermark),
- a direct-addressed offset table: `u32` per trigram over the whole 2^24 gram space, so a
  trigram's posting list is one indexed read (mostly zeros, relies on filesystem
  compression),
- a postings region: one delta+varint block of local document ordinals per present
  trigram,
- a sidecar: per ordinal, the document key, the change HLC, and a deleted flag.

Documents are addressed inside a segment by dense local *ordinals*; the sidecar maps an
ordinal back to its barrel document key for the confirm pass.

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

Which trigrams a document contributes is decided by a *selector*, applied identically by
the indexer and the query planner. The dense selector emits every trigram; the sparse
selector emits a content-defined sample for a smaller index. See [selectors](selectors.md).

## Cost

- A trigram lookup is one `pread` on the offset table plus one on the postings region.
- A query does trigram intersection then a batched multi-get for the confirm pass.
- Storage is roughly the postings (proportional to text size, smaller with the sparse
  selector) plus the offset table per segment.

## Intersection performance

Posting-list intersection is galloping over decoded ordinal lists. Measured over a
100k-document shard intersecting 12 lists (`barrel_ngram_bench:run/0`): intersection is
sub-millisecond for lists up to a few thousand ordinals and about 6 ms for lists of 50k,
and the cost is dominated by decoding the delta+varint blocks, not the intersection
itself. So the current path is fast for the configurations to use at scale: shard large
corpora (smaller per-shard lists) and use the sparse selector for code (fewer, smaller
posting lists).

The one slow regime is a large, dense, unsharded corpus with hot trigrams (a common gram
present in most documents), where decoding a very large block dominates. The postings
codec is a seam (`barrel_ngram_postings`): a roaring-bitmap backend (a compressed bitmap
with a native AND and no list materialization) can be dropped in there to remove that
decode cost if a deployment hits that regime. It is intentionally not built by default;
the measurement does not justify a native dependency for the sharded/sparse
configurations.
