# Selectors

A selector decides which trigrams a document contributes to the index. Read this when you
want to understand what gets indexed, or when you are tuning `phase2_selector_opts`.

## Two indexes, always both

Every corpus builds two indexes over the same documents, unconditionally: there is no
corpus-wide choice between them.

- **Phase-1 (dense)**: every overlapping trigram is indexed. `search/2,3` and `regex/2,3`
  always fall back to it: any substring of length three or more can be found, and regex is
  trigram-accelerated.
- **Phase-2 (sparse, positional)**: a trigram is kept only when a hash of its local byte
  window passes a sampling test, and (unlike phase-1) the byte offset where it occurs is
  stored too. A query uses these offsets to narrow candidates to specific byte positions
  and, with a `source` configured, to verify a match by reading a small window around that
  position instead of the whole document. See [How phase-2 accelerates a query](#how-phase-2-accelerates-a-query).

```erlang
ok = barrel_ngram:open(<<"code">>, #{db => <<"mydb">>}).
%% both indexes are built; phase-2 sampling uses its defaults
```

## Tuning phase-2 sampling

`phase2_selector_opts` tunes how much of the index phase-2 samples -- roughly one in
`sample_rate` trigrams is kept:

```erlang
ok = barrel_ngram:open(<<"code">>,
                       #{db => <<"mydb">>,
                         phase2_selector_opts => #{radius => 3, sample_rate => 4}}).
```

- `radius` (default 3): the window is the trigram plus `radius` bytes on each side.
- `sample_rate` (default 4): keep a trigram when `phash2(window, sample_rate) =:= 0`.

## The boundary rule

Phase-2 selection is content-defined, so a trigram is selected the same way wherever its
window falls entirely inside the surrounding bytes. For a query literal, only *interior*
trigrams (whose window is fully inside the literal) are reliable; trigrams near the
literal's edges see different bytes in a containing document and cannot be trusted. A
literal shorter than `3 + 2*radius` (or with no sampled interior trigram) has no reliable
phase-2 gram at all -- the query then narrows through phase-1 instead, the same as before
phase-2 existed.

## How phase-2 accelerates a query

For each segment, the planner ranks a literal's reliable grams by how many documents in
that segment carry them and distance-checks the two cheapest: two grams at the literal's
own byte distance apart, found at document offsets the same distance apart, back up to a
candidate match start. That candidate is still provisional -- confirmed the same way a
phase-1 candidate is -- but confirmation itself gets cheaper when the corpus has a `source`
configured (see [design](design.md)): instead of fetching the whole document, it reads
just `byte_size(Literal)` bytes at the candidate start (for a literal) or a small window
around the anchor (for a windowed regex, see [regex](regex.md)) and compares directly.
Without a `source`, phase-2 still narrows candidates -- fewer documents get fetched -- it
just fetches and re-scans them in full rather than reading a slice.

## Notes

- `phase2_selector_opts` and `fields` are fixed for the life of a corpus and persisted in
  its manifest: reopening with a different value fails with
  `{error, {config_mismatch, Field, Persisted, Requested}}` rather than silently
  reindexing under the new value. To change one, open a new corpus (a different data dir)
  and let it reindex.
- Phase-2 is skipped for a case-insensitive query (`case_sensitive => false`): its sampling
  decision is itself case-sensitive, so a stored gram can't be trusted to represent a
  case-insensitive match. See [regex](regex.md) and [getting started](getting-started.md).
