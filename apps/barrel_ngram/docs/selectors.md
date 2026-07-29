# Selectors

A selector decides which trigrams a document contributes to the index. Choosing one is the
main lever on index size and query selectivity. Read this when you are opening a corpus and
deciding between a smaller index (sparse) and maximum recall on short queries (dense).

## Dense (default)

The dense selector indexes every overlapping trigram. It is the simplest and most
predictable: any substring of length three or more can be found through the index, and
regex is trigram-accelerated. Use it for prose and moderate corpora.

```erlang
ok = barrel_ngram:open(<<"prose">>, #{db => <<"mydb">>}).
%% selector defaults to barrel_ngram_selector_dense
```

## Sparse (content-defined)

The sparse selector keeps a trigram only when a hash of its local byte window passes a
sampling test, so roughly one in `sample_rate` trigrams is indexed. The index is smaller
and candidate sets are smaller. Use it for large, code-like corpora where the index size
matters.

```erlang
ok = barrel_ngram:open(<<"code">>,
                       #{db => <<"mydb">>,
                         selector => barrel_ngram_selector_sparse,
                         selector_opts => #{radius => 3, sample_rate => 4}}).
```

- `radius` (default 3): the window is the trigram plus `radius` bytes on each side.
- `sample_rate` (default 4): keep a trigram when `phash2(window, sample_rate) =:= 0`.

## The boundary rule

Sparse selection is content-defined, so a trigram is selected the same way wherever its
window falls entirely inside the surrounding bytes. For a query literal, only *interior*
trigrams (whose window is fully inside the literal) are reliable; trigrams near the
literal's edges see different bytes in a containing document and cannot be trusted. The
planner therefore intersects only over interior trigrams, and a literal shorter than
`3 + 2*radius` (or with no sampled interior trigram) falls back to a brute-force scan of
the live set. This is handled for you; the effect is that very short queries on a sparse
corpus scan rather than use the index.

## Choosing

- Prose or short-query heavy: dense.
- Large code corpora where index size dominates: sparse, tune `sample_rate` up for a
  smaller index (at the cost of more brute-force fallbacks on short queries).

Results are identical either way: the sparse index is validated against the dense index by
a differential oracle, so `search` and `regex` return the same documents. The choice only
affects size and speed, never correctness.

## Notes

- The selector is fixed for a corpus. To change it, open a new corpus (a different data
  dir) and reindex.
- Regex on a sparse corpus brute-forces (its index does not hold every trigram); see
  [regex](regex.md).
