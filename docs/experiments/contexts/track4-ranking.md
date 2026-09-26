# S2: cross-context ranking (M1.5)

This page records spike S2 from `docs/architecture/contexts-action-plan.md`:
how well each merge mode ranks retrieval results across contexts, compared
with one index over the union, using a real embedding model. Read it when
you choose the default merge for federated retrieval (decision D4) or when
you decide whether `score` and `rerank` may be offered.

## Method

- **Corpus.** 1078 Erlang/OTP source modules from 33 applications (one
  document per module, `/opt/local/lib/erlang/lib/*/src`). Each application
  is one context (1 to 104 documents). The union database holds all 1078.
- **Queries.** 217 known-item queries: the first sentence of a module's
  `-moduledoc`; the relevant document is that module. The indexed text is
  the module body with the moduledoc removed (`body_nodoc`), so the query
  is not copied verbatim into its target.
- **Databases.** 33 record-mode barrel databases plus one union database,
  same policy: `fields => [path, text]`, `join => "\n"`, `mode => sync`,
  768 dimensions, embedder `{ollama, #{model => <<"nomic-embed-text">>}}`,
  HNSW with the default cosine distance, disk BM25. `text` is the first
  6000 characters of `body_nodoc`; the embedded text is exactly the policy
  text (`path "\n" text`). Vectors are computed once through
  `barrel_embed` (ollama provider) and cached, then written as client
  vectors (`_embedding`), so the databases never call the embedder.
  All 34 databases report the same B9 fingerprint.
- **Variants.** `plain`: text embedded as is. `prefix`: nomic task
  prefixes, `search_document: ` on documents and cards, `search_query: `
  on queries (the fingerprint does not see the prefix, see gaps).
- **Searches, k = 10.** Per context and on the union: `vector_top_k`
  (`barrel:search_vector/3` with the cached query vector), `bm25_top_k`
  (`barrel:search_bm25/3`), `hybrid_top_k` (`barrel_vectordb:search_hybrid/3`
  with `fusion => rrf` and `query_vector`, the call the BQL table function
  makes). Plus a brute-force exact cosine over the union as the oracle.
- **Merges across the 33 contexts** (through `barrel_ctx_merge`):
  - `grouped`: rows per context. Reported as the rank of the target inside
    its own application's group, i.e. what a caller gets when it looks at
    the right group. It is an oracle-scope number, not a ranked list.
  - `interleave`: round-robin by member rank, members in alphabetical order,
    and a second variant with members ordered by the card search below.
  - `score`: global sort by vector `_score`, refused unless every member
    reports the same fingerprint and cosine.
  - cross-context RRF over member ranks, and the naive global sort by raw
    hybrid (RRF) and raw BM25 scores, to quantify the documented failures.
  - `rerank`: `cross-encoder/ms-marco-MiniLM-L-6-v2` through
    `barrel_rerank`, pool of 100 candidates taken round-robin from the 33
    hybrid lists; for comparison the same reranker over the union's hybrid
    top 100. Rerank text is `path "\n" text` cut to 2000 characters (the
    model truncates at 512 tokens).
- **Scope selection.** One catalog card per application: title = the
  application name, body = the moduledoc first sentences of up to 10 of its
  modules (sorted by module name). For each query, the card of the target's
  application is rebuilt without the target's own sentence (leave-one-out,
  no leakage). Cards live in a record-mode catalog database; cards are
  ranked by vector, BM25 and hybrid search; the top 1, 3, 5, 10 or 33
  contexts are queried and merged.
- **Metrics.** One relevant document per query: recall@1, recall@10,
  MRR@10, nDCG@10 (ideal DCG = 1).

## Results (run 1, `plain` variant)

| method | recall@1 | recall@10 | MRR@10 | nDCG@10 |
|---|---|---|---|---|
| **union** vector, HNSW | 0.530 | 0.783 | 0.610 | 0.652 |
| **union** exact vector (brute force) | 0.539 | 0.802 | 0.624 | 0.667 |
| **union** bm25 | 0.452 | 0.783 | 0.565 | 0.618 |
| **union** hybrid (RRF) | 0.558 | 0.857 | 0.665 | 0.712 |
| grouped vector (target's group, oracle scope) | 0.618 | 0.908 | 0.714 | 0.761 |
| grouped bm25 (target's group, oracle scope) | 0.535 | 0.908 | 0.657 | 0.718 |
| grouped hybrid (target's group, oracle scope) | 0.599 | 0.926 | 0.720 | 0.771 |
| score vector (fingerprint checked) | 0.539 | 0.802 | 0.624 | 0.667 |
| interleave vector (alphabetical) | 0.005 | 0.078 | 0.024 | 0.037 |
| interleave bm25 (alphabetical) | 0.005 | 0.083 | 0.025 | 0.039 |
| interleave hybrid (alphabetical) | 0.005 | 0.078 | 0.026 | 0.038 |
| interleave hybrid (card order) | 0.272 | 0.493 | 0.337 | 0.375 |
| cross-context RRF (hybrid ranks) | 0.005 | 0.078 | 0.026 | 0.038 |
| raw hybrid RRF score sort | 0.032 | 0.433 | 0.115 | 0.188 |
| raw BM25 score sort (not valid) | 0.244 | 0.558 | 0.344 | 0.395 |
| rerank, pool 100 from 33 contexts | 0.438 | 0.733 | 0.541 | 0.588 |
| rerank, union hybrid top 100 | 0.396 | 0.724 | 0.501 | 0.555 |

`prefix` variant, main rows (full tables in the appendix):

| method | recall@1 | recall@10 | MRR@10 | nDCG@10 |
|---|---|---|---|---|
| union exact vector | 0.548 | 0.793 | 0.630 | 0.669 |
| union hybrid | 0.553 | 0.853 | 0.657 | 0.705 |
| score vector | 0.544 | 0.783 | 0.623 | 0.661 |
| grouped hybrid (oracle scope) | 0.608 | 0.922 | 0.718 | 0.767 |
| interleave hybrid (card order) | 0.267 | 0.548 | 0.354 | 0.400 |
| rerank, pool 100 from 33 contexts | 0.442 | 0.710 | 0.537 | 0.579 |

Rank-1 losses (`plain`): the target is rank 1 in its own context for 134
queries (vector). After `score` 17 of them are no longer rank 1 (a module
from another application is closer, which the union shows too); after
RRF or interleave 133 of them lose rank 1. For BM25, 116 queries have the
target at rank 1 in its context, and 63 lose it under a raw BM25 sort.

**Score distributions.** Across the 33 contexts, the Spearman correlation
between context size and the mean top-1 BM25 score is 0.965 (mean top-1
BM25 goes from 1.8 for `crypto`, 2 documents, to 10.8 for `stdlib`, 98
documents). For vectors it is 0.769 with a narrow range (0.48 to 0.59 mean
top-1). Per-context hybrid scores are RRF values bounded by
`0.5/61 + 0.5/61 = 0.0164`: every context's first hit scores about the same,
whatever its relevance. Full per-context table in the appendix.

### Scope selection (`plain`, card hybrid search; all rows in the appendix)

| contexts queried | true context selected | score vector recall@10 | score MRR@10 | grouped hybrid recall@10 |
|---|---|---|---|---|
| 1 | 0.521 | 0.452 | 0.334 | 0.465 |
| 3 | 0.668 | 0.571 | 0.433 | 0.608 |
| 5 | 0.733 | 0.627 | 0.468 | 0.673 |
| 10 | 0.843 | 0.710 | 0.534 | 0.783 |
| 33 (all) | 1.000 | 0.802 | 0.624 | 0.926 |

With `prefix`, card vector and hybrid search select better (top 5: 0.779
and 0.788 selected; score recall@10 0.650 and 0.654). Card BM25 is the
weakest selector in both variants.

### Run-to-run variation

HNSW level assignment uses `rand` in the store process, so vector and
hybrid rows change between runs; BM25 rows and the brute-force oracle do
not. A second run (no rerank) moved vector and hybrid numbers by at most
0.01 (union vector recall@10 0.783 to 0.793, score vector 0.802 to 0.797,
`prefix` score vector 0.783 to 0.788). Every conclusion below holds with
margins well above that.

## Interpretation

1. **`score` is exact for vector retrieval when fingerprints match.** The
   `score` merge equals the brute-force union oracle row for row in run 1
   (`plain`: 0.539 / 0.802 / 0.624 / 0.667 for both) and stays within HNSW
   noise otherwise. This is expected: the global top k is contained in the
   union of each member's top k when the scores are comparable. It is
   slightly better than the union's own HNSW (0.783), because small
   per-context graphs are closer to exact. B10's acceptance holds.
2. **Interleave and cross-context RRF are not rankings.** With 33 disjoint
   contexts, the first 10 slots hold the rank-1 hit of the first 10
   contexts: recall@10 drops to about 0.08. RRF over member ranks is the
   same thing with ties broken by context. Ordering members by card
   relevance helps (0.49 to 0.55), still well below `score`.
3. **Raw BM25 and raw hybrid scores must never be merged.** BM25 top
   scores track corpus size (Spearman 0.965): large contexts win. Raw BM25
   sort gives 0.558 recall@10 against 0.783 for BM25 on the union. Raw
   per-context RRF scores sit in a 0.015 to 0.0164 band and carry no
   cross-context signal (0.433).
4. **Grouped is honest but needs the right group.** Inside the target's
   group, recall@10 is 0.91 to 0.93, higher than any single list, because
   each group has fewer competitors. A caller only gets this when it knows
   which group to read, which is what scope selection tries to guess.
5. **Discovered scope is not exhaustive.** Picking contexts from cards
   finds the right application in the top 1 only half of the time and in
   the top 5 about 73 to 79% of the time. Querying 5 discovered contexts
   loses about 0.17 recall@10 against querying all 33 (0.627 vs 0.802).
   The skipped contexts must be reported (`scope_origin: discovered`).
6. **Rerank with this cross-encoder hurts on this corpus.** The MS MARCO
   MiniLM model, trained on web passages, ranks source code below plain
   vector similarity (0.733 federated, 0.724 on the union pool, vs 0.802
   for `score`). The federated rerank pool (100 candidates from 33
   contexts) did slightly better than reranking the union's top 100, so
   the pool construction is not the problem; the model is.
7. **Task prefixes do not change the merge picture.** nomic task prefixes
   moved retrieval by less than the run-to-run noise, and improved card
   selection by a few points.

## Recommendation

- **Default merge for retrieval: `grouped`**, as the plan proposes, for
  `bm25_top_k` and `hybrid_top_k`, and for `vector_top_k` whenever members
  do not share a fingerprint. It never claims a cross-context order.
- **`score` for `vector_top_k` whenever every member reports the same
  fingerprint and cosine**: it reproduces the single-index ranking, so the
  executor can select it automatically in that case (or make it the
  default for vector-only queries). Refuse it otherwise; the refusal path
  is tested (`score` returns `not_score_comparable` or
  `fingerprint_mismatch`).
- **`interleave` only as presentation**, labelled `relevance: false`, with
  members ordered by the request's scope order. Never call it ranking.
- **Never offer** raw BM25 sort, raw hybrid/RRF score sort, or RRF across
  contexts as a relevance order. `barrel_ctx_merge:rrf/3` exists only to
  document the failure and is labelled `relevance: false`.
- **`rerank` opt-in only**, and only with a cross-encoder validated for
  the corpus domain. With the default MS MARCO model it lowers recall on
  code. B12 should keep "reject when the sidecar is absent" and should not
  become a default.
- **A federated hybrid needs global statistics.** There is no valid
  cross-context merge for `hybrid_top_k` today. A later option: fuse the
  `score`-merged vector leg with a BM25 leg computed with shared corpus
  statistics (global document count, document frequencies, average
  length). That is new work, not in the backlog yet.

## B9 status

Done on this branch (commit `B9: embedding fingerprint`).

- `barrel_embed_provider` has an optional `model_info/1` callback; all 14
  built-in providers implement it (pure, from config, default model when
  unset). `revision` is reported only when the config pins one
  (`revision => <<"...">>`). Ollama names are normalized with an explicit
  tag (`nomic-embed-text` becomes `nomic-embed-text:latest`); Azure reports
  the deployment unless `model` is set.
- `barrel_embed:info/1` provider entries now carry `model` (and `revision`).
- `barrel:embedder_info/1` adds `provider`, `model`, `revision` (when
  known), `dimensions`, `distance` (from the vector index), `preprocessing`
  (`#{fields, join}` in record mode, `none` on plain databases) and
  `fingerprint`, `<<"sha256:", Hex/binary>>` of the canonical JSON (sorted
  keys) of `#{v => 1, embedder => Chain, dimensions, distance,
  preprocessing}`. Every provider of a fallback chain is part of the
  fingerprint. No `fingerprint` when any model in the chain is unknown or
  no embedder is configured. Unknown values are left out of the map.
- `barrel:info/1` carries the same identity under `embedder`.
- Tests: eunit per provider with stub configs, eunit for the fingerprint
  (every input changes it, write mode and metadata do not), a CT case in
  `barrel_record_SUITE` (same policy gives equal fingerprints, another
  model changes it), no network.

Gaps recorded, not addressed:

- The policy has no truncation setting and no task prefix; the harness
  truncates and prefixes client-side, so two databases embedding with and
  without nomic prefixes report the same fingerprint. Both belong in the
  policy's preprocessing if they move into barrel.
- Quantization is not in the fingerprint (plan 3.4 lists it): it changes
  the approximation, not the vector space. Revisit with B10.
- Revision is only what the config pins. Ollama's model digest
  (`/api/tags`) and Hugging Face revisions are not resolved automatically,
  so a re-pulled model with the same tag keeps its fingerprint.
- Fingerprints are not yet on cards or in HTTP meta (B11).

## Threats to validity

- Known-item queries built from moduledoc first sentences: one relevant
  document per query, a natural-language query against code. Real agent
  queries have several relevant documents and different phrasing.
- 217 queries, one corpus, 33 very unequal contexts (1 to 104 documents).
  Recall@1 differences under about 0.03 are within sampling noise.
- One embedding model (nomic-embed-text, 768 dims, 2048-token context in
  ollama; inputs are cut to 6000 characters and ollama truncates beyond the
  context). One cross-encoder, not trained on code.
- HNSW is randomized (see run-to-run variation).
- Cards: at most 10 sentences per application, sorted by module name;
  25 of 33 applications have fewer than 10 moduledocs and 9 have one or
  none, so for their queries the leave-one-out card is the application
  name alone. Card selection numbers are a lower bound for richer cards. Card BM25 IDF is computed over base and
  leave-one-out cards together.

## Deviations from the brief

- Queries use the cached query vector and call the same functions the BQL
  table functions call (`barrel:search_vector/3`, `barrel:search_bm25/3`,
  `barrel_vectordb:search_hybrid/3` with `query_vector`) instead of running
  BQL, because BQL `vector_top_k` embeds the query text itself (one ollama
  call per query per context, and no query prefix).
- `grouped` is scored inside the target's group (oracle scope), since a
  grouped response has no single ranked list.
- Rerank runs through a `barrel_rerank` gen_server started directly (not
  the application, whose start creates a venv) with an existing Python
  environment that already had `transformers` and the cached model,
  offline (`HF_HUB_OFFLINE=1`). Nothing was installed or downloaded.
- The sidecar reads one JSON line of at most 64 KiB (asyncio default); a
  100-document request failed. The harness sends 8 documents per call.
  B12 must chunk or raise the sidecar's limit.

## Remaining work before PR-ready

- Decide D4 with the numbers above; update the plan's 5.3 table to allow
  automatic `score` for vector retrieval with equal fingerprints.
- Wire `barrel_ctx_merge` into the federated executor (other track) and
  carry `meta` (`merge`, `relevance`) into the response.
- B11: fingerprint on cards and in the HTTP meta. `barrel_server`
  serializes `barrel:info/1`; the new `embedder` map is JSON-safe (atoms
  and binaries, unknown values omitted) but its HTTP shape is not reviewed.
- Add truncation and task prefixes to the policy preprocessing if barrel
  takes them over; fix the sidecar line limit for B12.
- Rerun S2 on a second corpus with multi-relevance queries and a
  code-aware reranker before letting `rerank` near a default.

## Reproduce

```sh
# 1. corpus: edit ROOT (an OTP lib dir with sources) if needed, then
mkdir -p /tmp/s2corpus && (cd /tmp/s2corpus && python3 /path/to/barrel/bench/ctx_ranking/build_corpus.py)
# 2. ollama running with nomic-embed-text; the optional third argument is
#    a Python with transformers and the cross-encoder already cached.
./bench/ctx_ranking/run.sh /tmp/s2corpus /tmp/barrel_ctx_ranking /path/to/python
```

The harness writes `results.md` and `embeddings.etf` (the cache) into the
work directory. Without the third argument, rerank is skipped. The
databases are rebuilt on every run.

## Appendix: generated output (run 1)


1078 docs, 33 contexts, 217 queries, k=10, model nomic-embed-text, rerank cross-encoder/ms-marco-MiniLM-L-6-v2

### Variant `plain`

Fingerprints across the 33 contexts: 1 distinct; union: <<"sha256:e6be009006773186146cc279463d97769ba1eba5420df91a34da1c67c08bb5ed">>
score merge refused when fingerprints are missing: true

#### Retrieval quality

| method | recall@1 | recall@10 | MRR@10 | nDCG@10 |
|---|---|---|---|---|
| union vector (HNSW) | 0.530 | 0.783 | 0.610 | 0.652 |
| union exact vector (brute force) | 0.539 | 0.802 | 0.624 | 0.667 |
| union bm25 | 0.452 | 0.783 | 0.565 | 0.618 |
| union hybrid (RRF) | 0.558 | 0.857 | 0.665 | 0.712 |
| grouped vector (target's group) | 0.618 | 0.908 | 0.714 | 0.761 |
| grouped bm25 (target's group) | 0.535 | 0.908 | 0.657 | 0.718 |
| grouped hybrid (target's group) | 0.599 | 0.926 | 0.720 | 0.771 |
| score vector (fingerprint checked) | 0.539 | 0.802 | 0.624 | 0.667 |
| interleave vector (alphabetical) | 0.005 | 0.078 | 0.024 | 0.037 |
| interleave bm25 (alphabetical) | 0.005 | 0.083 | 0.025 | 0.039 |
| interleave hybrid (alphabetical) | 0.005 | 0.078 | 0.026 | 0.038 |
| interleave hybrid (card order) | 0.272 | 0.493 | 0.337 | 0.375 |
| cross-context RRF (hybrid ranks) | 0.005 | 0.078 | 0.026 | 0.038 |
| raw hybrid RRF score sort | 0.032 | 0.433 | 0.115 | 0.188 |
| raw BM25 score sort (invalid) | 0.244 | 0.558 | 0.344 | 0.395 |
| rerank, pool 100 from 33 contexts | 0.438 | 0.733 | 0.541 | 0.588 |
| rerank, union hybrid top 100 | 0.396 | 0.724 | 0.501 | 0.555 |

Target rank 1 in its own context (vector): 134 queries; no longer rank 1 after score merge: 17, after RRF: 133, after interleave: 133.
Target rank 1 in its own context (bm25): 116; no longer rank 1 after raw BM25 sort: 63.

#### Scope selection by card search

| card search | contexts queried | true context selected | score vector recall@10 | score vector MRR@10 | grouped hybrid recall@10 | interleave hybrid recall@10 |
|---|---|---|---|---|---|---|
| card_vector | 1 | 0.498 | 0.429 | 0.334 | 0.452 | 0.452 |
| card_vector | 3 | 0.645 | 0.558 | 0.431 | 0.590 | 0.539 |
| card_vector | 5 | 0.724 | 0.613 | 0.476 | 0.659 | 0.539 |
| card_vector | 10 | 0.834 | 0.705 | 0.538 | 0.770 | 0.493 |
| card_vector | 33 | 1.000 | 0.802 | 0.624 | 0.926 | 0.493 |
| card_bm25 | 1 | 0.516 | 0.447 | 0.335 | 0.461 | 0.461 |
| card_bm25 | 3 | 0.641 | 0.548 | 0.401 | 0.581 | 0.512 |
| card_bm25 | 5 | 0.710 | 0.608 | 0.440 | 0.650 | 0.530 |
| card_bm25 | 10 | 0.816 | 0.691 | 0.518 | 0.756 | 0.470 |
| card_bm25 | 33 | 1.000 | 0.802 | 0.624 | 0.926 | 0.470 |
| card_hybrid | 1 | 0.521 | 0.452 | 0.334 | 0.465 | 0.465 |
| card_hybrid | 3 | 0.668 | 0.571 | 0.433 | 0.608 | 0.544 |
| card_hybrid | 5 | 0.733 | 0.627 | 0.468 | 0.673 | 0.553 |
| card_hybrid | 10 | 0.843 | 0.710 | 0.534 | 0.783 | 0.493 |
| card_hybrid | 33 | 1.000 | 0.802 | 0.624 | 0.926 | 0.493 |

#### Top-1 score per context over all queries (mean / median / max)

Spearman(context size, mean top-1 BM25) = 0.965; Spearman(context size, mean top-1 vector) = 0.769

| context | docs | bm25 top-1 | vector top-1 | hybrid top-1 |
|---|---|---|---|---|
| asn1 | 22 | 5.28 / 4.90 / 27.15 | 0.535 / 0.536 / 0.763 | 0.0159 / 0.0160 / 0.0164 |
| common_test | 47 | 8.46 / 7.30 / 22.36 | 0.584 / 0.581 / 0.742 | 0.0159 / 0.0161 / 0.0164 |
| compiler | 59 | 7.00 / 6.28 / 30.66 | 0.556 / 0.550 / 0.724 | 0.0157 / 0.0159 / 0.0164 |
| crypto | 2 | 1.84 / 1.66 / 6.82 | 0.503 / 0.504 / 0.660 | 0.0159 / 0.0164 / 0.0164 |
| debugger | 24 | 4.96 / 4.42 / 14.15 | 0.534 / 0.534 / 0.715 | 0.0159 / 0.0160 / 0.0164 |
| dialyzer | 29 | 5.56 / 5.15 / 15.83 | 0.548 / 0.543 / 0.727 | 0.0158 / 0.0159 / 0.0164 |
| diameter | 47 | 6.72 / 5.90 / 21.73 | 0.551 / 0.553 / 0.735 | 0.0158 / 0.0160 / 0.0164 |
| edoc | 21 | 4.91 / 4.58 / 21.79 | 0.509 / 0.503 / 0.662 | 0.0158 / 0.0159 / 0.0164 |
| eldap | 1 | 2.29 / 2.05 / 6.47 | 0.483 / 0.485 / 0.625 | 0.0162 / 0.0164 / 0.0164 |
| erts | 22 | 5.51 / 4.87 / 17.34 | 0.557 / 0.554 / 0.674 | 0.0160 / 0.0161 / 0.0164 |
| et | 6 | 3.89 / 3.28 / 16.75 | 0.539 / 0.539 / 0.702 | 0.0162 / 0.0163 / 0.0164 |
| eunit | 13 | 3.89 / 3.52 / 9.54 | 0.525 / 0.529 / 0.658 | 0.0160 / 0.0161 / 0.0164 |
| ftp | 6 | 2.90 / 2.33 / 14.71 | 0.514 / 0.518 / 0.707 | 0.0161 / 0.0163 / 0.0164 |
| inets | 63 | 7.33 / 6.50 / 30.45 | 0.542 / 0.542 / 0.695 | 0.0157 / 0.0159 / 0.0164 |
| kernel | 104 | 9.19 / 8.31 / 28.86 | 0.569 / 0.568 / 0.709 | 0.0157 / 0.0159 / 0.0164 |
| megaco | 65 | 7.60 / 6.99 / 26.17 | 0.551 / 0.550 / 0.774 | 0.0156 / 0.0158 / 0.0164 |
| mnesia | 31 | 5.25 / 5.00 / 16.26 | 0.555 / 0.557 / 0.759 | 0.0159 / 0.0160 / 0.0164 |
| observer | 44 | 6.40 / 5.76 / 20.50 | 0.544 / 0.547 / 0.731 | 0.0159 / 0.0161 / 0.0164 |
| odbc | 3 | 2.23 / 2.00 / 7.84 | 0.503 / 0.510 / 0.662 | 0.0160 / 0.0163 / 0.0164 |
| os_mon | 8 | 3.08 / 2.76 / 13.34 | 0.535 / 0.539 / 0.689 | 0.0160 / 0.0163 / 0.0164 |
| parsetools | 4 | 2.24 / 1.99 / 9.21 | 0.514 / 0.503 / 0.711 | 0.0161 / 0.0163 / 0.0164 |
| public_key | 42 | 7.56 / 7.15 / 22.94 | 0.553 / 0.541 / 0.758 | 0.0152 / 0.0154 / 0.0164 |
| reltool | 9 | 2.65 / 2.36 / 10.07 | 0.519 / 0.523 / 0.697 | 0.0159 / 0.0161 / 0.0164 |
| runtime_tools | 12 | 4.53 / 3.79 / 15.20 | 0.554 / 0.553 / 0.715 | 0.0160 / 0.0161 / 0.0164 |
| sasl | 17 | 4.99 / 4.17 / 26.89 | 0.537 / 0.539 / 0.761 | 0.0160 / 0.0161 / 0.0164 |
| snmp | 90 | 8.63 / 7.96 / 28.18 | 0.577 / 0.563 / 0.767 | 0.0157 / 0.0159 / 0.0164 |
| ssh | 43 | 6.46 / 5.88 / 17.83 | 0.548 / 0.546 / 0.754 | 0.0158 / 0.0160 / 0.0164 |
| ssl | 78 | 6.95 / 5.99 / 20.12 | 0.545 / 0.545 / 0.759 | 0.0155 / 0.0157 / 0.0164 |
| stdlib | 98 | 10.82 / 9.28 / 41.83 | 0.586 / 0.581 / 0.771 | 0.0159 / 0.0160 / 0.0164 |
| syntax_tools | 9 | 3.29 / 2.88 / 14.67 | 0.526 / 0.516 / 0.724 | 0.0160 / 0.0161 / 0.0164 |
| tftp | 8 | 3.43 / 3.05 / 12.23 | 0.523 / 0.524 / 0.650 | 0.0161 / 0.0163 / 0.0164 |
| tools | 16 | 4.97 / 4.11 / 27.47 | 0.554 / 0.549 / 0.781 | 0.0161 / 0.0161 / 0.0164 |
| xmerl | 35 | 5.40 / 4.91 / 20.89 | 0.526 / 0.527 / 0.704 | 0.0157 / 0.0159 / 0.0164 |
### Variant `prefix`

Fingerprints across the 33 contexts: 1 distinct; union: <<"sha256:e6be009006773186146cc279463d97769ba1eba5420df91a34da1c67c08bb5ed">>
score merge refused when fingerprints are missing: true

#### Retrieval quality

| method | recall@1 | recall@10 | MRR@10 | nDCG@10 |
|---|---|---|---|---|
| union vector (HNSW) | 0.539 | 0.779 | 0.616 | 0.655 |
| union exact vector (brute force) | 0.548 | 0.793 | 0.630 | 0.669 |
| union bm25 | 0.452 | 0.783 | 0.565 | 0.618 |
| union hybrid (RRF) | 0.553 | 0.853 | 0.657 | 0.705 |
| grouped vector (target's group) | 0.618 | 0.885 | 0.706 | 0.749 |
| grouped bm25 (target's group) | 0.535 | 0.908 | 0.657 | 0.718 |
| grouped hybrid (target's group) | 0.608 | 0.922 | 0.718 | 0.767 |
| score vector (fingerprint checked) | 0.544 | 0.783 | 0.623 | 0.661 |
| interleave vector (alphabetical) | 0.005 | 0.074 | 0.022 | 0.034 |
| interleave bm25 (alphabetical) | 0.005 | 0.083 | 0.025 | 0.039 |
| interleave hybrid (alphabetical) | 0.005 | 0.078 | 0.024 | 0.037 |
| interleave hybrid (card order) | 0.267 | 0.548 | 0.354 | 0.400 |
| cross-context RRF (hybrid ranks) | 0.005 | 0.078 | 0.024 | 0.037 |
| raw hybrid RRF score sort | 0.028 | 0.438 | 0.114 | 0.188 |
| raw BM25 score sort (invalid) | 0.244 | 0.558 | 0.344 | 0.395 |
| rerank, pool 100 from 33 contexts | 0.442 | 0.710 | 0.537 | 0.579 |
| rerank, union hybrid top 100 | 0.396 | 0.728 | 0.502 | 0.557 |

Target rank 1 in its own context (vector): 134 queries; no longer rank 1 after score merge: 16, after RRF: 133, after interleave: 133.
Target rank 1 in its own context (bm25): 116; no longer rank 1 after raw BM25 sort: 63.

#### Scope selection by card search

| card search | contexts queried | true context selected | score vector recall@10 | score vector MRR@10 | grouped hybrid recall@10 | interleave hybrid recall@10 |
|---|---|---|---|---|---|---|
| card_vector | 1 | 0.530 | 0.452 | 0.364 | 0.488 | 0.488 |
| card_vector | 3 | 0.705 | 0.594 | 0.474 | 0.650 | 0.581 |
| card_vector | 5 | 0.779 | 0.650 | 0.517 | 0.719 | 0.590 |
| card_vector | 10 | 0.871 | 0.719 | 0.568 | 0.806 | 0.521 |
| card_vector | 33 | 1.000 | 0.783 | 0.623 | 0.922 | 0.521 |
| card_bm25 | 1 | 0.516 | 0.429 | 0.331 | 0.461 | 0.461 |
| card_bm25 | 3 | 0.641 | 0.525 | 0.404 | 0.581 | 0.498 |
| card_bm25 | 5 | 0.710 | 0.585 | 0.444 | 0.645 | 0.525 |
| card_bm25 | 10 | 0.816 | 0.664 | 0.518 | 0.751 | 0.484 |
| card_bm25 | 33 | 1.000 | 0.783 | 0.623 | 0.922 | 0.484 |
| card_hybrid | 1 | 0.525 | 0.438 | 0.332 | 0.470 | 0.470 |
| card_hybrid | 3 | 0.714 | 0.599 | 0.470 | 0.654 | 0.571 |
| card_hybrid | 5 | 0.788 | 0.654 | 0.516 | 0.724 | 0.599 |
| card_hybrid | 10 | 0.899 | 0.728 | 0.578 | 0.829 | 0.548 |
| card_hybrid | 33 | 1.000 | 0.783 | 0.623 | 0.922 | 0.548 |

#### Top-1 score per context over all queries (mean / median / max)

Spearman(context size, mean top-1 BM25) = 0.965; Spearman(context size, mean top-1 vector) = 0.769

| context | docs | bm25 top-1 | vector top-1 | hybrid top-1 |
|---|---|---|---|---|
| asn1 | 22 | 5.28 / 4.90 / 27.15 | 0.610 / 0.606 / 0.792 | 0.0159 / 0.0160 / 0.0164 |
| common_test | 47 | 8.46 / 7.30 / 22.36 | 0.642 / 0.645 / 0.781 | 0.0159 / 0.0161 / 0.0164 |
| compiler | 59 | 7.00 / 6.28 / 30.66 | 0.621 / 0.615 / 0.789 | 0.0157 / 0.0159 / 0.0164 |
| crypto | 2 | 1.84 / 1.66 / 6.82 | 0.575 / 0.574 / 0.702 | 0.0159 / 0.0164 / 0.0164 |
| debugger | 24 | 4.96 / 4.42 / 14.15 | 0.602 / 0.602 / 0.741 | 0.0158 / 0.0159 / 0.0164 |
| dialyzer | 29 | 5.56 / 5.15 / 15.83 | 0.618 / 0.615 / 0.752 | 0.0159 / 0.0159 / 0.0164 |
| diameter | 47 | 6.72 / 5.90 / 21.73 | 0.630 / 0.629 / 0.789 | 0.0156 / 0.0158 / 0.0164 |
| edoc | 21 | 4.91 / 4.58 / 21.79 | 0.578 / 0.573 / 0.697 | 0.0158 / 0.0159 / 0.0164 |
| eldap | 1 | 2.29 / 2.05 / 6.47 | 0.557 / 0.559 / 0.669 | 0.0162 / 0.0164 / 0.0164 |
| erts | 22 | 5.51 / 4.87 / 17.34 | 0.625 / 0.620 / 0.724 | 0.0159 / 0.0161 / 0.0164 |
| et | 6 | 3.89 / 3.28 / 16.75 | 0.612 / 0.612 / 0.745 | 0.0162 / 0.0164 / 0.0164 |
| eunit | 13 | 3.89 / 3.52 / 9.54 | 0.603 / 0.603 / 0.736 | 0.0160 / 0.0161 / 0.0164 |
| ftp | 6 | 2.90 / 2.33 / 14.71 | 0.586 / 0.589 / 0.739 | 0.0160 / 0.0161 / 0.0164 |
| inets | 63 | 7.33 / 6.50 / 30.45 | 0.613 / 0.613 / 0.715 | 0.0156 / 0.0158 / 0.0164 |
| kernel | 104 | 9.19 / 8.31 / 28.86 | 0.632 / 0.632 / 0.772 | 0.0156 / 0.0159 / 0.0164 |
| megaco | 65 | 7.60 / 6.99 / 26.17 | 0.624 / 0.622 / 0.806 | 0.0156 / 0.0158 / 0.0164 |
| mnesia | 31 | 5.25 / 5.00 / 16.26 | 0.623 / 0.624 / 0.796 | 0.0158 / 0.0159 / 0.0164 |
| observer | 44 | 6.40 / 5.76 / 20.50 | 0.618 / 0.619 / 0.766 | 0.0158 / 0.0160 / 0.0164 |
| odbc | 3 | 2.23 / 2.00 / 7.84 | 0.595 / 0.600 / 0.701 | 0.0160 / 0.0163 / 0.0164 |
| os_mon | 8 | 3.08 / 2.76 / 13.34 | 0.606 / 0.608 / 0.731 | 0.0160 / 0.0161 / 0.0164 |
| parsetools | 4 | 2.24 / 1.99 / 9.21 | 0.586 / 0.577 / 0.735 | 0.0161 / 0.0163 / 0.0164 |
| public_key | 42 | 7.56 / 7.15 / 22.94 | 0.625 / 0.615 / 0.781 | 0.0152 / 0.0154 / 0.0164 |
| reltool | 9 | 2.65 / 2.36 / 10.07 | 0.584 / 0.581 / 0.743 | 0.0159 / 0.0161 / 0.0164 |
| runtime_tools | 12 | 4.53 / 3.79 / 15.20 | 0.618 / 0.615 / 0.744 | 0.0161 / 0.0161 / 0.0164 |
| sasl | 17 | 4.99 / 4.17 / 26.89 | 0.604 / 0.605 / 0.773 | 0.0160 / 0.0161 / 0.0164 |
| snmp | 90 | 8.63 / 7.96 / 28.18 | 0.638 / 0.627 / 0.786 | 0.0155 / 0.0157 / 0.0164 |
| ssh | 43 | 6.46 / 5.88 / 17.83 | 0.613 / 0.610 / 0.789 | 0.0158 / 0.0160 / 0.0164 |
| ssl | 78 | 6.95 / 5.99 / 20.12 | 0.607 / 0.610 / 0.785 | 0.0153 / 0.0155 / 0.0164 |
| stdlib | 98 | 10.82 / 9.28 / 41.83 | 0.648 / 0.642 / 0.785 | 0.0158 / 0.0160 / 0.0164 |
| syntax_tools | 9 | 3.29 / 2.88 / 14.67 | 0.588 / 0.579 / 0.739 | 0.0160 / 0.0163 / 0.0164 |
| tftp | 8 | 3.43 / 3.05 / 12.23 | 0.595 / 0.599 / 0.711 | 0.0161 / 0.0163 / 0.0164 |
| tools | 16 | 4.97 / 4.11 / 27.47 | 0.621 / 0.617 / 0.810 | 0.0161 / 0.0161 / 0.0164 |
| xmerl | 35 | 5.40 / 4.91 / 20.89 | 0.596 / 0.594 / 0.750 | 0.0157 / 0.0159 / 0.0164 |
