# Contexts: experiment results

Status: results (2026-09-24). This document records what was built and
measured to test the three directions of the
[contexts action plan](contexts-action-plan.md), what it showed, and what it
changes. The experiments ran on unpublished branches, in four tracks:
remote composition (track 1), portability (track 2), ngram packages
(track 3), and cross-context ranking (track 4). Their reports are in
`docs/experiments/contexts/`; the feature ships as ten stacked PRs (see
the build path in the action plan, section 12).

## Environment and data

- Apple M4 Pro, 14 cores, 48 GB, APFS; OTP 29; one BEAM per node, loopback.
  Background system load was present during the first runs; S1 was re-run at
  load average about 4 and only that run is reported.
- Corpus: OTP's own source, 1078 modules in 33 applications, 39 MB of text
  (one application = one context). 217 known-item queries (a module's first
  moduledoc sentence; the relevant document is that module, indexed without
  its moduledoc).
- Embeddings: ollama `nomic-embed-text` (768 dimensions, cosine). No other
  model was downloaded.

## Summary

| Direction | Verdict |
|---|---|
| M1 remote composition | **Ship.** Works end to end over REST, MCP and Erlang. Latency is the network round trip plus a few ms, flat from 1 to 8 members. Failures are reported precisely. |
| M1.5 ranking | **Resolved.** Grouped results by default; automatic score merge for vector search when fingerprints match (exactly equal to a single brute-force index). RRF and interleave across contexts are unusable as relevance orders. The reranker made results worse on code. |
| M2 portability | **Ship after two fixes.** Export and import of the whole corpus take about 50-70 ms each; ANN graphs load without rebuild. Blocked on the disk BM25 bug (below) and on cold-open cost of the in-memory BM25 rebuild. |
| M3 ngram packages | **Correct, not yet worth it.** Exact answers and honest partial results everywhere, but the v4 segment format makes the index 34x the text; wide queries fetch everything and are 20-70x slower than local. Fix the segment format first. |

Three findings apply to `main` independently of contexts:

1. **Disk BM25 loses its data when a database closes and reopens.**
   Reproduced on the current code with `barrel_vectordb` alone: 20 documents,
   `search_bm25` returns 5 hits, stop and restart, it returns `[]`. The
   hot layer is not flushed at close, a compaction replaces the previous
   disk segment instead of merging, and loading document statistics is a
   TODO (`barrel_vectordb_bm25_disk`). Disk BM25 is the default in record
   mode, so every record-mode database loses BM25 on restart.
2. **ngram v4 segments are 48-64 MB each regardless of content**, because
   the gram offset table is direct-addressed up to the highest gram present
   (2^24 entries of 4 bytes). The OTP corpus (39 MB of text) produces a
   1.34 GB index.
3. **Two ngram correctness bugs** (fixed in PR #49): a query
   concurrent with a compaction fails with `enoent`; posting, key, planner,
   and truncated-offset-table read errors silently return fewer or no
   matches. A merge read error crashed the shard process.

Smaller ones found on the way: `barrel:open` creates a database when a
query names a missing one (a mistyped card creates a database); record-mode
policies persist the embedder config, which may hold an `api_key`; MCP
resource reads depend on template registration order (worked around in
`barrel_server_mcp_resources`, the fix belongs in `barrel_mcp`); a hackney
connection outlives a killed caller; `barrel_bql_exec:fold_chunks` returns a
continuation past unseen rows when a fold stops mid-chunk; `rebar3 ct` can
create a Python venv under `apps/barrel_embed/priv` unless
`managed_venv => false` is configured; the `barrel_rerank` sidecar reads at
most 64 KiB per request line.

## M1: remote composition

Built: observed version (`instance_id`, `last_seq`) in query meta; `max_rows`
and `deadline_ms` on `POST /db/:db/query`; card store `barrel_ctx_catalog`;
remote client `barrel_ctx_remote` (hackney, streamed NDJSON, byte cap, node
slot limit, error line or missing meta means failure); executor
`barrel_ctx_query` with shape checks, bounded fanout, deadlines, provenance,
`sources` and `coverage`; counted, monitored leases in `barrel_dbs`; REST
`/contexts*` and MCP `context_list`, `context_inspect`, `context_query`.

Correctness (all passing): ordered federated queries equal the same query
over one database holding all documents (4 fixed and 25 generated
statements, plus over REST); closed port gives `unreachable`; stall gives
`timeout` within deadline + 10 %; an in-band error or a truncated stream
gives `error` with zero rows kept; every rejected shape returns its reason
before any member is contacted; no lease or remote slot is left held; no
database is created; a capability token sees its own space, gets
`unauthorized` for another local database, and reaches public remote ones.

S1 fanout (30 iterations, loopback, RTT injected by a proxy that delays data
but not the TCP handshake; ordered = `ORDER BY ... LIMIT 20`, grouped = 20
rows per member):

| Members | RTT ms | Query | p50 ms | p95 ms | Bytes |
|---|---|---|---|---|---|
| 1 | 0 | ordered | 1.6 | 2.1 | 1734 |
| 3 | 0 | ordered | 3.2 | 6.3 | 5316 |
| 8 | 0 | ordered | 5.6 | 7.4 | 15810 |
| 8 | 0 | grouped | 8.3 | 9.9 | 13981 |
| 1 | 20 | ordered | 28.2 | 32.0 | 1734 |
| 8 | 20 | ordered | 27.8 | 32.9 | 15810 |
| 1 | 100 | ordered | 109.9 | 116.7 | 1734 |
| 8 | 100 | ordered | 119.7 | 131.1 | 15810 |
| 8 | 100 | grouped | 120.6 | 129.7 | 13981 |

Reading: parallel fanout makes latency one round trip plus 2-8 ms of work,
independent of member count up to 8. There is no connection reuse yet, so a
real network adds one more round trip for the TCP handshake (and more for
TLS). Bytes grow linearly with members because every member returns its full
LIMIT for an ordered merge.

## M1.5: ranking across contexts

33 record-mode databases, one union database as baseline, same fingerprint
everywhere, k = 10, 217 queries (run 1; HNSW randomness moves vector rows by
up to 0.01 between runs, BM25 rows do not move).

| Method | R@1 | R@10 | MRR@10 | nDCG@10 |
|---|---|---|---|---|
| Union, hybrid | 0.558 | 0.857 | 0.665 | 0.712 |
| Union, exact vector (brute force) | 0.539 | 0.802 | 0.624 | 0.667 |
| Grouped hybrid, target within its own context's group | 0.599 | 0.926 | 0.720 | 0.771 |
| Score merge, vector, fingerprint checked | 0.539 | 0.802 | 0.624 | 0.667 |
| Rerank (MiniLM cross-encoder), pool 100 | 0.438 | 0.733 | 0.541 | 0.588 |
| Raw BM25 score sort (invalid) | 0.244 | 0.558 | 0.344 | 0.395 |
| Raw hybrid score sort | 0.032 | 0.433 | 0.115 | 0.188 |
| Interleave, members ordered by card search | 0.272 | 0.493 | 0.337 | 0.375 |
| Cross-context RRF / alphabetical interleave | 0.005 | 0.078 | 0.026 | 0.038 |

- Score merge of vector results equals the brute-force union exactly: with a
  shared fingerprint and cosine, cross-context vector scores are
  comparable.
- BM25 scores track corpus size (Spearman 0.965 between context size and
  mean top-1 score); per-context hybrid RRF scores all sit in 0.015-0.016.
  Neither carries a cross-context signal.
- Choosing contexts by card search misses the right one often: querying the
  top 5 discovered contexts gives R@10 0.63 (score) and 0.67 (grouped)
  instead of 0.80 and 0.93 with all 33. Cards were thin; this is a lower
  bound, but it confirms that discovered scope must be reported.
- The task prefixes of nomic did not change retrieval beyond noise; they
  improved card selection by a few points.

Threats: one corpus, one model, one known-item relevant document per query,
natural language against code, very unequal context sizes, a reranker not
trained on code.

## M2: portability and local materialization

Built: working sets (`barrel_ctx_ws`), quiesced export and import
(`barrel_ctx_export`, `barrel_ctx_manifest`), read-only opens in docdb,
vectordb and barrel, import identity through the TIMELINE sidecar
(`kind => import`), retrieved-set slices (`barrel_ctx_slice`), offline
coverage rules (`barrel_ctx_coverage`), and, on the integration branch,
working sets in the executor plus REST `/worksets*` and MCP
`context_attach`, `context_detach`, `context_materialize`,
`context_discover`.

Spike S3 (import identity): a naive copy under a new name loses every
document and local doc (keys embed the source name), gets an empty vector
store (its path is outside the database directory and unrecorded), and
reuses the source id when copied under the same name. The TIMELINE sidecar
with `kind => import` fixes all of it: keys keep the source keyspace, the
source id is fresh, there is no timeline parent. Restrictions: an import
cannot be branched, cannot take the source's own name, needs a keyprovider
that knows the source keyspace, and does not carry channel config.

Correctness: after export and import under a new name, plain and encrypted
databases give identical rows and scores for find, `vector_top_k` and
`bm25_top_k`; record mode matches exactly on find and vector and on ids for
BM25 (the import rebuilds BM25 in memory); the ANN graph loads
(`index_origin = loaded`) every time; writes to an import are refused; an
interrupted import never opens and resumes from verified files; slices
answer like the source restricted to their ids; over-budget operations write
nothing; attaching opens nothing.

S4 (5 runs each; medians):

| Apps | Vectors | Docs | Export ms | Bytes | Import ms | Cold open ms | Open without BM25 rebuild ms |
|---|---|---|---|---|---|---|---|
| 1 | yes | 104 | 21 | 6.1 MB | 23.5 | 327 | 88 |
| 10 | no | 695 | 37 | 30.9 MB | 37.5 | 80 | 85 |
| 10 | yes | 695 | 41 | 39.8 MB | 43.1 | 2693 | 90 |
| all | no | 1078 | 49 | 47.0 MB | 54.3 | 82 | 88 |
| all | yes | 1078 | 60 | 60.8 MB | 69.2 | 5780 | 96 |

Reading: moving a whole database is cheap at this size (whole-database
snapshots are an acceptable M2 package format: decision D6). The cost is the
in-memory BM25 rebuild at open, which dominates cold open (5.8 s for the full
corpus with vectors). Persisting BM25 correctly (the main bug above) removes
it.

## End-user run

`scripts/contexts-demo.sh` on the integration branch starts three
`barrel_server` nodes on loopback, loads one OTP application per node, and
drives node L over MCP JSON-RPC. It passed 25 of 25 checks on three
consecutive runs (two by the integration agent, one re-run for this
report): discover, inspect, ordered and BM25 queries across one local and
two remote contexts, working set creation, materializing a slice from a
remote context, a remote node stopped (partial coverage, no rows from the
failed source), offline mode (the slice answers as `retrieved_set`, the
remote member is `skipped_offline` and never contacted), snapshot import
answering offline as generation 1, rejections for `rrf` and a missing
LIMIT, and detach. Transcript:
`docs/experiments/contexts/demo-transcript.md`. User guide draft:
`docs/guides/contexts.md`. Both are on the integration branch.

## M3: ngram index packages

Built: segment sha256 in a v3 manifest, fsync of segments, manifests and
`corpus.meta` (with a small NIF for directory fsync), read errors
propagated, query leases on segment files, sealing into packages with
per-segment bloom filters, a publisher following the plan's section 6.3
(directory bucket and optional `barrel_ngram_s3` app, tested against MinIO),
`open_published` with exact pruning, a node-wide segment cache, and
`missing_segments` reporting.

Correctness: every complete published answer equals a brute-force scan
(ids and spans); a missing or corrupt segment always yields a partial
answer; the cache stays within budget under concurrent queries; a publisher
killed at any step leaves the previous or the new generation readable,
never a mix; GC keeps every referenced object.

S5, full corpus as one published corpus (27 segments; 1339.7 MB logical
index, 24.6 MB stored with gzip, which is what a full import transfers).
Latencies in ms; local = config A (full local index).

| Query | Local p50 | Published cold | Published warm p50 (20 % budget) | Bytes fetched | Segments |
|---|---|---|---|---|---|
| rare literal (`'AlgorithmInformation-2009'`) | 0.3 | 76-79 | 0.1 | 1.0 MB | 1/27 |
| absent literal | 0.2 | 0.0 | 0.0 | 0 | 0/27 |
| scoped literal (`-module(megaco`) | 2.1 | 391 | 2.4 | 4.3 MB | 4/27 |
| wide literal (`gen_server`) | 10.6 | 2585 | 2571 | 25.8 MB | 27/27 |
| wide regex (`[a-z_]+:start_link\(`) | 142 | 2735 | 2655 | 25.8 MB | 27/27 |

- Pruning is exact and effective for narrow queries: a rare literal fetches
  4 % of a full import and answers warm as fast as local.
- Wide queries select every segment. Because a segment is about 50 MB
  logical, no tested budget (up to 50 %, 670 MB) holds the corpus, so wide
  queries evict and refetch on every run and stay 20-250x slower than
  local. Cumulative bytes over the 12-query list are about 129-136 MB, 5x a
  full import, at every budget and for both the directory bucket and MinIO.
- The per-app layout (48 segments) prunes better for app-scoped queries but
  shows the same wide-query behaviour.

Reading: the plan's M3 threshold (under 25 % of full-import bytes at a 20 %
budget) is missed, and the cause is the segment format, not the package or
cache design. A compact gram directory (a segment format v5 that stores only
present grams) would shrink segments by one to two orders of magnitude at
this corpus size, which also fixes the local disk footprint on `main`. Range
reads are not the next step.

## What changes in the action plan

- **D4 resolved**: grouped by default; automatic score merge for
  `vector_top_k` when all members report the same fingerprint and cosine;
  interleave only as presentation (`relevance: false`); never raw BM25,
  hybrid, or RRF scores across contexts; rerank not offered until a
  code-aware reranker is validated.
- **D6 resolved**: whole-database snapshots are the M2 package format.
- **Budgets confirmed** as defaults (8 contexts, 4 parallel, 4 s per member,
  5 s per query); add connection reuse to remove the handshake round trip.
- **New prerequisite for M2**: fix disk BM25 persistence (a `main` bug).
- **M3 re-scoped**: segment format v5 (compact gram directory) comes before
  any further package work; the correctness fixes (leases, error
  propagation, checksums, fsync) should land on `main` now.
- **Hybrid retrieval across contexts** has no valid merge today. It would
  need shared BM25 statistics; it is new work, not in the backlog.
- **Discovery**: report skipped contexts whenever scope is discovered; card
  quality matters (thin cards lose about 0.17 R@10 at 5 contexts).
