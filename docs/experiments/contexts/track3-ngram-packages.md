# Track 3: ngram index packages (M3, B22-B25, spike S5)

Branch `exp/ngram-packages`, based on `docs/contexts-action-plan` (9bbb51d2).
Local commits only; nothing pushed, no PR.

## What was built

| Item | Status | Where |
|---|---|---|
| Failing tests first | done, then fixed | `barrel_ngram_integrity_SUITE` (b6403eca) |
| B22 segment integrity | done | `barrel_ngram_segment`, `_manifest`, `_corpus_config`, `_query`, `_planner`, `_merge`, new `barrel_ngram_fs` + `c_src/barrel_ngram_fs_nif.c` |
| B23 segment leases | done | `barrel_ngram_shard` (`lease_snapshot`, `release/2`), `barrel_ngram_query` |
| B24 seal and package | done | `barrel_ngram_package`, `barrel_ngram_bloom`, `barrel_ngram_merge:merge_split/3` |
| Publisher (plan 6.3) | done | `barrel_ngram_publish`, `barrel_ngram_bucket` behaviour, `barrel_ngram_bucket_dir` |
| B25 fetch seam + published mode | done | `barrel_ngram_fetch` behaviour, `barrel_ngram_cache`, `barrel_ngram_cache_sup`, `barrel_ngram_published`, `barrel_ngram_published_sup`, `barrel_ngram:open_published/2` |
| S3 bucket | done | new app `apps/barrel_ngram_s3` (profiles `s3`, `s3_server` only) |
| S5 bench | script committed, smoke run only | `scripts/bench-ngram-packages.sh`, `apps/barrel_ngram/test/barrel_ngram_bench_packages.erl` |
| B26 (executor / MCP surface) | not started | out of scope for this track |

### Commits mapped to items

| Commit | Item |
|---|---|
| b6403eca | failing tests: compaction race (enoent), swallowed read errors |
| b3c03d76 | B22 |
| e709dafb | B23 |
| 632c79c0 | B22 follow-up: declare `crypto` in the app file |
| f20521da | B24 |
| 02c843db | B24 fix: per-segment watermark (content addressing) |
| 46c40827 | B25 seams: `barrel_ngram_fetch`, `barrel_ngram_bucket`, directory bucket |
| 1dff3f10 | publisher and GC (6.3) |
| bf1e34f3 | B25 published mode, cache, `open_published/2` |
| f5fa6066 | S3 bucket app |
| bd82ebe3 | B25: named shared segment cache; sparse-write speedup; dialyzer spec |
| db86d42e | S5 bench |
| 5e07a695 | publisher/reader: errors instead of crashes on a bad package or root |

## Bugs reproduced and fixed

1. **Query racing a compaction fails with `enoent`.** The query snapshot returned
   segment paths; a compaction committed between the snapshot and the first open deleted
   them. Reproduced deterministically (meck runs `compact/1` right before the query's
   first `segment:open/1`): `search` and `regex` both returned `{error, enoent}`.
   Fix (B23): `lease_snapshot` monitors the query process and records its files;
   `apply_merge_result` commits the manifest at once but keeps leased inputs in a
   `doomed` list, deleted when the last lease ends (`release/2` or the query dies).
2. **Read errors read as "no match" (false negatives).** Reproduced by handing the query
   a segment handle whose fd is closed: a literal search returned `{ok, []}` (posting
   read), a 2-byte literal returned `{ok, []}` (key read). Found while fixing: the
   planner's phase-2 `pair/5` swallowed errors too, and an offset-table `eof` read as an
   absent gram, so a truncated segment silently lost matches. Fix (B22): `keys/2`,
   `entries/1`, `all_postings/1` return `{ok, _} | {error, _}`, short reads are errors,
   the query path throws inside a per-segment wrapper and returns
   `{error, {segment_read_failed, Path, Reason}}`.
3. **Merge read failure crashed the shard.** `all_postings/1` used `{ok, _} = pread`,
   and a synchronous `compact/1` runs the merge in the shard process. Now
   `{error, {merge_read_failed, _}}`.

All of these have regression cases in `barrel_ngram_integrity_SUITE` (10 cases).

## Format changes and compatibility

- **Manifest version 2 -> 3** (per shard). Each segment entry gains `sha256` and `bytes`,
  recorded at freeze and merge. A v2 manifest fails open with
  `{unsupported_manifest_version, 2, 3}`, the same path as earlier bumps:
  `on_legacy => reindex` rebuilds it, otherwise `delete_corpus` + `open`. Existing
  corpora must be rebuilt once.
- **Segment format unchanged (v4)**, the reader is unchanged except that `open/1` now
  checks the header layout (contiguous regions) against the file size, so a truncated or
  extended file is `{corrupt_segment, {size_mismatch, Expected, Got}}` instead of
  reading holes as absent grams.
- `open/2` option `verify_segments => checksum` (default) hashes every segment at corpus
  open; `layout` checks only header and size. Checksum cost is a full read of every
  segment at open (see surprises: segments are tens of MB each).
- Durability: segment, manifest and `corpus.meta` are fsynced before rename and the
  directory after. Erlang cannot open a directory, so a small NIF library
  (`barrel_ngram_fs_nif.so`, built by the existing CMake step) does the directory fsync.
- New package documents, all `format: 1`: `package.json` (root draft), `manifest_part`
  kind `index` (artifacts with `sha256`, `bytes`, `encoding`, `raw_sha256`,
  `raw_bytes`, `sparse_bytes`, `docs`, `shard`, `first_key`/`last_key`, and
  `summary.gram_filter` as `{kind: bloom, hash: fmix32-double, bits, hashes, b64}`),
  `index_dataset` (plan 3.4), root `generations/<n>.json` (`snapshot_manifest`, plan
  3.3) and `latest.json`. The bloom hash is specified in `barrel_ngram_bloom`'s
  moduledoc so another reader can evaluate it.
- API additions only; `barrel_ngram_segment:write/2`, `keys/2`, `entries/1`,
  `all_postings/1`, `all_positional_postings/1` and `barrel_ngram_merge:merge/2` changed
  return shapes (internal modules; tests updated).

## Deviations and why

- **Artifacts are gzip-encoded by default.** A v4 segment carries a direct-addressed
  offset table up to its highest gram: 48-64 MB for any segment holding UTF-8 text, even
  with 25 documents. Gzip makes the wire size about 1 MB. The part records both stored
  and raw sizes and checksums; the reader verifies both.
- **Cache budget counts logical (raw) bytes.** The cache writes segments sparse, but APFS
  mostly allocates seek-then-write gaps (measured on a 64 MB file: 4 KiB, 1 MiB and
  8 MiB gaps fully or nearly fully allocated, 64 KiB gaps about 60%), so counting sparse
  bytes would exceed the budget on disk on macOS.
  Logical accounting never exceeds it anywhere. `sparse_bytes` stays in the part as
  information.
- **Sealed segments get their own watermark** (newest HLC among their documents)
  instead of the corpus-wide one, so an unchanged key range seals to a byte-identical
  artifact and the next generation reuses it (verified by `seal_is_deterministic` and
  `next_generation_reuses_objects`).
- **Seal compacts by rewriting**, not by `compact/1`: each shard's leased snapshot is
  fully merged and split into key-range segments of `segment_docs` documents (bounded
  count, plus key-range locality that makes pruning work for app-scoped queries).
- **Quiescence** is checked, not enforced: last change HLC before and after the seal
  (paging `get_changes`; `descending` only reverses a page, it does not return the last
  change), plus an empty shard buffer after `refresh`. A write in between fails the
  seal with `{not_quiesced, Before, After}`.
- **Coverage** compares the indexed key set to the source's live ids (set equality, not
  only counts); `unindexed` lists at most 1000 ids plus `unindexed_count`, and
  `stale_entries` counts indexed keys absent from the source.
- **Confirmation uses a caller-supplied local database** of the same generation. The
  `_bulk_get` remote confirmation in B25's text depends on B18 and is not built.
- **Re-running `publish/3`** after a crash past the root write reuses the newest
  generation when its parts, sources and context equal the package's, then rewrites
  `latest.json`. Without create-only support (Garage) the publisher HEADs right before
  writing and refuses an existing root; a race inside that window is not detectable, as
  the plan says.
- **Readers need `latest.json` or an explicit generation**: the fetch seam has no list
  operation (the plan's `fetch(Key, DestPath, Opts)`).
- **A named, shared segment cache** (`start_segment_cache/2`, `cache => Name`) was added
  next to the private per-corpus cache: the plan's budget table has one segment cache
  budget per node, and per-corpus budgets made the per-app layout unusable.
- **`barrel_ngram:search/3` and `regex/3` also accept a published name**, returning
  `{error, {missing_segments, _}}` when incomplete, so an integrator can switch without
  new calls. `search_published/3` / `regex_published/3` return partial hits and stats.
- **S3 lives in a separate app** (`apps/barrel_ngram_s3`, deps `barrel_ngram` +
  `livery_s3`), listed only in the `s3` and `s3_server` profiles, following the
  `barrel_att_s3` precedent. A profile-only module inside `barrel_ngram` would make its
  xref and dialyzer depend on the profile and its Hex package unbuildable standalone.

## Surprises

- **Segment size is dominated by the offset table.** 25-30 source files give a 48-64 MB
  segment (30 726 distinct grams, table up to gram `E2 80 AF`). At 64 KiB granularity
  only 20 MB is non-zero, at 4 KiB 8.5 MB, gzip 0.8 MB. The design doc relies on
  filesystem compression for the zeros; APFS has none and does not create holes for
  small gaps. This decides the S5 outcome more than latency does (below).
- The live index pays the same cost: a 150-document local corpus is about 68 MB on disk.
- `get_changes(..., #{descending => true, limit => 1})` returns the first change, not the
  last (it reverses the page).
- Indexing is slow relative to the rest: `refresh` of 300 OTP modules took about 13 s
  (existing freeze path, unchanged here).
- Copying `_build` into a worktree keeps absolute paths in dependency CMake caches;
  `rebar3 as s3,test` then tries to rebuild rocksdb and iommap and fails until the cache
  paths are rewritten (environment only, not a code issue).

## Tests

Commands (umbrella root):

```
rebar3 eunit --app barrel_ngram
rebar3 ct --dir apps/barrel_ngram/test
rebar3 as s3 ct --dir apps/barrel_ngram_s3/test      # MinIO; skips when unreachable
rebar3 xref
rebar3 dialyzer
```

New suites and modules:

- `barrel_ngram_integrity_SUITE` (10): compaction race for search and regex, posting and
  key read errors, checksums in the manifest after freeze and merge, bit-flip corrupt
  segment fails open, truncated segment fails open (layout mode) and fails a query,
  v2 manifest rejected then rebuilt with `on_legacy => reindex`, leases pin files until
  released or the owner dies.
- `barrel_ngram_package_SUITE` (6): exact coverage, sharded seal, updates and deletes,
  deterministic artifacts, not-quiesced detection, closed corpus.
- `barrel_ngram_publish_SUITE` (7): first generation, idempotent re-run, object reuse,
  kill before every write (bucket always holds complete generations, `latest.json`
  names one, re-run finishes), concurrent root rejected (create-only and HEAD-check
  paths), GC keeps every referenced object and in-grace uploads, `retain_days`.
- `barrel_ngram_published_SUITE` (21, groups `single` and `sharded` x 10, plus 2):
  literals and regexes equal a brute-force scan (ids and spans), caseless equals the
  live corpus, exact pruning, missing object, corrupt object, cache budget respected
  under eviction, budget below one segment, 4 concurrent queries at a one-segment
  budget, shared cache, and a reader after a publisher killed before each write gets
  generation 0 or 1 with answers equal to that generation's documents.
- eunit: `barrel_ngram_bloom_tests` (no false negatives, ~1% FP at 10 bits/gram),
  `barrel_ngram_cache_tests` (LRU, pins, too_large, cache_full, waiters, dead fetcher,
  dead pin holder, adoption after restart), `barrel_ngram_fs_tests`.
- `barrel_ngram_s3_SUITE` (4, MinIO under `ngram-pkg-<rand>/`, prefix removed after):
  publish + idempotent re-run + exact queries, create-only root, missing object, GC.

Results at the last commit:

- `rebar3 eunit --app barrel_ngram`: 216 passed.
- `rebar3 ct --dir apps/barrel_ngram/test`: 154 passed (all pre-existing suites
  included); publish and published suites re-run after the last fix: 29 passed.
- `rebar3 as s3 ct --dir apps/barrel_ngram_s3/test`: 4 passed against MinIO; with
  `NGRAM_S3_ENDPOINT=http://127.0.0.1:1` the suite is skipped.
- `rebar3 xref`: no warning in `barrel_ngram` or `barrel_ngram_s3`.
- `rebar3 dialyzer` and `rebar3 as s3 dialyzer`: 11 warnings, all pre-existing on main
  (`barrel_att_store_none`, `barrel_vectordb_index_faiss`), none in `barrel_ngram` or
  `barrel_ngram_s3`.

## S5 smoke numbers (non-authoritative)

`scripts/bench-ngram-packages.sh --smoke` on the first 150 documents of the shared OTP
corpus (6.1 MB of text, 5 apps), `segment_docs` 25, directory bucket with 20 ms per
request, and MinIO on localhost. Other tracks were building at the same time, so
latencies are noise; bytes and pruning ratios are deterministic.

Index: single corpus 6 artifacts, 294 MB logical, 4.5 MB stored (what a full import
transfers). Per-app: 8 artifacts, 360 MB logical, 4.6 MB stored.

| Query (single corpus) | segments selected | bytes fetched (cold) | vs full import |
|---|---|---|---|
| rare module name (x3) | 1-2 of 6 | 0.60-1.36 MB | 13-30% |
| absent identifier | 0 of 6 | 0 | 0% |
| app-scoped literal / regex | 6 of 6 | 4.5 MB | 100% |
| `-module(`, `gen_server`, `ok`, broad regexes | 6 of 6 | 4.5 MB | 100% |

Per-app layout: a scoped query goes to one app's package (2 of 2 segments, 1.27 MB,
27%); rare literals select 1 of 8 (0.57-0.70 MB, 12-15%).

Cache budget (logical bytes, shared cache):

- 5% (15-18 MB): smaller than any segment, every selected segment is `too_large`; the
  answer is reported partial, never as complete.
- 20%: single corpus holds one segment; some segments are larger than the budget
  (missing, reported). Per-app: all answers complete and exact.
- 50%: all answers complete and exact, but wide queries evict and refetch on every
  run, so warm equals cold.
- Cumulative bytes over the 12-query list: 26-36 MB at 20-50% versus 4.5 MB for a
  full import (6-8x), because of that refetching.

Every complete answer in every configuration equalled the brute-force oracle; every
incomplete one was flagged with its missing segments. Latency (smoke, noisy): local
corpus 1-20 ms; published narrow queries 45-130 ms cold and under 1 ms warm; wide
queries 0.25-0.7 s per run, dominated by gunzip and writing 50 MB logical segments.

Reading: with v4 segments, pruning works (rare and absent literals touch 0-30% of the
bytes) but the cache cannot hold a useful working set at 5-20% because each segment is
tens of MB of mostly empty offset table. The plan's threshold ("at a 20% budget,
transfer < 25% of a full import over the list") is missed by a wide margin in this
smoke. A compact gram directory (sorted grams + offsets, binary searched) would shrink
logical segment size about 30-60x and is the first thing to try before range reads.

## Remaining work before PR-ready

- Split into PRs: B22+B23 (with the failing tests), B24, publisher + seams, B25,
  `barrel_ngram_s3`, bench. Version bumps (barrel_ngram 0.11.0 for manifest v3).
- A full S5 campaign on the whole corpus (both layouts, 5 warm runs) on a quiet machine.
- Compact gram directory (segment v5) or a cache that keeps artifacts compressed with a
  reader that decompresses on demand; either changes the segment reader.
- Seal and merge load whole shards in memory; streaming merge for large shards.
- S3 GET is not streamed (whole object in memory); fine for gzip artifacts of a few MB.
- Cache adoption at start hashes every cached file; a persisted index would avoid it.
- CI: a leaf job for `barrel_ngram_s3` with MinIO; the `barrel_ngram` leaf job builds
  the new NIF library through the existing CMake step.
- `_bulk_get` confirmation against a remote location (needs B18), B26 executor/MCP
  surface.
- Review `verify_segments => checksum` as the default: it reads every segment at open.

## Bench command

```
scripts/bench-ngram-packages.sh [--smoke] CORPUS.jsonl [OUT.jsonl]
```

It builds with `rebar3 as s3,test` when MinIO answers (`NGRAM_S3_ENDPOINT`, default
`http://127.0.0.1:19000`; `NGRAM_S3=off` to skip S3), else `rebar3 as test`, then runs
`barrel_ngram_bench_packages:run/1`. Results are JSON lines (one per query and
configuration, plus index and cumulative rows); a table is printed as it runs. It
refuses a corpus under `~/Projects`. The S3 run uses a random `ngram-bench-<rand>/`
prefix and deletes it.
