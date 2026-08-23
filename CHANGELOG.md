# Changelog

All notable changes to the Barrel umbrella are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and each app
is versioned independently under [Semantic Versioning](https://semver.org/).

## [2026-08-23] barrel_vectordb store server without gen_batch_server

`barrel_vectordb`'s per-store process is a plain `gen_server` again. The only
thing `gen_batch_server` provided, merging the writes queued at the same time
into one RocksDB batch, is now done by the write handler itself, bounded by
`batch.max_batch_size`; reads are served in arrival order instead of waiting
for a whole batch. The library's prefix `catch` uses warned on every OTP 29
build, for the umbrella and for every Hex consumer.

| App | Version | Change |
|-----|---------|--------|
| barrel_vectordb | 2.3.0 | store server is a gen_server with write coalescing; gen_batch_server removed; requires OTP 26+ |
| barrel | 1.3.1 | requires barrel_vectordb 2.3 and OTP 26+ |

## [2026-08-23] Stray-message hardening and embed accessors

`barrel_vectordb`'s store server traps exits but only handled call ops, so
any `'EXIT'` or plain message reaching it killed the store; the managed-venv
pip install in `barrel_embed`, run inside the store at startup, left exactly
such an `'EXIT'` behind. The server now routes info/cast ops explicitly and
ignores them, and `barrel_embed` runs its venv shell commands in a throwaway
owner process so no port message can reach the caller (`barrel_rerank`'s venv
helper had the same leak and gets the same fix). `barrel_vectordb` also
declares `iommap` (disk BM25 / DiskANN) in `applications`. `barrel` gains
`embed/2`, `embed_batch/2` and `embedder_info/1` on the handle, so callers can
embed through the database's own embedder without reading handle fields. The
same `applications` audit caught `mimerl` in `barrel_docdb`, `crypto` in
`barrel_embed`, and `mimerl`/`livery` in `barrel_att_s3`.

| App | Version | Change |
|-----|---------|--------|
| barrel_vectordb | 2.2.1 | info/cast ops no longer crash the store; `iommap` in `applications` |
| barrel_embed | 2.3.2 | venv commands run their port in an owner process; no mailbox leak; `crypto` in `applications` |
| barrel | 1.3.0 | `embed/2`, `embed_batch/2`, `embedder_info/1` |
| barrel_docdb | 1.3.1 | `mimerl` in `applications` |
| barrel_att_s3 | 0.1.1 | `mimerl` and `livery` in `applications` |
| barrel_rerank | 1.0.2 | venv commands run their port in an owner process; no mailbox leak |

## [2026-08-15] barrel_ngram corpus lifecycle hardening

`barrel_ngram`'s `open/2`/`close/1` are now serialized per corpus by a
one-shot lifecycle coordinator, backed by a corpus-level config file
(database, shard count, tuning, postings codec) checked before any shard
starts and committed only once every shard is up -- a mismatched reopen is
now rejected instead of silently reindexing, rebinding to a different
database, or orphaning an old shard set on a shard-count change. A
database deleted and recreated under the same name is now detected both
at open and continuously on every shard resubscribe. The regex analyzer
fails closed on lazy/possessive quantifiers, PCRE control verbs, and
unrecognized escapes instead of mis-parsing them as literal text; a
positive `source`-verified match is now always re-confirmed against live
`barrel_docdb` content before being returned. `open/2` validates every
option up front, including the corpus name itself (closing a
path-traversal risk). `barrel_docdb` gains `db_instance_id/1`, the
read-only accessor this needed. `barrel_server`'s `ngram_search` MCP tool
no longer silently stops returning results for a corpus indexed before
this change -- it reindexes automatically on first use instead.

| App | Version | Change |
|-----|---------|--------|
| barrel_ngram | 0.9.0 | corpus lifecycle lock + corpus-level config; database-instance recreation detection; regex parser soundness; live re-confirmation of `source`-verified matches; full `open/2` option validation. Breaking: `close/1` now returns `ok \| {error, term()}`; a corpus indexed before this change needs a fresh `data_dir` |
| barrel_docdb | 1.3.0 | `db_instance_id/1` |
| barrel_server | 1.5.0 | `ngram_search` auto-reindexes a pre-0.9.0 corpus instead of silently going empty |

## [2026-08-09] S3-compatible attachment backend

New sibling app `barrel_att_s3` implements `barrel_att_backend` against
S3-compatible object stores (AWS S3, MinIO, Garage) via `livery_s3`, kept out
of the default embeddable build behind a new `s3` rebar3 profile. It covers
whole-blob and streaming put/get/delete, opt-in write-conflict detection
(`create_only`/`expected_etag`) with a real per-store capability probe, a
local attachment feed that makes S3-backed databases full bidirectional
replication participants, non-blocking eager-copy branching, and a background
sweeper that garbage-collects multipart uploads abandoned by a crash
mid-upload. Both backend selection and write-conflict detection are reachable
over `barrel_server`'s HTTP routes. `barrel_docdb` gains the pluggable-backend
resolution this needed, plus a fix so continuous/persistent replication tasks
notice attachment-only writes (they have their own feed, independent of the
doc changes feed). CI gains an `s3` leg against real MinIO and Garage
containers and an opt-in e2e job.

| App | Version | Change |
|-----|---------|--------|
| barrel_att_s3 | 0.1.0 | initial release -- S3-compatible attachment backend, HTTP surfacing, multipart-upload GC |
| barrel_docdb | 1.2.0 | pluggable attachment-backend resolution; continuous-replication attachment-phase fix; clean error branching a feedless backend |
| barrel_server | 1.4.0 | `att_opts` on `PUT /db/:name`; `If-Match`/`If-None-Match` wired to `create_only`/`expected_etag` |
| barrel | 1.2.0 | `put_attachment/5` |

## [2026-07-18] Code-review hardening + non-blocking database opens

A multi-dimension review (C NIFs, supervision, concurrency, resource leaks,
security) drove a round of fixes across the umbrella: the vector NIFs bounds-check
their inputs, replication no longer spins on a non-advancing source, compaction
and sweeps run off the database writer loop, and the sync surface closes a
signed-attachment forgery, an mTLS config gap, and a filter ReDoS. The database
lifecycle manager now opens databases in a worker off its message loop, with
request coalescing, so a cold or wedged open no longer blocks node-wide lifecycle
calls; this rides on a new `barrel:open` `store_supervised` option that parents
the vector store to a supervisor instead of the caller.

| App | Version | Change |
|-----|---------|--------|
| barrel | 1.1.0 | non-blocking `barrel_dbs` opens (worker + coalescing); `barrel:open` `store_supervised` option; reopen no longer leaks the vector store |
| barrel_docdb | 1.1.1 | replication no-progress guard + task lifecycle; compaction/retention/TTL off the writer loop; `fold_docs` honors `id_prefix`; wire-filter ReDoS bound; cursor snapshot handoff |
| barrel_server | 1.2.1 | signed-attachment body binding; mTLS `verify_peer` gate; wire-filter ReDoS + search `k` clamp; live-query bridge opens off its loop |
| barrel_vectordb | 2.2.0 | supervised store (`start_supervised`); batch-ADC NIF bounds checks; RocksDB batch/ETS/monitor cleanup |
| barrel_faiss | 1.0.1 | `search` caps `k` and allocates inside the try; resource re-open on upgrade; strict metric atoms |
| barrel_rerank | 1.0.1 | startup fails fast on a Python exit instead of hanging |
| barrel_embed | 2.3.1 | venv pip/rm commands bounded so a torch install is not killed at 60s |

## [2026-07-17] Sync-wire auth hardening + multi-protocol serving

`barrel_server` gains opt-in Ed25519 signed-request auth and an mTLS transport
gate for the replication wire (bearer stays the default and is unchanged), and
serves HTTP/1.1, HTTP/2, and HTTP/3 via a new `listeners` config. The signing
helpers ship in `barrel_docdb` (`barrel_sync_sig`), used by both the server
verifier and the replication client. H3 is TLS-serving but not yet a client-cert
gate; mapping a client cert to an identity needs a livery change and is deferred.

| App | Version | Change |
|-----|---------|--------|
| barrel_server | 1.2.0 | signed-request + mTLS auth (opt-in); H1/H2/H3 serving |
| barrel_docdb | 1.1.0 | `barrel_sync_sig`; transport signing + `ssl_options` |

## [2026-07-14] Embeddable server + GitHub CI

The umbrella is tagged `v1.1.0`. `barrel_server` gains an embeddable route API:
`barrel_server_api` exposes the REST/sync routes as a livery route list and
compiled router that a host livery application can mount under a sub-path and
guard with its own auth. The standalone server is unchanged. Umbrella CI now
runs on GitHub Actions (`.github/workflows/ci.yml`), mirroring the Forgejo
pipeline.

| App | Version | Change |
|-----|---------|--------|
| barrel_server | 1.1.0 | embeddable route API (`barrel_server_api`) |

`barrel` (the library, 1.0.1) and the other apps are unchanged.

## [2026-07-11] Dependency-metadata fix

Four packages are re-released to correct their declared Hex dependencies. The
first stable release published `barrel_spaces`, `barrel_server`, and
`barrel_vectordb` with sibling dependencies missing from the package metadata,
because those apps declared the siblings in a `hex` profile and `rebar3_hex`
builds a package's requirements only from the default-profile lock. A consumer
installing them would fail at runtime with an `undef`.

| App | Version | Fix |
|-----|---------|-----|
| barrel_vectordb | 2.1.2 | declare `barrel_embed` |
| barrel | 1.0.1 | repoint to `barrel_vectordb` 2.1.2; `~>` sibling pins |
| barrel_spaces | 1.0.1 | declare `barrel`, `barrel_docdb`, `barrel_crypto` |
| barrel_server | 1.0.1 | declare `barrel`, `barrel_spaces` |

Sibling dependencies now live in the default `deps`, matching `barrel`, which
published correctly. `barrel_crypto`, `barrel_embed`, `barrel_docdb`,
`barrel_rerank`, and `barrel_faiss` were already correct and are unchanged.

## [2026-07-10] First stable release

The 0.x apps move to 1.0.0: their APIs are frozen and will not break without a
major version. `barrel_vectordb` and `barrel_embed` keep their 2.x lines; they
were already past 1.0, and `barrel_embed` 2.2.1 is on Hex.

| App | Version |
|-----|---------|
| barrel | 1.0.0 |
| barrel_crypto | 1.0.0 |
| barrel_docdb | 1.0.0 |
| barrel_vectordb | 2.1.1 |
| barrel_embed | 2.3.0 |
| barrel_rerank | 1.0.0 |
| barrel_spaces | 1.0.0 |
| barrel_faiss | 1.0.0 |
| barrel_server | 1.0.0 |

### Changed (breaking)
- `barrel_docdb:query/2,3` now returns `{error, {table_fn_requires_barrel, Fn}}`
  where it returned `{error, {table_fn_requires_facade, Fn}}`. Taken before 1.0
  because the atom appears in no published release; after 1.0 it would cost a
  major.

### Fixed
- `barrel_vectordb_docdb_backend:init/2` raised `badarg` on every start: it
  called `atom_to_binary/2` on a store name the store had already normalised to
  a binary, and `maps:get/3` evaluates its default eagerly. The `docstore` seam
  was unusable.
- Hex packages declared no sibling dependencies. `rebar3_hex` builds a package's
  requirements from `rebar.lock`, so a dep resolved through `_checkouts` never
  reached the tarball. `barrel_spaces` and `barrel_vectordb` did not declare
  theirs at all. `scripts/check_hex_requirements.py` now gates a publish.

### Added
- `examples/agent_layer.erl`, run by a test suite so it cannot drift.

### Testing and tooling
- Docker end-to-end replication test (`test/e2e/`): two `barrel_server`
  containers replicate over a real network, asserting push/pull convergence and
  delete propagation. Opt-in CI job (`workflow_dispatch`).
- The `barrel_docdb` benchmark runner no longer depends on the archived per-app
  git repository; it builds against the umbrella. A reference run is recorded in
  `test/e2e/BENCHMARKS.md`.

## [2026-07-08] First coordinated tagged release

First tagged release of every app in the umbrella. Versions:

| App | Version |
|-----|---------|
| barrel | 0.2.0 |
| barrel_crypto | 0.3.0 |
| barrel_docdb | 0.9.0 |
| barrel_vectordb | 2.1.0 |
| barrel_embed | 2.3.0 |
| barrel_rerank | 0.2.0 |
| barrel_spaces | 0.2.0 |
| barrel_faiss | 0.3.0 |
| barrel_server | 0.2.0 |

### Changed
- Documentation refreshed for the current app set: the umbrella README and
  architecture overview cover all nine apps; the barrel_docdb doc set drops the
  removed rev-tree model and its `put_rev`/`revsdiff` functions in favor of the
  HLC version-vector protocol; guides fix "later phase" notes for shipped
  features (browser vector search, continuous SSE, wire replication).
- Package `links` in every app point at the umbrella repository.
- Added missing `LICENSE` and `CHANGELOG` files across apps.

### Testing
- CI now runs the full `barrel_server` suite set (REST, sync/replication,
  convergence, attachments, auth, spaces, CORS, audit, encryption, timeline,
  MCP), the FAISS suite, and the backend-free `barrel_embed` request tests,
  which previously only compiled.
- New unit tests: embedding-provider request building (openai, cohere) and the
  rerank sidecar response decoder.
