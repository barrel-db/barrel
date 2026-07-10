# Changelog

All notable changes to the Barrel umbrella are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and each app
is versioned independently under [Semantic Versioning](https://semver.org/).

## [2026-07-10] First stable release

The 0.x apps move to 1.0.0: their APIs are frozen and will not break without a
major version. `barrel_vectordb` and `barrel_embed` keep their 2.x lines; they
were already past 1.0, and `barrel_embed` 2.2.1 is on Hex.

| App | Version |
|-----|---------|
| barrel | 1.0.0 |
| barrel_crypto | 1.0.0 |
| barrel_docdb | 1.0.0 |
| barrel_vectordb | 2.1.0 |
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
