# Changelog

All notable changes to the Barrel umbrella are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and each app
is versioned independently under [Semantic Versioning](https://semver.org/).

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
