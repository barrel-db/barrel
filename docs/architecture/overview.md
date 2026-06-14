# Architecture overview

This note describes how the Barrel umbrella is split today and where it is
headed. Read it when you need to know which application owns a responsibility,
or before adding a new application to the umbrella.

## Current applications

Barrel is a rebar3 umbrella. Each application is a separate OTP application with
a clear responsibility and its own public API.

- **barrel_docdb**: the document layer. Document metadata, MVCC, the changes
  feed, replication primitives, manifests, and the catalog/indexing foundation.
  Usable standalone as an embedded document database.
- **barrel_vectordb**: the vector layer. The embedded vector database, local ANN
  indexes (HNSW, FAISS, DiskANN), BM25, and the local hybrid search that
  combines them. Usable standalone as an embedded vector database.
- **barrel_embed**: embedding generation across providers (local Python,
  Ollama, OpenAI, and others). `barrel_vectordb` uses it for text and hybrid
  search.
- **barrel_rerank**: cross-encoder reranking. Optional, used by
  `barrel_vectordb`.
- **barrel_faiss**: Erlang NIF bindings for FAISS. Optional backend for
  `barrel_vectordb`. Needs the FAISS C++ library, so it is excluded from the
  default build.

## Intended split

The longer-term shape of the stack separates four concerns:

- **docdb** (exists): the source of truth for document metadata and the
  catalog. MVCC, changes, and replication primitives.
- **vectordb** (exists): vector segments and local vector/text/hybrid search.
- **object** (future, `barrel_object`): object storage abstraction. Local disk,
  S3/MinIO, and remote nodes. Owns object bytes.
- **fabric** (future, `barrel_fabric`): dataset agents, durability, placement,
  replication orchestration, and query routing across nodes.

A separate `barrel_agents` application is expected to build on the fabric later.

## Boundary rules

These rules keep the umbrella maintainable as it grows:

- Clear OTP application boundaries. Each app owns its modules and public API.
- No circular dependencies between apps. The current direction is
  `barrel_vectordb` -> `barrel_embed` (and optionally `barrel_faiss`,
  `barrel_rerank`); the other apps are leaves.
- Prefer per-app modules over shared global modules. Avoid a common "junk
  drawer" application.
- Local use needs no external services. S3, remote replication, and the fabric
  agents are never required to run an app locally.
- Optional heavy backends (for example FAISS) stay opt-in and out of the
  default build.

## Build layout

- `apps/` holds the applications.
- The top-level `rebar.config` coordinates the build and aggregates each app's
  declared dependencies. It restricts the default app set so the FAISS app is
  opt-in (`rebar3 as faiss compile`).
- `rel/` holds the umbrella release skeleton.
- `docs/` holds umbrella-level documentation; per-app docs stay under each app.
