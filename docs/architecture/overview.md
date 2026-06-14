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

## Scope and rationale

This umbrella holds the Barrel data-stack libraries only: `barrel_docdb`,
`barrel_vectordb`, `barrel_embed`, `barrel_rerank`, and `barrel_faiss`. Products
and clients that build on top of the stack stay in their own repositories.

### What the umbrella buys you

- Version coordination: one resolved dependency set across the apps. It already
  forced a single `hackney` (4.4.0) and `meck` (1.2.0) where the separate repos
  had drifted, and surfaced the OTP 29 prefix-`catch` fix in one pass.
- Atomic cross-app changes: `barrel_vectordb` and its backends (`barrel_embed`,
  `barrel_rerank`, `barrel_faiss`) co-evolve. A change to the vectordb API plus
  its adapters is one commit and one CI run, not tag-and-chase across repos.
- One build, test, and release surface: shared profiles (the FAISS opt-in), one
  PLT, one `rel/`.
- A staging ground for the fabric: `barrel_object` and `barrel_fabric` are meant
  to span docdb and vectordb, and a layer that unifies two apps needs a repo
  that holds both. `barrel_docdb` shares no code with `barrel_vectordb` today, so
  its place here is forward-looking, justified by that fabric work.

### Out of scope (kept separate on purpose)

- `barrel_memory` and `barrellm`: application/service layers that consume the
  stack (`barrel_memory` -> `barrel_vectordb`; `barrellm` -> `barrel_docdb` +
  `barrel_embed`). They have their own HTTP/MCP surfaces, release cadence, and
  much older pinned deps (for example rocksdb 2.5.0, hackney 1.20.1/2.0.1).
  Folding them in would invert the layering and drag the stack back to older
  dependencies.
- `barrel_memory_macos`: a native macOS (Swift) client. It talks to the memory
  service over HTTP and cannot be a rebar3 OTP application.
- `barrel_llm`: an empty placeholder directory. Nothing to import.

### If products need co-development later

Keep them as separate repos that depend on the stack, via path deps for local
work (`{path, "../barrel/apps/barrel_vectordb"}`) or the published packages. If
they grow, group them in their own applications umbrella (for example
`barrel_apps`) that depends on this one, rather than flattening libraries and
products together.

## Build layout

- `apps/` holds the applications.
- The top-level `rebar.config` coordinates the build and aggregates each app's
  declared dependencies. It restricts the default app set so the FAISS app is
  opt-in (`rebar3 as faiss compile`).
- `rel/` holds the umbrella release skeleton.
- `docs/` holds umbrella-level documentation; per-app docs stay under each app.
