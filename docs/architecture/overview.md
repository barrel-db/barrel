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
- **barrel_crypto**: encryption-at-rest primitives. AES-256-GCM envelope,
  offset-addressable CTR, HKDF key derivation, and pluggable key providers. Used
  by docdb and vectordb for encrypted databases.
- **barrel**: the embeddable database. Composes `barrel_docdb`,
  `barrel_vectordb`, and `barrel_crypto` so a document, its attachments (blobs),
  and its vector share one id. Adds record mode and the timeline
  (branch/PITR/merge). Pulls no transports. The hex "edge" package.
- **barrel_spaces**: the agent layer. Spaces (shared context databases),
  capability tokens, sessions with TTL, and handoffs, built on `barrel`.
- **barrel_server**: the network server. Exposes `barrel` and `barrel_spaces`
  over HTTP (REST/JSON) and MCP using `livery`. Opt-in behind the `server`
  profile; not part of the default embeddable build.

## Intended split

The longer-term shape of the stack separates these concerns:

- **docdb** (exists): the source of truth for document metadata and the
  catalog. MVCC, changes, replication primitives, and attachment (blob) storage.
- **vectordb** (exists): vector segments and local vector/text/hybrid search.
- **barrel** (exists): composes docdb and vectordb under one id space.
- **server** (exists, `barrel_server`): transports over the `barrel` API.
- **object storage**: blobs are docdb attachments with a pluggable backend per
  database (`barrel_att_backend`: RocksDB BlobDB today; local filesystem and S3
  planned as additional backends). There is no separate object-store app; a
  document owns its blob, matching how vectors and metadata attach to the same id.
- **agent layer** (exists, `barrel_spaces` + MCP): spaces, capability tokens,
  sessions, and handoffs over the `barrel` API; exposed through `barrel_server`.
- **fabric** (future, `barrel_fabric`): dataset agents, durability, placement,
  replication orchestration, and query routing across nodes.

## Boundary rules

These rules keep the umbrella maintainable as it grows:

- Clear OTP application boundaries. Each app owns its modules and public API.
- No circular dependencies between apps. The direction is
  `barrel_server` -> `barrel` -> {`barrel_docdb`, `barrel_vectordb`}, and
  `barrel_vectordb` -> `barrel_embed` (and optionally `barrel_faiss`,
  `barrel_rerank`). `barrel_docdb` and `barrel_vectordb` are leaves;
  `barrel_docdb` -> `barrel_vectordb` is forbidden.
- Prefer per-app modules over shared global modules. Avoid a common "junk
  drawer" application.
- Local use needs no external services. S3, remote replication, and the fabric
  agents are never required to run an app locally.
- Optional heavy backends (for example FAISS) stay opt-in and out of the
  default build.

## Scope and rationale

This umbrella holds the Barrel data-stack libraries (`barrel_docdb`,
`barrel_vectordb`, `barrel_embed`, `barrel_rerank`, `barrel_faiss`), the
embeddable database (`barrel`), and the network server (`barrel_server`). Products
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
- A home for the layers that span the stack: `barrel` (the full database) already
  unifies docdb and vectordb under one id, and `barrel_fabric` (future) will span
  them across nodes. A layer that unifies two apps needs a repo that holds both.
  `barrel_docdb` shares no code with `barrel_vectordb`, so its place here is
  justified by `barrel` and the coming fabric work.

### Out of scope (kept separate on purpose)

- `barrellm`: an application/service layer that consumes the stack
  (`barrellm` -> `barrel_docdb` + `barrel_embed`). It has its own HTTP/MCP
  surface, release cadence, and much older pinned deps (for example
  rocksdb 2.5.0, hackney 1.20.1/2.0.1). Folding it in would invert the
  layering and drag the stack back to older dependencies.
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
