# Barrel

Barrel is the umbrella repository for the Barrel data stack. It groups the
existing Barrel applications into one rebar3 umbrella so they can be developed
and released together, while each application keeps its own responsibility and
stays usable on its own.

This repository is an organization change. The imported applications keep their
public APIs, tests, examples, and documentation. No behavior is redesigned here.

## Applications

| App | Path | Responsibility | Notes |
|-----|------|----------------|-------|
| `barrel_docdb` | `apps/barrel_docdb` | Document metadata, MVCC, changes feed, replication primitives, manifests, catalog/indexing foundation. | Standalone embedded document database. |
| `barrel_vectordb` | `apps/barrel_vectordb` | Embedded vector database, local ANN indexes (HNSW/FAISS/DiskANN), BM25, and local hybrid search. | Standalone embedded vector database. |
| `barrel_embed` | `apps/barrel_embed` | Embedding generation across providers (local, Ollama, OpenAI, and more). | Used by `barrel_vectordb` for text and hybrid search. |
| `barrel_rerank` | `apps/barrel_rerank` | Cross-encoder reranking. | Optional, used by `barrel_vectordb`. |
| `barrel_faiss` | `apps/barrel_faiss` | Erlang NIF bindings for FAISS. | Optional. Needs the FAISS C++ library; excluded from the default build. |

Dependency direction (no cycles): `barrel_vectordb` depends on `barrel_embed`,
and optionally on `barrel_faiss` and `barrel_rerank`. `barrel_docdb`,
`barrel_embed`, `barrel_rerank`, and `barrel_faiss` depend only on external
packages.

## Build

You need Erlang/OTP, rebar3, CMake, and a C/C++ compiler (the `barrel_vectordb`
NIF is built with CMake).

```
rebar3 compile
```

This builds `barrel_docdb`, `barrel_vectordb`, `barrel_embed`, and
`barrel_rerank`. `barrel_faiss` is left out of the default build because it
needs the FAISS C++ library.

To include the FAISS app:

```
rebar3 as faiss compile
```

## Test

```
rebar3 as test eunit   # barrel_vectordb, barrel_embed, barrel_rerank
rebar3 as test ct      # barrel_docdb (Common Test)
```

## Development shell

```
rebar3 shell
```

Starts `barrel_docdb` and `barrel_vectordb`.

## Standalone use

Each application remains a valid OTP application with its public API unchanged.
You can keep using `barrel_vectordb` as an embedded vector database (including
`start_link/1`, `add/4`, `search/3`, `search_vector/3`, `search_bm25/3`,
`search_hybrid/3` with `barrel_embed` for text and hybrid modes) and
`barrel_docdb` as an embedded document database, without S3, remote replication,
or any cluster fabric.

## Documentation

- Architecture: [docs/architecture/overview.md](docs/architecture/overview.md)
- Planned responsibility moves: [docs/migration/responsibilities.md](docs/migration/responsibilities.md)
- Per-app docs live under each app's own `docs/` directory.

## Roadmap

Future applications will join this umbrella as the Barrel dataset fabric takes
shape:

- `barrel_object`: object storage abstraction for local disk, S3/MinIO, and
  remote nodes.
- `barrel_fabric`: dataset agents, durability, placement, replication
  orchestration, and query routing.
- `barrel_agents`: agent runtime built on the fabric.

## License

Each application is licensed under Apache-2.0. See the `LICENSE` file in each
app directory.
