# Barrel

Barrel is an embeddable edge-AI database for Erlang: documents, their
attachments (blobs), and their vectors under one id, with local vector and
keyword search, encryption at rest, branching/PITR, replication, and an agent
layer. This umbrella groups the applications so they develop and release
together, while each stays a standalone OTP application with its own public API.

## Applications

| App | Path | Responsibility |
|-----|------|----------------|
| `barrel` | `apps/barrel` | The embeddable database. Composes docdb + vectordb + crypto so a document, its blobs, and its vector share one id. Record mode, timeline (branch/PITR/merge), BQL. Pulls no transports. |
| `barrel_docdb` | `apps/barrel_docdb` | The document layer. HLC version-vector MVCC, changes feed, replication, attachments (blobs), TTL, retained history, BQL query. Standalone embedded document database. |
| `barrel_vectordb` | `apps/barrel_vectordb` | The vector layer. Local ANN indexes (HNSW, DiskANN, FAISS), BM25, hybrid search, quantization. Standalone embedded vector database. |
| `barrel_embed` | `apps/barrel_embed` | Embedding generation across providers (local Python, Ollama, OpenAI, and more). Used by `barrel_vectordb` for text and hybrid search. |
| `barrel_rerank` | `apps/barrel_rerank` | Cross-encoder reranking. Optional, used by `barrel_vectordb`. |
| `barrel_crypto` | `apps/barrel_crypto` | Encryption-at-rest primitives: AES-256-GCM envelope, offset-addressable CTR, HKDF key derivation, key providers. |
| `barrel_spaces` | `apps/barrel_spaces` | The agent layer: spaces (shared context databases), capability tokens, sessions with TTL, and handoffs. |
| `barrel_server` | `apps/barrel_server` | The network server. Exposes `barrel` over HTTP (REST/JSON) and MCP using `livery`. Opt-in behind the `server` profile. |
| `barrel_faiss` | `apps/barrel_faiss` | Erlang NIF bindings for FAISS. Optional; needs the FAISS C++ library, so it is excluded from the default build. |

Dependency direction (no cycles): `barrel_server` -> {`barrel`, `barrel_spaces`};
`barrel_spaces` -> {`barrel`, `barrel_docdb`, `barrel_crypto`}; `barrel` ->
{`barrel_docdb`, `barrel_vectordb`, `barrel_crypto`}; `barrel_vectordb` ->
{`barrel_embed`, `barrel_crypto`} (optionally `barrel_faiss`, `barrel_rerank`);
`barrel_docdb` -> `barrel_crypto`. Leaves: `barrel_crypto`, `barrel_embed`,
`barrel_rerank`, `barrel_faiss`.

## Build

You need Erlang/OTP, rebar3, CMake, and a C/C++ compiler (the `barrel_vectordb`
NIF is built with CMake).

```
rebar3 compile
```

This builds the default app set (`barrel_crypto`, `barrel_docdb`,
`barrel_vectordb`, `barrel_embed`, `barrel_rerank`, `barrel`, `barrel_spaces`).
`barrel_faiss` and `barrel_server` are opt-in:

```
rebar3 as faiss compile      # add the FAISS NIF app
rebar3 as server compile     # add the HTTP/MCP server
```

## Test

```
rebar3 eunit             # unit tests
rebar3 ct                # Common Test suites
rebar3 as server ct      # the HTTP/MCP server (opt-in profile)
```

## Development shell

```
rebar3 shell             # starts the embeddable stack
rebar3 as server shell   # starts barrel_server
```

## Standalone use

Each application stays a valid OTP application usable on its own. You can embed
`barrel` (the full database) directly with no transports, or use `barrel_docdb` and
`barrel_vectordb` as standalone embedded databases, without a server, remote
replication, or any cluster fabric.

## Documentation

- Docs index: [docs/README.md](docs/README.md)
- Feature status: [docs/features.md](docs/features.md)
- Architecture: [docs/architecture/overview.md](docs/architecture/overview.md)
  and [docs/architecture/vision.md](docs/architecture/vision.md)
- Guides live under [docs/guides/](docs/guides/); per-app docs under each app.
- Distributing the apps (Hex + git mirrors):
  [docs/guides/distributing-apps.md](docs/guides/distributing-apps.md)

## Roadmap

- `barrel_fabric` (future): dataset agents, durability, placement, replication
  orchestration, and query routing across nodes.

Products that consume the stack (for example `barrel_memory`) stay in their own
repositories rather than inside this umbrella.

## License

Apache-2.0. See the `LICENSE` file at the repository root and in each app
directory.
