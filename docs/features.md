# Features

What Barrel does today and what is planned, by capability. Read this to see
which features are usable now, which are opt-in, and which are still ahead. Each
row links to a guide where one exists.

## Status legend

- **Ready**: usable now, has tests.
- **Opt-in**: usable now, off by default or behind a build profile.
- **Planned**: not built yet.

## Data model

A barrel record is one id with three faces: a JSON document, its attachments
(blobs), and its vector. Documents live in `barrel_docdb`, vectors in
`barrel_vectordb`, blobs are document attachments. The `barrel` facade composes
them; each underlying app stays usable on its own.

## Capabilities

| Capability | Status | Notes |
|---|---|---|
| Documents (CRUD, MVCC, query) | Ready | `barrel_docdb`; `find/2` over the path index. See [embedding](guides/embedding.md). |
| Batches (`put_docs`/`get_docs`/`delete_docs`, `vector_add_batch`) | Ready | Per-element results, in order. |
| Attachments (blobs) | Ready | Document attachments; pluggable backend per db via `barrel_att_backend` (RocksDB BlobDB default). Streaming read/write. |
| Vector search | Ready | `barrel_vectordb`; HNSW by default. |
| BM25 keyword search | Opt-in | Enable with `bm25_backend => memory` (or `disk`) at open. |
| Hybrid search (vector + BM25) | Opt-in | Needs BM25 enabled and an embedder for text queries. |
| Embeddings | Opt-in | `barrel_embed` (local Python, Ollama, OpenAI, ...). Auto-embed and text hybrid need it. |
| Reranking | Opt-in | `barrel_rerank` (cross-encoder). |
| FAISS index backend | Opt-in | `barrel_faiss`; needs the FAISS C++ library, excluded from the default build (`rebar3 as faiss`). |
| Changes feed | Ready | `changes/2`, `subscribe/2`; HLC cursor via `hlc_encode/1`. |
| Synchronization / replication | Partial | Same-VM, documents only. See [synchronization](guides/synchronization.md). |
| REST/JSON server | Opt-in | `barrel_server` over `livery`, `rebar3 as server`. See [rest-server](guides/rest-server.md). |
| Encryption at rest | Planned | RocksDB 3.1.0 EncryptedEnv (whole-DB) + value encryption for blobs. |
| gRPC, HTTP/3, WebTransport, unix socket, OpenAPI | Planned | Phase 2 transports on `barrel_server`. |
| Network replication (HTTP/gRPC transport) | Planned | Phase 3; a `barrel_rep_transport` implementation. |
| SQL API, MCP tools, agentfs | Planned | Phase 5. |

## Where things run

- **Embedded**: depend on `barrel` (no transports pulled in). See [embedding](guides/embedding.md).
- **Server**: run `barrel_server` for HTTP. See [rest-server](guides/rest-server.md).
