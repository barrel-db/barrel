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
| Record mode (policy-driven vector indexing) | Ready | Documents auto-embed per policy; async (healed) or sync (read-your-write); explicit vectors via put option. See [record-mode](guides/record-mode.md). |
| BM25 keyword search | Opt-in | Enable with `bm25_backend => memory` (or `disk`) at open; disk by default in record mode. |
| Hybrid search (vector + BM25) | Opt-in | Needs BM25 enabled; results carry text/metadata. Text queries need an embedder (or `query_vector`). |
| Embeddings | Opt-in | `barrel_embed` (local Python, Ollama, OpenAI, ...). Auto-embed and text hybrid need it. |
| Reranking | Opt-in | `barrel_rerank` (cross-encoder). |
| FAISS index backend | Opt-in | `barrel_faiss`; needs the FAISS C++ library, excluded from the default build (`rebar3 as faiss`). |
| Changes feed | Ready | `changes/2`, `subscribe/2`; HLC cursor via `hlc_encode/1`. |
| BQL queries | Ready | PartiQL dialect over docs, vectors, and BM25; live SUBSCRIBE queries. See [query-bql](guides/query-bql.md). |
| Synchronization / replication | Ready | Same VM and over HTTP (`/db/:db/_sync/*`); documents, attachments, channels, continuous tasks. Vectors rebuild locally. See [synchronization](guides/synchronization.md). |
| Timeline (branch, PITR, merge) | Ready | O(1) forks, point-in-time rewind, merge back as sync. See [timeline](guides/timeline.md). |
| Audit and provenance | Ready | Retained history log; actor/session/source on writes; past bodies by version. See [audit-provenance](guides/audit-provenance.md). |
| Document TTL | Ready | `expires_at` write option, lazy expiry, opt-in sweeper (`ttl_sweep_interval`). |
| Encryption at rest | Opt-in | Per-database keys via `barrel_keyprovider`; EncryptedEnv plus a sector cipher for flat files. See [encryption](guides/encryption.md). |
| REST/JSON server | Opt-in | `barrel_server` over `livery`, `rebar3 as server`. See [rest-server](guides/rest-server.md). |
| Agent layer (spaces, capabilities, sessions, handoffs) | Ready | `barrel_spaces`; capability bearers on REST and MCP. See [spaces](guides/spaces.md). |
| MCP endpoint (tools, resources, live queries) | Opt-in | `/mcp` in `barrel_server`, on by default there. See [mcp](guides/mcp.md). |
| gRPC, HTTP/3, WebTransport, unix socket, OpenAPI | Planned | Later transports on `barrel_server`. |
| SQL API, agentfs | Planned | Later. |

## Where things run

- **Embedded**: depend on `barrel` (no transports pulled in). See [embedding](guides/embedding.md).
- **Server**: run `barrel_server` for HTTP. See [rest-server](guides/rest-server.md).
