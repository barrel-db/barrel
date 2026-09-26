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
`barrel_vectordb`, blobs are document attachments. The `barrel` API composes
them; each underlying app stays usable on its own.

## Capabilities

| Capability | Status | Notes |
|---|---|---|
| Documents (CRUD, MVCC, query) | Ready | `barrel_docdb`; `find/2` over the path index. See [embedding](guides/embedding.md). |
| Batches (`put_docs`/`get_docs`/`delete_docs`, `vector_add_batch`) | Ready | Per-element results, in order. A `put_docs` entry can be `{Doc, DocOpts}` with its own `outbox` tags and `sync` flag. |
| Write throughput | Ready | Concurrent writes to one database share one batch and one sync (group commit, `max_group`); documents are prepared in the caller and a committer process writes each group (`write_chunk`). See `apps/barrel_docdb/docs/benchmarks.md`. |
| Read-only opens | Ready | `read_only => true` on `barrel:open/2`, `barrel_docdb` and `barrel_vectordb`: RocksDB `OpenForReadOnly`, no file created or rewritten, several nodes can serve one directory. Writes answer `{error, read_only}`. |
| Attachments (blobs) | Ready | Document attachments; pluggable backend per db via `barrel_att_backend` (RocksDB BlobDB default, `none` for databases without attachments). Streaming read/write. |
| S3 attachment backend | Opt-in | `barrel_att_s3` (`rebar3 as s3`): attachments in S3-compatible storage, with replication and multipart-upload GC. |
| Vector search | Ready | `barrel_vectordb`; HNSW by default. |
| Record mode (policy-driven vector indexing) | Ready | Documents auto-embed per policy; async (healed) or sync (read-your-write); explicit vectors via put option. See [record-mode](guides/record-mode.md). |
| BM25 keyword search | Opt-in | Enable with `bm25_backend => memory` (or `disk`) at open; disk by default in record mode. The disk index is durable across close and kill; the memory index is rebuilt from the stored text at open. |
| Trigram substring and regex search | Opt-in | `barrel_ngram`: an index over `barrel_docdb` documents with checksummed segments (`verify_segments`); served as the `ngram_search` MCP tool. |
| Hybrid search (vector + BM25) | Opt-in | Needs BM25 enabled; results carry text/metadata. Text queries need an embedder (or `query_vector`). |
| Embeddings | Opt-in | `barrel_embed` (local Python, Ollama, OpenAI, ...). Auto-embed and text hybrid need it. `barrel:embedder_info/1` reports the model identity and a `fingerprint`: two databases with the same fingerprint produce comparable vector scores. See [record-mode](guides/record-mode.md). |
| Reranking | Opt-in | `barrel_rerank` (cross-encoder). |
| FAISS index backend | Opt-in | `barrel_faiss`; needs the FAISS C++ library, excluded from the default build (`rebar3 as faiss`). |
| Changes feed | Ready | `changes/2`, `subscribe/2`; HLC cursor via `hlc_encode/1`. |
| BQL queries | Ready | PartiQL dialect over docs, vectors, and BM25; live SUBSCRIBE queries. Results carry the observed version (`instance_id`, `last_seq`); the REST query route takes `max_rows` and `deadline_ms` and reports `bound`. See [query-bql](guides/query-bql.md). |
| Contexts | Ready | Named datasets on this node or other `barrel_server` nodes, queried together with one BQL statement; per-source status, version and coverage; working sets with saved slices and imported snapshots that answer offline. Erlang (`barrel_ctx`), REST (`/contexts`, `/worksets`) and MCP (`context_*` tools). See [contexts](guides/contexts.md). |
| Export and read-only import | Ready | `barrel_ctx_export`: a checksummed copy of a database, imported and verified elsewhere and served read only. See [contexts](guides/contexts.md#import-a-snapshot). |
| Database lifecycle | Ready | `barrel_dbs`: idle close, LRU eviction, pinning, counted leases, exclusive holds, `must_exist` opens. |
| Synchronization / replication | Ready | Same VM and over HTTP (`/db/:db/_sync/*`); documents, attachments, channels, continuous tasks. Bearer, Ed25519 signed-request, and mTLS auth (opt-in; bearer is the default). Vectors rebuild locally. See [synchronization](guides/synchronization.md). |
| Timeline (branch, PITR, merge) | Ready | O(1) forks, point-in-time rewind, merge back as sync. See [timeline](guides/timeline.md). |
| Audit and provenance | Ready | Retained history log; actor/session/source on writes; past bodies by version. See [audit-provenance](guides/audit-provenance.md). |
| Document TTL | Ready | `expires_at` write option, lazy expiry, opt-in sweeper (`ttl_sweep_interval`). |
| Encryption at rest | Opt-in | Per-database keys via `barrel_keyprovider`; EncryptedEnv plus a sector cipher for flat files. See [encryption](guides/encryption.md). |
| REST/JSON server | Opt-in | `barrel_server` over `livery`, `rebar3 as server`; serves HTTP/1.1 (default) plus opt-in HTTP/2 and HTTP/3 over TLS. See [rest-server](guides/rest-server.md). |
| Agent layer (spaces, capabilities, sessions, handoffs) | Ready | `barrel_spaces`; capability bearers on REST and MCP. See [spaces](guides/spaces.md). |
| MCP endpoint (tools, resources, live queries) | Opt-in | `/mcp` in `barrel_server`, on by default there. See [mcp](guides/mcp.md). |
| Browser client (barrel-lite) | Ready | Offline-first TypeScript client (`clients/barrel-lite`): OPFS store, HLC-stamped writes, sync over the wire (polling or continuous SSE), multi-tab, attachment sync, a local BQL subset matching the server, and vector search (embedding pull + brute-force cosine top-k over the synced set, with server `/search` delegation; no ANN in the browser). See [barrel-lite](guides/barrel-lite.md). |
| CORS + capability tokens on `/db` | Ready | CORS middleware and `bsp_` bearers scoped to their space's `/db` routes, so browsers can sync. See [rest-server](guides/rest-server.md). |
| gRPC, WebTransport, unix socket, OpenAPI | Planned | Later transports on `barrel_server`. |
| SQL API, agentfs | Planned | Later. |

## Where things run

- **Embedded**: depend on `barrel` (no transports pulled in). See [embedding](guides/embedding.md).
- **Server**: run `barrel_server` for HTTP. See [rest-server](guides/rest-server.md).
