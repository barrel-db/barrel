# Responsibility map

This note records which application owns which responsibility today, and the one
move still planned. Read it before proposing a refactor that crosses app
boundaries.

## Where responsibilities live

| Responsibility | App | Status |
|----------------|-----|--------|
| Document metadata, MVCC, changes, replication, history | `barrel_docdb` | exists |
| Object bytes (blobs) | `barrel_docdb` attachments (`barrel_att_backend`); S3-compatible storage in `barrel_att_s3` | exists |
| Vector segments, ANN, BM25, hybrid search | `barrel_vectordb` | exists |
| Embedding generation | `barrel_embed` | exists |
| Reranking | `barrel_rerank` | exists |
| Trigram substring and regex index | `barrel_ngram` | exists |
| Encryption at rest | `barrel_crypto` | exists |
| One id over docs + blobs + vectors, record mode, BQL, database lifecycle | `barrel` (the timeline itself is in `barrel_docdb`) | exists |
| Contexts: catalog, federated queries across nodes, working sets, export and import | `barrel` (`barrel_ctx*`) | exists |
| Agent layer: spaces, capabilities, sessions, handoffs | `barrel_spaces` | exists |
| Transports (REST/JSON, MCP), contexts routes and tools | `barrel_server` | exists |
| Durability, placement, replication orchestration across nodes | `barrel_fabric` | future |

## Settled decisions

- Object storage is not a separate app. Blobs are `barrel_docdb` attachments
  with a pluggable backend per database (`barrel_att_backend`): a document owns
  its blob, matching how vectors and metadata attach to the same id. See
  [../architecture/overview.md](../architecture/overview.md).
- The agent layer shipped as `barrel_spaces` plus the MCP surface in
  `barrel_server`, not as a separate `barrel_agents` app.
- Queries across nodes shipped as contexts in `barrel` (explicit, named
  datasets with per-source provenance), not as transparent routing. See
  [../guides/contexts.md](../guides/contexts.md).

## Still planned

- `barrel_fabric`: dataset agents, durability, placement, and replication
  orchestration across nodes. Moving cross-node concerns
  there is its own reviewable change with its own tests, not folded into any
  other app.
