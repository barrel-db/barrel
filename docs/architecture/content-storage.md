# Content-addressed storage: design

Status: proposal. Nothing on this page ships yet.

This page describes how Barrel would store attachments, agent media and
backups as immutable, content-addressed chunks, using barrel_xet (an Erlang
implementation of the Hugging Face Xet storage protocol). Read it before you
start work on `barrel_xet`, the `barrel_att_xet` attachment backend or the
`barrel_vault` application, or when you need to decide between the blob, S3
and Xet attachment backends.

## Why

Barrel stores attachments in RocksDB (BlobDB) or in S3. The S3 backend
treats the bucket as a mutable store, and most of its recent fixes came from
that: conditional writes, metadata attached after a multipart upload, a
background copy sweep for branches, resumed reads after a dropped
connection, and encryption added on top (barrel_att_s3 0.1.5 to 0.2.1).

Immutable, content-addressed chunks avoid those problems by design:

- An object is written once and never changed, so the store only needs put,
  get-range and list. A local directory, NFS or any object store will do; no
  conditional writes, no multipart metadata.
- A branch shares its parent's chunks. There is nothing to copy and no
  window where a branch's attachments are missing.
- Replication sends only the chunks the other side lacks, so a small edit
  to a large file costs a few chunks.
- Each chunk is sealed with an AEAD and named by a keyed hash, so a
  changed byte fails the read. This is stronger than the unauthenticated
  CTR plus digest check of barrel_att_s3 0.2.0.
- Old chunks stay until garbage collection, which follows the retention
  window, so point-in-time branches can cover attachments.

Plakar's kloset engine is the reference for the feature set: content-defined
chunking, compression, encryption at the source, immutable packfiles on
pluggable stores, snapshots, integrity checks, sync of missing blocks and
non-blocking garbage collection.

## Decisions

- **Xet compatible.** Chunk boundaries, chunk and file hashes, xorb and
  shard formats and the HTTP protocol follow draft-denis-xet-05 (vendored in
  `barrel_xet/docs/spec/`). A Xet client can upload to and download from a
  barrel_xet server, including content written by Barrel.
- **Dedup per database.** Chunks are shared within one database and its
  branches, never across databases. Sizes and access patterns of one tenant
  never reveal anything about another.
- **One barrel_xet.** The project is not split. Its HTTP server and the HTTP
  transport of its client become optional; the core runs inside a Barrel
  node without livery or a listener.
- **Two consumers.** An attachment backend for barrel_docdb, and a separate
  application for agent media and backups.

## Pieces

| Piece | Where | Role |
|---|---|---|
| Chunking, hashing, xorbs, shards, dedup, reconstruction | `barrel_xet` core (`xet_chunker`, `xet_hash`, `xet_merkle`, `xet_xorb`, `xet_shard`, `xet_dedup`, `xet_upload_plan`, `xet_reconstruction`) | Xet formats, unchanged |
| Blob stores | `xet_blob_store` behaviour: `xet_store_fs`, `xet_store_s3` | Where xorbs live |
| Metadata | `xet_metadata` behaviour, RocksDB backend | Xorb and file registry, chunk index for dedup |
| In-process API (new) | `barrel_xet` | Store, read a range, delete, stat, GC, scrub, without HTTP |
| HTTP server and client (optional) | `xet_server`, `xet_h_*`, `xet_client*` | Xet protocol for external clients and remote stores |
| Repositories (new) | `barrel_xet` | One namespace per database: metadata, dedup keys, encryption key |
| Attachment backend (new) | `barrel_att_xet` in the Barrel umbrella | `barrel_att_backend` over a repository |
| Media and backups (new) | `barrel_vault` in the Barrel umbrella | Objects by content for agents; snapshots, check, sync, restore |

## Repository: one per database

A repository is a Xet namespace bound to one Barrel keyspace. It holds the
metadata (xorb registry, file registry, chunk index), the dedup HMAC keys,
and the database's encryption key from `barrel_keyprovider`. A branch
resolves to its parent's keyspace, so it opens the same repository and
shares its chunks. Two databases never share a repository, so dedup and keys
stay per database.

## Encryption at rest

Encryption must not break Xet compatibility. Chunk ids and Merkle hashes
are computed on the plaintext exactly as the spec defines them, and xorbs
are encrypted per repository with the database key before they reach the
store (AES-256-GCM per xorb segment, or AES-GCM-SIV; the plan settles the
mode and the segment size so range reads stay cheap). The server decrypts
when it serves xorbs or reconstructions, so a Xet client receives
conformant bytes.

Visible at rest: the number and sizes of xorbs, and in the metadata store
(itself opened under the database's encrypted env) nothing in clear. Visible
to a Xet client with access: what the protocol exposes, as for any Xet
server.

## Attachments: barrel_att_xet

- **Write:** chunk the stream, look up each chunk in the repository index,
  pack only new chunks into xorbs, register the file (its reconstruction).
- **Read:** plan the reconstruction for the requested range and read only
  the xorb ranges it needs.
- **Index:** the barrel_docdb attachment feed (`barrel_att_feed`) keeps one
  row per attachment pointing at its Xet file hash. Changes, replication,
  branches, point-in-time branches and the attachment count (barrel_docdb
  2.4.0) keep working unchanged.
- **Branch:** a checkpoint of the feed; chunks are already shared.
- **Delete:** a feed tombstone; the chunks go when garbage collection finds
  them unreferenced.

## Garbage collection

Mark the file hashes referenced by live attachment rows of the database and
of every branch of its keyspace, including rows still inside the retention
window; sweep xorbs that hold no marked chunk. It runs in the background and
never blocks reads or writes: a xorb written after the mark starts is never
swept in that pass. barrel_xet's `xet_gc` and `xet_gc_guard` are the
starting point.

## Replication

The attachment sync routes compare chunk lists instead of whole files. The
source sends the reconstruction of a file; the target asks for the chunks
its repository does not have, then registers the same file. Between a
browser copy and a server the same exchange keeps transfers small.

## barrel_vault: media and backups

A separate application, independent of attachments:

- **Objects for agents.** Put and get media and large objects by content
  (images, audio, model files, datasets), with ranges and dedup, in a
  repository the agent's space owns.
- **Snapshots.** A snapshot of a database (documents and attachments) or of
  a directory tree, stored as an immutable manifest over Xet files, indexed
  for browsing with bounded memory.
- **Check, sync, restore.** Re-verify every chunk; copy only missing chunks
  to another store; restore a snapshot to a database or a directory.

A Xet server in front of a vault repository is optional.

## Steps

Each step ships on its own, one PR per repository, gated commits:

1. barrel_xet: in-process API; HTTP server and HTTP client transport
   optional; finish conformance tasks T12.4 to T12.10.
2. barrel_xet: repositories (per-database namespaces) and encryption at
   rest, with the conformance and interop suites still green.
3. Barrel: `barrel_att_xet` on the fs and S3 stores, feed index and counts,
   branches by sharing.
4. Barrel: chunk-level attachment replication.
5. Barrel: `barrel_vault`.

## Open questions

- The encryption mode and segment size that keep range reads cheap.
- Small attachments: below some size, keep them inline in the feed or in
  RocksDB rather than paying a xorb each.
- Garbage collection across a keyspace with many branches: one mark per
  keyspace, or per branch with shared results.
- Whether `barrel_vault` repositories can be shared across spaces on
  purpose (an explicit, opt-in exception to per-database dedup).
