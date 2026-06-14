# barrel_docdb Design Document

## Overview

barrel_docdb is an embeddable document database for Erlang applications. It provides document storage with MVCC (Multi-Version Concurrency Control), binary attachments, secondary indexes, a changes feed, and replication primitives.

## Design Goals

1. **Embeddable**: Run as part of your Erlang application, no external services
2. **Reliable**: ACID transactions via RocksDB, crash-safe operations
3. **Replication-ready**: CouchDB-compatible revision model for sync
4. **Distributed**: HLC-based ordering for decentralized deployments
5. **Efficient**: Optimized storage for documents and large attachments
6. **Reactive**: Real-time subscriptions for document changes
7. **Simple API**: Clean, intuitive public interface

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application                               │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     barrel_docdb (Public API)                   │
│  put_doc, get_doc, find, subscribe, replicate, get_hlc, ...    │
└─────────────────────────────────────────────────────────────────┘
                               │
    ┌──────────────┬───────────┼───────────┬──────────────┐
    ▼              ▼           ▼           ▼              ▼
┌────────┐  ┌────────────┐ ┌────────┐ ┌────────┐  ┌────────────┐
│barrel  │  │barrel_query│ │barrel  │ │barrel  │  │barrel_     │
│db_server│  │(find)      │ │_rep    │ │_sub    │  │query_sub   │
└────────┘  └────────────┘ └────────┘ └────────┘  └────────────┘
    │              │           │           │              │
    │              │           │           └──────────────┘
    │              │           │                  │
    ▼              ▼           ▼                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      barrel_hlc (HLC Clock)                     │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                   barrel_store_rocksdb                          │
│                   (storage abstraction)                         │
└─────────────────────────────────────────────────────────────────┘
          │                                        │
          ▼                                        ▼
┌──────────────────────────┐         ┌──────────────────────────┐
│    Document Store        │         │   Attachment Store       │
│    (RocksDB)             │         │   (RocksDB + BlobDB)     │
└──────────────────────────┘         └──────────────────────────┘
```

## Core Components

### 1. Database Server (barrel_db_server)

Each database runs as a separate `gen_server` process:

```erlang
-record(state, {
    name      :: binary(),           %% Database name
    db_path   :: string(),           %% Data directory path
    store_ref :: rocksdb:db_handle(),%% Document store reference
    att_ref   :: rocksdb:db_handle(),%% Attachment store reference
    view_sup  :: pid()               %% View supervisor
}).
```

**Responsibilities:**
- Document CRUD operations
- Revision management
- Sequence number generation
- Coordination with views and changes

**Process Registry:**
Databases are registered in `persistent_term` for fast lookup:
```erlang
persistent_term:put({barrel_db, DbName}, Pid)
```

### 2. Document Model

Documents use a CouchDB-compatible revision model:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Document                                 │
├─────────────────────────────────────────────────────────────────┤
│ id          : <<"user:alice">>                                  │
│ body        : #{<<"name">> => <<"Alice">>, ...}                 │
├─────────────────────────────────────────────────────────────────┤
│                       Revision Tree                              │
│                                                                  │
│         1-aaa (root)                                            │
│            │                                                     │
│         2-bbb                                                    │
│        ╱     ╲                                                   │
│     3-ccc   3-ddd  (conflict)                                   │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│ Current Rev : 3-ccc (winning)                                   │
│ Deleted     : false                                             │
│ Sequence    : {0, 42}                                           │
└─────────────────────────────────────────────────────────────────┘
```

**Revision Format:** `<generation>-<sha256_hex>`

The hash is computed from:
```erlang
crypto:hash(sha256, term_to_binary({DocBody, ParentRev, Deleted}))
```

### 3. Storage Layer

#### Dual-Database Architecture

Each barrel database uses two separate RocksDB instances:

| Store | Purpose | Optimization |
|-------|---------|--------------|
| Document Store | Docs, metadata, sequences, views | Standard RocksDB, optimized for small values |
| Attachment Store | Binary attachments | BlobDB enabled, optimized for large values |

**Rationale:** Separating large attachments from small documents avoids write amplification during compaction. RocksDB's BlobDB stores large values in separate blob files.

#### Key Schema

```
Document Store Keys:
├── doc_info/{db}/{docid}              → DocInfo (metadata + revtree)
├── doc_rev/{db}/{docid}/{rev}         → Document body
├── doc_hlc/{db}/{hlc}                 → Change entry (HLC-ordered)
├── path_hlc/{db}/{topic}/{hlc}        → Path-indexed change (exact match)
├── local/{db}/{docid}                 → Local document (not replicated)
├── ars/{db}/{path}                    → Path index for queries
├── view_meta/{db}/{viewid}            → View metadata
├── view_hlc/{db}/{viewid}             → View indexed HLC
├── view_index/{db}/{viewid}:{key}:{docid} → View index entry
└── view_by_docid/{db}/{viewid}:{docid}    → Reverse index

Posting CF Keys (posting_cf):
├── ars_posting/{db}/{field}/{value}   → DocId posting list
└── prefix_changes/{db}/{prefix}/{bucket} → HLC-ordered changes posting list

Attachment Store Keys:
└── att/{db}/{docid}/{attname}         → Attachment binary data
```

Keys are designed for efficient range scans and prefix matching.

**Key Prefixes:**
| Prefix | Hex | Description |
|--------|-----|-------------|
| DOC_INFO | 0x01 | Document metadata |
| DOC_REV | 0x02 | Document body by revision |
| DOC_HLC | 0x0D | Changes ordered by HLC |
| PATH_HLC | 0x0E | Path-indexed changes (exact match) |
| PREFIX_CHANGES | 0x1B | Sharded prefix changes (wildcard) |
| ARS | 0x09 | Path index for queries |
| LOCAL | 0x03 | Local documents |
| VIEW_* | 0x04-0x08 | View storage |

#### RocksDB Optimizations

Both stores share a common LRU block cache managed by `barrel_cache`:

```
┌─────────────────────────────────────────────────────────────────┐
│                  Shared Block Cache (512MB)                      │
├─────────────────────────────────────────────────────────────────┤
│  Document Store          │  Attachment Store                    │
│  ├── Data blocks         │  ├── Metadata blocks                 │
│  ├── Index blocks        │  └── BlobDB index                    │
│  └── Bloom filters       │                                      │
└─────────────────────────────────────────────────────────────────┘
```

**Document Store Optimizations:**
| Setting | Default | Purpose |
|---------|---------|---------|
| `block_cache` | 512MB shared | LRU cache for data/index blocks |
| `bloom_filter` | 10 bits/key | Reduce disk reads for point lookups |
| `write_buffer_size` | 64MB | Memtable size before flush |
| `max_write_buffer_number` | 3 | Concurrent memtables |
| `compression` | snappy | Fast compression for all levels |
| `bottommost_compression` | snappy | Configurable (zstd if available) |
| `level0_file_num_compaction_trigger` | 4 | Trigger L0→L1 compaction |
| `max_background_jobs` | schedulers | Parallel compaction threads |

**Attachment Store Optimizations:**
| Setting | Default | Purpose |
|---------|---------|---------|
| `enable_blob_files` | true | Store large values in blob files |
| `min_blob_size` | 4KB | Threshold for blob storage |
| `blob_file_size` | 256MB | Maximum blob file size |
| `blob_compression_type` | snappy | Blob compression (zstd if available) |
| `blob_garbage_collection_age_cutoff` | 0.25 | GC blobs older than 25% |
| `blob_garbage_collection_force_threshold` | 0.5 | Force GC at 50% garbage |

**Configuration:**
```erlang
%% Application environment (sys.config)
{barrel_docdb, [
    {block_cache_size, 536870912},  %% 512MB shared cache
    {data_dir, "/var/lib/barrel"}
]}

%% Per-database options
barrel_docdb:create_db(<<"mydb">>, #{
    store_opts => #{
        write_buffer_size => 128 * 1024 * 1024,  %% 128MB
        max_open_files => 2000,
        rate_limit_bytes_per_sec => 100 * 1024 * 1024  %% 100MB/s
    },
    att_opts => #{
        blob_file_size => 512 * 1024 * 1024,  %% 512MB blobs
        min_blob_size => 8192  %% 8KB threshold
    }
}).
```

### 4. HLC (Hybrid Logical Clock)

Every document modification is timestamped with an HLC:

```
HLC Timestamp: {WallTime, LogicalCounter, NodeId}
├── WallTime       : Physical time in microseconds
├── LogicalCounter : Logical extension for same-time events
└── NodeId         : Unique node identifier
```

**Properties:**
- Causally consistent ordering across distributed nodes
- Monotonically increasing within a node
- No central coordinator required
- Clock skew detection and handling

**API:**
```erlang
%% Get current HLC
Ts = barrel_docdb:get_hlc().

%% Generate new timestamp (advances clock)
NewTs = barrel_docdb:new_hlc().

%% Sync with remote node
{ok, SyncedTs} = barrel_docdb:sync_hlc(RemoteTs).
```

### 5. Changes Feed

Every document modification generates an HLC-timestamped change entry:

```erlang
%% Change entry structure
#{
    id => DocId,
    hlc => HlcTimestamp,
    rev => RevId,
    deleted => boolean(),
    changes => [RevId]
}
```

Changes are stored by HLC for efficient streaming:

```erlang
%% Get changes since HLC timestamp
barrel_changes:fold_changes(StoreRef, DbName, SinceHlc, Fun, Acc)
```

**Use Cases:**
- Real-time notifications
- Incremental view updates
- Replication source
- Path-filtered change feeds

### 6. Path Subscriptions

Subscribe to document changes matching MQTT-style path patterns:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Path Subscription Flow                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Client subscribes with pattern (e.g., "users/+/profile")    │
│                         │                                        │
│                         ▼                                        │
│  2. barrel_sub registers pattern in match_trie                  │
│                         │                                        │
│                         ▼                                        │
│  3. On document write, paths extracted from document            │
│                         │                                        │
│                         ▼                                        │
│  4. Paths matched against registered patterns                   │
│                         │                                        │
│                         ▼                                        │
│  5. Matching subscribers receive notification message           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Pattern Syntax:**
| Pattern | Description |
|---------|-------------|
| `users/alice` | Exact match |
| `users/+` | Single segment wildcard |
| `users/#` | Multi-segment wildcard |
| `+/orders/#` | Mixed wildcards |

### 7. Query Subscriptions

Subscribe to changes for documents matching a query:

```erlang
Query = #{where => [{path, [<<"type">>], <<"user">>}]},
{ok, SubRef} = barrel_docdb:subscribe_query(DbName, Query).
```

**Optimization:**
- Query paths are extracted at subscription time
- Changes are first filtered by path intersection
- Full query evaluation only when paths overlap
- Avoids evaluating query for every document change

### 8. Path-Indexed Changes

Changes are indexed by path at write time for efficient filtered queries:

```
Key: path_hlc/{db}/{topic}/{hlc} → {doc_id, rev, deleted}
```

Example document:
```erlang
#{<<"type">> => <<"user">>, <<"org">> => <<"acme">>}
```

Creates index entries:
```
path_hlc/mydb/type/user/{hlc1}
path_hlc/mydb/org/acme/{hlc1}
```

**Benefits:**
- O(k) query time where k = matching changes (vs O(n) full scan)
- Efficient filtered replication
- Real-time path subscriptions use same index

### 8b. Sharded Prefix Changes (Wildcard Queries)

For wildcard path queries (e.g., `paths => [<<"users/#">>]`), a separate sharded posting list index provides efficient HLC-ordered iteration:

```
Key: prefix_changes/{db}/{prefix}/{bucket} → Posting list of changes
```

**Key Format:**
```
PREFIX_CHANGES (0x1B) | encoded_db | prefix | 0x00 | bucket (4 bytes BE)
```

**Value Format (Posting List):**
```
Each entry: << HLC:12/binary, DocId/binary, Rev/binary, Deleted:1 >>
```

**Bucketing Strategy:**
- Bucket = `wall_time div 3600` (1-hour granularity)
- Bounds posting list growth for high-write workloads
- Range scan discovers only existing buckets (no empty iteration)

**Example for document:**
```erlang
#{<<"type">> => <<"user">>, <<"status">> => <<"active">>}
```

Creates entries in multiple prefix buckets:
```
prefix_changes/mydb/type/user/482345         → [<< hlc1, "doc1", ... >>]
prefix_changes/mydb/status/active/482345     → [<< hlc1, "doc1", ... >>]
```

**Query Execution:**
1. Compute start bucket from `since` HLC
2. Range scan `posting_cf` for prefix + bucket keys
3. Iterate sorted entries, filter by HLC
4. Collect until `limit` reached

**Performance:**
- 50x faster than path_hlc prefix scan with deduplication
- Native RocksDB merge operator keeps entries sorted
- No post-processing or deduplication needed

### 9. Replication

Replication follows the CouchDB protocol with HLC-based ordering:

```
┌──────────────────────────────────────────────────────────────┐
│                    Replication Flow                           │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Read checkpoint (last_hlc from local doc)                │
│                     │                                         │
│                     ▼                                         │
│  2. Get changes from source since last_hlc                   │
│     (optionally filtered by paths/query)                     │
│                     │                                         │
│                     ▼                                         │
│  3. Sync target HLC with source timestamps                   │
│                     │                                         │
│                     ▼                                         │
│  4. For each change:                                         │
│     ├── Call revsdiff(target, docid, revs)                   │
│     ├── Get missing revisions from source with history       │
│     └── Put to target using put_rev(doc, history, deleted)   │
│                     │                                         │
│                     ▼                                         │
│  5. Write checkpoint with new last_hlc                       │
│                     │                                         │
│                     ▼                                         │
│  6. Repeat until no more changes                             │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

**Filtered Replication:**
Replicate only documents matching filters:
```erlang
barrel_rep:replicate(Source, Target, #{
    filter => #{
        paths => [<<"users/#">>],           %% Path pattern filter
        query => #{where => [...]}          %% Query filter
    }
}).
%% Both filters use AND logic when combined
```

**Transport Abstraction:**
The `barrel_rep_transport` behaviour allows pluggable transports:
- `barrel_rep_transport_local` - Same Erlang VM
- Custom HTTP, TCP, or other transports

**Checkpoints:**
Stored as local documents (not replicated):
```erlang
Key: <<"replication-checkpoint-{rep_id}">>
Value: #{<<"history">> => [#{<<"source_last_hlc">> => ...}]}
```

### 10. Query Parallelization

Large queries (>100 documents) use parallel CBOR decode + condition matching via `barrel_parallel`:

```
                    Query Coordinator
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
     Worker 1          Worker 2           Worker N
  (DocIds 1-100)    (DocIds 101-200)   (DocIds N*100+)
        │                  │                  │
   Decode CBOR        Decode CBOR        Decode CBOR
   Match Conds        Match Conds        Match Conds
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
                    Merge Results
                   (preserve order)
```

**Configuration:**
- 4 workers (PostgreSQL-style conservative default)
- Threshold: >100 documents triggers parallel execution
- Worker pool managed by `barrel_parallel` gen_server

**Tradeoffs:**

| Workers | Throughput | Reason |
|---------|------------|--------|
| 4 | Best | Optimal for RocksDB |
| 8 | ~50% slower | RocksDB contention, cache thrashing |
| 2 | ~30% slower | Underutilizes CPU |

### 11. Chunked Query Execution

Queries return results in chunks with continuation tokens for memory-efficient iteration:

```erlang
%% First chunk (default 1000 results)
{ok, Results, #{has_more := true, continuation := Token}} =
    barrel_docdb:find(Db, Query).

%% Next chunk
{ok, More, #{has_more := false}} =
    barrel_docdb:find(Db, Query, #{continuation => Token}).
```

**Cursor Management:**
- Cursors stored in ETS with 60-second TTL
- Each access extends TTL
- Cursors hold RocksDB snapshots for consistent reads
- Snapshots released when `has_more => false` or cursor expires

## Design Decisions

### Why RocksDB?

1. **Embedded**: No external service dependencies
2. **LSM-tree**: Optimized for write-heavy workloads
3. **Atomic batches**: Multiple operations in one atomic write
4. **BlobDB**: Efficient large value storage
5. **Snapshots**: Consistent reads during iteration

### Why Revision Trees?

1. **Conflict detection**: Multiple concurrent updates create branches
2. **Replication**: Only transfer missing revisions
3. **History**: Track document evolution
4. **Deterministic winners**: Same data = same winning revision

### Why Separate Attachment Store?

RocksDB stores values inline in SST files. Large values cause:
- High write amplification during compaction
- Wasted space in block cache
- Slower reads due to large block sizes

BlobDB stores large values in separate blob files, solving these issues.

### Why Local Documents?

Local documents:
- Are not replicated
- Don't have revision history
- Are used for per-database metadata (checkpoints, config)

This separates replication state from user data.

## Data Flow Examples

### Put Document

```
put_doc(Db, Doc)
    │
    ├── Validate document
    ├── Generate/validate ID
    ├── Compute new revision hash
    │
    ├── If update:
    │   ├── Check existing doc exists
    │   ├── Verify _rev matches current
    │   └── Extend revision tree
    │
    ├── Get next sequence number
    │
    ├── Atomic batch write:
    │   ├── doc_info (metadata + revtree)
    │   ├── doc_rev (body at new revision)
    │   └── doc_seq (change entry)
    │
    └── Return {ok, #{id, rev, ok}}
```

### Replicate

```
replicate(Source, Target)
    │
    ├── Generate replication ID
    ├── Read checkpoint (get last_seq)
    │
    ├── Loop:
    │   ├── Get changes batch from source
    │   │
    │   ├── For each change:
    │   │   ├── revsdiff(target, docid, revs)
    │   │   ├── If missing revs:
    │   │   │   ├── get_doc(source, docid, {history: true})
    │   │   │   └── put_rev(target, doc, history, deleted)
    │   │
    │   ├── Write checkpoint
    │   └── Continue until no changes
    │
    └── Return {ok, stats}
```

## Supervision Tree

```
barrel_docdb_sup (one_for_one)
├── barrel_cache              # Shared RocksDB block cache
├── barrel_hlc_clock          # Global HLC clock
├── barrel_sub                # Path subscriptions manager
├── barrel_query_sub          # Query subscriptions manager
├── barrel_path_dict          # Path ID interning for posting lists
├── barrel_query_cursor       # Chunked query cursor management
├── barrel_parallel           # Worker pool for parallel queries
└── barrel_db_sup (simple_one_for_one)
    ├── barrel_db_server (db1)
    ├── barrel_db_server (db2)
    │   └── ...
    └── ...
```

## Performance Considerations

### Write Path
- Batch operations reduce disk I/O
- Sequence numbers enable efficient change tracking
- Revision computation is CPU-bound (SHA-256)

### Read Path
- Document lookup is O(1) key access
- View queries use RocksDB iterators
- Snapshots provide consistent reads

### Memory Usage
- RocksDB block cache for hot data
- Views process changes incrementally
- Large attachments stored in blob files

### Disk Usage
- RocksDB compaction reclaims space
- Old revisions can be pruned
- Attachments use content-addressable storage

## Future Considerations

### Planned Features
- Conflict resolution helpers
- External tiering layer built on the changes feed, metrics, and the reserved `created_at`/`expires_at`/`tier` entity columns
- External sharding and cluster coordination built on the replication engine and system documents

### Extension Points

barrel_docdb is a document-database building block. Higher-level capabilities
(sharding, tiering, clustering) are meant to be built on top of its public API
rather than baked in:

- Custom storage backends (via behaviour)
- Custom replication transports (via the `barrel_rep_transport` behaviour)
- Cluster/tiering substrate: changes feed, revision primitives (`put_rev`/`revsdiff`), HLC, system and local documents, and store/index introspection (`get_db_size`, range scans, snapshots)

## File Structure

```
src/
├── barrel_docdb_app.erl        # Application callbacks
├── barrel_docdb_sup.erl        # Top-level supervisor
├── barrel_docdb.erl            # Public API
│
├── barrel_db_server.erl        # Per-database gen_server
├── barrel_db_sup.erl           # Database supervisor
│
├── barrel_doc.erl              # Document utilities
├── barrel_revtree_bin.erl      # Revision tree (compact binary encoding)
│
├── barrel_hlc.erl              # HLC clock management
│
├── barrel_att.erl              # Attachment API
├── barrel_att_store.erl        # Attachment storage
│
├── barrel_changes.erl          # Changes feed API (HLC-based)
├── barrel_changes_stream.erl   # Streaming changes
│
├── barrel_sub.erl              # Path subscriptions manager
├── barrel_query_sub.erl        # Query subscriptions manager
│
├── barrel_query.erl            # Declarative query compiler & executor
├── barrel_query_cursor.erl     # Chunked query cursor management
├── barrel_ars.erl              # Path index API
├── barrel_ars_index.erl        # Path index implementation
├── barrel_path_dict.erl        # Path ID interning for posting lists
│
├── barrel_parallel.erl         # Parallel map/filtermap with worker pool
├── barrel_doc_body_store.erl   # Batch document body operations
│
├── barrel_cache.erl            # Shared RocksDB block cache
├── barrel_store_rocksdb.erl    # RocksDB storage (optimized)
├── barrel_store_keys.erl       # Key encoding (doc_hlc, path_hlc, ars)
├── barrel_docdb_codec_cbor.erl # CBOR encoding/decoding
│
├── barrel_rep.erl              # Replication API (filtered)
├── barrel_rep_alg.erl          # Replication algorithm (HLC sync)
├── barrel_rep_checkpoint.erl   # Checkpoint management (HLC-based)
├── barrel_rep_transport.erl    # Transport behaviour
└── barrel_rep_transport_local.erl  # Local transport
```

## References

- [CouchDB Replication Protocol](https://docs.couchdb.org/en/stable/replication/protocol.html)
- [RocksDB Documentation](https://rocksdb.org/docs/)
- [RocksDB BlobDB](https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html)
- [Hybrid Logical Clocks](https://cse.buffalo.edu/tech-reports/2014-04.pdf)
- [hlc library](https://gitlab.com/barrel-db/hlc)
- [match_trie library](https://gitlab.com/barrel-db/match_trie)
