# barrel_docdb Design Document

## Overview

barrel_docdb is an embeddable document database for Erlang applications. It provides document storage with MVCC (Multi-Version Concurrency Control), binary attachments, secondary indexes, a changes feed, and replication primitives.

## Design Goals

1. **Embeddable**: Run as part of your Erlang application, no external services
2. **Reliable**: ACID transactions via RocksDB, crash-safe operations
3. **Replication-ready**: HLC version-vector model for conflict-free sync
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
- Version and conflict management
- HLC change-sequence generation
- Coordination with views and changes

**Process Registry:**
Databases are registered in `persistent_term` for fast lookup:
```erlang
persistent_term:put({barrel_db, DbName}, Pid)
```

### 2. Document Model

Documents are versioned with Hybrid Logical Clocks, not revision trees. Each write is a **version** `{HLC, Author}` whose API token is `<hex(hlc)>@<author>` (the value in `<<"_rev">>`). Each document carries a **version vector** tracking the highest HLC seen from each author. Concurrent writes are resolved by last-write-wins with the losing versions retained.

```
┌─────────────────────────────────────────────────────────────────┐
│                         Document                                 │
├─────────────────────────────────────────────────────────────────┤
│ id          : <<"user:alice">>                                  │
│ body        : #{<<"name">> => <<"Alice">>, ...}                 │
├─────────────────────────────────────────────────────────────────┤
│ version     : 0000018abc...@f1e0...   (winner)                  │
│ vv          : #{f1e0... => hlc1, a2b3... => hlc2}               │
│ nconflicts  : 1                                                  │
│ deleted     : false                                             │
├─────────────────────────────────────────────────────────────────┤
│ version chain (0x1D): retained superseded + conflict siblings   │
│   0000018def...@a2b3...   (conflict, live)                      │
└─────────────────────────────────────────────────────────────────┘
```

**Version token:** `<hex(hlc)>@<author>`

- The HLC (fixed-width hex) gives causally meaningful ordering, so token order equals causal order.
- The author is the writing database's stable source id (per-database, 8 random bytes; see `barrel_version` and `barrel_db_server:ensure_source_id/2`). Authorship is per-database, not per-node, so two databases on one node detect each other's writes as concurrent.
- The winner among siblings is the maximum under `barrel_version:compare/2` (HLC first, author as tie-break), a commutative rule: any replica seeing the same version set picks the same winner.

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
├── doc_entity/{db}/{docid}            → Wide-column entity (version, vv,
│                                         nconflicts, deleted, hlc, body cols)
├── doc_version/{db}/{docid}:{version} → Version chain sibling (superseded
│                                         or conflict) + archived body
├── history/{db}/{hlc}                 → Retained history log entry
├── doc_hlc/{db}/{hlc}                 → Change entry (HLC-ordered)
├── path_hlc/{db}/{topic}/{hlc}        → Path-indexed change (exact match)
├── local/{db}/{docid}                 → Local document (not replicated)
├── view_meta/{db}/{viewid}            → View metadata
├── view_index/{db}/{viewid}:{key}:{docid} → View index entry
└── view_by_docid/{db}/{viewid}:{docid}    → Reverse index

Posting CF Keys (posting_cf):
├── path_posting/{db}/{field}/{value}     → DocId posting list
└── prefix_changes/{db}/{prefix}/{bucket} → HLC-ordered changes posting list

Attachment Store Keys:
└── att/{db}/{docid}/{attname}         → Attachment (content-addressed blob)
```

Keys are designed for efficient range scans and prefix matching. The document body and its version metadata live together in one wide-column entity; older revision-tree keys (`doc_info`, `doc_rev`, `doc_tree`) are gone.

**Key Prefixes:**
| Prefix | Hex | Description |
|--------|-----|-------------|
| DOC_ENTITY | 0x18 | Wide-column document entity (current version) |
| DOC_HLC | 0x0D | Changes ordered by HLC |
| PATH_HLC | 0x0E | Path-indexed changes (exact match) |
| PREFIX_CHANGES | 0x1B | Sharded prefix changes (wildcard) |
| HISTORY | 0x1C | Retained history log |
| DOC_VERSION | 0x1D | Per-doc version chain (superseded + conflict siblings) |
| LOCAL | 0x05 | Local documents |
| PATH_POSTING | 0x14 | Path index posting lists for queries |

#### Entity Codec (v4)

The document entity (`DOC_ENTITY`, 0x18) is a fixed positional binary. Columns:

```
<<VerLen:16, Version/binary,        %% COL_VERSION: encoded {HLC, Author}
  Deleted:8,                        %% COL_DELETED
  HlcLen:16, Hlc/binary,            %% COL_HLC: change-sequence HLC
  VVLen:32, VV/binary,              %% COL_VV: encoded version vector
  CreatedAtLen:16, CreatedAt/binary,%% COL_CREATED_AT (reserved for tiering)
  ExpiresAt:64,                     %% COL_EXPIRES_AT (TTL)
  Tier:8,                           %% COL_TIER (hot/warm/cold)
  NConflicts:16,                    %% COL_NCONFLICTS: live conflict siblings
  Ext/binary>>                      %% optional extension tail
```

The `COL_VERSION` / `COL_VV` / `COL_NCONFLICTS` columns replaced the old `COL_REV` / `COL_REVTREE`. The extension tail is backward compatible: an embedding appends `<<EmbLen:32, Emb/binary, Src:8>>`; entities also carrying provenance switch to a tagged form (sentinel `16#FFFFFFFF`, then `(Tag:8, Len:32, Value)*`). Old readers ignore the tail without crashing.

#### Retained History and the Version Chain

Two structures give the version model its durability and conflict retention:

- **Version chain** (`DOC_VERSION`, 0x1D): per document, one row per non-winning version. Each entry is `<<Flag:8, Deleted:8, VV/binary>>` where flag 0 = superseded (covered by the current winner, e.g. an old winner after a fast-forward) and 1 = conflict (a live concurrent sibling). The version's body is archived under its version token so any retained version stays resolvable within the retention window.
- **Retained history log** (`HISTORY`, 0x1C): an append-only, HLC-ordered log of every write the database applies (local writes, replicated versions including losing siblings, and conflict resolutions). Entries carry identity only (doc id, version, deleted, cause, version vector), never bodies, and are written in the same atomic batch as the document. A retention sweep (`sweep_retention/1`) prunes both structures below the history floor.

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
    rev => VersionToken,           %% <hex(hlc)>@<author>
    deleted => boolean(),
    changes => [#{rev => VersionToken}]
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

Replication is a version-vector protocol: one diff round-trip per batch of changes, then a read plus apply per missing document. All conflict handling lives in the target's `put_version`, so the algorithm needs no ancestor negotiation.

```
┌──────────────────────────────────────────────────────────────┐
│                    Replication Flow                           │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Read checkpoint (last_hlc from local doc)                │
│                     │                                         │
│                     ▼                                         │
│  2. Get changes from source since last_hlc                   │
│     (optionally filtered by paths/query; each change          │
│      carries the doc's current version token)                 │
│                     │                                         │
│                     ▼                                         │
│  3. Sync target HLC past the newest source change            │
│                     │                                         │
│                     ▼                                         │
│  4. diff_versions(target, #{DocId => Token})                 │
│     → per doc: have (vv contains it) | missing               │
│                     │                                         │
│                     ▼                                         │
│  5. For each missing doc:                                     │
│     ├── get_doc_for_replication(source, docid)               │
│     │     → {doc, version, vv, deleted}                       │
│     └── put_version(target, doc, version, vv, deleted)       │
│                     │                                         │
│                     ▼                                         │
│  6. Write checkpoint, repeat until no more changes           │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

**Applying a version (`do_put_version`):**
- `contains` (already covered): idempotent no-op.
- remote VV `dominates`: fast-forward; the old winner joins the version chain as superseded.
- `concurrent`: an optional per-db `conflict_merger` may merge; otherwise last-write-wins by version keeps a winner and retains the loser as a live conflict sibling. Vectors merge either way.

**Diffing (`do_diff_versions`):** batch `have`/`missing` by version-vector containment; no per-document ancestor negotiation.

**Filtered Replication:**
Replicate only documents matching filters (path AND query when combined):
```erlang
barrel_rep:replicate(Source, Target, #{
    filter => #{
        paths => [<<"users/#">>],
        query => #{where => [...]}
    }
}).
```

**Transport Abstraction:**
The `barrel_rep_transport` behaviour allows pluggable transports:
- `barrel_rep_transport_local` - same Erlang VM
- `barrel_rep_transport_http` - remote over the `/db/:db/_sync/*` wire (hackney)
- Custom transports for other protocols

`barrel_rep_tasks` layers persistent one-shot and continuous tasks over `barrel_rep`.

**Checkpoints:**
Stored as local documents (not replicated), keyed by a replication id derived from the endpoints and any filter:
```erlang
Key: <<"replication-checkpoint-{rep_id}">>
Value: #{<<"history">> => [#{<<"source_last_hlc">> => ...}]}
```

### 9b. Timeline (Branch and Merge)

`barrel_timeline` forks a database into a branch and merges it back:

- **Branch** (`branch_db/3`): an instant fork. Both RocksDB stores (docs and attachments) are checkpointed into the branch's directory via hard links (O(1) in data size, copy-on-write at the file level). The branch opens as a normal database; its storage keys keep the parent's name through keyspace indirection (`barrel_keyspace`), but it mints its own author id so its writes are attributable and detected as concurrent with the parent's. Lineage is linear (branching a branch is rejected).
- **Merge** (`merge_branch/2`): replays the branch's writes back into the parent through the same version protocol, so divergent edits land as fast-forwards or retained conflict siblings, never silent overwrites.

```erlang
{ok, _} = barrel_docdb:branch_db(<<"mydb">>, <<"mydb-exp">>, #{}),
%% ... write on the branch ...
{ok, _Stats} = barrel_docdb:merge_branch(<<"mydb-exp">>, #{}).
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

### Why Version Vectors?

1. **Conflict detection**: version vectors distinguish causal updates (fast-forward) from concurrent ones without a rev-tree
2. **Replication**: the target diffs by vector containment and pulls only what it is missing, no ancestor negotiation
3. **Deterministic winners**: the max-version rule is commutative, so every replica converges on the same winner
4. **No revision-tree bookkeeping**: bounded per-doc metadata; retained siblings and the history log are pruned by the retention sweep

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
    ├── CAS check: _rev must equal the current winner's token
    │   (a create, or recreate over a tombstone, needs no token)
    │
    ├── Mint version {NextHlc, source_id(Db)}, bump the version vector
    │
    ├── Atomic batch write:
    │   ├── doc_entity (body cols + version, vv, nconflicts, deleted)
    │   ├── change entry (doc_hlc) + path/prefix change indexes
    │   └── retained history log entry (0x1C)
    │
    └── Return {ok, #{<<"id">> => Id, <<"rev">> => Token}}
```

### Replicate

```
replicate(Source, Target)
    │
    ├── Derive replication ID (endpoints + filter)
    ├── Read checkpoint (get last_seq)
    │
    ├── Loop per batch:
    │   ├── Get changes batch from source (each carries a version token)
    │   ├── Sync target HLC past the newest change
    │   ├── diff_versions(target, #{docid => token}) → missing docs
    │   ├── For each missing doc:
    │   │   ├── get_doc_for_replication(source, docid)
    │   │   └── put_version(target, doc, version, vv, deleted)
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
- Cluster/tiering substrate: changes feed, version primitives (`put_version`/`diff_versions`/`get_doc_for_replication`), HLC, system and local documents, and store/index introspection (range scans, snapshots)

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
├── barrel_version.erl          # Version {HLC, Author} + token codec
├── barrel_vv.erl               # Version vectors (compare, contains, merge)
├── barrel_history.erl          # Retained history log
├── barrel_timeline.erl         # Branch / merge (keyspace forks)
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
├── barrel_store_rocksdb.erl    # RocksDB storage + v4 entity codec
├── barrel_store_keys.erl       # Key encoding (entity, version chain, history)
├── barrel_docdb_reader.erl     # Caller-side reads (get_doc, replication reads)
├── barrel_docdb_codec_cbor.erl # CBOR encoding/decoding
│
├── barrel_rep.erl              # Replication API (filtered)
├── barrel_rep_alg.erl          # Replication algorithm (diff + apply)
├── barrel_rep_tasks.erl        # Persistent one-shot / continuous tasks
├── barrel_rep_checkpoint.erl   # Checkpoint management (HLC-based)
├── barrel_rep_transport.erl    # Transport behaviour
├── barrel_rep_transport_local.erl # Local transport
└── barrel_rep_transport_http.erl  # HTTP transport (/db/:db/_sync/*)
```

## References

- [Version Vectors (Wikipedia)](https://en.wikipedia.org/wiki/Version_vector)
- [RocksDB Documentation](https://rocksdb.org/docs/)
- [RocksDB BlobDB](https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html)
- [Hybrid Logical Clocks](https://cse.buffalo.edu/tech-reports/2014-04.pdf)
- [hlc library](https://gitlab.com/barrel-db/hlc)
- [match_trie library](https://gitlab.com/barrel-db/match_trie)
