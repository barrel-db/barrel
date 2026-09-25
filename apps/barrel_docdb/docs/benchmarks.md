# Benchmarks

Performance benchmarks for Barrel DocDB operations using the built-in benchmark suite.

> #### Test Environment
>
> - **Hardware**: Apple M1, 16GB RAM, SSD
> - **Erlang/OTP**: 27
> - **Dataset**: 5,000 documents (~500 bytes each)
> - **Database**: Single node, default configuration

## CRUD Operations

Basic document operations show strong single-document performance:

| Operation | Throughput | p50 Latency | p99 Latency |
|-----------|------------|-------------|-------------|
| Insert | 5,785 ops/s | 147 us | 252 us |
| Read | 111,111 ops/s | 8 us | 22 us |
| Update | 118,343 ops/s | 8 us | 21 us |
| Delete | 5,660 ops/s | 8 us | 20 us |

**Key observations:**

- Single-document reads are very fast (8 us median)
- Updates are read-modify-write cycles with excellent latency
- Writes include RocksDB sync and indexing overhead
- Bulk inserts can achieve higher throughput with batching

## Concurrent Synced Writers

Each writer runs `put_doc(Db, Doc, #{outbox => [Tag], return_hlc => true, sync => true})`
on distinct ids, against one database. Measured on a 14-CPU Mac (Apple
Silicon, APFS), 1,280 writes per row.

| Writers | 1.5.0 (writes/s) | 1.6.0 (writes/s) |
|---------|------------------|------------------|
| 1 | 176 | 185 |
| 4 | 211 | 703 |
| 16 | 217 | 2,302 |
| 64 | 181 | 6,588 |

In 1.5.0 each write paid its own fsync, so throughput stayed flat whatever
the number of writers. From 1.6.0 the writes waiting at the database commit
in one batch and one sync. The single writer is unchanged: it still pays one
sync per write.

`barrel_group_commit_SUITE:concurrent_synced_throughput` runs the 1 and 64
writer cases and prints the ratio and the group sizes.

## The Writer of One Database

Every document write to a database goes through that database's
`barrel_db_server`. This section is where a document's time went in that
process in 1.6.0, what 1.7.0 moved out of it and why, and the result.

### Reproduce

```bash
cd apps/barrel_docdb/bench
./writer_bench.sh cases                                  # the cases
./writer_bench.sh run new_unsynced_64 3000               # docs/s, p50, p99
./writer_bench.sh profile new_unsynced_64 call_time      # tprof per process
./writer_bench.sh profile new_unsynced_64 call_memory
./writer_bench.sh profile new_unsynced_64 call_time 3000 clients
./writer_bench.sh sample new_unsynced_64                 # untraced sampling
./writer_bench.sh phases 5000                            # step costs, untraced
./writer_compare.sh new_unsynced_64 5 3000 /path/to/1.6.0 /path/to/this
```

`ROOT=/path/to/checkout ./writer_bench.sh ...` runs the same harness
against another compiled checkout. The cases write a small execution-record
document (six fields, a nested map, a 200-byte string), from 64 or 1
writers; the synced ones use `#{outbox => [Tag], sync => true}` like
`barrel_group_commit_SUITE:concurrent_synced_throughput`.

### Where a document's time went (1.6.0)

tprof `call_time` on the server process, 64 writers of new documents,
unsynced. Exclusive time per document, traced (tracing inflates small
calls, so read the shares, not the absolute values):

| Bucket | us/doc | Share |
|--------|--------|-------|
| key encoding (`barrel_store_keys`) | 74.5 | 23% |
| lists, maps, binaries (callers mostly the rows above and below) | 72.9 | 23% |
| RocksDB `write_batch` | 48.0 | 15% |
| CBOR (`barrel_docdb_codec_cbor`) | 40.6 | 13% |
| path analysis and index rows (`barrel_ars*`, `barrel_changes`) | 37.8 | 12% |
| RocksDB batch building (a NIF call per op, 75 ops per new doc) | 21.6 | 7% |
| calls (`hlc:now`, `barrel_sub:match`) | 9.5 | 3% |
| reads (entity get) | 3.9 | 1% |

The same steps timed without tracing, one process, per document:

| Step | us | Needs state or HLC |
|------|----|--------------------|
| `make_doc_record` | 0.6 | no |
| body CBOR (`encode_cbor`) | 5.6 | no |
| path analysis and topics | 3.4 | no |
| path index rows of a fresh document | 16.2 | no |
| change row (holds a second, indexed CBOR of the body) | 15.0 | the HLC and rev, not the CBOR |
| path feed rows (17 prefixes per doc here) | 21.7 | the HLC, not the key heads |
| update path diff (old and new bodies analysed) | 6.8 to 9.4 | the old body |
| read of the current state on an update | 10.5 to 13.6 | yes |
| `hlc:now` | 0.9 | yes |
| `barrel_sub:match` | 0.8 | no subscriber: none needed |
| RocksDB batch build and write, groups of 64 | 50.8 | writes in order |

`call_memory`: 9,400 words allocated in the writer per document, 75% in
key encoding, CBOR and path analysis.

Updates spent more in paths (19%) and CBOR (15%): the old body was decoded,
then analysed twice (index diff and feed rows). Synced writes had the same
profile plus the sync (31% in `write_batch`).

### What moved, and why

- **Into the caller** (`barrel_docdb:put_doc`, `put_docs`): the document
  record, both CBOR encodings of the body, path analysis, the path index
  rows of a fresh document, and the key heads of its feed rows. None of it
  needs the database state or the HLC, and it was most of the writer's time.
  The writer reads, checks, stamps the HLC and assembles the same batch in
  the same order (`barrel_writer_SUITE:prepared_batch_equals_inline`
  compares both builds for new docs, updates with outbox tags, TTL,
  provenance, embeddings, tombstones, recreation, with and without
  channels).
- **Subscriber work, when there is none**: a table per subscription
  manager says which databases have subscriptions. With none, a write does
  no path analysis for notification, no call and no cast
  (`barrel_writer_SUITE:no_subscriber_no_notify` traces 1,000 writes).
  With some, a commit sends one cast per manager.
- **The RocksDB write, into a committer process**: once the per-document
  work left, the writer spent its time waiting for `write_batch`. A
  committer writes each group, in order, and answers its callers while the
  writer builds the next group. Unsynced groups are handed over every
  `write_chunk` (16) documents so building and writing overlap. The writer
  builds the RocksDB batch of an unsynced group; a synced group streams its
  ops to the committer, which builds the batch while the group is built,
  then writes it with one sync.
- **Reads, batched**: the current state of the documents of the waiting
  requests is read with two `multi_get` calls instead of one or two dirty
  NIF calls per document.
- **Updates**: the old body is analysed once instead of twice.

### What did not move, and why

- The read, the conflict check, the HLC and every row keyed by it (entity,
  change and feed rows, history, outbox, archive, version chain, channel
  rows): they need the database state or the HLC, so they stay serialized.
- The update path diff and the removal of the old feed rows: they need the
  old body. A caller could read it optimistically, but the profile after
  the other changes puts it at about 20% of an update's writer time, and a
  stale read would need a second check in the writer.
- HLCs for a group in one clock call: `hlc:now` is 3.7 to 5 us per
  document in the final writer (8 to 11%). The clock has no call that
  reserves a range, and deriving HLCs locally lets the TTL sweeper, which
  mints its own on the same keyspace, collide with them. Not worth a
  wrong feed key.
- Pre-encoding posting merge values in the caller: `term_to_binary` inside
  `rocksdb:batch_merge`, about 2 us per document; it would change the op
  list the batch is built from.
- Histogram exemplars in the caller: each `put_doc` records a latency
  histogram whose exemplar reservoir is one ETS row per database, read and
  rewritten by every caller (54 and 83 us traced per call at 64 writers).
  Removing the call did not change throughput (9,800 docs/s either way),
  so it stays; the contention is in the `instrument` library.

### After (this release)

Same case, same harness. The writer now spends 71 us per document traced
(322 before) and the committer 51 us, nearly all in `write_batch`:

| Process | Bucket | us/doc |
|---------|--------|--------|
| writer | RocksDB batch building | 20.9 |
| writer | lists, maps, binaries | 17.0 |
| writer | own code (group building, assembly) | 10.5 |
| writer | feed and index rows (HLC-keyed) | 7.6 |
| writer | reads (two `multi_get` per batch of requests) | 7.5 |
| writer | calls (`hlc:now`) | 2.2 |
| committer | RocksDB `write_batch` | 48.4 |

`call_memory`: 3,500 words per document in the writer, 240 in the
committer. In a synced wave the writer spends 38 us per document and the
committer 150 us, of which 110 in the synced `write_batch`.

### Before and after

64 writers unless noted, one database, median of 5 runs of 3 s per side,
sides alternating in rotating order on a 14-core Mac (Apple Silicon,
APFS). The machine was shared: load average 25 to 130 during the runs, so
compare the sides of one row, not rows between tables. 1.6.0 is the
group-commit release, 1.7.0-ro is the read-only change before this work.

| Case | 1.6.0 docs/s | 1.7.0-ro docs/s | 1.7.0 docs/s | vs 1.6.0 | p50 us (1.6.0 / 1.7.0) | p99 us (1.6.0 / 1.7.0) |
|------|-------------|-----------------|--------------|----------|------------------------|------------------------|
| new docs, unsynced | 6,891 | 7,104 | 14,325 | 2.08x | 8,602 / 3,251 | 10,840 / 10,905 |
| new docs, synced | 4,480 | 4,181 | 6,251 | 1.40x | 12,976 / 9,809 | 16,560 / 14,363 |
| updates, unsynced | 6,677 | 6,741 | 10,768 | 1.61x | 9,584 / 5,792 | 12,033 / 11,838 |
| updates, synced | 4,032 | 4,601 | 5,248 | 1.30x | 13,864 / 12,038 | 19,582 / 15,317 |
| put_docs of 8, unsynced | 6,827 | 6,656 | 20,411 | 2.99x | 74,571 / 23,430 | 103,743 / 46,489 |
| put_docs of 8, synced | 5,973 | 6,144 | 11,435 | 1.91x | 84,246 / 44,341 | 111,041 / 73,192 |
| 1 writer, unsynced (7 runs) | 6,760 | | 6,730 | 1.00x | 141 / 143 | 213 / 207 |
| 1 writer, synced | 83 | 90 | 91 | 1.10x | 5,954 / 5,957 | 31,909 / 32,191 |

A second run of the synced case at load 45 to 80: 4,309 (1.6.0), 4,288
(1.7.0-ro), 5,909 (1.7.0), 1.37x. A third run of the 64-writer new-doc
cases at load 7 to 53: synced 4,971 / 4,928 / 6,272 (1.26x, p50 12,596
/ 10,060 us), unsynced 7,296 / 7,317 / 18,501 (2.54x, p50 8,533 / 3,258
us).

Synced writes stay bound by the sync. 64 synced writers form one group per
sync, and a cycle is the group's build, then its write and sync. Only the
build got shorter. The write and the sync are the same work as before,
since the batch is the same: about 8 ms per group of 64 on this machine
under load (the committer spends 150 us traced per document, 110 of them
in the synced `write_batch`). Splitting a synced group so that its
building overlaps an earlier sync would make documents visible before
their sync, which this release does not do. The 1.5x target for synced
writers is not met here.
## Query Performance

Query performance varies based on query pattern and result set size.

### Index-Only Queries

These queries use `include_docs => false` and benefit from pure index scans:

| Query Type | Throughput | p50 Latency | Notes |
|------------|------------|-------------|-------|
| Prefix with LIMIT 10 | 39,354 ops/s | 23 us | Autocomplete use case |
| Prefix (all matches) | 44,033 ops/s | 18 us | ~1,111 docs returned |
| Simple equality + LIMIT 10 | 27,800 ops/s | 27 us | Early termination |
| Pure compare + LIMIT 10 | 6,222 ops/s | 157 us | Range scan with limit |
| Pure compare (age>50) | 2,101 ops/s | 459 us | ~2,380 docs returned |
| Pure Top-K (ORDER BY + LIMIT) | 3,889 ops/s | 233 us | No filter, just sort |
| Selective equality | 488 ops/s | 2.0 ms | ~1,666 docs (1/3) |
| Nested path | 55 ops/s | 18 ms | Nested object access |
| Top-K with filter | 82 ops/s | 12 ms | ORDER BY + LIMIT + filter |

### Paginated Queries

Paginated queries with continuation tokens deliver excellent performance:

| Page Size | Throughput | p50 Latency |
|-----------|------------|-------------|
| 100 docs/page | 4,906 pages/s | 187 us |
| 500 docs/page | 3,163 pages/s | 347 us |

> #### Pagination is Essential
>
> **Always paginate large result sets.** Fetching thousands of documents in a single query is slow and memory-intensive. Use `limit` and continuation tokens to stream results efficiently:
>
> ```erlang
> %% First page
> {ok, Results, #{continuation := Token}} =
>     barrel_docdb:find(Db, #{where => Query, limit => 100}).
>
> %% Next pages
> {ok, More, #{continuation := NextToken}} =
>     barrel_docdb:find(Db, #{where => Query, limit => 100, continuation => Token}).
> ```
>
> Paginated queries run in **microseconds per page** vs **hundreds of milliseconds** for unbounded queries.

## Changes Feed

| Operation | Throughput | p50 Latency | Notes |
|-----------|------------|-------------|-------|
| Full scan (5K docs) | - | 98 ms | One-time scan |
| Incremental (100/batch) | 2,857 batches/s | 319 us | Continuous polling |
| Subscription notification | 13,823 ops/s | 65 us | Pub/sub latency |

**Subscription latency of 65 us is excellent for real-time applications.**

## Architecture: ARS Model

Barrel DocDB is built on an **ARS (Append, Reduce, Stream)** storage model:

- **Append**: All writes are append-only with MVCC versioning
- **Reduce**: Indexes are derived by reducing over the append-only log
- **Stream**: Changes feed provides a replayable event stream

This architecture enables:

1. **Flexible query engines**: The storage layer is decoupled from query execution. Different query engines can be built on top of the same storage.
2. **Custom databases**: The ARS model can support different data models (document, graph, time-series) with the same underlying storage.
3. **Efficient replication**: Append-only logs are naturally suited for P2P sync.
4. **Time-travel queries**: MVCC enables querying historical states.

The benchmark numbers reflect the current document query engine. Future query engines could optimize for different access patterns (e.g., graph traversal, analytics) while reusing the same storage layer.

## When to Use Barrel DocDB

### Best Use Cases

| Use Case | Why |
|----------|-----|
| **P2P Replication** | Built-in one-shot and continuous sync |
| **Real-time subscriptions** | 65 us notification latency |
| **Prefix/autocomplete** | 18-23 us query latency |
| **Edge computing** | Embedded in Erlang, sync when online |
| **MVCC conflict detection** | Revision tracking built-in |
| **Paginated APIs** | Fast continuation-based pagination |

### Trade-offs

When other tools may be better:

| If you need... | Consider instead |
|----------------|------------------|
| Maximum single-node query speed | SQLite with JSON1, DuckDB |
| Simple key-value access | Direct RocksDB, ETS |
| Complex SQL analytics | PostgreSQL, DuckDB |
| Full-text search | Meilisearch, Elasticsearch |

### Honest Comparison

For **single-node performance only**, other databases will often be faster:

- **SQLite**: 3-10x faster for complex queries, excellent JSON support
- **RocksDB direct**: 2-5x faster for raw key-value access
- **Mnesia**: Native Erlang, different consistency model

Barrel's value is in the **combination** of:

1. Embedded Erlang integration
2. Document model with automatic indexing
3. P2P replication topologies
4. Real-time change subscriptions
5. MVCC for conflict-free sync
6. **ARS architecture** for future extensibility

If you only need single-node storage without replication, simpler tools exist. Choose Barrel when you need **sync**, **real-time**, and **architectural flexibility**.

> #### Remote access
>
> These numbers are for the embedded Erlang API, which is how barrel_docdb is used. To reach a database over HTTP, run the `barrel_server` app; network I/O and JSON serialization add overhead on top of the figures above.

## Running Benchmarks

Run the benchmark suite on your hardware:

```bash
cd bench
./run_bench.sh                 # Default: 10,000 docs, 10,000 iterations
./run_bench.sh 5000 100        # Custom: 5,000 docs, 100 iterations
./run_bench.sh doc_types       # Document type comparison
./run_bench.sh doc_types 500 200  # Doc types with custom docs and iterations
```

Or from Erlang:

```erlang
barrel_bench:run(#{num_docs => 5000, iterations => 100}).

%% Run specific workloads
barrel_bench:run_crud(#{num_docs => 1000}).
barrel_bench:run_query(#{num_docs => 5000, iterations => 100}).
barrel_bench:run_changes(#{num_docs => 1000}).
```

Results are saved to `bench/results/` as JSON with timestamps.

## Query Building Guidelines

Building efficient queries is crucial for performance. See the [Query Guide](queries.md) for full syntax reference.

### Rule 1: Always Paginate

Never fetch unbounded result sets. Use `limit` and continuation tokens:

```erlang
%% BAD: Fetches all matching documents
{ok, All, _} = barrel_docdb:find(Db, #{where => Query}).

%% GOOD: Paginate with continuation
{ok, Page, #{continuation := Token}} =
    barrel_docdb:find(Db, #{where => Query, limit => 100}).
```

### Rule 2: Use Index-Only Queries When Possible

Set `include_docs => false` to skip document body fetches:

```erlang
%% Returns only doc IDs - 18-27 us
{ok, Ids, _} = barrel_docdb:find(Db, #{
    where => [{path, [<<"type">>], <<"user">>}],
    include_docs => false,
    limit => 100
}).

%% Then fetch specific docs you need
{ok, Doc} = barrel_docdb:get_doc(Db, hd(Ids)).
```

### Rule 3: Put Selective Conditions First

More selective conditions reduce the search space:

```erlang
%% GOOD: Most selective condition first
#{where => [
    {path, [<<"user_id">>], <<"specific_user">>},  %% Very selective
    {path, [<<"type">>], <<"event">>}               %% Less selective
]}

%% LESS OPTIMAL: Broad condition first
#{where => [
    {path, [<<"type">>], <<"event">>},              %% Matches many docs
    {path, [<<"user_id">>], <<"specific_user">>}
]}
```

### Rule 4: Prefer Prefix Over Regex

Prefix queries use efficient index range scans:

```erlang
%% FAST: 18-23 us - Uses index range scan
{prefix, [<<"name">>], <<"John">>}

%% SLOW: Full scan with regex matching
{regex, [<<"name">>], <<"^John.*">>}
```

### Rule 5: Use LIMIT with ORDER BY

Top-K queries are fast when limited:

```erlang
%% FAST: 233 us - Early termination
#{where => [],
  order_by => {[<<"created_at">>], desc},
  limit => 10}

%% SLOW: Must sort all documents first
#{where => [],
  order_by => {[<<"created_at">>], desc}}
```

### Query Execution Strategies

Use `explain/2` to see how queries execute:

```erlang
{ok, Plan} = barrel_docdb:explain(Db, Query).
%% Plan.strategy tells you the execution path
```

| Strategy | Performance | When Used |
|----------|-------------|-----------|
| `index_seek` | Excellent | Equality on indexed path |
| `index_scan` | Good | Range queries, prefix |
| `multi_index` | Good | Multiple conditions |
| `full_scan` | Avoid | No index available |

### Anti-Patterns to Avoid

| Pattern | Problem | Solution |
|---------|---------|----------|
| No LIMIT | Fetches entire database | Always paginate |
| OR with many terms | Creates large unions | Consider multiple queries |
| NOT on large sets | Scans exclusions | Restructure query |
| Regex for prefix | Full scan | Use `{prefix, ...}` |
| Sorting without limit | Sorts all results | Add LIMIT |

## Optimizing Performance

### Write Optimization

1. **Batch writes** for bulk inserts
2. **Write concurrently** to one database: concurrent writes share a batch and a sync
3. **Disable sync** for non-critical writes: `#{sync => false}`
4. **Use specific paths** in change subscriptions vs wildcards

### Configuration Tuning

See [Architecture](design.md) for RocksDB tuning:

- Adjust block cache size for your memory budget
- Configure write buffer size for write-heavy workloads
- Enable compression for large documents
