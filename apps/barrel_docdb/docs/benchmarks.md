# Benchmarks

Performance benchmarks for Barrel DocDB operations using the built-in benchmark suite.

!!! note "Test Environment"
    - **Hardware**: Apple M1, 16GB RAM, SSD
    - **Erlang/OTP**: 27
    - **Dataset**: 5,000 documents (~500 bytes each)
    - **Database**: Single node, default configuration

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

!!! tip "Pagination is Essential"
    **Always paginate large result sets.** Fetching thousands of documents in a single query is slow and memory-intensive. Use `limit` and continuation tokens to stream results efficiently:

    ```erlang
    %% First page
    {ok, Results, #{continuation := Token}} =
        barrel_docdb:find(Db, #{where => Query, limit => 100}).

    %% Next pages
    {ok, More, #{continuation := NextToken}} =
        barrel_docdb:find(Db, #{where => Query, limit => 100, continuation => Token}).
    ```

    Paginated queries run in **microseconds per page** vs **hundreds of milliseconds** for unbounded queries.

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

## HTTP API vs Direct Erlang API

The HTTP API provides remote access but adds overhead from network I/O, JSON serialization, and authentication.

### Regular Database (Non-Sharded)

| Operation | Direct API (ops/s) | HTTP API (ops/s) | HTTP Overhead |
|-----------|-------------------|------------------|---------------|
| Insert | 8,223 | 1,206 | 85% |
| Read | 18,900 | 790 | 96% |
| Update | 38,066 | 1,124 | 97% |

!!! tip "HTTP API Usage Guidelines"
    **Use the direct Erlang API when possible.** The HTTP API is best for:

    - Cross-language access (Python, JavaScript, etc.)
    - External service integration
    - Administrative operations from CLI tools
    - Distributed deployments where HTTP is required

    For Erlang applications running on the same node, always prefer the direct API (100-200x faster for simple operations).

### Connection Pooling

The benchmarks above use single connections. In production, use HTTP connection pooling:

```erlang
%% With hackney pool
Options = [{pool, my_barrel_pool}],
hackney:request(get, Url, Headers, Body, Options).
```

Connection pooling can improve HTTP throughput by 3-5x by reusing TCP connections.

## Running Benchmarks

Run the benchmark suite on your hardware:

```bash
cd bench
./run_bench.sh              # Default: 10,000 docs, 10,000 iterations
./run_bench.sh 5000 100     # Custom: 5,000 docs, 100 iterations
./run_bench.sh http 1000 100 # HTTP API vs Direct API comparison
```

Or from Erlang:

```erlang
barrel_bench:run(#{num_docs => 5000, iterations => 100}).

%% Run specific workloads
barrel_bench:run_crud(#{num_docs => 1000}).
barrel_bench:run_query(#{num_docs => 5000, iterations => 100}).
barrel_bench:run_changes(#{num_docs => 1000}).
barrel_bench:run_http(#{num_docs => 1000, iterations => 100}).
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
2. **Disable sync** for non-critical writes: `#{sync => false}`
3. **Use specific paths** in change subscriptions vs wildcards

### Configuration Tuning

See [Architecture](design.md) for RocksDB tuning:

- Adjust block cache size for your memory budget
- Configure write buffer size for write-heavy workloads
- Enable compression for large documents
