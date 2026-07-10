# barrel_vectordb Features

Additional features and advanced functionality for barrel_vectordb.

## DiskANN Index

DiskANN is an SSD-optimized vector index based on the Vamana graph algorithm. It provides efficient billion-scale vector search with sub-linear memory usage.

### Key Features

- **Two-pass Vamana construction** - Builds a high-quality graph with alpha-RNG pruning
- **FreshVamana streaming updates** - Insert and delete without full rebuild
- **Hot layer** - Sub-millisecond writes absorbed in memory, compacted to disk
- **Lazy graph loading** - O(1) startup time with on-demand node loading
- **PQ compression** - Product quantization for reduced memory footprint
- **4KB sector-aligned I/O** - Optimized for SSD performance

### Usage

```erlang
%% Create a new DiskANN index
{ok, Index} = barrel_vectordb_diskann:new(#{
    dimension => 768,
    r => 64,              %% Max out-degree
    l_build => 100,       %% Build search width
    l_search => 100,      %% Query search width
    alpha => 1.2,         %% Pruning factor (>1 for long-range edges)
    distance_fn => cosine %% cosine or euclidean
}).

%% Build from a list of vectors
Vectors = [{<<"id1">>, Vector1}, {<<"id2">>, Vector2}, ...],
{ok, Index1} = barrel_vectordb_diskann:build(Options, Vectors).

%% Insert a single vector
{ok, Index2} = barrel_vectordb_diskann:insert(Index1, <<"id3">>, Vector3).

%% Batch insert (more efficient)
{ok, Index3} = barrel_vectordb_diskann:insert_batch(Index2, [
    {<<"id4">>, Vector4},
    {<<"id5">>, Vector5}
], #{}).

%% Search for K nearest neighbors
Results = barrel_vectordb_diskann:search(Index3, QueryVector, 10).
%% => [{<<"id1">>, 0.95}, {<<"id2">>, 0.89}, ...]

%% Search with options
Results = barrel_vectordb_diskann:search(Index3, QueryVector, 10, #{
    l_search => 200  %% Higher = better recall, slower
}).

%% Delete a vector (lazy delete)
{ok, Index4} = barrel_vectordb_diskann:delete(Index3, <<"id1">>).

%% Check if consolidation needed (deleted > 10% of active)
case barrel_vectordb_diskann:needs_consolidation(Index4) of
    true -> {ok, Index5} = barrel_vectordb_diskann:consolidate_deletes(Index4);
    false -> Index5 = Index4
end.

%% Get index info
Info = barrel_vectordb_diskann:info(Index5).
%% => #{size => 4, dimension => 768, r => 64, ...}
```

### Disk Mode

For large indexes, use disk mode for persistence and lazy loading:

```erlang
%% Create with disk storage
{ok, Index} = barrel_vectordb_diskann:new(#{
    dimension => 768,
    storage_mode => disk,
    base_path => <<"/path/to/index">>
}).

%% Build and persist
{ok, Index1} = barrel_vectordb_diskann:build(Options, Vectors),
ok = barrel_vectordb_diskann:sync(Index1).

%% Open existing index (O(1) startup)
{ok, Index2} = barrel_vectordb_diskann:open(<<"/path/to/index">>).

%% Close when done
ok = barrel_vectordb_diskann:close(Index2).
```

### Hot Layer

Enable the hot layer for sub-millisecond write latency:

```erlang
{ok, Index} = barrel_vectordb_diskann:new(#{
    dimension => 768,
    storage_mode => disk,
    base_path => <<"/path/to/index">>,
    hot_enabled => true,
    hot_max_size => 10000,           %% Max vectors in hot layer
    hot_compaction_threshold => 0.8  %% Compact at 80% capacity
}).

%% Inserts go to hot layer first (sub-ms latency)
{ok, Index1} = barrel_vectordb_diskann:insert(Index, <<"id">>, Vector).

%% Search combines hot layer + disk results
Results = barrel_vectordb_diskann:search(Index1, Query, 10).

%% Manual compaction (hot layer -> disk)
{ok, Index2} = barrel_vectordb_diskann:compact(Index1).
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dimension` | required | Vector dimension |
| `r` | 64 | Max out-degree (connections per node) |
| `l_build` | 100 | Search width during construction |
| `l_search` | 100 | Default search width for queries |
| `alpha` | 1.2 | Pruning factor (>1 keeps long-range edges) |
| `distance_fn` | cosine | Distance function: `cosine` or `euclidean` |
| `storage_mode` | memory | `memory` or `disk` |
| `use_pq` | false | Enable product quantization |

### File Layout (Disk Mode)

```
/path/to/index/
  diskann.meta     # Erlang term metadata
  diskann.graph    # Vamana graph (4KB aligned nodes)
  diskann.vectors  # Full float32 vectors (4KB aligned)
  diskann.pq       # PQ codebooks + codes
  diskann.idmap    # ID to offset mapping
```

## Document Backend (docstore)

By default a store keeps each vector's text and metadata in its own RocksDB
column families, written in the same atomic batch as the vector. Set `docstore`
to move them somewhere else, so documents and vectors can share one source of
truth. Vectors always stay local.

`barrel_vectordb_docdb_backend` stores them as documents in a `barrel_docdb`
database:

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => docs,
    dimension => 768,
    docstore => {barrel_vectordb_docdb_backend, #{db => <<"docs">>}}
}).
```

Options: `db` (the docdb database name, default the store name) and `docdb_opts`
(passed to `barrel_docdb:create_db/2`). Metadata round-trips through
`term_to_binary`, so atom keys and nested terms survive exactly.

The backend is optional and resolved at runtime from this config, so
`barrel_vectordb` does not depend on `barrel_docdb`. Add `barrel_docdb` to your
own application to use it. Writes are vector-first, doc-second: the vector batch
commits, then text and metadata are written.

Implement `barrel_vectordb_docstore` to route them anywhere else.

## Benchmarks

barrel_vectordb includes a comprehensive benchmark suite for measuring performance.

### Quick Start

```bash
# Run all benchmarks
./scripts/run_benchmarks.sh

# Quick run (fewer iterations)
./scripts/run_benchmarks.sh --quick

# Full benchmark suite
./scripts/run_benchmarks.sh --full

# Export results
./scripts/run_benchmarks.sh --json
./scripts/run_benchmarks.sh --csv
./scripts/run_benchmarks.sh --all-formats

# Compare with baseline
./scripts/run_benchmarks.sh --compare baseline.json
```

### Programmatic Usage

```erlang
%% Run all benchmarks
rebar3 as bench shell
barrel_vectordb_bench:run_all().

%% Run with options
barrel_vectordb_bench:run_all(#{
    iterations => 100,
    warmup_iterations => 10,
    dimension => 128,
    output_format => json,  %% console | json | csv | all
    output_file => "bench_results"
}).

%% Run a specific benchmark
{ok, Result} = barrel_vectordb_bench:run(insert_single).
{ok, Result} = barrel_vectordb_bench:run(search_k10, #{iterations => 500}).
```

### Available Benchmarks

| Benchmark | Description |
|-----------|-------------|
| `insert_single` | Single vector insert |
| `insert_batch_10` | Batch insert 10 vectors |
| `insert_batch_100` | Batch insert 100 vectors |
| `insert_batch_1000` | Batch insert 1000 vectors |
| `search_k1` | Search for top 1 result |
| `search_k10` | Search for top 10 results |
| `search_k50` | Search for top 50 results |
| `search_filtered` | Search with metadata filter |
| `index_build_1k` | Build index with 1K vectors |
| `index_build_10k` | Build index with 10K vectors |
| `get_single` | Get single document by ID |
| `delete_single` | Delete single document |
| `concurrent_writers` | Concurrent write operations |

### Backend Comparison

Compare HNSW, FAISS, and DiskANN performance:

```bash
# Quick comparison
./scripts/run_backend_bench.sh --quick

# Default comparison
./scripts/run_backend_bench.sh

# Full benchmark suite
./scripts/run_backend_bench.sh --full
```

Or programmatically:

```erlang
rebar3 as bench_faiss shell
barrel_vectordb_backend_bench:run_all().
```

#### Backend Overview

| Backend | Memory Usage | Build Time | Search Latency | Best For |
|---------|--------------|------------|----------------|----------|
| **HNSW** | High (~4KB/vector) | Fast | ~0.25ms | Small-medium datasets, low latency |
| **FAISS** | Medium | Fast | Very fast | GPU acceleration, batch queries |
| **DiskANN** | Low (~530B/vector in disk mode) | Slower | ~1ms | Large-scale datasets, memory-constrained |

#### DiskANN Backend

DiskANN is ideal for billion-scale vector search where memory is limited:

- **Memory mode**: Vectors in RAM, graph + PQ codes in memory
- **Disk mode**: Vectors on SSD with LRU cache, ~7.5x memory reduction vs HNSW

See the [DiskANN Index](#diskann-index) section above for detailed API documentation.

### Benchmark Output

Results include:

- **ops_per_sec** - Operations per second (throughput)
- **mean_ms** - Mean latency in milliseconds
- **p50_ms** - Median latency
- **p95_ms** - 95th percentile latency
- **p99_ms** - 99th percentile latency
- **min_ms** / **max_ms** - Min/max latency

Example JSON output:

```json
{
  "benchmark": "search_k10",
  "ops_per_sec": 12500,
  "mean_ms": 0.08,
  "p50_ms": 0.07,
  "p95_ms": 0.12,
  "p99_ms": 0.18,
  "iterations": 100
}
```

### Comparing Results

```bash
# Run baseline
./scripts/run_benchmarks.sh --json
mv bench_results.json baseline.json

# Make changes, then compare
./scripts/run_benchmarks.sh --compare baseline.json
```

The comparison shows percentage differences for each metric, highlighting regressions and improvements.
