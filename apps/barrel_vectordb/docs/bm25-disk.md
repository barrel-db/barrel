# Disk-Native BM25 Backend

barrel_vectordb provides a disk-native BM25 backend optimized for large-scale
text search with predictable memory usage and fast query performance.

## Overview

The `bm25_disk` backend stores inverted indexes on SSD with Block-Max MaxScore
for early termination, similar to how production search engines like Elasticsearch
and Lucene operate.

### Key Features

- **Disk-native storage** - Postings on SSD, only metadata in RAM
- **Block-Max MaxScore** - Early termination skips 95%+ of postings
- **Hot layer** - Sub-millisecond writes, background compaction
- **O(1) startup** - Lazy loading, no full index load
- **Hybrid search** - Combine BM25 + vector search with RRF or linear fusion

### When to Use

| Use Case | Recommended Backend |
|----------|---------------------|
| Small corpus (<100K docs) | `memory` - simpler, lower latency |
| Large corpus (>100K docs) | `disk` - predictable memory |
| Memory-constrained environments | `disk` |
| Write-heavy workloads | `disk` with large hot layer |
| Hybrid search (BM25 + vectors) | Either - both support hybrid |

### Memory Usage

| Component | Size | Notes |
|-----------|------|-------|
| Lexicon | ~40 bytes/term | Term → integer ID in RocksDB |
| Block-max index | ~50 bytes/block | mmap'd, OS cached |
| Doc stats | ~8 bytes/doc | In ETS |
| Hot layer | configurable | Default 50K docs max |

**Example sizing for 1M documents with 500K unique terms:**

- **Memory backend:** ~500 MB - 1 GB
- **Disk backend:** ~50 MB + OS page cache

## Configuration

### Standalone Store

```erlang
%% Create store with disk BM25
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/data/my_store",
    dimensions => 768,
    bm25_backend => disk,
    bm25_disk => #{
        base_path => "/data/my_store/bm25",
        hot_max_size => 50000,
        hot_compaction_threshold => 0.8,
        k1 => 1.2,
        b => 0.75
    }
}).
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `base_path` | `{store_path}/bm25` | Directory for BM25 files |
| `hot_max_size` | `50000` | Max docs in hot layer before compaction |
| `hot_compaction_threshold` | `0.8` | Trigger compaction at this % of max |
| `k1` | `1.2` | BM25 term frequency saturation |
| `b` | `0.75` | BM25 document length normalization |
| `block_size` | `128` | Documents per posting block |

### BM25 Parameters

The BM25 scoring formula is:

```
score = sum(IDF(t) * TF(t,d) * (k1 + 1) / (TF(t,d) + k1 * (1 - b + b * |d| / avgdl)))
```

- **k1** (default: 1.2) - Controls term frequency saturation. Higher values give more weight to repeated terms.
- **b** (default: 0.75) - Controls document length normalization. 0 = no normalization, 1 = full normalization.

## API Reference

### Adding Documents

Documents are automatically indexed in BM25 when added to the store:

```erlang
%% Add single document - automatically indexed in BM25
ok = barrel_vectordb:add(Store, <<"doc1">>, <<"Hello world">>, #{}).

%% Add with metadata
ok = barrel_vectordb:add(Store, <<"doc2">>, <<"Erlang is great">>,
                         #{type => article, author => <<"joe">>}).

%% Batch add (more efficient)
{ok, _} = barrel_vectordb:add_batch(Store, [
    {<<"doc3">>, <<"First document">>, #{}},
    {<<"doc4">>, <<"Second document">>, #{}}
]).
```

### BM25 Search

```erlang
%% Basic BM25 search
{ok, Results} = barrel_vectordb:search_bm25(Store, <<"erlang programming">>, #{k => 10}).
%% Returns: [{<<"doc2">>, 2.45}, {<<"doc1">>, 1.82}, ...]

%% With more results
{ok, Results} = barrel_vectordb:search_bm25(Store, <<"erlang">>, #{k => 50}).
```

### Hybrid Search (BM25 + Vector)

Combine keyword matching with semantic similarity:

```erlang
%% Combined search with RRF fusion (recommended)
{ok, Results} = barrel_vectordb:search_hybrid(Store, <<"erlang concurrency">>, #{
    k => 10,
    bm25_weight => 0.5,
    vector_weight => 0.5,
    fusion => rrf
}).

%% Linear fusion (simpler, score-based)
{ok, Results} = barrel_vectordb:search_hybrid(Store, <<"erlang">>, #{
    k => 10,
    bm25_weight => 0.3,
    vector_weight => 0.7,
    fusion => linear
}).
```

**Fusion Algorithms:**

| Algorithm | Description | Best For |
|-----------|-------------|----------|
| `rrf` | Reciprocal Rank Fusion with k=60 | General use, rank-based |
| `linear` | Weighted sum of normalized scores | When score magnitude matters |

### Index Management

```erlang
%% Force compaction (hot layer -> disk)
ok = barrel_vectordb_server:bm25_compact(Store).

%% Get index statistics
{ok, Stats} = barrel_vectordb_server:bm25_info(Store).
%% Returns: #{
%%     backend => disk,
%%     total_docs => 100000,
%%     vocab_size => 50000,
%%     hot_docs => 1234,
%%     disk_docs => 98766,
%%     avgdl => 150.5
%% }
```

## Architecture

### File Layout

```
/data/my_store/bm25/
├── bm25.meta           # Header + global stats (4KB aligned)
├── bm25.postings       # Compressed posting blocks (4KB aligned)
├── bm25.blockmax       # Block-max index (mmap'd)
└── bm25.ids/           # RocksDB: term/doc ID mapping
    ├── terms_fwd       # term string → integer ID
    ├── terms_rev       # integer ID → term string
    ├── docs_fwd        # doc string ID → integer ID
    └── docs_rev        # integer ID → doc string ID
```

### Block-Max MaxScore Algorithm

The search algorithm efficiently skips blocks that cannot contribute to top-k:

```
1. Initialize
   - Load block-max lists for query terms
   - Set threshold = 0

2. For each document position:
   a. Find pivot: smallest doc_id where sum(max_impacts) > threshold
   b. For each term's block at pivot:
      - If block.max_impact < threshold - accumulated: SKIP block
      - Otherwise: decompress and score
   c. If score > threshold:
      - Add to result heap
      - Update threshold = heap.min_score

3. Return top-K from heap
```

This typically skips 95%+ of postings for selective queries.

### Hot Layer Architecture

New documents go to an in-memory hot layer for fast writes:

```
Write Request
     │
     ▼
┌─────────────┐
│  Hot Layer  │ ← In-memory maps
│  (fast)     │   Sub-ms writes
└──────┬──────┘
       │ Compaction (at 80% capacity)
       ▼
┌─────────────┐
│ Disk Layer  │ ← Block-max postings
│ (large)     │   mmap'd block-max index
└─────────────┘
       │
       ▼
   Search combines both layers
```

### Varint Encoding

Postings use delta-encoded varints for compact storage:

```
Original doc IDs:  [100, 105, 108, 150]
Delta encoded:     [100,   5,   3,  42]
Varint bytes:      [0x64, 0x05, 0x03, 0x2A]  (4 bytes vs 16 bytes)
```

## Performance Tuning

### Memory vs Latency Tradeoffs

| Setting | Effect |
|---------|--------|
| Increase `hot_max_size` | More RAM, fewer compactions, faster writes |
| Decrease `hot_max_size` | Less RAM, more frequent compactions |
| Decrease `block_size` | Finer skip granularity, more overhead |
| Larger OS page cache | More hot blocks in RAM, lower disk reads |

### Recommended Settings

**Write-heavy workload:**
```erlang
#{
    hot_max_size => 100000,
    hot_compaction_threshold => 0.9
}
```

**Read-heavy workload:**
```erlang
#{
    hot_max_size => 10000,
    hot_compaction_threshold => 0.5
}
```

**Memory-constrained:**
```erlang
#{
    hot_max_size => 5000
    %% Rely on disk + OS page cache
}
```

**High-throughput hybrid search:**
```erlang
#{
    hot_max_size => 50000,
    %% Ensure embedder is configured for vector search
}
```

## Comparison with Memory Backend

| Feature | Memory Backend | Disk Backend |
|---------|----------------|--------------|
| Storage | In-memory maps | Disk + hot layer |
| Startup | Load all data | O(1) lazy load |
| Memory | O(corpus size) | O(vocab + hot) |
| Write latency | ~microseconds | ~milliseconds |
| Search latency | ~milliseconds | ~milliseconds |
| Max corpus | Limited by RAM | Limited by SSD |
| Persistence | No | Yes |

## Migration Guide

### From Memory Backend

```erlang
%% 1. Stop writes to the store
%% 2. Update config to use disk backend
NewConfig = OldConfig#{bm25_backend => disk},

%% 3. Restart store - documents need to be re-indexed
barrel_vectordb:stop(Store),
{ok, _} = barrel_vectordb:start_link(NewConfig).

%% 4. Re-index existing documents if needed
%% (Documents added after restart will be auto-indexed)
```

### From External Search Engine

```erlang
%% Export documents and index them
Documents = external_search:export_all(),
lists:foreach(fun({Id, Text, Meta}) ->
    barrel_vectordb:add(Store, Id, Text, Meta)
end, Documents).
```

## Troubleshooting

### High Memory Usage

If memory usage is higher than expected:

1. Check hot layer size: `barrel_vectordb_server:bm25_info(Store)`
2. Reduce `hot_max_size` in config
3. Lower `hot_compaction_threshold` for more frequent compaction

### Slow Search

If BM25 searches are slow:

1. Ensure OS has sufficient page cache for block-max index
2. Check if hot layer is very large (may need compaction)
3. For multi-term queries, ensure block-max index is loaded

### Disk Space

The disk backend uses approximately:

- ~50 bytes per posting (term-doc pair) after compression
- ~20 bytes per block-max entry
- RocksDB overhead for ID mapping

For 1M documents with average 100 terms each:
- Postings: ~5 GB
- Block-max: ~100 MB
- ID mapping: ~50 MB

## Testing

The BM25 implementation includes tests for formula correctness and relevance quality.

### Running BM25 Tests

```bash
# Formula correctness tests (11 tests)
rebar3 eunit --module=barrel_vectordb_bm25_formula_tests

# Corpus-based relevance tests (10 tests)
rebar3 eunit --module=barrel_vectordb_bm25_corpus_tests

# Both modules
rebar3 eunit --module=barrel_vectordb_bm25_formula_tests,barrel_vectordb_bm25_corpus_tests

# All BM25 tests (includes memory backend tests)
rebar3 eunit --module=barrel_vectordb_bm25_tests,barrel_vectordb_bm25_disk_tests,barrel_vectordb_bm25_formula_tests,barrel_vectordb_bm25_corpus_tests
```

### Formula Correctness Tests

Located in `test/barrel_vectordb_bm25_formula_tests.erl`, these tests verify the BM25 scoring formula with hand-calculated expected values:

| Test | Description |
|------|-------------|
| `single_term_single_doc` | Verifies basic BM25 score calculation |
| `multi_term_score` | Scores should sum across query terms |
| `tf_impact` | Higher TF should increase score |
| `idf_impact` | Rare terms should have higher IDF contribution |
| `doc_length_normalization` | Shorter docs favored with equal TF |
| `k1_parameter` | k1=0 saturates TF immediately, k1=high allows more TF effect |
| `b_parameter` | b=0 no length normalization, b=1 full normalization |
| `tf_saturation` | Score(TF=10) < 2 × Score(TF=5) due to diminishing returns |
| `edge_cases` | Empty queries, missing terms, single doc corpus |
| `hand_calculated_example` | Full worked example from documentation |
| `backend_consistency` | Memory and disk backends produce identical scores |

### Corpus-Based Relevance Tests

Located in `test/barrel_vectordb_bm25_corpus_tests.erl`, these tests evaluate search quality using IR metrics:

**Test Corpus:** 100 documents across 5 topic clusters:

- Erlang/OTP (docs 1-20)
- Python/Data Science (docs 21-40)
- Java/Enterprise (docs 41-60)
- Distributed Systems (docs 61-80)
- General Programming (docs 81-100)

**Test Queries:** 20 queries with relevance judgments covering:

- Single-term topic queries
- Multi-term queries
- Cross-topic queries
- Rare term queries (high IDF)
- Common term queries (low IDF)

**Metrics Evaluated:**

| Test | Description |
|------|-------------|
| `precision_at_5` | P@5 averaged across all queries |
| `precision_at_10` | P@10 averaged across all queries |
| `recall_at_10` | R@10 for highly relevant documents |
| `ndcg_at_10` | Normalized DCG with graded relevance |
| `relevant_in_top_k` | At least one relevant doc in top 5 for topic queries |
| `ranking_order` | Results sorted by descending score |
| `cross_topic_queries` | Queries matching multiple topic clusters |
| `rare_term_queries` | High IDF queries return correct results |
| `memory_disk_consistency` | Both backends produce similar results |

### Test Data Files

| File | Description |
|------|-------------|
| `test/data/bm25_test_corpus.terms` | 100 documents in Erlang term format |
| `test/data/bm25_test_queries.terms` | 20 queries with relevance judgments |

### Adding Custom Test Documents

The corpus file uses Erlang term format:

```erlang
%% test/data/bm25_test_corpus.terms
[
    {<<"doc1">>, <<"Your document text here">>},
    {<<"doc2">>, <<"Another document">>},
    ...
].
```

Query judgments specify relevant and partially relevant documents:

```erlang
%% test/data/bm25_test_queries.terms
[
    {<<"query text">>, #{
        relevant => [1, 2, 3],      %% Highly relevant doc numbers
        partial => [10, 15]         %% Partially relevant
    }},
    ...
].
```
