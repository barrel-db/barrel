# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-06-14

### Removed

- **Clustering**: removed the Ra/Raft mesh, sharding, scatter-gather, and the
  cluster API (`start_cluster`, `cluster_join`, `cluster_status`,
  `create_collection`, `cluster_add`, `cluster_search`, and related functions).
- **HTTP API**: removed the cowboy-based HTTP server, routes, and handlers.
- **Multi-tenant gateway**: removed the gateway, API keys, quotas, and rate limiting.
- Dropped the `ra` and `cowboy` dependencies.

barrel_vectordb is now an embedded-only library. The cluster and HTTP code is
preserved on the `backup/cluster-http-api` branch.

## [1.5.0] - 2026-02-03

### Added

- **BM25 Disk Backend**: RocksDB-based persistent BM25 index with Block-Max MaxScore algorithm
  - Disk-native storage using RocksDB column families for term posting lists
  - Block-Max indexing for faster query evaluation with early termination
  - MaxScore optimization that skips low-scoring documents
  - Configurable block size (default: 128 documents per block)
  - Full persistence with automatic recovery on restart
  - New module: `barrel_vectordb_bm25_disk`

- **BM25 Cluster Integration**: Distributed BM25 search with scatter-gather
  - Scatter queries across shards, gather and merge results
  - IDF synchronization across cluster for consistent scoring
  - Global document statistics for accurate term weighting
  - Works with both memory and disk backends

- **BM25 HTTP Endpoints**: REST API for BM25 and hybrid search
  - `POST /vectordb/collections/:collection/search/bm25` - Keyword search
  - `POST /vectordb/collections/:collection/search/hybrid` - Combined BM25 + vector search
  - Fusion algorithms: RRF (Reciprocal Rank Fusion) and linear combination
  - Configurable weights for BM25 and vector components

- **BM25 Formula Correctness Tests**: Verification suite with hand-calculated expected scores
  - 11 formula tests covering TF/IDF impact, parameter effects, edge cases
  - 100-document test corpus across 5 topic clusters
  - 20 test queries with relevance judgments
  - IR evaluation metrics: Precision@K, Recall@K, nDCG@10

- **BM25 Performance Benchmarks**: Benchmark suite for BM25 backends
  - Memory vs disk backend comparison
  - Index build, search, and update performance metrics

### Fixed

- Fix hybrid_search benchmark to handle missing embedder gracefully

## [1.4.0] - 2026-01-18

### Added

- **Multi-Tenant HTTP Gateway**: REST API with complete multi-tenancy support
  - Tenant isolation via API key authentication (`X-Api-Key` header)
  - Transparent collection name prefixing (`{hash}_{tenant}_{collection}`)
  - Token bucket rate limiting per tenant (configurable RPM)
  - Quota enforcement for vectors, collections, and storage per tenant
  - Backend abstraction supporting both standalone and clustered deployments
  - Admin endpoints for tenant management, API key rotation, and usage monitoring
  - New modules: `barrel_vectordb_gateway`, `barrel_vectordb_gateway_keys`,
    `barrel_vectordb_gateway_quotas`, `barrel_vectordb_gateway_rate`,
    `barrel_vectordb_gateway_stores`, `barrel_vectordb_system_db`
  - Docker support with `docker-compose.gateway.yml` and environment variables
  - See [Gateway Documentation](docs/gateway.md) for full details

## [1.3.1] - 2026-01-10

### Fixed

- Fix cluster collection creation timeout with retry logic:
  - Increase DEFAULT_TIMEOUT from 5000ms to 10000ms
  - Add LONG_TIMEOUT (30000ms) for create_collection operations
  - Add retry logic with exponential backoff for node registration
  - Nodes now retry up to 5 times when registering in the Ra state machine
  - Fixes bug where nodes join Ra Raft cluster but fail to register in application-level state machine

## [1.3.0] - 2026-01-09

### Changed

- **Remove jsx dependency**: Replace `jsx` library with Erlang's built-in `json` module (OTP 27+)
  - `jsx:encode/1` → `iolist_to_binary(json:encode/1)`
  - `jsx:decode/2` → `json:decode/1`

### Fixed

- Fix 49 dialyzer warnings across cluster and embedding modules:
  - Add `aten`, `gen_batch_server` to plt_extra_apps
  - Fix unmatched return values in Ra, cluster events, discovery, health, mesh modules
  - Fix typo: `barrel_vectordb_embedder` → `barrel_vectordb_embed` in scatter module
  - Fix key name: `dimension` → `dimensions` in shard_manager
  - Fix unreachable patterns in http_handlers and reshard modules
  - Replace `lager:error` with `logger:error` in embed_local
  - Update `create_collection` spec to match Ra state machine return type

### Added

- Comprehensive test coverage for scatter module (19 new tests)
  - Tests for `search/4`, `search_vector/3`, `search_local_shard/3`
  - Tests for embedder configuration, local/remote shards, RPC handling
  - Tests for result gathering, deduplication, and score ordering

## [1.2.5] - 2026-01-05

### Fixed

- Fix dimension config propagation: pass `dimensions` key to embed provider init in `barrel_vectordb_store`
- Add error handling for empty embeddings in `barrel_vectordb_embed_local:embed/2`
- Handle edge cases: empty vector `[[]]`, no embeddings `[]`, and unexpected result formats

### Added

- Unit tests for embed error handling in `barrel_vectordb_embed_local_tests`

## [1.2.4] - 2026-01-05

### Changed

- Update `rocksdb` dependency from 2.2.0 to 2.4.1

## [1.2.3] - 2026-01-04

### Fixed

- Fix CI cache configuration: invalidate old cache and use proper path expansion

## [1.2.2] - 2026-01-04

### Fixed

- Fix dialyzer warnings in `barrel_vectordb_models` (dead code removal, type spec update)
- Fix dialyzer warnings in `barrel_vectordb_python_queue` (unmatched return values)
- Exclude optional `barrel_vectordb_index_faiss` module from dialyzer checks

## [1.2.1] - 2026-01-04

### Fixed

- Add missing `gen_batch_server` to application dependencies in app.src

## [1.2.0] - 2026-01-04

### Added

- **FAISS backend**: Optional high-performance vector indexing via [barrel_faiss](https://gitlab.enki.io/barrel-db/barrel_faiss)
- **Pluggable backend architecture**: New `barrel_vectordb_index` behaviour for index backends
- **Backend selection**: Choose backend at store initialization with `#{backend => hnsw | faiss}`
- **FAISS features**:
  - HNSW32 index type (fast approximate search)
  - Soft delete with compact/rebuild support
  - Cosine and Euclidean distance functions
  - Full serialization/deserialization support
- **Backend benchmarks**: New `barrel_vectordb_backend_bench` module for comparing backends
- **Benchmark script**: `scripts/run_backend_bench.sh` for easy performance testing

### Performance

FAISS vs HNSW benchmark results (64 dimensions, 500 vectors):

| Operation | HNSW | FAISS | Winner |
|-----------|------|-------|--------|
| insert_single | 14.5K ops/s | 23.8K ops/s | FAISS 1.6x |
| insert_batch_100 | 3.6K ops/s | 11.0K ops/s | FAISS 3.1x |
| search_k10 | 2.2K ops/s | 5.0K ops/s | FAISS 2.2x |
| index_build_1k | 729 ops/s | 4.7K ops/s | FAISS 6.5x |
| delete_single | 34.7K ops/s | 19.4K ops/s | HNSW 1.8x |

### Changed

- **State record**: `hnsw_index` renamed to `index` in server state
- **Stats output**: Index info now under `index` key instead of `hnsw`

## [1.1.0] - 2026-01-01

### Added

- **Search options**: `include_text` and `include_metadata` options to skip unnecessary RocksDB lookups
- **Search option**: `ef_search` option to control search width at query time
- **Batch vector API**: `add_vector_batch/2` for efficient bulk vector insertion
- **Checkpoint API**: `checkpoint/1` for manual HNSW index persistence
- **gen_batch_server integration**: Automatic write batching for improved throughput
- **Benchmark framework**: Performance benchmark suite in `bench/` directory
- **Multi-writer tests**: Concurrent writer stress tests
- **Improved documentation**: Comprehensive README with all API functions and options

### Changed

- **HNSW search optimization**: Replaced `lists:sort/1` with `gb_trees` for O(log N) candidate management instead of O(N log N)
- **Batch RocksDB lookups**: Search now uses `rocksdb:multi_get/4` instead of sequential `rocksdb:get/3` calls
- **Vector storage**: Use float32 for vector storage (50% size reduction)
- **HNSW persistence**: Deferred HNSW persistence for faster inserts
- **Dependencies**: Updated `rocksdb` from 2.0.0 to 2.2.0 for `multi_get` support

### Fixed

- **Benchmark warmup**: Fixed store reset between warmup and actual benchmark runs
- **Search latency variance**: Reduced max search latency from 670ms to sub-10ms by fixing cold-start and optimizing lookups

### Performance

These changes significantly reduced search latency variance:

| Metric | Before | After |
|--------|--------|-------|
| P50 | 1.3ms | ~1ms |
| Max | 670ms | <10ms |
| Variance | 500x | <10x |

Key optimizations:
1. Fixed benchmark warmup keeping HNSW index warm
2. Batch RocksDB lookups with `multi_get` (2 calls per result -> 2 total)
3. Skip text/metadata lookups with `include_text => false`
4. O(log N) HNSW candidate management with `gb_trees`

## [1.0.0] - 2025-12-01

### Added

- Initial release
- RocksDB-backed vector storage with column families
- HNSW approximate nearest neighbor search
- 8-bit vector quantization with norm caching
- Pluggable embedding providers:
  - Local (Python sentence-transformers)
  - Ollama
  - OpenAI
- Provider chain for fallback
- Metadata filtering on search
- GitLab CI configuration
- HexDocs integration

### Features

- `barrel_vectordb:add/4` - Add document with text embedding
- `barrel_vectordb:add_vector/5` - Add document with pre-computed vector
- `barrel_vectordb:search/3` - Text-based semantic search
- `barrel_vectordb:search_vector/3` - Vector-based search
- `barrel_vectordb:get/2` - Retrieve document by ID
- `barrel_vectordb:update/4` - Update existing document
- `barrel_vectordb:upsert/4` - Insert or update document
- `barrel_vectordb:delete/2` - Delete document
- `barrel_vectordb:peek/2` - Sample random documents
- `barrel_vectordb:count/1` - Count total documents
