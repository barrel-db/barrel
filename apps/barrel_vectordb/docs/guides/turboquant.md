# TurboQuant Vector Quantization

TurboQuant provides efficient vector compression for embedding storage with no training required. Part of barrel_vectordb, it's ideal for reducing memory usage in large-scale vector search applications.

## Overview

TurboQuant is a data-oblivious 3-bit vector quantization algorithm based on [Google Research's PolarQuant/QJL technique](https://arxiv.org/abs/2504.19874). Unlike Product Quantization (PQ), it requires no training phase and delivers deterministic, reproducible results.

**Key benefits:**

- **No training required** - Works immediately on any data
- **Deterministic** - Same seed produces same results
- **~8x compression** - 768-dim vectors: 3KB to ~388 bytes
- **1-3% recall loss** - Minimal accuracy impact vs float32
- **SIMD-accelerated** - AVX2/NEON optimized distance computation

## Quick Start

### Basic Encode/Decode

```erlang
%% Create quantizer (no training needed)
{ok, TQ} = barrel_vectordb_turboquant:new(#{
    dimension => 768,
    bits => 3
}).

%% Encode a vector
Vector = lists:seq(1, 768),  %% Your embedding
Code = barrel_vectordb_turboquant:encode(TQ, Vector).
%% Code is ~388 bytes vs 3072 bytes (768 * 4)

%% Decode back to approximate vector
Decoded = barrel_vectordb_turboquant:decode(TQ, Code).
```

### Distance Computation

```erlang
%% Precompute lookup tables for query (do once per query)
Query = [0.1, 0.2, ...],  %% 768-dim query vector
Tables = barrel_vectordb_turboquant:precompute_tables(TQ, Query).

%% Fast distance computation (SIMD-accelerated NIF)
Distance = barrel_vectordb_turboquant:distance_nif(Tables, Code).

%% Batch distance for multiple codes
Codes = [Code1, Code2, Code3],
Distances = barrel_vectordb_turboquant:batch_distance_nif(Tables, Codes).
```

## Configuration Options

```erlang
{ok, TQ} = barrel_vectordb_turboquant:new(#{
    dimension => 768,           %% Required, must be even
    bits => 3,                  %% 2-4, default: 3
    seed => 42,                 %% Random seed, default: 42
    qjl_iterations => 5,        %% Error correction iterations, default: 5
    qjl_learning_rate => 0.1    %% Gradient step size, default: 0.1
}).
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `dimension` | integer | required | Vector dimension (must be even) |
| `bits` | 2-4 | 3 | Bits per polar angle (higher = more accurate, larger) |
| `seed` | integer | 42 | Random seed for rotation matrix |
| `qjl_iterations` | integer | 5 | QJL error correction iterations |
| `qjl_learning_rate` | float | 0.1 | Gradient descent step size |

## API Reference

### new/1

Create a new TurboQuant configuration.

```erlang
-spec new(map()) -> {ok, tq_config()} | {error, term()}.
```

### encode/2

Encode a vector to compact binary representation.

```erlang
-spec encode(tq_config(), [float()]) -> tq_code().
```

### decode/2

Decode a TurboQuant code back to approximate vector.

```erlang
-spec decode(tq_config(), tq_code()) -> [float()].
```

### precompute_tables/2

Precompute distance lookup tables for a query vector. Call once per query, then use for many distance computations.

```erlang
-spec precompute_tables(tq_config(), [float()]) -> distance_tables().
```

### distance/2

Compute asymmetric distance using precomputed tables (pure Erlang).

```erlang
-spec distance(distance_tables(), tq_code()) -> float().
```

### distance_nif/2

Compute ADC distance using SIMD-accelerated NIF. Use this for production workloads.

```erlang
-spec distance_nif(distance_tables(), tq_code()) -> float().
```

### batch_distance_nif/2

Compute ADC distance for multiple codes. Amortizes NIF call overhead for batch operations.

```erlang
-spec batch_distance_nif(distance_tables(), [tq_code()]) -> [float()].
```

### batch_encode/2

Encode multiple vectors.

```erlang
-spec batch_encode(tq_config(), [[float()]]) -> [tq_code()].
```

### info/1

Get configuration info including compression ratio.

```erlang
-spec info(tq_config()) -> map().
```

Returns:

```erlang
#{
    bits => 3,
    qjl_bits => 1,
    dimension => 768,
    rotation_seed => 42,
    qjl_iterations => 5,
    qjl_learning_rate => 0.1,
    bytes_per_vector => 388,
    compression_ratio => 7.92,
    training_required => false
}
```

## Subspace-TurboQuant

For large dimensions (1024+), Subspace-TurboQuant improves performance by splitting vectors into independent subspaces. This reduces rotation matrix memory from O(D^2) to O(D^2/M).

### When to Use

- Dimensions > 768
- Memory-constrained environments
- When encode latency matters

### API

```erlang
%% Create with auto-selected M
{ok, TQS} = barrel_vectordb_turboquant_subspace:new(#{
    dimension => 1536
}).

%% Or specify M explicitly
{ok, TQS} = barrel_vectordb_turboquant_subspace:new(#{
    dimension => 1536,
    m => 16  %% 16 subspaces of 96 dimensions each
}).

%% API is identical to TurboQuant
Code = barrel_vectordb_turboquant_subspace:encode(TQS, Vector),
Tables = barrel_vectordb_turboquant_subspace:precompute_tables(TQS, Query),
Distance = barrel_vectordb_turboquant_subspace:distance_nif(Tables, Code).
```

### Auto M Selection

| Dimension | M | Subdim |
|-----------|---|--------|
| <= 128 | 1 | D |
| <= 256 | 2 | D/2 |
| <= 512 | 4 | D/4 |
| <= 1024 | 8 | D/8 |
| <= 2048 | 16 | D/16 |
| > 2048 | 32 | D/32 |

### Memory Comparison

For D=768:

| Variant | Rotation Memory | Encode Latency |
|---------|-----------------|----------------|
| TurboQuant | 4.7MB | ~3.6ms |
| Subspace (M=8) | 590KB | ~0.5ms |

## Performance

### Compression Ratios

| Dimension | Bits | Bytes | Compression |
|-----------|------|-------|-------------|
| 384 | 3 | 196 | 7.8x |
| 768 | 3 | 388 | 7.9x |
| 1536 | 3 | 772 | 8.0x |
| 768 | 2 | 340 | 9.0x |
| 768 | 4 | 436 | 7.0x |

### NIF Speedup

Distance computation performance (768-dim, 1000 vectors):

| Method | Time | Speedup |
|--------|------|---------|
| `distance/2` (Erlang) | ~50ms | 1x |
| `distance_nif/2` (C) | ~2ms | 25x |
| `batch_distance_nif/2` | ~1.5ms | 33x |

### Recall vs Compression

At 3 bits with default settings:

| Dataset | Recall@10 (float32) | Recall@10 (TurboQuant) | Loss |
|---------|---------------------|------------------------|------|
| SIFT1M | 98.2% | 96.1% | -2.1% |
| GloVe | 97.5% | 95.8% | -1.7% |
| OpenAI | 99.1% | 97.3% | -1.8% |

## Integration with HNSW

TurboQuant integrates with barrel_vectordb's HNSW index for compressed vector search:

```erlang
%% Create HNSW index with TurboQuant compression
{ok, Index} = barrel_vectordb_hnsw:new(#{
    dimension => 768,
    metric => l2,
    quantization => #{
        type => turboquant,
        bits => 3
    }
}).

%% Add vectors (automatically quantized)
barrel_vectordb_hnsw:add(Index, Id, Vector).

%% Search (uses ADC for distance computation)
{ok, Results} = barrel_vectordb_hnsw:search(Index, Query, 10).
```

### When Quantization Happens

1. **Index creation** - TurboQuant config is initialized
2. **Add vector** - Vector is encoded and stored as compact code
3. **Search** - Query tables are precomputed once, ADC used for all distance computations
4. **Reranking** - Optional: decode top candidates for exact distance reranking

## Best Practices

1. **Reuse tables** - Precompute tables once per query, reuse for all distance computations
2. **Use NIF functions** - Always prefer `distance_nif/2` over `distance/2` in production
3. **Batch operations** - Use `batch_distance_nif/2` for multiple codes
4. **Consider subspace** - For D > 768, Subspace-TurboQuant offers better memory/latency tradeoffs
5. **Tune bits** - 3 bits is a good default; use 4 for higher accuracy, 2 for more compression
