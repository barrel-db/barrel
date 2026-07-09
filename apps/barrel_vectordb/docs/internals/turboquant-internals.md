# TurboQuant Internals

This document describes the internal implementation of TurboQuant vector quantization in barrel_vectordb.

## Algorithm Overview

TurboQuant combines three techniques:

1. **PolarQuant** - Random rotation followed by polar coordinate conversion
2. **QJL** - Johnson-Lindenstrauss transform for error correction
3. **ADC** - Asymmetric Distance Computation for fast search

```
Vector ─> Rotate ─> Polar Coords ─> Quantize ─> QJL Signs ─> Binary Code
                                                                  │
Query ──> Rotate ─> Precompute Tables ────────────────────────────┼─> ADC Distance
                                                                  │
                                                        Compare signs for correction
```

## Rotation Matrix

### Generation

The rotation matrix is generated via Modified Gram-Schmidt orthogonalization of a random Gaussian matrix. This produces an orthogonal matrix Q where Q^T = Q^-1.

```erlang
%% Generate D x D orthogonal rotation matrix
generate_rotation_matrix(Dim, Seed) ->
    rand:seed(exsss, {Seed, Seed + 1, Seed + 2}),
    RandomCols = [random_gaussian_vector(Dim) || _ <- lists:seq(1, Dim)],
    OrthogonalCols = modified_gram_schmidt(RandomCols),
    Rows = transpose(OrthogonalCols),
    << <<F:64/float-little>> || Row <- Rows, F <- Row >>.
```

### Storage Format

- **Binary format**: Row-major float64 (IEEE 754 double precision)
- **Size**: D * D * 8 bytes (e.g., 768 * 768 * 8 = 4.7MB for D=768)
- **Access pattern**: Sequential row access during rotation

### Caching Strategy

The rotation matrix is generated once per TurboQuant config and stored in the config record. Regeneration uses the same seed for deterministic results.

## Polar Coordinate Conversion

### Cartesian to Polar

Vector components are paired and converted to polar form:

```
(x_i, y_i) -> (r_i, theta_i)
where:
  r = sqrt(x^2 + y^2)
  theta = atan2(y, x)
```

This yields D/2 radius-angle pairs from a D-dimensional vector.

### Radius Quantization

Radii are quantized to 16-bit using a log scale for better dynamic range:

```erlang
quantize_radius(R) ->
    LogR = math:log(R + 1.0),
    Scaled = LogR / math:log(11.0) * 65535,
    min(65535, max(0, round(Scaled))).

dequantize_radius(QuantizedR) ->
    LogR = QuantizedR / 65535.0 * math:log(11.0),
    math:exp(LogR) - 1.0.
```

The log scale maps typical embedding radii [0, 10] to the full 16-bit range.

### Angle Quantization

Angles are quantized to N-bit buckets (N = `bits` parameter):

```
NumLevels = 2^bits  (e.g., 8 levels for 3-bit)
BucketSize = 2*pi / NumLevels
AngleIdx = floor((theta + pi) / BucketSize)
```

Angle bucket centers are precomputed for fast lookup during decoding:

```erlang
compute_angle_levels(NumLevels) ->
    BucketSize = 2 * math:pi() / NumLevels,
    [(-math:pi() + BucketSize * (I + 0.5)) || I <- lists:seq(0, NumLevels - 1)].
```

## QJL Error Correction

### Sign Matrix Generation

The QJL matrix is a D x D random sign matrix where each element is +1 or -1:

```erlang
generate_qjl_matrix(Dim, QJLDim, Seed) ->
    rand:seed(exsss, {Seed, Seed + 1, Seed + 2}),
    Signs = [rand:uniform(2) - 1 || _ <- lists:seq(1, Dim * QJLDim)],
    pack_bits_simple(Signs).  %% Store as bits: 1 = +1, 0 = -1
```

Storage: D^2 / 8 bytes (e.g., 73KB for D=768).

### Sign Computation

During encoding, compute signs of the projection:

```
sign_i = sign(QJL_row_i . Vector)
```

These signs are stored with the code and used during decoding to refine the reconstruction.

### Iterative Refinement

During decoding, the algorithm iteratively adjusts the reconstructed vector to match stored signs:

```erlang
apply_qjl_correction(Vector, StoredSigns, QJLMatrix, QJLDim, Iterations, LR) ->
    %% For each iteration:
    %%   1. Compute current signs
    %%   2. Find mismatches with stored signs
    %%   3. Apply gradient toward sign-consistent solution
```

The gradient for a sign mismatch at row i is:

```
gradient += target_sign * QJL_row_i
```

Learning rate and iteration count control convergence:
- Default: 5 iterations, LR = 0.1
- Gradient is normalized relative to vector magnitude to prevent overshooting

## Binary Code Format

### TurboQuant Code Structure

```
+--------+------+-------+----------+----------+
| Header |  Radii       |  Angles  | QJL Signs|
+--------+--------------+----------+----------+
| 4B     | D/2 * 2B     | ceil(D/2 * bits/8) | ceil(D/8) |
```

Header format:
```
<<Version:8, Bits:8, Flags:16>>
```

- **Version**: 1 for TurboQuant, 2 for Subspace-TurboQuant
- **Bits**: Quantization bits (2-4)
- **Flags**: Reserved

### Subspace-TurboQuant Code Structure

```
+--------+----------------+----------------+-----+
| Header | Subspace1 Code | Subspace2 Code | ... |
+--------+----------------+----------------+-----+
| 4B     | Variable       | Variable       |     |
```

Header format:
```
<<Version:8, Bits:8, M:8, Flags:8>>
```

Each subspace code contains radii, angles, and QJL signs for that subspace (no individual headers).

### Bit Packing

Angle indices are bit-packed for efficiency:

```erlang
%% Pack list of N-bit integers into binary
pack_bits([Idx | Rest], Bits, Acc, Buffer, BufferBits) ->
    NewBuffer = (Buffer bsl Bits) bor Idx,
    NewBufferBits = BufferBits + Bits,
    case NewBufferBits >= 8 of
        true ->
            %% Extract complete bytes
            extract_bytes(Acc, NewBuffer, NewBufferBits);
        false ->
            pack_bits(Rest, Bits, Acc, NewBuffer, NewBufferBits)
    end.
```

## ADC Distance Formula

### Table Precomputation

For each pair (D/2 pairs total), precompute:

1. **QRSq**: Query radius squared = r_q^2
2. **CosTerms**: For each angle bucket i: 2 * r_q * cos(theta_q - angle_center_i)

Table size per pair: (1 + NumLevels) * 4 bytes

```erlang
precompute_tables(Config, Query) ->
    RotatedQuery = apply_rotation(RotMat, Query),
    Tables = lists:map(
        fun(PairIdx) ->
            {QX, QY} = get_pair(RotatedQuery, PairIdx),
            QR = math:sqrt(QX*QX + QY*QY),
            QRSq = QR * QR,
            QTheta = math:atan2(QY, QX),
            CosTerms = [2.0 * QR * math:cos(QTheta - AngleCenter)
                        || AngleCenter <- Levels],
            {QRSq, CosTerms}
        end,
        lists:seq(0, NumPairs - 1)
    ),
    %% Pack as binary
    ...
```

### Distance Computation

Per-pair distance using the polar distance formula:

```
d^2 = r_q^2 + r_d^2 - 2*r_q*r_d*cos(theta_diff)
    = QRSq + DR^2 - CosTerm[angle_idx] * DR
```

Total distance:
```
Distance = sqrt(sum over all pairs of (QRSq + DR^2 - CosTerm * DR))
```

### SIMD Implementation

The NIF implements ADC with AVX2/NEON intrinsics:

```c
// Pseudocode for SIMD ADC
float tq_adc_distance_simd(tables, code, bits, num_pairs) {
    __m256 sum_sq = _mm256_setzero_ps();

    for (int i = 0; i < num_pairs; i += 8) {
        // Load 8 radii and angle indices
        __m256 dr = load_and_dequantize_radii(code, i);
        __m256i angle_idx = load_angle_indices(code, i);

        // Gather QRSq and CosTerms from tables
        __m256 qrsq = gather_qrsq(tables, i);
        __m256 costerm = gather_costerm(tables, angle_idx, i);

        // Compute: QRSq + DR^2 - CosTerm * DR
        __m256 dr_sq = _mm256_mul_ps(dr, dr);
        __m256 contrib = _mm256_fmadd_ps(costerm, dr, dr_sq);
        contrib = _mm256_add_ps(qrsq, _mm256_sub_ps(dr_sq, contrib));

        sum_sq = _mm256_add_ps(sum_sq, contrib);
    }

    return sqrt(horizontal_sum(sum_sq));
}
```

## Subspace Partitioning

### Why Subspaces?

Standard TurboQuant has O(D^2) scaling due to the rotation matrix. For large D, this becomes prohibitive:

| D | Rotation Memory | Encode Time |
|---|-----------------|-------------|
| 768 | 4.7MB | 3.6ms |
| 1536 | 18.9MB | 14.4ms |
| 3072 | 75.5MB | 57.6ms |

### Subspace Approach

Split D-dimensional vector into M subspaces of dimension D/M:

```
Vector = [v_1, ..., v_D]
       = [subvec_1, subvec_2, ..., subvec_M]
```

Each subspace has its own rotation matrix: M matrices of (D/M)^2 elements = D^2/M total.

```erlang
split_subvectors(Vec, M, SubDim) ->
    split_subvectors(Vec, M, SubDim, []).

split_subvectors([], 0, _SubDim, Acc) ->
    lists:reverse(Acc);
split_subvectors(Vec, Remaining, SubDim, Acc) when Remaining > 0 ->
    {Subvec, Rest} = lists:split(SubDim, Vec),
    split_subvectors(Rest, Remaining - 1, SubDim, [Subvec | Acc]).
```

### Distance Aggregation

Subspace distances are computed independently and summed:

```
Total_distance^2 = sum over subspaces of distance_subspace^2
Distance = sqrt(Total_distance^2)
```

This preserves the L2 distance property since:
```
||v - q||^2 = sum_i (v_i - q_i)^2
            = sum_subspaces sum_dims_in_subspace (v_i - q_i)^2
```

### Trade-offs

| Aspect | TurboQuant | Subspace (M=8) |
|--------|------------|----------------|
| Memory | D^2 * 8 | D^2/M * 8 |
| Encode | O(D^2) | O(D^2/M) |
| Accuracy | Baseline | -1-2% recall |
| Parallelism | Serial | Per-subspace |

The slight accuracy loss comes from quantizing rotation independently per subspace rather than globally.
