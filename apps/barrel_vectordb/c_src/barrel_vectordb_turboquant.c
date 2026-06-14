/**
 * barrel_vectordb_turboquant.c
 *
 * SIMD-accelerated NIF implementation for TurboQuant ADC (Asymmetric Distance Computation).
 * Supports AVX2 on x86_64 and NEON on ARM (if available).
 *
 * ADC Formula (per pair):
 *   contrib = QRSq + DR^2 - CosTerm * DR
 *   distance = sqrt(sum(contrib))
 */

#include "barrel_vectordb_nif_common.h"

/* TurboQuant code header format */
#define TQ_VERSION 1
#define TQ_HEADER_SIZE 4

/* ============================================================
 * Scalar Implementation (used when SIMD not available)
 * ============================================================ */

#if !defined(USE_AVX2) && !defined(USE_NEON)
static double adc_distance_scalar(
    const float* tables,
    int table_row_floats,
    const uint16_t* radii,
    const uint32_t* angle_indices,
    int num_pairs
) {
    double acc = 0.0;

    for (int i = 0; i < num_pairs; i++) {
        const float* row = tables + i * table_row_floats;
        float qr_sq = row[0];
        float cos_term = row[1 + angle_indices[i]];

        float dr = dequantize_radius(radii[i]);
        float dr_sq = dr * dr;

        double contrib = (double)qr_sq + (double)dr_sq - (double)cos_term * (double)dr;
        acc += contrib;
    }

    return sqrt(acc > 0.0 ? acc : 0.0);
}
#endif

/* ============================================================
 * AVX2 Implementation
 * ============================================================ */

#ifdef USE_AVX2
static double adc_distance_avx2(
    const float* tables,
    int table_row_floats,
    const uint16_t* radii,
    const uint32_t* angle_indices,
    int num_pairs
) {
    __m256 acc = _mm256_setzero_ps();
    int i = 0;

    /* Process 8 pairs at a time */
    for (; i + 7 < num_pairs; i += 8) {
        float dr_vals[8];
        for (int j = 0; j < 8; j++) {
            dr_vals[j] = dequantize_radius(radii[i + j]);
        }
        __m256 dr = _mm256_loadu_ps(dr_vals);
        __m256 dr_sq = _mm256_mul_ps(dr, dr);

        float qr_sq_vals[8];
        for (int j = 0; j < 8; j++) {
            qr_sq_vals[j] = tables[(i + j) * table_row_floats];
        }
        __m256 qr_sq = _mm256_loadu_ps(qr_sq_vals);

        float cos_vals[8];
        for (int j = 0; j < 8; j++) {
            const float* row = tables + (i + j) * table_row_floats;
            cos_vals[j] = row[1 + angle_indices[i + j]];
        }
        __m256 cos_term = _mm256_loadu_ps(cos_vals);

        __m256 contrib = _mm256_add_ps(qr_sq, dr_sq);
        contrib = _mm256_fnmadd_ps(cos_term, dr, contrib);

        acc = _mm256_add_ps(acc, contrib);
    }

    /* Horizontal sum */
    __m128 hi = _mm256_extractf128_ps(acc, 1);
    __m128 lo = _mm256_castps256_ps128(acc);
    __m128 sum128 = _mm_add_ps(hi, lo);
    sum128 = _mm_hadd_ps(sum128, sum128);
    sum128 = _mm_hadd_ps(sum128, sum128);
    double result = (double)_mm_cvtss_f32(sum128);

    /* Handle remaining pairs */
    for (; i < num_pairs; i++) {
        const float* row = tables + i * table_row_floats;
        float qr_sq = row[0];
        float cos_term = row[1 + angle_indices[i]];
        float dr = dequantize_radius(radii[i]);
        float dr_sq = dr * dr;
        result += (double)qr_sq + (double)dr_sq - (double)cos_term * (double)dr;
    }

    return sqrt(result > 0.0 ? result : 0.0);
}
#endif

/* ============================================================
 * NEON Implementation
 * ============================================================ */

#ifdef USE_NEON
static double adc_distance_neon(
    const float* tables,
    int table_row_floats,
    const uint16_t* radii,
    const uint32_t* angle_indices,
    int num_pairs
) {
    float32x4_t acc = vdupq_n_f32(0.0f);
    int i = 0;

    /* Process 4 pairs at a time */
    for (; i + 3 < num_pairs; i += 4) {
        float dr_vals[4];
        for (int j = 0; j < 4; j++) {
            dr_vals[j] = dequantize_radius(radii[i + j]);
        }
        float32x4_t dr = vld1q_f32(dr_vals);
        float32x4_t dr_sq = vmulq_f32(dr, dr);

        float qr_sq_vals[4];
        for (int j = 0; j < 4; j++) {
            qr_sq_vals[j] = tables[(i + j) * table_row_floats];
        }
        float32x4_t qr_sq = vld1q_f32(qr_sq_vals);

        float cos_vals[4];
        for (int j = 0; j < 4; j++) {
            const float* row = tables + (i + j) * table_row_floats;
            cos_vals[j] = row[1 + angle_indices[i + j]];
        }
        float32x4_t cos_term = vld1q_f32(cos_vals);

        float32x4_t contrib = vaddq_f32(qr_sq, dr_sq);
        contrib = vmlsq_f32(contrib, cos_term, dr);

        acc = vaddq_f32(acc, contrib);
    }

    /* Horizontal sum */
    float result = vaddvq_f32(acc);

    /* Handle remaining pairs */
    for (; i < num_pairs; i++) {
        const float* row = tables + i * table_row_floats;
        float qr_sq = row[0];
        float cos_term = row[1 + angle_indices[i]];
        float dr = dequantize_radius(radii[i]);
        float dr_sq = dr * dr;
        result += qr_sq + dr_sq - cos_term * dr;
    }

    return sqrt(result > 0.0 ? result : 0.0);
}
#endif

/* ============================================================
 * Dispatch Function
 * ============================================================ */

static double do_adc_distance(
    const float* tables,
    int table_row_floats,
    const uint16_t* radii,
    const uint32_t* angle_indices,
    int num_pairs
) {
#ifdef USE_AVX2
    return adc_distance_avx2(tables, table_row_floats, radii, angle_indices, num_pairs);
#elif defined(USE_NEON)
    return adc_distance_neon(tables, table_row_floats, radii, angle_indices, num_pairs);
#else
    return adc_distance_scalar(tables, table_row_floats, radii, angle_indices, num_pairs);
#endif
}

/* ============================================================
 * NIF Functions
 * ============================================================ */

/**
 * tq_adc_distance(Tables, Code, Bits) -> float()
 */
ERL_NIF_TERM nif_tq_adc_distance(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary tables_bin, code_bin;
    int bits;

    if (!enif_inspect_binary(env, argv[0], &tables_bin) ||
        !enif_inspect_binary(env, argv[1], &code_bin) ||
        !enif_get_int(env, argv[2], &bits)) {
        return enif_make_badarg(env);
    }

    if (bits < 2 || bits > 4) {
        return enif_make_badarg(env);
    }

    if (code_bin.size < TQ_HEADER_SIZE) {
        return enif_make_badarg(env);
    }

    /* Verify header */
    const uint8_t* code = code_bin.data;
    if (code[0] != TQ_VERSION || code[1] != (uint8_t)bits) {
        return enif_make_badarg(env);
    }

    /* Calculate dimensions from table size */
    int num_levels = 1 << bits;
    int table_row_floats = 1 + num_levels;
    int table_row_bytes = table_row_floats * sizeof(float);

    if (tables_bin.size % table_row_bytes != 0) {
        return enif_make_badarg(env);
    }

    int num_pairs = tables_bin.size / table_row_bytes;

    /* Calculate code component sizes */
    int radius_bytes = num_pairs * 2;
    int angle_bits = num_pairs * bits;
    int angle_bytes = (angle_bits + 7) / 8;

    if (code_bin.size < TQ_HEADER_SIZE + radius_bytes + angle_bytes) {
        return enif_make_badarg(env);
    }

    /* Parse code components */
    const uint16_t* radii = (const uint16_t*)(code + TQ_HEADER_SIZE);
    const uint8_t* angles_packed = code + TQ_HEADER_SIZE + radius_bytes;

    /* Unpack angle indices */
    uint32_t* angle_indices = enif_alloc(num_pairs * sizeof(uint32_t));
    if (!angle_indices) {
        return enif_make_badarg(env);
    }
    unpack_bits(angles_packed, bits, num_pairs, angle_indices);

    /* Compute distance */
    const float* tables = (const float*)tables_bin.data;
    double distance = do_adc_distance(tables, table_row_floats, radii, angle_indices, num_pairs);

    enif_free(angle_indices);

    return enif_make_double(env, distance);
}

/**
 * tq_batch_adc_distance(Tables, Codes, Bits) -> [float()]
 */
ERL_NIF_TERM nif_tq_batch_adc_distance(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary tables_bin;
    int bits;

    if (!enif_inspect_binary(env, argv[0], &tables_bin) ||
        !enif_is_list(env, argv[1]) ||
        !enif_get_int(env, argv[2], &bits)) {
        return enif_make_badarg(env);
    }

    if (bits < 2 || bits > 4) {
        return enif_make_badarg(env);
    }

    /* Calculate dimensions from table size */
    int num_levels = 1 << bits;
    int table_row_floats = 1 + num_levels;
    int table_row_bytes = table_row_floats * sizeof(float);

    if (tables_bin.size % table_row_bytes != 0) {
        return enif_make_badarg(env);
    }

    int num_pairs = tables_bin.size / table_row_bytes;
    int radius_bytes = num_pairs * 2;
    int angle_bits = num_pairs * bits;
    int angle_bytes = (angle_bits + 7) / 8;
    size_t min_code_size = TQ_HEADER_SIZE + radius_bytes + angle_bytes;

    const float* tables = (const float*)tables_bin.data;

    /* Get list length */
    unsigned int list_len;
    if (!enif_get_list_length(env, argv[1], &list_len)) {
        return enif_make_badarg(env);
    }

    /* Allocate result array */
    ERL_NIF_TERM* results = enif_alloc(list_len * sizeof(ERL_NIF_TERM));
    if (!results) {
        return enif_make_badarg(env);
    }

    /* Allocate angle indices buffer (reused) */
    uint32_t* angle_indices = enif_alloc(num_pairs * sizeof(uint32_t));
    if (!angle_indices) {
        enif_free(results);
        return enif_make_badarg(env);
    }

    /* Process each code */
    ERL_NIF_TERM list = argv[1];
    ERL_NIF_TERM head, tail;
    unsigned int i = 0;

    while (enif_get_list_cell(env, list, &head, &tail)) {
        ErlNifBinary code_bin;
        if (!enif_inspect_binary(env, head, &code_bin) ||
            code_bin.size < min_code_size ||
            code_bin.data[0] != TQ_VERSION ||
            code_bin.data[1] != (uint8_t)bits) {
            enif_free(angle_indices);
            enif_free(results);
            return enif_make_badarg(env);
        }

        const uint8_t* code = code_bin.data;
        const uint16_t* radii = (const uint16_t*)(code + TQ_HEADER_SIZE);
        const uint8_t* angles_packed = code + TQ_HEADER_SIZE + radius_bytes;

        unpack_bits(angles_packed, bits, num_pairs, angle_indices);

        double distance = do_adc_distance(tables, table_row_floats, radii, angle_indices, num_pairs);
        results[i] = enif_make_double(env, distance);

        list = tail;
        i++;
    }

    enif_free(angle_indices);

    ERL_NIF_TERM result_list = enif_make_list_from_array(env, results, list_len);
    enif_free(results);

    return result_list;
}
