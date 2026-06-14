/**
 * barrel_vectordb_turboquant_subspace.c
 *
 * SIMD-accelerated NIF implementation for Subspace-TurboQuant ADC.
 * Processes M independent subspaces and sums their distance contributions.
 *
 * Supports AVX2 on x86_64 and NEON on ARM (if available).
 */

#include "barrel_vectordb_nif_common.h"

/* Subspace TurboQuant code header format */
#define TQS_VERSION 2
#define TQS_HEADER_SIZE 4

/* ============================================================
 * Per-Subspace Distance Computation
 * ============================================================ */

#if !defined(USE_AVX2) && !defined(USE_NEON)
static double subspace_adc_distance_scalar(
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

    return acc;  /* Return squared distance (no sqrt here) */
}
#endif

#ifdef USE_AVX2
static double subspace_adc_distance_avx2(
    const float* tables,
    int table_row_floats,
    const uint16_t* radii,
    const uint32_t* angle_indices,
    int num_pairs
) {
    __m256 acc = _mm256_setzero_ps();
    int i = 0;

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

    return result;
}
#endif

#ifdef USE_NEON
static double subspace_adc_distance_neon(
    const float* tables,
    int table_row_floats,
    const uint16_t* radii,
    const uint32_t* angle_indices,
    int num_pairs
) {
    float32x4_t acc = vdupq_n_f32(0.0f);
    int i = 0;

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

    float result = vaddvq_f32(acc);

    for (; i < num_pairs; i++) {
        const float* row = tables + i * table_row_floats;
        float qr_sq = row[0];
        float cos_term = row[1 + angle_indices[i]];
        float dr = dequantize_radius(radii[i]);
        float dr_sq = dr * dr;
        result += qr_sq + dr_sq - cos_term * dr;
    }

    return (double)result;
}
#endif

static double compute_subspace_dist_sq(
    const float* tables,
    int table_row_floats,
    const uint16_t* radii,
    const uint32_t* angle_indices,
    int num_pairs
) {
#ifdef USE_AVX2
    return subspace_adc_distance_avx2(tables, table_row_floats, radii, angle_indices, num_pairs);
#elif defined(USE_NEON)
    return subspace_adc_distance_neon(tables, table_row_floats, radii, angle_indices, num_pairs);
#else
    return subspace_adc_distance_scalar(tables, table_row_floats, radii, angle_indices, num_pairs);
#endif
}

/* ============================================================
 * NIF Functions
 * ============================================================ */

/**
 * tqs_adc_distance(Tables, Code, Bits, M) -> float()
 */
ERL_NIF_TERM nif_tqs_adc_distance(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary tables_bin, code_bin;
    int bits, m;

    if (!enif_inspect_binary(env, argv[0], &tables_bin) ||
        !enif_inspect_binary(env, argv[1], &code_bin) ||
        !enif_get_int(env, argv[2], &bits) ||
        !enif_get_int(env, argv[3], &m)) {
        return enif_make_badarg(env);
    }

    if (bits < 2 || bits > 4 || m < 1) {
        return enif_make_badarg(env);
    }

    if (code_bin.size < TQS_HEADER_SIZE) {
        return enif_make_badarg(env);
    }

    /* Verify header */
    const uint8_t* code = code_bin.data;
    if (code[0] != TQS_VERSION || code[1] != (uint8_t)bits || code[2] != (uint8_t)m) {
        return enif_make_badarg(env);
    }

    int num_levels = 1 << bits;
    int table_row_floats = 1 + num_levels;

    const uint8_t* tables_ptr = tables_bin.data;
    const uint8_t* code_ptr = code + TQS_HEADER_SIZE;

    double total_sq = 0.0;

    for (int s = 0; s < m; s++) {
        /* Parse table size prefix (4 bytes big-endian) */
        if (tables_ptr + 4 > tables_bin.data + tables_bin.size) {
            return enif_make_badarg(env);
        }
        uint32_t table_size = ((uint32_t)tables_ptr[0] << 24) |
                              ((uint32_t)tables_ptr[1] << 16) |
                              ((uint32_t)tables_ptr[2] << 8) |
                              ((uint32_t)tables_ptr[3]);
        tables_ptr += 4;

        if (tables_ptr + table_size > tables_bin.data + tables_bin.size) {
            return enif_make_badarg(env);
        }

        const float* table_data = (const float*)tables_ptr;
        tables_ptr += table_size;

        /* Calculate subspace dimensions */
        int table_row_bytes = table_row_floats * sizeof(float);
        int num_pairs = table_size / table_row_bytes;
        int subdim = num_pairs * 2;

        /* Calculate code component sizes */
        int radius_bytes = num_pairs * 2;
        int angle_bits = num_pairs * bits;
        int angle_bytes = (angle_bits + 7) / 8;
        int qjl_bytes = (subdim + 7) / 8;
        int subspace_code_size = radius_bytes + angle_bytes + qjl_bytes;

        if (code_ptr + subspace_code_size > code_bin.data + code_bin.size) {
            return enif_make_badarg(env);
        }

        /* Parse radii and angles */
        const uint16_t* radii = (const uint16_t*)code_ptr;
        const uint8_t* angles_packed = code_ptr + radius_bytes;

        /* Unpack angle indices */
        uint32_t* angle_indices = enif_alloc(num_pairs * sizeof(uint32_t));
        if (!angle_indices) {
            return enif_make_badarg(env);
        }
        unpack_bits(angles_packed, bits, num_pairs, angle_indices);

        /* Compute squared distance for this subspace */
        double subspace_sq = compute_subspace_dist_sq(table_data, table_row_floats,
                                                       radii, angle_indices, num_pairs);
        total_sq += subspace_sq;

        enif_free(angle_indices);
        code_ptr += subspace_code_size;
    }

    return enif_make_double(env, sqrt(total_sq > 0.0 ? total_sq : 0.0));
}

/**
 * tqs_batch_adc_distance(Tables, Codes, Bits, M) -> [float()]
 */
ERL_NIF_TERM nif_tqs_batch_adc_distance(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary tables_bin;
    int bits, m;

    if (!enif_inspect_binary(env, argv[0], &tables_bin) ||
        !enif_is_list(env, argv[1]) ||
        !enif_get_int(env, argv[2], &bits) ||
        !enif_get_int(env, argv[3], &m)) {
        return enif_make_badarg(env);
    }

    if (bits < 2 || bits > 4 || m < 1) {
        return enif_make_badarg(env);
    }

    unsigned int list_len;
    if (!enif_get_list_length(env, argv[1], &list_len)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM* results = enif_alloc(list_len * sizeof(ERL_NIF_TERM));
    if (!results) {
        return enif_make_badarg(env);
    }

    int num_levels = 1 << bits;
    int table_row_floats = 1 + num_levels;

    /* Pre-parse table structure to get subspace dimensions */
    typedef struct {
        const float* data;
        uint32_t size;
        int num_pairs;
        int subdim;
        int radius_bytes;
        int angle_bytes;
        int code_size;
    } subspace_info_t;

    subspace_info_t* subspace_info = enif_alloc(m * sizeof(subspace_info_t));
    if (!subspace_info) {
        enif_free(results);
        return enif_make_badarg(env);
    }

    const uint8_t* tables_ptr = tables_bin.data;
    for (int s = 0; s < m; s++) {
        uint32_t table_size = ((uint32_t)tables_ptr[0] << 24) |
                              ((uint32_t)tables_ptr[1] << 16) |
                              ((uint32_t)tables_ptr[2] << 8) |
                              ((uint32_t)tables_ptr[3]);
        tables_ptr += 4;

        subspace_info[s].data = (const float*)tables_ptr;
        subspace_info[s].size = table_size;
        tables_ptr += table_size;

        int table_row_bytes = table_row_floats * sizeof(float);
        int num_pairs = table_size / table_row_bytes;
        int subdim = num_pairs * 2;

        subspace_info[s].num_pairs = num_pairs;
        subspace_info[s].subdim = subdim;
        subspace_info[s].radius_bytes = num_pairs * 2;

        int angle_bits = num_pairs * bits;
        subspace_info[s].angle_bytes = (angle_bits + 7) / 8;

        int qjl_bytes = (subdim + 7) / 8;
        subspace_info[s].code_size = subspace_info[s].radius_bytes +
                                      subspace_info[s].angle_bytes + qjl_bytes;
    }

    /* Find max num_pairs for buffer allocation */
    int max_pairs = 0;
    for (int s = 0; s < m; s++) {
        if (subspace_info[s].num_pairs > max_pairs) {
            max_pairs = subspace_info[s].num_pairs;
        }
    }

    uint32_t* angle_indices = enif_alloc(max_pairs * sizeof(uint32_t));
    if (!angle_indices) {
        enif_free(subspace_info);
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
            code_bin.size < TQS_HEADER_SIZE ||
            code_bin.data[0] != TQS_VERSION ||
            code_bin.data[1] != (uint8_t)bits ||
            code_bin.data[2] != (uint8_t)m) {
            enif_free(angle_indices);
            enif_free(subspace_info);
            enif_free(results);
            return enif_make_badarg(env);
        }

        const uint8_t* code_ptr = code_bin.data + TQS_HEADER_SIZE;
        double total_sq = 0.0;

        for (int s = 0; s < m; s++) {
            const uint16_t* radii = (const uint16_t*)code_ptr;
            const uint8_t* angles_packed = code_ptr + subspace_info[s].radius_bytes;

            unpack_bits(angles_packed, bits, subspace_info[s].num_pairs, angle_indices);

            double subspace_sq = compute_subspace_dist_sq(
                subspace_info[s].data, table_row_floats,
                radii, angle_indices, subspace_info[s].num_pairs);
            total_sq += subspace_sq;

            code_ptr += subspace_info[s].code_size;
        }

        results[i] = enif_make_double(env, sqrt(total_sq > 0.0 ? total_sq : 0.0));
        list = tail;
        i++;
    }

    enif_free(angle_indices);
    enif_free(subspace_info);

    ERL_NIF_TERM result_list = enif_make_list_from_array(env, results, list_len);
    enif_free(results);

    return result_list;
}
