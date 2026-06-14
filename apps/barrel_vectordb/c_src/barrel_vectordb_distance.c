/**
 * barrel_vectordb_distance.c
 *
 * NIF implementation for SIMD-accelerated vector distance computation.
 * Supports AVX2 on x86_64 and NEON on ARM (if available).
 */

#include "barrel_vectordb_nif_common.h"

/* ============================================================
 * Fallback (scalar) implementations
 * Used when SIMD is not available, or for norm calculation
 * ============================================================ */

#if !defined(USE_AVX2) && !defined(USE_NEON)
static double dot_product_scalar(const float* a, const float* b, int n) {
    double sum = 0.0;
    for (int i = 0; i < n; i++) {
        sum += (double)a[i] * (double)b[i];
    }
    return sum;
}

static double euclidean_distance_sq_scalar(const float* a, const float* b, int n) {
    double sum = 0.0;
    for (int i = 0; i < n; i++) {
        double diff = (double)a[i] - (double)b[i];
        sum += diff * diff;
    }
    return sum;
}
#endif

static double norm_scalar(const float* a, int n) {
    double sum = 0.0;
    for (int i = 0; i < n; i++) {
        sum += (double)a[i] * (double)a[i];
    }
    return sqrt(sum);
}

/* ============================================================
 * SIMD implementations
 * ============================================================ */

#ifdef USE_AVX2
static double dot_product_avx2(const float* a, const float* b, int n) {
    __m256 sum = _mm256_setzero_ps();
    int i = 0;

    /* Process 8 floats at a time */
    for (; i + 7 < n; i += 8) {
        __m256 va = _mm256_loadu_ps(a + i);
        __m256 vb = _mm256_loadu_ps(b + i);
        sum = _mm256_fmadd_ps(va, vb, sum);
    }

    /* Horizontal sum */
    __m128 hi = _mm256_extractf128_ps(sum, 1);
    __m128 lo = _mm256_castps256_ps128(sum);
    __m128 sum128 = _mm_add_ps(hi, lo);
    sum128 = _mm_hadd_ps(sum128, sum128);
    sum128 = _mm_hadd_ps(sum128, sum128);

    double result = _mm_cvtss_f32(sum128);

    /* Handle remaining elements */
    for (; i < n; i++) {
        result += (double)a[i] * (double)b[i];
    }

    return result;
}

static double euclidean_distance_sq_avx2(const float* a, const float* b, int n) {
    __m256 sum = _mm256_setzero_ps();
    int i = 0;

    for (; i + 7 < n; i += 8) {
        __m256 va = _mm256_loadu_ps(a + i);
        __m256 vb = _mm256_loadu_ps(b + i);
        __m256 diff = _mm256_sub_ps(va, vb);
        sum = _mm256_fmadd_ps(diff, diff, sum);
    }

    __m128 hi = _mm256_extractf128_ps(sum, 1);
    __m128 lo = _mm256_castps256_ps128(sum);
    __m128 sum128 = _mm_add_ps(hi, lo);
    sum128 = _mm_hadd_ps(sum128, sum128);
    sum128 = _mm_hadd_ps(sum128, sum128);

    double result = _mm_cvtss_f32(sum128);

    for (; i < n; i++) {
        double diff = (double)a[i] - (double)b[i];
        result += diff * diff;
    }

    return result;
}
#endif

#ifdef USE_NEON
static double dot_product_neon(const float* a, const float* b, int n) {
    float32x4_t sum = vdupq_n_f32(0.0f);
    int i = 0;

    for (; i + 3 < n; i += 4) {
        float32x4_t va = vld1q_f32(a + i);
        float32x4_t vb = vld1q_f32(b + i);
        sum = vmlaq_f32(sum, va, vb);
    }

    float result = vaddvq_f32(sum);

    for (; i < n; i++) {
        result += a[i] * b[i];
    }

    return (double)result;
}

static double euclidean_distance_sq_neon(const float* a, const float* b, int n) {
    float32x4_t sum = vdupq_n_f32(0.0f);
    int i = 0;

    for (; i + 3 < n; i += 4) {
        float32x4_t va = vld1q_f32(a + i);
        float32x4_t vb = vld1q_f32(b + i);
        float32x4_t diff = vsubq_f32(va, vb);
        sum = vmlaq_f32(sum, diff, diff);
    }

    float result = vaddvq_f32(sum);

    for (; i < n; i++) {
        float diff = a[i] - b[i];
        result += diff * diff;
    }

    return (double)result;
}
#endif

/* ============================================================
 * Dispatch functions (choose best implementation)
 * ============================================================ */

static double do_dot_product(const float* a, const float* b, int n) {
#ifdef USE_AVX2
    return dot_product_avx2(a, b, n);
#elif defined(USE_NEON)
    return dot_product_neon(a, b, n);
#else
    return dot_product_scalar(a, b, n);
#endif
}

static double do_euclidean_distance_sq(const float* a, const float* b, int n) {
#ifdef USE_AVX2
    return euclidean_distance_sq_avx2(a, b, n);
#elif defined(USE_NEON)
    return euclidean_distance_sq_neon(a, b, n);
#else
    return euclidean_distance_sq_scalar(a, b, n);
#endif
}

/* ============================================================
 * NIF functions
 * ============================================================ */

/**
 * dot_product(Binary1, Binary2) -> float()
 */
ERL_NIF_TERM nif_dot_product(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary bin1, bin2;

    if (!enif_inspect_binary(env, argv[0], &bin1) ||
        !enif_inspect_binary(env, argv[1], &bin2)) {
        return enif_make_badarg(env);
    }

    if (bin1.size != bin2.size || bin1.size % sizeof(float) != 0) {
        return enif_make_badarg(env);
    }

    int n = bin1.size / sizeof(float);
    const float* a = (const float*)bin1.data;
    const float* b = (const float*)bin2.data;

    double result = do_dot_product(a, b, n);

    return enif_make_double(env, result);
}

/**
 * cosine_distance(Binary1, Binary2) -> float()
 */
ERL_NIF_TERM nif_cosine_distance(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary bin1, bin2;

    if (!enif_inspect_binary(env, argv[0], &bin1) ||
        !enif_inspect_binary(env, argv[1], &bin2)) {
        return enif_make_badarg(env);
    }

    if (bin1.size != bin2.size || bin1.size % sizeof(float) != 0) {
        return enif_make_badarg(env);
    }

    int n = bin1.size / sizeof(float);
    const float* a = (const float*)bin1.data;
    const float* b = (const float*)bin2.data;

    double dot = do_dot_product(a, b, n);
    double norm_a = norm_scalar(a, n);
    double norm_b = norm_scalar(b, n);

    double denom = norm_a * norm_b;
    if (denom < 1.0e-10) {
        return enif_make_double(env, 1.0);
    }

    double cosine = dot / denom;
    double distance = 1.0 - cosine;

    return enif_make_double(env, distance);
}

/**
 * cosine_distance_normalized(Binary1, Binary2) -> float()
 */
ERL_NIF_TERM nif_cosine_distance_normalized(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary bin1, bin2;

    if (!enif_inspect_binary(env, argv[0], &bin1) ||
        !enif_inspect_binary(env, argv[1], &bin2)) {
        return enif_make_badarg(env);
    }

    if (bin1.size != bin2.size || bin1.size % sizeof(float) != 0) {
        return enif_make_badarg(env);
    }

    int n = bin1.size / sizeof(float);
    const float* a = (const float*)bin1.data;
    const float* b = (const float*)bin2.data;

    double dot = do_dot_product(a, b, n);
    double distance = 1.0 - dot;

    return enif_make_double(env, distance);
}

/**
 * euclidean_distance(Binary1, Binary2) -> float()
 */
ERL_NIF_TERM nif_euclidean_distance(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary bin1, bin2;

    if (!enif_inspect_binary(env, argv[0], &bin1) ||
        !enif_inspect_binary(env, argv[1], &bin2)) {
        return enif_make_badarg(env);
    }

    if (bin1.size != bin2.size || bin1.size % sizeof(float) != 0) {
        return enif_make_badarg(env);
    }

    int n = bin1.size / sizeof(float);
    const float* a = (const float*)bin1.data;
    const float* b = (const float*)bin2.data;

    double sq_dist = do_euclidean_distance_sq(a, b, n);
    double distance = sqrt(sq_dist);

    return enif_make_double(env, distance);
}

/**
 * to_binary(List) -> binary()
 */
ERL_NIF_TERM nif_to_binary(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    unsigned int len;

    if (!enif_get_list_length(env, argv[0], &len)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM binary;
    float* data = (float*)enif_make_new_binary(env, len * sizeof(float), &binary);
    if (!data) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM list = argv[0];
    ERL_NIF_TERM head, tail;
    unsigned int i = 0;

    while (enif_get_list_cell(env, list, &head, &tail)) {
        double val;
        if (!enif_get_double(env, head, &val)) {
            /* Try integer */
            long lval;
            if (!enif_get_long(env, head, &lval)) {
                return enif_make_badarg(env);
            }
            val = (double)lval;
        }
        data[i++] = (float)val;
        list = tail;
    }

    return binary;
}

/**
 * from_binary(Binary) -> [float()]
 */
ERL_NIF_TERM nif_from_binary(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    ErlNifBinary bin;

    if (!enif_inspect_binary(env, argv[0], &bin) || bin.size % sizeof(float) != 0) {
        return enif_make_badarg(env);
    }

    int n = bin.size / sizeof(float);
    const float* data = (const float*)bin.data;

    ERL_NIF_TERM* terms = enif_alloc(n * sizeof(ERL_NIF_TERM));
    if (!terms) {
        return enif_make_badarg(env);
    }

    for (int i = 0; i < n; i++) {
        terms[i] = enif_make_double(env, (double)data[i]);
    }

    ERL_NIF_TERM list = enif_make_list_from_array(env, terms, n);
    enif_free(terms);

    return list;
}
