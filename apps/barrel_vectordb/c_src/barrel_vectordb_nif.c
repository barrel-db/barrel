/**
 * barrel_vectordb_nif.c
 *
 * Main NIF entry point for barrel_vectordb.
 * Consolidates all NIF functions into a single shared library.
 */

#include "barrel_vectordb_nif_common.h"

/* Distance functions (from barrel_vectordb_distance.c) */
extern ERL_NIF_TERM nif_dot_product(ErlNifEnv*, int, const ERL_NIF_TERM[]);
extern ERL_NIF_TERM nif_cosine_distance(ErlNifEnv*, int, const ERL_NIF_TERM[]);
extern ERL_NIF_TERM nif_cosine_distance_normalized(ErlNifEnv*, int, const ERL_NIF_TERM[]);
extern ERL_NIF_TERM nif_euclidean_distance(ErlNifEnv*, int, const ERL_NIF_TERM[]);
extern ERL_NIF_TERM nif_to_binary(ErlNifEnv*, int, const ERL_NIF_TERM[]);
extern ERL_NIF_TERM nif_from_binary(ErlNifEnv*, int, const ERL_NIF_TERM[]);

/* TurboQuant functions (from barrel_vectordb_turboquant.c) */
extern ERL_NIF_TERM nif_tq_adc_distance(ErlNifEnv*, int, const ERL_NIF_TERM[]);
extern ERL_NIF_TERM nif_tq_batch_adc_distance(ErlNifEnv*, int, const ERL_NIF_TERM[]);

/* Subspace TurboQuant functions (from barrel_vectordb_turboquant_subspace.c) */
extern ERL_NIF_TERM nif_tqs_adc_distance(ErlNifEnv*, int, const ERL_NIF_TERM[]);
extern ERL_NIF_TERM nif_tqs_batch_adc_distance(ErlNifEnv*, int, const ERL_NIF_TERM[]);

static ERL_NIF_TERM nif_simd_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    (void)argc;
    (void)argv;
#ifdef USE_AVX2
    return enif_make_atom(env, "avx2");
#elif defined(USE_NEON)
    return enif_make_atom(env, "neon");
#else
    return enif_make_atom(env, "scalar");
#endif
}

static ErlNifFunc nif_funcs[] = {
    /* Distance */
    {"dot_product", 2, nif_dot_product, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"cosine_distance", 2, nif_cosine_distance, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"cosine_distance_normalized", 2, nif_cosine_distance_normalized, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"euclidean_distance", 2, nif_euclidean_distance, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"to_binary", 1, nif_to_binary, 0},
    {"from_binary", 1, nif_from_binary, 0},
    /* TurboQuant */
    {"tq_adc_distance", 3, nif_tq_adc_distance, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"tq_batch_adc_distance", 3, nif_tq_batch_adc_distance, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    /* Subspace TurboQuant */
    {"tqs_adc_distance", 4, nif_tqs_adc_distance, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"tqs_batch_adc_distance", 4, nif_tqs_batch_adc_distance, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    /* Info */
    {"simd_info", 0, nif_simd_info, 0}
};

static int load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info) {
    (void)env;
    (void)priv;
    (void)info;
    return 0;
}

static int upgrade(ErlNifEnv* env, void** priv, void** old, ERL_NIF_TERM info) {
    (void)env;
    (void)priv;
    (void)old;
    (void)info;
    return 0;
}

ERL_NIF_INIT(barrel_vectordb_nif, nif_funcs, load, NULL, upgrade, NULL)
