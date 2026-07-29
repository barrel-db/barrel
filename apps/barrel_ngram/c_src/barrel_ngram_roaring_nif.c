/*
 * Roaring bitmap NIF for barrel_ngram posting lists.
 *
 * A self-contained intersection primitive over 32-bit integer ordinals,
 * backed by the vendored CRoaring library. Set operations return a
 * serialized bitmap so they compose (the regex query tree ANDs and ORs);
 * roaring_decode materializes the ordinals only at the end.
 *
 * Ordinals are per-shard local ids and fit in 32 bits, so the 32-bit
 * roaring API is used.
 */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "erl_nif.h"
#include "roaring/roaring.h"

/* Build a bitmap from an Erlang list of non-negative integers. */
static int list_to_bitmap(ErlNifEnv *env, ERL_NIF_TERM list,
                          roaring_bitmap_t **out) {
    roaring_bitmap_t *r = roaring_bitmap_create();
    if (r == NULL) return 0;
    ERL_NIF_TERM head, tail = list;
    unsigned int v;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        if (!enif_get_uint(env, head, &v)) {
            roaring_bitmap_free(r);
            return 0;
        }
        roaring_bitmap_add(r, (uint32_t)v);
    }
    *out = r;
    return 1;
}

/* Serialize a bitmap into a fresh Erlang binary term. */
static ERL_NIF_TERM bitmap_to_binary(ErlNifEnv *env, const roaring_bitmap_t *r) {
    size_t sz = roaring_bitmap_portable_size_in_bytes(r);
    ERL_NIF_TERM bin_term;
    unsigned char *buf = enif_make_new_binary(env, sz, &bin_term);
    roaring_bitmap_portable_serialize(r, (char *)buf);
    return bin_term;
}

/* Deserialize an Erlang binary term to a bitmap. NULL on a non-binary, a
 * payload whose framing does not fit the buffer, or a structurally invalid
 * bitmap. deserialize_size bounds the read; deserialize_safe never reads
 * past the buffer; internal_validate rejects a well-framed but corrupt
 * bitmap (e.g. bit rot on disk) before any set op reads it. */
static roaring_bitmap_t *binary_to_bitmap(ErlNifEnv *env, ERL_NIF_TERM term) {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin)) return NULL;
    if (roaring_bitmap_portable_deserialize_size((const char *)bin.data,
                                                 bin.size) == 0) {
        return NULL;
    }
    roaring_bitmap_t *r = roaring_bitmap_portable_deserialize_safe(
        (const char *)bin.data, bin.size);
    if (r == NULL) return NULL;
    const char *reason = NULL;
    if (!roaring_bitmap_internal_validate(r, &reason)) {
        roaring_bitmap_free(r);
        return NULL;
    }
    return r;
}

/* roaring_encode(List) -> Binary */
static ERL_NIF_TERM encode_nif(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {
    if (argc != 1 || !enif_is_list(env, argv[0])) return enif_make_badarg(env);
    roaring_bitmap_t *r;
    if (!list_to_bitmap(env, argv[0], &r)) return enif_make_badarg(env);
    roaring_bitmap_run_optimize(r);
    ERL_NIF_TERM out = bitmap_to_binary(env, r);
    roaring_bitmap_free(r);
    return out;
}

/* roaring_decode(Binary) -> [non_neg_integer()] (ascending) */
static ERL_NIF_TERM decode_nif(ErlNifEnv *env, int argc,
                               const ERL_NIF_TERM argv[]) {
    if (argc != 1) return enif_make_badarg(env);
    roaring_bitmap_t *r = binary_to_bitmap(env, argv[0]);
    if (r == NULL) return enif_make_badarg(env);
    uint64_t card = roaring_bitmap_get_cardinality(r);
    ERL_NIF_TERM list = enif_make_list(env, 0);
    if (card > 0) {
        /* guard the multiply against size_t truncation before malloc */
        if (card > SIZE_MAX / sizeof(uint32_t)) {
            roaring_bitmap_free(r);
            return enif_make_badarg(env);
        }
        uint32_t *arr = (uint32_t *)malloc(sizeof(uint32_t) * (size_t)card);
        if (arr == NULL) {
            roaring_bitmap_free(r);
            return enif_make_badarg(env);
        }
        roaring_bitmap_to_uint32_array(r, arr);
        for (int64_t i = (int64_t)card - 1; i >= 0; i--) {
            list = enif_make_list_cell(env, enif_make_uint(env, arr[i]), list);
        }
        free(arr);
    }
    roaring_bitmap_free(r);
    return list;
}

/* roaring_intersect_all([Binary]) -> Binary (AND fold; [] -> empty) */
static ERL_NIF_TERM intersect_all_nif(ErlNifEnv *env, int argc,
                                      const ERL_NIF_TERM argv[]) {
    if (argc != 1 || !enif_is_list(env, argv[0])) return enif_make_badarg(env);
    ERL_NIF_TERM head, tail = argv[0];
    roaring_bitmap_t *acc = NULL;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        roaring_bitmap_t *r = binary_to_bitmap(env, head);
        if (r == NULL) {
            if (acc != NULL) roaring_bitmap_free(acc);
            return enif_make_badarg(env);
        }
        if (acc == NULL) {
            acc = r;
        } else {
            roaring_bitmap_and_inplace(acc, r);
            roaring_bitmap_free(r);
        }
    }
    if (acc == NULL) acc = roaring_bitmap_create();
    ERL_NIF_TERM out = bitmap_to_binary(env, acc);
    roaring_bitmap_free(acc);
    return out;
}

/* roaring_union_all([Binary]) -> Binary (OR fold; [] -> empty) */
static ERL_NIF_TERM union_all_nif(ErlNifEnv *env, int argc,
                                  const ERL_NIF_TERM argv[]) {
    if (argc != 1 || !enif_is_list(env, argv[0])) return enif_make_badarg(env);
    ERL_NIF_TERM head, tail = argv[0];
    roaring_bitmap_t *acc = NULL;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        roaring_bitmap_t *r = binary_to_bitmap(env, head);
        if (r == NULL) {
            if (acc != NULL) roaring_bitmap_free(acc);
            return enif_make_badarg(env);
        }
        if (acc == NULL) {
            acc = r;
        } else {
            roaring_bitmap_or_inplace(acc, r);
            roaring_bitmap_free(r);
        }
    }
    if (acc == NULL) acc = roaring_bitmap_create();
    ERL_NIF_TERM out = bitmap_to_binary(env, acc);
    roaring_bitmap_free(acc);
    return out;
}

static ErlNifFunc nif_funcs[] = {
    {"encode", 1, encode_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"decode", 1, decode_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"intersect_all", 1, intersect_all_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"union_all", 1, union_all_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND}};

ERL_NIF_INIT(barrel_ngram_roaring, nif_funcs, NULL, NULL, NULL, NULL)
