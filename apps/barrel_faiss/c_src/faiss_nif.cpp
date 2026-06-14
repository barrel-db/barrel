#include "erl_nif.h"
#include "atoms.h"
#include "faiss_index.h"

namespace faiss_nif {

// Atom definitions
ERL_NIF_TERM ATOM_OK;
ERL_NIF_TERM ATOM_ERROR;
ERL_NIF_TERM ATOM_TRUE;
ERL_NIF_TERM ATOM_FALSE;
ERL_NIF_TERM ATOM_L2;
ERL_NIF_TERM ATOM_INNER_PRODUCT;
ERL_NIF_TERM ATOM_BADARG;
ERL_NIF_TERM ATOM_NOT_TRAINED;

int init_atoms(ErlNifEnv* env) {
    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_L2 = enif_make_atom(env, "l2");
    ATOM_INNER_PRODUCT = enif_make_atom(env, "inner_product");
    ATOM_BADARG = enif_make_atom(env, "badarg");
    ATOM_NOT_TRAINED = enif_make_atom(env, "not_trained");
    return 0;
}

ERL_NIF_TERM make_error(ErlNifEnv* env, const char* reason) {
    return enif_make_tuple2(env, ATOM_ERROR,
        enif_make_string(env, reason, ERL_NIF_LATIN1));
}

ERL_NIF_TERM make_error(ErlNifEnv* env, ERL_NIF_TERM reason) {
    return enif_make_tuple2(env, ATOM_ERROR, reason);
}

}  // namespace faiss_nif

// NIF callbacks
static int on_load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
    if (faiss_nif::init_atoms(env) != 0) {
        return -1;
    }
    if (faiss_nif::init_resources(env) != 0) {
        return -1;
    }
    return 0;
}

static int on_upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
    if (faiss_nif::init_atoms(env) != 0) {
        return -1;
    }
    return 0;
}

static ErlNifFunc nif_funcs[] = {
    // Phase 1: Index management
    {"new", 2, faiss_nif::nif_new, 0},
    {"index_factory", 3, faiss_nif::nif_index_factory, 0},
    {"close", 1, faiss_nif::nif_close, 0},
    {"dimension", 1, faiss_nif::nif_dimension, 0},
    {"is_trained", 1, faiss_nif::nif_is_trained, 0},
    {"ntotal", 1, faiss_nif::nif_ntotal, 0},

    // Phase 2: Core operations - dirty CPU
    {"add", 2, faiss_nif::nif_add, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"search", 3, faiss_nif::nif_search, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"train", 2, faiss_nif::nif_train, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    // Phase 3: Serialization - dirty CPU
    {"serialize", 1, faiss_nif::nif_serialize, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"deserialize", 1, faiss_nif::nif_deserialize, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    // Phase 4: File I/O - dirty IO
    {"write_index", 2, faiss_nif::nif_write_index, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"read_index", 1, faiss_nif::nif_read_index, ERL_NIF_DIRTY_JOB_IO_BOUND},

    // Phase 5: Deletion - dirty CPU
    {"remove_ids", 2, faiss_nif::nif_remove_ids, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"add_with_ids", 3, faiss_nif::nif_add_with_ids, ERL_NIF_DIRTY_JOB_CPU_BOUND},
};

ERL_NIF_INIT(barrel_faiss, nif_funcs, on_load, nullptr, on_upgrade, nullptr)
