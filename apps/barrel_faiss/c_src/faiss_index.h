#pragma once

#include "erl_nif.h"
#include <faiss/Index.h>
#include <shared_mutex>
#include <string>

namespace faiss_nif {

// Index resource with RWLock for thread safety
struct IndexResource {
    faiss::Index* index;
    int dimension;
    std::string description;
    mutable std::shared_mutex rw_mutex;

    IndexResource() : index(nullptr), dimension(0) {}
    ~IndexResource();
};

// Resource type
extern ErlNifResourceType* INDEX_RESOURCE_TYPE;

// Initialize resource type - call from on_load
int init_resources(ErlNifEnv* env);

// Resource cleanup callback
void index_resource_cleanup(ErlNifEnv* env, void* arg);

// NIF functions - Phase 1
ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_index_factory(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_dimension(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_is_trained(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_ntotal(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// NIF functions - Phase 2
ERL_NIF_TERM nif_add(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_search(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_train(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// NIF functions - Phase 3
ERL_NIF_TERM nif_serialize(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_deserialize(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// NIF functions - Phase 4
ERL_NIF_TERM nif_write_index(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_read_index(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// NIF functions - Phase 5 (Deletion)
ERL_NIF_TERM nif_remove_ids(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
ERL_NIF_TERM nif_add_with_ids(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

}  // namespace faiss_nif
