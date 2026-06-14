#include "faiss_index.h"
#include "atoms.h"

#include <faiss/IndexFlat.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>
#include <faiss/impl/io.h>
#include <faiss/impl/IDSelector.h>

#include <cstring>
#include <mutex>
#include <vector>

namespace faiss_nif {

ErlNifResourceType* INDEX_RESOURCE_TYPE = nullptr;

IndexResource::~IndexResource() {
    if (index) {
        delete index;
        index = nullptr;
    }
}

void index_resource_cleanup(ErlNifEnv* env, void* arg) {
    IndexResource* res = static_cast<IndexResource*>(arg);
    res->~IndexResource();
}

int init_resources(ErlNifEnv* env) {
    INDEX_RESOURCE_TYPE = enif_open_resource_type(
        env,
        nullptr,
        "faiss_index",
        index_resource_cleanup,
        static_cast<ErlNifResourceFlags>(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER),
        nullptr
    );
    return INDEX_RESOURCE_TYPE ? 0 : -1;
}

// Helper to get metric type from atom
static faiss::MetricType get_metric(ErlNifEnv* env, ERL_NIF_TERM term) {
    if (enif_is_identical(term, ATOM_INNER_PRODUCT)) {
        return faiss::METRIC_INNER_PRODUCT;
    }
    return faiss::METRIC_L2;
}

// Phase 1: new/2 - create flat index
ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    int dimension;
    if (!enif_get_int(env, argv[0], &dimension) || dimension <= 0) {
        return enif_make_badarg(env);
    }

    faiss::MetricType metric = get_metric(env, argv[1]);

    try {
        faiss::Index* index = new faiss::IndexFlat(dimension, metric);

        IndexResource* res = static_cast<IndexResource*>(
            enif_alloc_resource(INDEX_RESOURCE_TYPE, sizeof(IndexResource))
        );
        new (res) IndexResource();
        res->index = index;
        res->dimension = dimension;
        res->description = "Flat";

        ERL_NIF_TERM result = enif_make_resource(env, res);
        enif_release_resource(res);

        return enif_make_tuple2(env, ATOM_OK, result);
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }
}

// Phase 1: index_factory/3
ERL_NIF_TERM nif_index_factory(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    int dimension;
    if (!enif_get_int(env, argv[0], &dimension) || dimension <= 0) {
        return enif_make_badarg(env);
    }

    ErlNifBinary desc_bin;
    if (!enif_inspect_binary(env, argv[1], &desc_bin)) {
        return enif_make_badarg(env);
    }
    std::string description(reinterpret_cast<char*>(desc_bin.data), desc_bin.size);

    faiss::MetricType metric = get_metric(env, argv[2]);

    try {
        faiss::Index* index = faiss::index_factory(dimension, description.c_str(), metric);

        IndexResource* res = static_cast<IndexResource*>(
            enif_alloc_resource(INDEX_RESOURCE_TYPE, sizeof(IndexResource))
        );
        new (res) IndexResource();
        res->index = index;
        res->dimension = dimension;
        res->description = description;

        ERL_NIF_TERM result = enif_make_resource(env, res);
        enif_release_resource(res);

        return enif_make_tuple2(env, ATOM_OK, result);
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }
}

// Phase 1: close/1
ERL_NIF_TERM nif_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    std::unique_lock<std::shared_mutex> lock(res->rw_mutex);
    if (res->index) {
        delete res->index;
        res->index = nullptr;
    }

    return ATOM_OK;
}

// Phase 1: dimension/1
ERL_NIF_TERM nif_dimension(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    std::shared_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    return enif_make_int(env, res->index->d);
}

// Phase 1: is_trained/1
ERL_NIF_TERM nif_is_trained(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    std::shared_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    return res->index->is_trained ? ATOM_TRUE : ATOM_FALSE;
}

// Phase 1: ntotal/1
ERL_NIF_TERM nif_ntotal(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    std::shared_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    return enif_make_int64(env, res->index->ntotal);
}

// Phase 2: add/2 - add vectors to index
ERL_NIF_TERM nif_add(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    ErlNifBinary vectors_bin;
    if (!enif_inspect_binary(env, argv[1], &vectors_bin)) {
        return enif_make_badarg(env);
    }

    std::unique_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    if (!res->index->is_trained) {
        return make_error(env, "not_trained");
    }

    int dimension = res->index->d;
    size_t vector_size = dimension * sizeof(float);

    if (vectors_bin.size % vector_size != 0) {
        return make_error(env, "invalid_vector_size");
    }

    int64_t n = vectors_bin.size / vector_size;
    const float* vectors = reinterpret_cast<const float*>(vectors_bin.data);

    try {
        res->index->add(n, vectors);
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }

    return ATOM_OK;
}

// Phase 2: search/3
ERL_NIF_TERM nif_search(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    ErlNifBinary query_bin;
    if (!enif_inspect_binary(env, argv[1], &query_bin)) {
        return enif_make_badarg(env);
    }

    long k_long;
    if (!enif_get_long(env, argv[2], &k_long) || k_long <= 0) {
        return enif_make_badarg(env);
    }
    int64_t k = static_cast<int64_t>(k_long);

    std::shared_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    int dimension = res->index->d;
    size_t vector_size = dimension * sizeof(float);

    if (query_bin.size % vector_size != 0) {
        return make_error(env, "invalid_query_size");
    }

    int64_t nq = query_bin.size / vector_size;
    const float* queries = reinterpret_cast<const float*>(query_bin.data);

    std::vector<float> distances(nq * k);
    std::vector<int64_t> labels(nq * k);

    try {
        res->index->search(nq, queries, k, distances.data(), labels.data());
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }

    // Return as binaries
    ERL_NIF_TERM dist_term, labels_term;
    unsigned char* dist_buf = enif_make_new_binary(env, nq * k * sizeof(float), &dist_term);
    unsigned char* labels_buf = enif_make_new_binary(env, nq * k * sizeof(int64_t), &labels_term);

    memcpy(dist_buf, distances.data(), nq * k * sizeof(float));
    memcpy(labels_buf, labels.data(), nq * k * sizeof(int64_t));

    return enif_make_tuple3(env, ATOM_OK, dist_term, labels_term);
}

// Phase 2: train/2
ERL_NIF_TERM nif_train(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    ErlNifBinary vectors_bin;
    if (!enif_inspect_binary(env, argv[1], &vectors_bin)) {
        return enif_make_badarg(env);
    }

    std::unique_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    int dimension = res->index->d;
    size_t vector_size = dimension * sizeof(float);

    if (vectors_bin.size % vector_size != 0) {
        return make_error(env, "invalid_vector_size");
    }

    int64_t n = vectors_bin.size / vector_size;
    const float* vectors = reinterpret_cast<const float*>(vectors_bin.data);

    try {
        res->index->train(n, vectors);
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }

    return ATOM_OK;
}

// VectorIOWriter for serialize
class VectorIOWriter : public faiss::IOWriter {
public:
    std::vector<uint8_t> data;

    size_t operator()(const void* ptr, size_t size, size_t nitems) override {
        size_t bytes = size * nitems;
        const uint8_t* src = static_cast<const uint8_t*>(ptr);
        data.insert(data.end(), src, src + bytes);
        return nitems;
    }
};

// VectorIOReader for deserialize
class VectorIOReader : public faiss::IOReader {
public:
    const uint8_t* data;
    size_t size;
    size_t pos;

    VectorIOReader(const uint8_t* d, size_t s) : data(d), size(s), pos(0) {}

    size_t operator()(void* ptr, size_t sz, size_t nitems) override {
        size_t bytes = sz * nitems;
        size_t remaining = size - pos;
        if (bytes > remaining) {
            bytes = remaining;
            nitems = bytes / sz;
        }
        memcpy(ptr, data + pos, bytes);
        pos += bytes;
        return nitems;
    }
};

// Phase 3: serialize/1
ERL_NIF_TERM nif_serialize(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    std::shared_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    try {
        VectorIOWriter writer;
        faiss::write_index(res->index, &writer);

        ERL_NIF_TERM binary_term;
        unsigned char* buf = enif_make_new_binary(env, writer.data.size(), &binary_term);
        memcpy(buf, writer.data.data(), writer.data.size());

        return enif_make_tuple2(env, ATOM_OK, binary_term);
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }
}

// Phase 3: deserialize/1
ERL_NIF_TERM nif_deserialize(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    try {
        VectorIOReader reader(bin.data, bin.size);
        faiss::Index* index = faiss::read_index(&reader);

        IndexResource* res = static_cast<IndexResource*>(
            enif_alloc_resource(INDEX_RESOURCE_TYPE, sizeof(IndexResource))
        );
        new (res) IndexResource();
        res->index = index;
        res->dimension = index->d;

        ERL_NIF_TERM result = enif_make_resource(env, res);
        enif_release_resource(res);

        return enif_make_tuple2(env, ATOM_OK, result);
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }
}

// Phase 4: write_index/2
ERL_NIF_TERM nif_write_index(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    ErlNifBinary path_bin;
    if (!enif_inspect_binary(env, argv[1], &path_bin)) {
        return enif_make_badarg(env);
    }
    std::string path(reinterpret_cast<char*>(path_bin.data), path_bin.size);

    std::shared_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    try {
        faiss::write_index(res->index, path.c_str());
        return ATOM_OK;
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }
}

// Phase 4: read_index/1
ERL_NIF_TERM nif_read_index(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifBinary path_bin;
    if (!enif_inspect_binary(env, argv[0], &path_bin)) {
        return enif_make_badarg(env);
    }
    std::string path(reinterpret_cast<char*>(path_bin.data), path_bin.size);

    try {
        faiss::Index* index = faiss::read_index(path.c_str());

        IndexResource* res = static_cast<IndexResource*>(
            enif_alloc_resource(INDEX_RESOURCE_TYPE, sizeof(IndexResource))
        );
        new (res) IndexResource();
        res->index = index;
        res->dimension = index->d;

        ERL_NIF_TERM result = enif_make_resource(env, res);
        enif_release_resource(res);

        return enif_make_tuple2(env, ATOM_OK, result);
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }
}

// Phase 5: remove_ids/2 - remove vectors by ID batch
ERL_NIF_TERM nif_remove_ids(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    ErlNifBinary ids_bin;
    if (!enif_inspect_binary(env, argv[1], &ids_bin)) {
        return enif_make_badarg(env);
    }

    std::unique_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    size_t n = ids_bin.size / sizeof(int64_t);
    if (ids_bin.size % sizeof(int64_t) != 0) {
        return make_error(env, "invalid_ids_size");
    }

    const int64_t* ids = reinterpret_cast<const int64_t*>(ids_bin.data);

    try {
        faiss::IDSelectorBatch selector(n, ids);
        size_t removed = res->index->remove_ids(selector);
        return enif_make_tuple2(env, ATOM_OK, enif_make_uint64(env, removed));
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }
}

// Phase 5: add_with_ids/3 - add vectors with explicit IDs
ERL_NIF_TERM nif_add_with_ids(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    IndexResource* res;
    if (!enif_get_resource(env, argv[0], INDEX_RESOURCE_TYPE, reinterpret_cast<void**>(&res))) {
        return enif_make_badarg(env);
    }

    ErlNifBinary vectors_bin, ids_bin;
    if (!enif_inspect_binary(env, argv[1], &vectors_bin) ||
        !enif_inspect_binary(env, argv[2], &ids_bin)) {
        return enif_make_badarg(env);
    }

    std::unique_lock<std::shared_mutex> lock(res->rw_mutex);
    if (!res->index) {
        return make_error(env, "index_closed");
    }

    if (!res->index->is_trained) {
        return make_error(env, "not_trained");
    }

    int dimension = res->index->d;
    size_t vector_size = dimension * sizeof(float);

    if (vectors_bin.size % vector_size != 0) {
        return make_error(env, "invalid_vector_size");
    }

    int64_t n = vectors_bin.size / vector_size;

    if (ids_bin.size != static_cast<size_t>(n) * sizeof(int64_t)) {
        return make_error(env, "ids_count_mismatch");
    }

    const float* vectors = reinterpret_cast<const float*>(vectors_bin.data);
    const int64_t* ids = reinterpret_cast<const int64_t*>(ids_bin.data);

    try {
        res->index->add_with_ids(n, vectors, ids);
        return ATOM_OK;
    } catch (const std::exception& e) {
        return make_error(env, e.what());
    }
}

}  // namespace faiss_nif
