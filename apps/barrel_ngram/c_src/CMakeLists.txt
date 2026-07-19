cmake_minimum_required(VERSION 3.14)
project(barrel_ngram_nif C)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake")

# Find Erlang
include(FindErlang)

# Output to priv/
set(priv_dir "${PROJECT_SOURCE_DIR}/../priv")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${priv_dir})

# Vendored CRoaring (32-bit API) plus the NIF
file(GLOB CROARING_SOURCES
    "${PROJECT_SOURCE_DIR}/croaring/src/*.c"
    "${PROJECT_SOURCE_DIR}/croaring/src/containers/*.c"
)

add_library(barrel_ngram_nif SHARED
    barrel_ngram_roaring_nif.c
    ${CROARING_SOURCES}
)

target_include_directories(barrel_ngram_nif PRIVATE
    ${ERLANG_ERTS_INCLUDE_PATH}
    "${PROJECT_SOURCE_DIR}/croaring/include"
)

# -O3 and PIC; rely on CRoaring's runtime SIMD dispatch rather than
# -march=native, so the shared object stays portable.
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -fPIC")

set_target_properties(barrel_ngram_nif PROPERTIES
    PREFIX ""
    SUFFIX ".so"
)

if(APPLE)
    set_target_properties(barrel_ngram_nif PROPERTIES
        LINK_FLAGS "-flat_namespace -undefined suppress"
    )
endif()
