cmake_minimum_required(VERSION 3.14)
project(barrel_ngram_nif C)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake")

# Find Erlang
include(FindErlang)

set(CMAKE_BUILD_TYPE Release)

# Output to priv/; the Visual Studio generator would add a Release/ subdir
set(priv_dir "${PROJECT_SOURCE_DIR}/../priv")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${priv_dir})
if(MSVC)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}")
endif()

# Rely on CRoaring's runtime SIMD dispatch rather than -march=native, so the
# shared object stays portable.
if(MSVC)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /O2")
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -fPIC")
endif()

# Vendored CRoaring (32-bit API) plus the NIF
file(GLOB CROARING_SOURCES
    "${PROJECT_SOURCE_DIR}/croaring/src/*.c"
    "${PROJECT_SOURCE_DIR}/croaring/src/containers/*.c"
)

# MODULE libraries get the .so suffix on Linux and macOS and .dll on Windows
add_library(barrel_ngram_nif MODULE
    barrel_ngram_roaring_nif.c
    ${CROARING_SOURCES}
)

target_include_directories(barrel_ngram_nif PRIVATE
    ${ERLANG_ERTS_INCLUDE_PATH}
    "${PROJECT_SOURCE_DIR}/croaring/include"
)

# Directory fsync (barrel_ngram_fs)
add_library(barrel_ngram_fs_nif MODULE barrel_ngram_fs_nif.c)
target_include_directories(barrel_ngram_fs_nif PRIVATE ${ERLANG_ERTS_INCLUDE_PATH})

foreach(nif barrel_ngram_nif barrel_ngram_fs_nif)
    set_target_properties(${nif} PROPERTIES PREFIX "")
    if(APPLE)
        set_target_properties(${nif} PROPERTIES
            LINK_FLAGS "-flat_namespace -undefined suppress"
        )
    endif()
endforeach()
