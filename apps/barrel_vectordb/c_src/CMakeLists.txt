cmake_minimum_required(VERSION 3.14)
project(barrel_vectordb_nif C)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake")

# Find Erlang
include(FindErlang)

# Output to priv/
set(priv_dir "${PROJECT_SOURCE_DIR}/../priv")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${priv_dir})

# Sources
set(SOURCES
    barrel_vectordb_nif.c
    barrel_vectordb_distance.c
    barrel_vectordb_turboquant.c
    barrel_vectordb_turboquant_subspace.c
)

# Build shared library
add_library(barrel_vectordb_nif SHARED ${SOURCES})

# Include paths
target_include_directories(barrel_vectordb_nif PRIVATE ${ERLANG_ERTS_INCLUDE_PATH})

# Base compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -Wall -fPIC")

# SIMD detection and platform-specific flags
include(CheckCCompilerFlag)

if(APPLE)
    # macOS: use -mcpu=native on ARM, -march=native on x86_64
    execute_process(COMMAND uname -m OUTPUT_VARIABLE ARCH OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(ARCH STREQUAL "arm64")
        check_c_compiler_flag("-mcpu=native" HAS_MCPU_NATIVE)
        if(HAS_MCPU_NATIVE)
            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcpu=native")
        endif()
    else()
        check_c_compiler_flag("-march=native" HAS_MARCH_NATIVE)
        if(HAS_MARCH_NATIVE)
            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
        endif()
    endif()
elseif(UNIX)
    # Linux/other Unix
    check_c_compiler_flag("-march=native" HAS_MARCH_NATIVE)
    if(HAS_MARCH_NATIVE)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=native")
    endif()
endif()

# Check for AVX2 support (x86_64)
check_c_compiler_flag("-mavx2" HAS_AVX2)
if(HAS_AVX2)
    target_compile_options(barrel_vectordb_nif PRIVATE -mavx2)
endif()

# Output properties
set_target_properties(barrel_vectordb_nif PROPERTIES
    PREFIX ""
    SUFFIX ".so"
)

# macOS specific linker flags
if(APPLE)
    set_target_properties(barrel_vectordb_nif PROPERTIES
        LINK_FLAGS "-flat_namespace -undefined suppress"
    )
endif()

# Link math library on Unix
if(UNIX)
    target_link_libraries(barrel_vectordb_nif m)
endif()
