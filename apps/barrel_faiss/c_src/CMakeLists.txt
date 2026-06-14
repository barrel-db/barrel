cmake_minimum_required(VERSION 3.14...3.30 FATAL_ERROR)
project(FaissNIF CXX)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake")

# Platform-specific paths
if(APPLE)
    # Homebrew on Apple Silicon
    if(EXISTS "/opt/homebrew")
        list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
        list(APPEND CMAKE_INCLUDE_PATH "/opt/homebrew/include")
        list(APPEND CMAKE_LIBRARY_PATH "/opt/homebrew/lib")
    endif()
    # Homebrew on Intel
    if(EXISTS "/usr/local/include")
        list(APPEND CMAKE_PREFIX_PATH "/usr/local")
        list(APPEND CMAKE_INCLUDE_PATH "/usr/local/include")
        list(APPEND CMAKE_LIBRARY_PATH "/usr/local/lib")
    endif()
    # MacPorts
    if(EXISTS "/opt/local")
        list(APPEND CMAKE_PREFIX_PATH "/opt/local")
        list(APPEND CMAKE_INCLUDE_PATH "/opt/local/include")
        list(APPEND CMAKE_LIBRARY_PATH "/opt/local/lib")
    endif()
endif()

# FreeBSD
if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    list(APPEND CMAKE_PREFIX_PATH "/usr/local")
    list(APPEND CMAKE_INCLUDE_PATH "/usr/local/include")
    list(APPEND CMAKE_LIBRARY_PATH "/usr/local/lib")
endif()

set(priv_dir "${PROJECT_SOURCE_DIR}/../priv")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${priv_dir})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find Erlang
include(FindErlang)

# Find FAISS
find_package(FAISS REQUIRED)

# Find OpenMP (needed by FAISS for HNSW)
find_package(OpenMP QUIET)
if(NOT OpenMP_CXX_FOUND)
    # Manual search for macOS with Homebrew libomp
    if(APPLE)
        find_path(OpenMP_INCLUDE_DIR NAMES omp.h
            HINTS
                /opt/homebrew/opt/libomp/include
                /usr/local/opt/libomp/include
                /opt/local/include/libomp
        )
        find_library(OpenMP_LIBRARY NAMES omp
            HINTS
                /opt/homebrew/opt/libomp/lib
                /usr/local/opt/libomp/lib
                /opt/local/lib/libomp
        )
        if(OpenMP_INCLUDE_DIR AND OpenMP_LIBRARY)
            set(OpenMP_CXX_FOUND TRUE)
            message(STATUS "Found OpenMP: ${OpenMP_LIBRARY}")
        endif()
    endif()
endif()

# NIF sources
set(NIF_SOURCES
    faiss_nif.cpp
    faiss_index.cpp
)

# Create NIF shared library
add_library(barrel_faiss MODULE ${NIF_SOURCES})

target_include_directories(barrel_faiss PRIVATE
    ${ERLANG_ERTS_INCLUDE_PATH}
    ${FAISS_INCLUDE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

if(OpenMP_CXX_FOUND)
    if(OpenMP_INCLUDE_DIR)
        target_include_directories(barrel_faiss PRIVATE ${OpenMP_INCLUDE_DIR})
    endif()
endif()

# Link libraries
set(LINK_LIBS ${FAISS_LIBRARY})

if(OpenMP_CXX_FOUND)
    if(OpenMP_LIBRARY)
        list(APPEND LINK_LIBS ${OpenMP_LIBRARY})
    elseif(OpenMP_CXX_LIBRARIES)
        list(APPEND LINK_LIBS ${OpenMP_CXX_LIBRARIES})
    endif()
endif()

# BLAS/LAPACK for FAISS
if(APPLE)
    find_library(ACCELERATE_FRAMEWORK Accelerate)
    if(ACCELERATE_FRAMEWORK)
        list(APPEND LINK_LIBS ${ACCELERATE_FRAMEWORK})
    endif()
else()
    find_package(BLAS)
    find_package(LAPACK)
    if(BLAS_FOUND)
        list(APPEND LINK_LIBS ${BLAS_LIBRARIES})
    endif()
    if(LAPACK_FOUND)
        list(APPEND LINK_LIBS ${LAPACK_LIBRARIES})
    endif()
endif()

# Platform-specific linker flags
if(APPLE)
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -flat_namespace -undefined suppress")
endif()

target_link_libraries(barrel_faiss PRIVATE ${LINK_LIBS})

set_target_properties(barrel_faiss PROPERTIES
    PREFIX ""
    SUFFIX ".so"
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)
