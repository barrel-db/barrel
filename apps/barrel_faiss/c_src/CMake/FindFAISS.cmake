# Copyright (c) 2018-2025 Benoit Chesneau
#
# This source code is licensed under the Apache License, Version 2.0
# found in the LICENSE file in the root directory of this source tree.
#
# Find the FAISS library for similarity search
#
# Sets:
#   FAISS_FOUND        - True if FAISS was found
#   FAISS_INCLUDE_DIR  - Path to FAISS include directory
#   FAISS_LIBRARY      - Path to FAISS library
#   FAISS_GPU_LIBRARY  - Path to FAISS GPU library (if found)
#

# Try to find FAISS headers
find_path(FAISS_INCLUDE_DIR
    NAMES faiss/Index.h faiss/IndexFlat.h
    HINTS
        ${FAISS_ROOT}
        $ENV{FAISS_ROOT}
        /opt/local          # MacPorts
        /usr/local          # Homebrew, manual install, FreeBSD
        /opt/homebrew       # Homebrew on Apple Silicon
        /usr
    PATH_SUFFIXES include
)

# Try to find FAISS library (CPU version)
find_library(FAISS_LIBRARY
    NAMES faiss
    HINTS
        ${FAISS_ROOT}
        $ENV{FAISS_ROOT}
        /opt/local          # MacPorts
        /usr/local          # Homebrew, manual install, FreeBSD
        /opt/homebrew       # Homebrew on Apple Silicon
        /usr
    PATH_SUFFIXES lib lib64
)

# Try to find FAISS GPU library (optional)
find_library(FAISS_GPU_LIBRARY
    NAMES faiss_gpu
    HINTS
        ${FAISS_ROOT}
        $ENV{FAISS_ROOT}
        /opt/local          # MacPorts
        /usr/local          # Homebrew, manual install, FreeBSD
        /opt/homebrew       # Homebrew on Apple Silicon
        /usr
    PATH_SUFFIXES lib lib64
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    FAISS DEFAULT_MSG
    FAISS_LIBRARY FAISS_INCLUDE_DIR)

if(FAISS_INCLUDE_DIR AND FAISS_LIBRARY)
    set(FAISS_FOUND TRUE)
    message(STATUS "Found FAISS include: ${FAISS_INCLUDE_DIR}")
    message(STATUS "Found FAISS library: ${FAISS_LIBRARY}")

    if(FAISS_GPU_LIBRARY)
        message(STATUS "Found FAISS GPU library: ${FAISS_GPU_LIBRARY}")
    endif()
endif()

if(NOT FAISS_FOUND)
    message(FATAL_ERROR "FAISS not found. Install via: brew install faiss, port install libfaiss, or apt install libfaiss-dev")
endif()

mark_as_advanced(FAISS_INCLUDE_DIR FAISS_LIBRARY FAISS_GPU_LIBRARY)
