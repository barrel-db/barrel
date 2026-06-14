# syntax=docker/dockerfile:1.4
#
# Barrel VectorDB Production Dockerfile
# Supports: linux/amd64, linux/arm64
#

# Build stage
FROM --platform=$BUILDPLATFORM erlang:27-slim AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    cmake \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    python3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Setup git to use CI_JOB_TOKEN for private GitLab repos
ARG CI_JOB_TOKEN=""
RUN if [ -n "$CI_JOB_TOKEN" ]; then \
    git config --global url."https://gitlab-ci-token:${CI_JOB_TOKEN}@gitlab.enki.io/".insteadOf "https://gitlab.enki.io/"; \
    fi

# RocksDB build settings - adjust parallelism based on available resources
ARG ROCKSDB_JOBS=4
ENV ERLANG_ROCKSDB_BUILDOPTS="-j ${ROCKSDB_JOBS}"
ENV ERLANG_ROCKSDB_OPTS="-DWITH_SNAPPY=ON -DWITH_LZ4=ON -DWITH_ZSTD=ON -DPORTABLE=1"
ENV CXXFLAGS="-std=c++20"

# Copy rebar config first for better caching
COPY rebar.config rebar.lock* ./

# Fetch dependencies
RUN rebar3 get-deps

# Copy source
COPY src/ src/
COPY include/ include/
COPY c_src/ c_src/
COPY priv/ priv/
COPY config/ config/
COPY do_cmake.sh ./

# Initialize git repo (needed by rocksdb build hooks)
RUN git init && git config user.email "build@local" && git config user.name "build"

# Build release
RUN rebar3 as prod compile && rebar3 as prod release

# Runtime stage
FROM debian:bookworm-slim

# Build args for labels
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

# OCI Image labels
LABEL org.opencontainers.image.title="Barrel VectorDB" \
      org.opencontainers.image.description="Embeddable vector database with HNSW indexing" \
      org.opencontainers.image.vendor="Enki" \
      org.opencontainers.image.url="https://gitlab.enki.io/barrel-db/barrel_vectordb" \
      org.opencontainers.image.source="https://gitlab.enki.io/barrel-db/barrel_vectordb" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="${VERSION}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssl \
    libncurses6 \
    libstdc++6 \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    curl \
    jq \
    ca-certificates \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r barrel && useradd -r -g barrel barrel

WORKDIR /app

# Copy release from builder
COPY --from=builder /app/_build/prod/rel/barrel_vectordb ./

# Copy entrypoint script
COPY scripts/docker-entrypoint.sh /app/

# Create directories and set permissions
RUN mkdir -p /app/data /app/log \
    && chmod +x /app/docker-entrypoint.sh \
    && chown -R barrel:barrel /app

# ============================================
# Configuration via Environment Variables
# ============================================
#
# barrel_vectordb is an embedded library. This image runs it as a standalone
# Erlang node that you interact with over Erlang distribution / a remote shell.
#
# Node Configuration:
#   BARREL_NODE_NAME     - Erlang node name (default: barrel_vectordb@<hostname>)
#   RELEASE_COOKIE       - Erlang distribution cookie
#
# Storage:
#   BARREL_DATA_PATH     - Data directory path (default: /app/data)
#
# Embedding (optional):
#   BARREL_EMBEDDER_PROVIDER - Embedding provider: ollama, openai, local, fastembed
#   BARREL_EMBEDDER_MODEL    - Model name for embeddings
#   BARREL_EMBEDDER_URL      - URL for embedding service (ollama/openai)
#   OPENAI_API_KEY           - OpenAI API key (if using openai provider)
#
# Performance:
#   BARREL_SCHEDULER_COUNT  - Erlang scheduler count (default: auto)
#   BARREL_ASYNC_THREADS    - Async thread pool size (default: 64)
#
# Logging:
#   BARREL_LOG_LEVEL        - Log level: debug, info, warning, error (default: info)
#
# ============================================

# Environment defaults
ENV RELEASE_COOKIE=barrel_vectordb_secret \
    BARREL_DATA_PATH=/app/data \
    BARREL_LOG_LEVEL=info \
    BARREL_ASYNC_THREADS=64

# EPMD port
EXPOSE 4369

# Erlang distribution ports
EXPOSE 9100-9200

# Volume for persistent data
VOLUME ["/app/data"]

# Health check - ping the Erlang node
HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=3 \
    CMD /app/bin/barrel_vectordb ping || exit 1

# Run as non-root user
USER barrel

# Use tini as init system for proper signal handling
ENTRYPOINT ["/usr/bin/tini", "--", "/app/docker-entrypoint.sh"]
CMD ["foreground"]
