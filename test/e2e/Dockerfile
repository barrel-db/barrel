# Build the barrel_server release on Linux, then run it on a slim image.
#
# The vector NIF (cmake + C) and the rocksdb dependency (bundled C++) compile
# from source, so the builder needs a toolchain. The runtime only needs the C++
# and TLS runtime libraries the release's .so files link against; the release
# ships its own ERTS (prod profile, include_erts).

FROM erlang:28 AS build

# cmake + git build the vector NIF and rocksdb's bundled C++. rocksdb configures
# WITH_ZSTD, so it needs the zstd/zlib/bz2 development headers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake libzstd-dev zlib1g-dev libbz2-dev liblz4-dev libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# rebar3 and relx query git during the build (release version, dep resolution).
# The build context excludes .git, so give the copied sources a throwaway repo.
RUN git init -q -b main \
    && git add -A \
    && git -c user.email=build@barrel -c user.name=build commit -qm "e2e build"

# barrel_faiss is not in the server release (it needs a system FAISS build), so
# this compiles barrel_server and its actual deps only.
RUN rebar3 as server,prod release -n barrel_server

FROM debian:trixie-slim AS runtime

# The release's .so files (liberocksdb, the vector NIF) link the C++ runtime and
# the compression libs rocksdb was built against.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libstdc++6 libssl3 libncurses6 ca-certificates \
        libzstd1 zlib1g libbz2-1.0 liblz4-1 libsnappy1v5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/_build/server+prod/rel/barrel_server /opt/barrel_server

ENV PATH="/opt/barrel_server/bin:${PATH}"
# Databases live under the release dir. Data is ephemeral: each E2E run starts
# from empty containers, which is what a convergence test wants.
EXPOSE 8080

# foreground keeps the release attached to PID 1 so docker stop works.
CMD ["barrel_server", "foreground"]
