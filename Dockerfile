# barrel_server: the barrel HTTP node, with the filesystem and S3
# attachment backends.
#
#   docker build -t barrel-server .
#   docker run -p 8080:8080 -v barrel-data:/data -e BARREL_AUTH_TOKENS=secret barrel-server
#
# See docs/guides/docker.md for the settings.

FROM erlang:28 AS build

# cmake builds the vector NIF; rocksdb's bundled C++ needs the compression headers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake libzstd-dev zlib1g-dev libbz2-dev liblz4-dev libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# rebar3 and relx query git during the build; the context excludes .git.
RUN git init -q -b main \
    && git add -A \
    && git -c user.email=build@barrel -c user.name=build commit -qm build

# The Barrel bundle this image is built for (YYYY.MM.N); "dev" for a local build.
ARG BARREL_BUNDLE=dev
RUN BARREL_BUNDLE=$BARREL_BUNDLE rebar3 as s3_server,prod release -n barrel_server

FROM debian:trixie-slim

ARG BARREL_BUNDLE=dev

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libstdc++6 libssl3 libncurses6 ca-certificates curl tini \
        libzstd1 zlib1g libbz2-1.0 liblz4-1 libsnappy1v5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1000 --home-dir /data barrel \
    && mkdir -p /data /etc/barrel \
    && chown barrel:barrel /data

COPY --from=build /src/_build/s3_server+prod/rel/barrel_server /opt/barrel_server
COPY rel/docker/entrypoint.sh /usr/local/bin/barrel

LABEL org.opencontainers.image.title="barrel_server" \
      org.opencontainers.image.description="Barrel database HTTP node" \
      org.opencontainers.image.source="https://github.com/barrel-db/barrel" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="$BARREL_BUNDLE"

USER barrel
WORKDIR /data
VOLUME /data
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${BARREL_HTTP_PORT:-8080}/ready" || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/barrel"]
CMD ["start"]
