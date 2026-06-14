#!/bin/bash
set -e

# ============================================
# Barrel VectorDB Docker Entrypoint
# ============================================
#
# barrel_vectordb is an embedded library. This image runs it as a standalone
# Erlang node you interact with over Erlang distribution / a remote shell.
# This script generates configuration from environment variables and starts
# the Erlang release.
#

# Colors for output (if terminal supports it)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    NC=''
fi

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Node name from environment or generate from hostname
NODE_NAME="${BARREL_NODE_NAME:-barrel_vectordb@$(hostname -f)}"

# Find the release version directory
RELEASE_DIR=$(ls -d /app/releases/*/ 2>/dev/null | head -1)
if [ -z "$RELEASE_DIR" ]; then
    log_error "No release directory found"
    exit 1
fi

log_info "Configuring Barrel VectorDB"
log_info "  Node name: ${NODE_NAME}"
log_info "  Data path: ${BARREL_DATA_PATH:-/app/data}"

# Parse log level
case "${BARREL_LOG_LEVEL:-info}" in
    debug)   LOG_LEVEL="debug" ;;
    info)    LOG_LEVEL="info" ;;
    warning) LOG_LEVEL="warning" ;;
    warn)    LOG_LEVEL="warning" ;;
    error)   LOG_LEVEL="error" ;;
    *)       LOG_LEVEL="info" ;;
esac

# Build default embedder configuration if provider is set
EMBEDDER_CONFIG=""
if [ -n "$BARREL_EMBEDDER_PROVIDER" ]; then
    log_info "  Embedder provider: ${BARREL_EMBEDDER_PROVIDER}"

    case "$BARREL_EMBEDDER_PROVIDER" in
        ollama)
            EMBEDDER_CONFIG="{default_embedder, {ollama, #{
                model => <<\"${BARREL_EMBEDDER_MODEL:-nomic-embed-text}\">>,
                url => <<\"${BARREL_EMBEDDER_URL:-http://localhost:11434}\">>
            }}},"
            ;;
        openai)
            EMBEDDER_CONFIG="{default_embedder, {openai, #{
                model => <<\"${BARREL_EMBEDDER_MODEL:-text-embedding-3-small}\">>
            }}},"
            ;;
        fastembed)
            EMBEDDER_CONFIG="{default_embedder, {fastembed, #{
                model => <<\"${BARREL_EMBEDDER_MODEL:-BAAI/bge-small-en-v1.5}\">>
            }}},"
            ;;
        local)
            EMBEDDER_CONFIG="{default_embedder, {local, #{
                model => <<\"${BARREL_EMBEDDER_MODEL:-all-MiniLM-L6-v2}\">>
            }}},"
            ;;
    esac
fi

# Generate sys.config
cat > "${RELEASE_DIR}sys.config" << EOF
[
    {barrel_vectordb, [
        ${EMBEDDER_CONFIG}
        {data_path, "${BARREL_DATA_PATH:-/app/data}"}
    ]},
    {kernel, [
        {logger_level, ${LOG_LEVEL}},
        {logger, [
            {handler, default, logger_std_h, #{
                level => ${LOG_LEVEL},
                formatter => {logger_formatter, #{
                    single_line => true,
                    template => [time, " [", level, "] ", msg, "\n"]
                }}
            }}
        ]},
        {inet_dist_listen_min, 9100},
        {inet_dist_listen_max, 9200}
    ]},
    {hackney, [
        {max_connections, 100}
    ]}
].
EOF

# Generate vm.args
# Use -sname for short names (Docker hostnames) or -name for FQDNs
if [[ "$NODE_NAME" == *"."* ]]; then
    NAME_FLAG="-name"
else
    NAME_FLAG="-sname"
fi

# Scheduler count
SCHEDULER_OPTS=""
if [ -n "$BARREL_SCHEDULER_COUNT" ]; then
    SCHEDULER_OPTS="+S ${BARREL_SCHEDULER_COUNT}"
fi

cat > "${RELEASE_DIR}vm.args" << EOF
${NAME_FLAG} ${NODE_NAME}
-setcookie ${RELEASE_COOKIE:-barrel_vectordb_secret}
+K true
+A ${BARREL_ASYNC_THREADS:-64}
${SCHEDULER_OPTS}
-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9200
-heart
EOF

log_info "Configuration written to ${RELEASE_DIR}"
log_info "Starting Barrel VectorDB..."

# Execute the release
exec /app/bin/barrel_vectordb "$@"
