#!/bin/bash
# Run HNSW vs FAISS backend comparison benchmarks
#
# Usage:
#   ./scripts/run_backend_bench.sh              # Quick run (default)
#   ./scripts/run_backend_bench.sh --full       # Full benchmark suite
#   ./scripts/run_backend_bench.sh --quick      # Minimal quick test

set -e

cd "$(dirname "$0")/.."

# Parse arguments
MODE="default"
for arg in "$@"; do
    case $arg in
        --full)
            MODE="full"
            shift
            ;;
        --quick)
            MODE="quick"
            shift
            ;;
    esac
done

# Set options based on mode
case $MODE in
    quick)
        OPTS="#{iterations => 10, warmup_iterations => 2, dimension => 32, index_size => 100}"
        ;;
    full)
        OPTS="#{iterations => 100, warmup_iterations => 10, dimension => 128, index_size => 1000}"
        ;;
    *)
        OPTS="#{iterations => 30, warmup_iterations => 5, dimension => 64, index_size => 300}"
        ;;
esac

echo "Building with bench_faiss profile..."
rebar3 as bench_faiss compile

echo ""
echo "Running backend comparison benchmarks ($MODE mode)..."
echo ""

erl -pa _build/bench_faiss/lib/*/ebin \
    -pa _build/bench_faiss/lib/barrel_vectordb/bench \
    -noshell \
    -eval "
application:ensure_all_started(rocksdb),
barrel_vectordb_backend_bench:run_all($OPTS),
init:stop()."
