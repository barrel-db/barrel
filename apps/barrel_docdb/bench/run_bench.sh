#!/bin/bash
#
# Run barrel_docdb benchmarks.
#
# barrel_docdb is an app of the umbrella, so this builds against the umbrella's
# compiled output (its real rocksdb and deps) instead of resolving barrel_docdb
# as an external dependency.
#
# Usage:
#   ./run_bench.sh                 # default: 10000 docs, 10000 iterations
#   ./run_bench.sh 1000 1000       # custom num_docs and iterations
#   ./run_bench.sh doc_types       # document-type comparison
#   ./run_bench.sh doc_types 500 200

set -e

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$BENCH_DIR/../../.." && pwd)"     # umbrella root
OUT="$BENCH_DIR/_bench_ebin"

echo "Compiling the umbrella..."
( cd "$ROOT" && rebar3 compile >/dev/null )

echo "Compiling the benchmark..."
rm -rf "$OUT" && mkdir -p "$OUT"
PA=""
for d in "$ROOT"/_build/default/lib/*/ebin; do PA="$PA -pa $d"; done
# shellcheck disable=SC2086
erlc -o "$OUT" -I "$ROOT/apps/barrel_docdb/include" $PA \
    "$BENCH_DIR"/src/*.erl "$BENCH_DIR"/src/workloads/*.erl

DATA="${TMPDIR:-/tmp}/barrel_bench_data"
rm -rf "$DATA"

run_erl() {  # run_erl <erlang-expr>
    # shellcheck disable=SC2086
    erl -noshell -pa "$OUT" $PA \
        -eval "application:set_env(barrel_docdb, data_dir, \"$DATA\"),
               {ok, _} = application:ensure_all_started(barrel_docdb),
               $1,
               init:stop()."
}

case "$1" in
    doc_types)
        NUM_DOCS=${2:-1000}
        ITERATIONS=${3:-500}
        echo "Running document type benchmark (num_docs=$NUM_DOCS, iterations=$ITERATIONS)..."
        run_erl "barrel_bench:run_doc_types(#{num_docs => $NUM_DOCS, iterations => $ITERATIONS})"
        ;;
    *)
        NUM_DOCS=${1:-10000}
        ITERATIONS=${2:-10000}
        echo "Running benchmark (num_docs=$NUM_DOCS, iterations=$ITERATIONS)..."
        run_erl "barrel_bench:run(#{num_docs => $NUM_DOCS, iterations => $ITERATIONS})"
        ;;
esac
