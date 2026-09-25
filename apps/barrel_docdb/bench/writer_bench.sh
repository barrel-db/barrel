#!/bin/bash
#
# Writer benchmark and profile for one database (see barrel_bench_writer).
#
# Usage:
#   ./writer_bench.sh run CASE [DURATION_MS]
#   ./writer_bench.sh profile CASE call_time|call_memory [DURATION_MS] [server|clients]
#   ./writer_bench.sh sample CASE [DURATION_MS]
#   ./writer_bench.sh phases [DOCS]
#   ./writer_bench.sh cases
#
# ROOT=/path/to/umbrella runs against another checkout (its _build must be
# compiled), so two releases can be measured with the same harness.

set -e

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="${ROOT:-$(cd "$BENCH_DIR/../../.." && pwd)}"
OUT="$(mktemp -d "${TMPDIR:-/tmp}/barrel_writer_bench.XXXXXX")"
DATA="$OUT/data"
trap 'rm -rf "$OUT"' EXIT

PA=""
for d in "$ROOT"/_build/default/lib/*/ebin; do PA="$PA -pa $d"; done
# shellcheck disable=SC2086
erlc -o "$OUT" $PA "$BENCH_DIR/src/barrel_bench_writer.erl"

run_erl() {
    # shellcheck disable=SC2086
    erl -noshell -pa "$OUT" $PA \
        -eval "logger:set_primary_config(level, warning),
               ok = application:load(barrel_docdb),
               application:set_env(barrel_docdb, data_dir, \"$DATA\"),
               {ok, _} = application:ensure_all_started(barrel_docdb),
               $1,
               init:stop()."
}

case "$1" in
    run)
        run_erl "barrel_bench_writer:run($2, #{duration => ${3:-3000}})"
        ;;
    profile)
        run_erl "barrel_bench_writer:profile($2, $3, #{duration => ${4:-3000}, target => ${5:-server}})"
        ;;
    sample)
        run_erl "barrel_bench_writer:sample($2, #{duration => ${3:-3000}})"
        ;;
    phases)
        run_erl "barrel_bench_writer:phases(${2:-5000})"
        ;;
    cases)
        run_erl "io:format(\"~p~n\", [maps:keys(barrel_bench_writer:cases())])"
        ;;
    *)
        sed -n '3,12p' "$0"
        exit 1
        ;;
esac
