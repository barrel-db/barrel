#!/bin/sh
# Read-only opens: baseline (RocksDB read-write open) against the
# prototype (OpenForReadOnly) on imported generations.
# Usage: bench/rocksdb_readonly/run.sh CORPUS.jsonl [WORK_DIR]
# Env: BASE_REF (origin/ctx/10-demo-guide-bench), ITER (5)
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CORPUS=${1:?corpus.jsonl}
WORK=${2:-${TMPDIR:-/tmp}/barrel_rocksdb_ro}
BASE_REF=${BASE_REF:-origin/ctx/10-demo-guide-bench}
ITER=${ITER:-5}

rm -rf "$WORK"
mkdir -p "$WORK/ebin" "$WORK/base_src" "$WORK/base_ebin"
(cd "$ROOT" && rebar3 compile >/dev/null)
erlc -o "$WORK/ebin" "$ROOT/bench/rocksdb_readonly/rocksdb_ro_bench.erl"

# Baseline: the modules the prototype changed, compiled from BASE_REF
# and put first in the code path (erl -pa prepends in reverse order).
for f in $(git -C "$ROOT" diff --name-only "$BASE_REF" HEAD -- 'apps/*/src/*.erl'); do
    if git -C "$ROOT" cat-file -e "$BASE_REF:$f" 2>/dev/null; then
        git -C "$ROOT" show "$BASE_REF:$f" > "$WORK/base_src/$(basename "$f")"
    fi
done
# shellcheck disable=SC2046
erlc -o "$WORK/base_ebin" -I "$ROOT/apps/barrel_docdb/include" \
    -I "$ROOT/apps/barrel_vectordb/include" \
    $(for d in "$ROOT"/_build/default/lib/*/ebin; do printf -- '-pa %s ' "$d"; done) \
    "$WORK"/base_src/*.erl

LIBS=$(ls -d "$ROOT"/_build/default/lib/*/ebin)
bench() {
    side=$1; shift
    case $side in
        baseline) path="$LIBS $WORK/ebin $WORK/base_ebin" ;;
        *) path="$LIBS $WORK/ebin" ;;
    esac
    args=""
    for a in "$@"; do args="$args\"$a\", "; done
    # shellcheck disable=SC2086
    erl -noshell -pa $path -eval "try rocksdb_ro_bench:main([${args%, }]) of _ -> halt(0)
        catch C:E:S -> io:format(standard_error, \"~p:~p~n~p~n\", [C, E, S]), halt(1) end."
}

echo "prepare: $(uptime)" >> "$WORK/uptime.log"
bench baseline prepare "$CORPUS" "$WORK"
bench baseline import "$WORK" baseline
bench prototype import "$WORK" prototype

# One BEAM per run, never two runs at once; the side order alternates.
i=1
while [ "$i" -le "$ITER" ]; do
    if [ $((i % 2)) -eq 1 ]; then order="baseline prototype"; else order="prototype baseline"; fi
    for c in clean wal; do
        for s in $order; do
            up=$(uptime)
            echo "$s $c #$i: $up" >> "$WORK/uptime.log"
            bench "$s" run "$WORK" "$s" "$c" "$i" "$up"
        done
    done
    i=$((i + 1))
done

# Two BEAMs on one directory: the second opens while the first holds it.
for s in baseline prototype; do
    echo "$s concurrent: $(uptime)" >> "$WORK/uptime.log"
    bench "$s" hold "$WORK" "$s" &
    hold=$!
    bench "$s" second "$WORK" "$s"
    wait "$hold"
done

for s in baseline prototype; do
    echo "$s legacy: $(uptime)" >> "$WORK/uptime.log"
    bench "$s" legacy "$WORK" "$s"
done

bench prototype report "$WORK"
