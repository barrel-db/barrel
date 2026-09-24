#!/bin/sh
# Integrity and lease cost on a JSONL corpus ({"id", "body"} per line).
# Usage: apps/barrel_ngram/bench/integrity_bench.sh CORPUS.jsonl OUT_DIR [RUNS]
# Run from the umbrella root; OUT_DIR is wiped. Prints medians of RUNS (default 5).
set -eu

CORPUS=$1
OUT=$2
RUNS=${3:-5}
BENCH_DIR=$(cd "$(dirname "$0")" && pwd)

case "$(cd "$(dirname "$CORPUS")" && pwd)/ $OUT" in
    "$HOME"/Projects/*|*" $HOME"/Projects/*) echo "refusing a path under ~/Projects" >&2; exit 1 ;;
esac

rm -rf "$OUT"
mkdir -p "$OUT/ebin"
rebar3 compile >/dev/null
erlc -o "$OUT/ebin" "$BENCH_DIR/barrel_ngram_bench_integrity.erl"
echo "load: $(uptime)"
erl -noshell -pa _build/default/lib/*/ebin -pa "$OUT/ebin" \
    -run barrel_ngram_bench_integrity main "$CORPUS" "$OUT" "$RUNS"
echo "load: $(uptime)"
