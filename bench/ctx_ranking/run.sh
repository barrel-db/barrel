#!/bin/sh
# Spike S2 harness: ./bench/ctx_ranking/run.sh CORPUS_DIR [WORK_DIR] [RERANK_PYTHON|embed]
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CORPUS=${1:?corpus dir with corpus.jsonl and queries.jsonl}
WORK=${2:-${TMPDIR:-/tmp}/barrel_ctx_ranking}
EXTRA=${3:-}
mkdir -p "$WORK/ebin"
(cd "$ROOT" && rebar3 compile)
erlc -o "$WORK/ebin" "$ROOT/bench/ctx_ranking/ctx_ranking.erl"
exec erl -noshell -pa "$ROOT"/_build/default/lib/*/ebin -pa "$WORK/ebin" \
    -eval "ok = ctx_ranking:main([\"$CORPUS\", \"$WORK\", \"$EXTRA\"]), halt(0)."
