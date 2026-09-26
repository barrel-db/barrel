#!/bin/sh
# Spike S1: cost of remote fanout (1, 3, 8 members x RTT 0, 20, 100 ms).
# Usage: bench/ctx_remote/run.sh CORPUS.jsonl
# Env: MEMBERS (1,3,8) RTTS (0,20,100) ITER (30) DATA_DIR (temp) OUT (json)
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CORPUS=${1:-${CORPUS:-}}
if [ -z "$CORPUS" ]; then
    echo "usage: $0 CORPUS.jsonl" >&2
    exit 2
fi
cd "$ROOT"
rebar3 as server compile >/dev/null
WORK=$(mktemp -d)
DATA_DIR=${DATA_DIR:-$WORK/data}
export DATA_DIR
erlc -o "$WORK" bench/ctx_remote/barrel_ctx_bench.erl \
    apps/barrel_server/test/barrel_ctx_delay_proxy.erl
erl -noshell -pa _build/server/lib/*/ebin -pa "$WORK" \
    -eval "try barrel_ctx_bench:main([\"$CORPUS\"]) of _ -> halt(0)
           catch C:E:S -> io:format(\"~p:~p~n~p~n\", [C, E, S]), halt(1) end."
