#!/bin/bash
#
# Compare checkouts on one writer case with the same harness: the sides
# run one after the other, in an order that rotates each iteration, and
# the load average is recorded before every run.
#
# Usage:
#   ./writer_compare.sh CASE ITERATIONS DURATION_MS ROOT...
#
# Each ROOT is an umbrella checkout with a compiled _build. Prints one
# line per run, then the median docs/s, p50 and p99 per side.

set -e

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
CASE="$1"; ITER="$2"; DUR="$3"; shift 3
ROOTS=("$@")
N=${#ROOTS[@]}
RES="$(mktemp "${TMPDIR:-/tmp}/barrel_writer_compare.XXXXXX")"
trap 'rm -f "$RES"' EXIT

for ((i = 0; i < ITER; i++)); do
    for ((k = 0; k < N; k++)); do
        idx=$(( (k + i) % N ))
        root="${ROOTS[$idx]}"
        load=$(uptime | sed 's/.*load averages*: //')
        line=$(ROOT="$root" "$BENCH_DIR/writer_bench.sh" run "$CASE" "$DUR" 2>&1 \
               | tr -d '\n' | sed 's/.*RESULT//')
        docs=$(echo "$line" | sed 's/.*docs_per_s => \([0-9]*\).*/\1/')
        p50=$(echo "$line" | sed 's/.*p50_us => \([0-9]*\).*/\1/')
        p99=$(echo "$line" | sed 's/.*p99_us => \([0-9]*\).*/\1/')
        echo "$i $idx $docs $p50 $p99 load=[$load] $root"
        echo "$idx $docs $p50 $p99" >> "$RES"
    done
done

median() { sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'; }
echo "CASE $CASE"
for ((k = 0; k < N; k++)); do
    d=$(awk -v k=$k '$1 == k {print $2}' "$RES" | median)
    p50=$(awk -v k=$k '$1 == k {print $3}' "$RES" | median)
    p99=$(awk -v k=$k '$1 == k {print $4}' "$RES" | median)
    echo "MEDIAN docs_per_s=$d p50_us=$p50 p99_us=$p99 ${ROOTS[$k]}"
done
