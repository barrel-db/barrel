#!/usr/bin/env bash
#
# Replication when peers are killed (SIGKILL) mid-run.
#
#   1. The target of a continuous push is killed while it receives a
#      batch; once it is back, the task resumes and every document lands.
#   2. The peer running the task is killed; acknowledged writes survive,
#      the task is restored on restart and finishes.
#   3. Both peers write the same new ids while a two-way task runs and one
#      of them is killed. Every write that was acknowledged must be on both
#      peers once they converge.
#
# Usage: test/e2e/crash.sh
# Requires: docker, docker compose, curl, jq. Exit 0 = pass.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib.sh"
WORK="$(mktemp -d)"

cleanup() {
    echo "--- tearing down"
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$WORK"
}
trap cleanup EXIT

bulk() {  # bulk <peer> <db> <from> <to>
    jq -nc --argjson s "$3" --argjson e "$4" \
        '{docs: [range($s; $e + 1) | {id: "d\(.)", v: .}]}' \
    | curl -fsS -X POST "$(url "$1")/db/$2/_bulk_docs" \
        -H 'content-type: application/json' -d @- >/dev/null
}

start_task() {  # start_task <db> <direction>
    on a "{ok, T} = barrel_rep_tasks:start_task(#{source => <<\"$1\">>, target => $(endpoint b "$1"), target_transport => barrel_rep_transport_http, mode => continuous, direction => $2}), binary_to_list(T)." \
        | tr -d '\r\n' | sed -E 's/^"//; s/"$//'
}

kill_peer() {  # kill_peer <peer>: SIGKILL, then start again
    docker kill -s KILL "barrel-e2e-peer-$1" >/dev/null
    docker start "barrel-e2e-peer-$1" >/dev/null
    wait_healthy "$1"
}

has_count() { [ "$(count "$1" "$2")" = "$3" ]; }

echo "--- starting two peers"
$COMPOSE up -d --build peer-a peer-b
for p in a b; do wait_healthy $p; done

DB=cr
create_db a $DB
create_db b $DB

echo "--- 1. the target is killed while a continuous push runs"
for s in 1 501 1001 1501; do bulk a $DB $s $((s + 499)); done
T1=$(start_task $DB push)
started() { [ "$(count b $DB)" -gt 0 ] 2>/dev/null; }
wait_until 30 started || true
echo "  peer-b has $(count b $DB) docs when killed"
kill_peer b
wait_until 180 has_count b $DB 2000 || true
check "every document reached the restarted target" 2000 "$(count b $DB)"
for id in d1 d1000 d2000; do same_everywhere $DB $id a b; done

echo "--- 2. the peer running the task is killed"
for s in 2001 2501 3001 3501; do bulk a $DB $s $((s + 499)); done
kill_peer a
check "acknowledged writes survive the kill" 4000 "$(count a $DB)"
wait_until 180 has_count b $DB 4000 || true
check "the restored task finishes the run" 4000 "$(count b $DB)"
same_everywhere $DB d4000 a b
on a "barrel_rep_tasks:stop_task(<<\"$T1\">>)." >/dev/null 2>&1 || true

echo "--- 3. both peers write the same ids while one is killed"
DB2=cr2
create_db a $DB2
create_db b $DB2
T2=$(start_task $DB2 both)
writer() {  # writer <peer>: create s1..s100, log what was acknowledged
    for i in $(seq 1 100); do
        if curl -fsS -m 5 -X PUT "$(url "$1")/db/$DB2/doc/s$i" \
            -H 'content-type: application/json' \
            -d "{\"v\":\"$1$i\"}" >/dev/null 2>&1; then
            echo "s$i $1$i" >> "$WORK/acked"
        fi
    done
}
writer a & WA=$!
writer b & WB=$!
sleep 1
docker kill -s KILL barrel-e2e-peer-b >/dev/null
wait $WA $WB || true
docker start barrel-e2e-peer-b >/dev/null
wait_healthy b
echo "  $(wc -l < "$WORK/acked" | tr -d ' ') writes acknowledged"

converged() {
    for i in $(seq 1 100); do
        [ "$(digest a $DB2 s$i 2>/dev/null || true)" = \
          "$(digest b $DB2 s$i 2>/dev/null || true)" ] || return 1
    done
}
wait_until 180 converged || true
diverged=0
for i in $(seq 1 100); do
    [ "$(digest a $DB2 s$i 2>/dev/null || true)" = \
      "$(digest b $DB2 s$i 2>/dev/null || true)" ] || diverged=$((diverged + 1))
done
check "every document has the same state on both peers" 0 "$diverged"
missing=0
while read -r id v; do
    for p in a b; do
        values $p $DB2 "$id" | jq -e --arg v "$v" 'index($v) != null' >/dev/null \
            || { echo "  missing $v of $id on peer-$p"; missing=$((missing + 1)); }
    done
done < "$WORK/acked"
check "every acknowledged write is on both peers" 0 "$missing"
on a "barrel_rep_tasks:stop_task(<<\"$T2\">>)." >/dev/null 2>&1 || true

finish
