#!/usr/bin/env bash
#
# Active-active replication across three containerised barrel_server peers.
#
# Every peer takes writes. The scenarios write the same documents on
# several peers, replicate over HTTP, and check that every peer ends with
# the same document state (same digest), that no concurrent write is lost,
# and that a replaced version never comes back. They cover concurrent
# edits, edits over the winner, delete against edit, resolving over HTTP,
# a late empty joiner, a peer that stops and returns, concurrent
# resolutions, filtered replication of a delete, and a continuous task in
# both directions.
#
# Usage: test/e2e/active-active.sh
# Requires: docker, docker compose, curl, jq. Exit 0 = pass.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="docker compose -f $DIR/docker-compose.yml"

pass=0
fail=0
check() {  # check <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "  ok   $1"
        pass=$((pass + 1))
    else
        echo "  FAIL $1: expected [$2], got [$3]"
        fail=$((fail + 1))
    fi
}

cleanup() {
    echo "--- tearing down"
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

url() {  # url <a|b|c>
    case "$1" in a) echo http://127.0.0.1:8091 ;;
                 b) echo http://127.0.0.1:8092 ;;
                 c) echo http://127.0.0.1:8093 ;; esac
}

wait_healthy() {  # wait_healthy <peer>
    for _ in $(seq 1 60); do
        if curl -fsS "$(url "$1")/health" >/dev/null 2>&1; then
            echo "  peer-$1 is up"; return 0
        fi
        sleep 2
    done
    echo "  peer-$1 did not become healthy"; return 1
}

create_db() { curl -fsS -X PUT "$(url "$1")/db/$2" >/dev/null; }

put() {  # put <peer> <db> <id> <json>
    curl -fsS -X PUT "$(url "$1")/db/$2/doc/$3" \
        -H 'content-type: application/json' -d "$4" >/dev/null
}

del() { curl -fsS -X DELETE "$(url "$1")/db/$2/doc/$3" >/dev/null; }

doc() { curl -sS "$(url "$1")/db/$2/doc/$3?conflicts=true"; }
rev() { doc "$@" | jq -r '._rev'; }
nconf() { doc "$@" | jq '(._conflicts // []) | length'; }
code() { curl -s -o /dev/null -w '%{http_code}' "$(url "$1")/db/$2/doc/$3"; }

state() { curl -fsS "$(url "$1")/db/$2/_sync/state/$3"; }
digest() { state "$@" | jq -r '.digest'; }
# the live bodies' "v" fields, sorted, one line
values() { state "$@" | jq -c '[.bodies[] | objects | select(has("v")) | .v] | sort'; }

# Push to and pull from another peer, from inside <from>, over HTTP.
sync() {  # sync <from> <to> <db> [extra rep opts]
    local opts="${4:-}"
    local expr="E = barrel_rep_transport_http:endpoint(<<\"http://peer-$2:8080/db/$3\">>),
        {ok, _} = barrel_rep:replicate(<<\"$3\">>, E, #{target_transport => barrel_rep_transport_http$opts}),
        {ok, _} = barrel_rep:replicate(E, <<\"$3\">>, #{source_transport => barrel_rep_transport_http$opts}),
        ok."
    $COMPOSE exec -T "peer-$1" barrel_server eval "$expr" >/dev/null \
        || { echo "  FAIL sync $1 <-> $2 ($3)"; fail=$((fail + 1)); }
}

push() {  # push <from> <to> <db> <extra rep opts>
    local expr="E = barrel_rep_transport_http:endpoint(<<\"http://peer-$2:8080/db/$3\">>),
        {ok, _} = barrel_rep:replicate(<<\"$3\">>, E, #{target_transport => barrel_rep_transport_http$4}),
        ok."
    $COMPOSE exec -T "peer-$1" barrel_server eval "$expr" >/dev/null \
        || { echo "  FAIL push $1 -> $2 ($3)"; fail=$((fail + 1)); }
}

same_everywhere() {  # same_everywhere <db> <id> <peers...>
    local db=$1 id=$2; shift 2
    local first; first=$(digest "$1" "$db" "$id")
    for p in "$@"; do
        check "$id: peer-$p has the same state as peer-$1" "$first" "$(digest "$p" "$db" "$id")"
    done
}

echo "--- building and starting three peers"
$COMPOSE up -d --build
for p in a b c; do wait_healthy $p; done

DB=aa
create_db a $DB
create_db b $DB

echo "--- 1. concurrent edits on two peers become one conflict"
put a $DB c1 '{"v":"0"}'
sync a b $DB
put a $DB c1 "{\"v\":\"a\",\"_rev\":\"$(rev a $DB c1)\"}"
put b $DB c1 "{\"v\":\"b\",\"_rev\":\"$(rev b $DB c1)\"}"
sync a b $DB
same_everywhere $DB c1 a b
check "c1: peer-a shows one conflict" 1 "$(nconf a $DB c1)"
check "c1: peer-b shows one conflict" 1 "$(nconf b $DB c1)"
check "c1: same winner on both peers" "$(rev a $DB c1)" "$(rev b $DB c1)"
check "c1: both writes are kept" '["a","b"]' "$(values a $DB c1)"
LOSER=$(doc a $DB c1 | jq -r '.v as $w | if $w == "a" then "b" else "a" end')

echo "--- 2. nine edits over the winner keep the losing write"
for i in 1 2 3 4 5 6 7 8 9; do
    put a $DB c1 "{\"v\":\"w$i\",\"_rev\":\"$(rev a $DB c1)\"}"
    sync a b $DB
done
same_everywhere $DB c1 a b
check "c1: still one conflict on peer-a" 1 "$(nconf a $DB c1)"
check "c1: still one conflict on peer-b" 1 "$(nconf b $DB c1)"
check "c1: the losing write survives on peer-b" "[\"$LOSER\",\"w9\"]" "$(values b $DB c1)"

echo "--- 3. a delete on one peer against an edit on another"
put a $DB d1 '{"v":"1"}'
sync a b $DB
del a $DB d1
put b $DB d1 "{\"v\":\"2\",\"_rev\":\"$(rev b $DB d1)\"}"
sync a b $DB
same_everywhere $DB d1 a b
check "d1: the edit survives the delete on peer-a" '["2"]' "$(values a $DB d1)"
check "d1: the edit survives the delete on peer-b" '["2"]' "$(values b $DB d1)"

echo "--- 4. resolving over HTTP with _observed"
OBS=$(doc b $DB c1 | jq -c '._observed')
put b $DB c1 "{\"v\":\"merged\",\"_observed\":$OBS}"
sync b a $DB
same_everywhere $DB c1 a b
check "c1: no conflict left on peer-a" 0 "$(nconf a $DB c1)"
check "c1: no conflict left on peer-b" 0 "$(nconf b $DB c1)"
check "c1: peer-a has the resolution" '["merged"]' "$(values a $DB c1)"

echo "--- 5. a new empty peer joins"
create_db c $DB
sync c a $DB
same_everywhere $DB c1 a c
same_everywhere $DB d1 a c
check "c1: peer-c has the resolution" '["merged"]' "$(values c $DB c1)"
check "d1: peer-c has the delete and the edit" "$(nconf a $DB d1)" "$(nconf c $DB d1)"

echo "--- 6. a peer stops, the others edit, it returns with its own edit"
$COMPOSE stop peer-b >/dev/null
put a $DB c1 "{\"v\":\"a-while-b-away\",\"_rev\":\"$(rev a $DB c1)\"}"
sync a c $DB
$COMPOSE start peer-b >/dev/null
wait_healthy b
put b $DB c1 "{\"v\":\"b-back\",\"_rev\":\"$(rev b $DB c1)\"}"
sync b c $DB          # peer-b learns peer-a's edit through peer-c
sync c a $DB
same_everywhere $DB c1 a b c
check "c1: one conflict on peer-a" 1 "$(nconf a $DB c1)"
check "c1: both edits kept, the replaced resolution gone" \
    '["a-while-b-away","b-back"]' "$(values a $DB c1)"

echo "--- 7. two peers resolve the same conflict at once, a third settles it"
OBS_A=$(doc a $DB c1 | jq -c '._observed')
OBS_B=$(doc b $DB c1 | jq -c '._observed')
put a $DB c1 "{\"v\":\"res-a\",\"_observed\":$OBS_A}"
put b $DB c1 "{\"v\":\"res-b\",\"_observed\":$OBS_B}"
sync a b $DB
sync b c $DB
same_everywhere $DB c1 a b c
check "c1: the two resolutions are in conflict" '["res-a","res-b"]' "$(values c $DB c1)"
OBS_C=$(doc c $DB c1 | jq -c '._observed')
put c $DB c1 "{\"v\":\"final\",\"_observed\":$OBS_C}"
sync c a $DB
sync c b $DB
same_everywhere $DB c1 a b c
check "c1: settled on peer-b" '["final"]' "$(values b $DB c1)"
check "c1: no conflict on peer-b" 0 "$(nconf b $DB c1)"

echo "--- 8. a filtered replication carries deletes"
FLT=flt
create_db a $FLT
create_db b $FLT
put a $FLT f1 '{"type":"keep"}'
put a $FLT f2 '{"type":"skip"}'
FILTER=', filter => #{query => #{where => [{path, [<<"type">>], <<"keep">>}]}}'
push a b $FLT "$FILTER"
check "f1: matches the filter, replicated" 200 "$(code b $FLT f1)"
check "f2: outside the filter, not replicated" 404 "$(code b $FLT f2)"
del a $FLT f1
push a b $FLT "$FILTER"
check "f1: the delete passes the filter" 404 "$(code b $FLT f1)"
same_everywhere $FLT f1 a b

echo "--- 9. a continuous task in both directions with concurrent writes"
CONT=cont
create_db a $CONT
create_db b $CONT
START='{ok, T} = barrel_rep_tasks:start_task(#{source => <<"cont">>, target => barrel_rep_transport_http:endpoint(<<"http://peer-b:8080/db/cont">>), target_transport => barrel_rep_transport_http, mode => continuous, direction => both}), binary_to_list(T).'
TASK=$($COMPOSE exec -T peer-a barrel_server eval "$START" | tr -d '\r\n' | sed -E 's/^"//; s/"$//')
echo "  task $TASK"
put a $CONT k1 '{"v":"0"}'
for _ in $(seq 1 30); do [ "$(code b $CONT k1)" = 200 ] && break; sleep 1; done
check "k1: reached peer-b through the task" 200 "$(code b $CONT k1)"
put a $CONT k1 "{\"v\":\"a\",\"_rev\":\"$(rev a $CONT k1)\"}" &
put b $CONT k1 "{\"v\":\"b\",\"_rev\":\"$(rev b $CONT k1)\"}" &
wait
for _ in $(seq 1 30); do
    [ "$(digest a $CONT k1)" = "$(digest b $CONT k1)" ] \
        && [ "$(values a $CONT k1)" = '["a","b"]' ] && break
    sleep 1
done
same_everywhere $CONT k1 a b
check "k1: both concurrent writes kept" '["a","b"]' "$(values b $CONT k1)"
$COMPOSE exec -T peer-a barrel_server eval \
    "barrel_rep_tasks:stop_task(<<\"$TASK\">>)." >/dev/null 2>&1 || true

echo
echo "=== $pass passed, $fail failed"
[ "$fail" -eq 0 ]
