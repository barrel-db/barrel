#!/usr/bin/env bash
#
# Replication across power cuts. Each peer's data directory is on LazyFS
# (Dockerfile.lazyfs), which keeps every write that was not fsynced in its
# own memory. A power cut kills the server, drops that memory and restarts
# the container: what was not synced is gone, as after a real power loss.
#
#   0. The harness drops unsynced data and keeps synced data.
#   1. Writes made with `sync => true` survive a power cut.
#   2. A target loses a batch it acknowledged; the next run sends it again.
#   3. The same with a continuous task.
#   4. A peer loses writes that had already reached the other peer; a
#      two-way task brings them back.
#   5. Attachments a run copied survive a power cut on the target: the
#      target synced them before the checkpoint, nothing is sent again.
#   6. With att_durability => best_effort the target may lose them; the
#      next run sends them again.
#
# Usage: test/e2e/power-fail.sh
# Requires: docker (privileged containers, for FUSE), docker compose,
# curl, jq. Exit 0 = pass.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$DIR/lib.sh"
BASE="$COMPOSE"
COMPOSE="docker compose -f $DIR/docker-compose.power-fail.yml"

cleanup() {
    echo "--- tearing down"
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

power_cut() {  # power_cut <peer>
    docker exec "barrel-e2e-peer-$1" sh -c \
        'pkill -KILL beam.smp; echo lazyfs::clear-cache > /tmp/faults.fifo; sleep 1'
    docker kill "barrel-e2e-peer-$1" >/dev/null
    docker start "barrel-e2e-peer-$1" >/dev/null
    wait_healthy "$1"
}

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

has_count() { [ "$(count "$1" "$2")" = "$3" ]; }

put_atts() {  # put_atts <peer> <db> <n>
    on "$1" "[{ok, _} = barrel_docdb:put_attachment(<<\"$2\">>, <<\"d\", (integer_to_binary(I))/binary>>, <<\"f\">>, binary:copy(<<I:32>>, 1024)) || I <- lists:seq(1, $3)], ok." >/dev/null
}

att_count() {  # att_count <peer> <db> <n>: attachments present, read on the node
    curl -fsS "$(url "$1")/db/$2" >/dev/null
    on "$1" "length([I || I <- lists:seq(1, $3), {ok, _} <- [barrel_docdb:get_attachment(<<\"$2\">>, <<\"d\", (integer_to_binary(I))/binary>>, <<\"f\">>)]])." \
        | tr -d '\r\n'
}

att_push() {  # att_push <from> <to> <db> [extra rep opts]: attachments written
    on "$1" "{ok, R} = barrel_rep:replicate(<<\"$3\">>, $(endpoint "$2" "$3"), #{target_transport => barrel_rep_transport_http${4:-}}), maps:get(atts_written, maps:get(att_sync, R))." \
        | tr -d '\r\n'
}

echo "--- building and starting two peers on LazyFS"
$BASE build peer-a
$COMPOSE up -d --build
for p in a b; do wait_healthy $p; done

echo "--- 0. the harness drops unsynced data only"
docker exec barrel-e2e-peer-a sh -c \
    'echo lost > /lazyfs/mnt/probe-unsynced
     echo kept | dd of=/lazyfs/mnt/probe-synced conv=fsync 2>/dev/null
     echo lazyfs::clear-cache > /tmp/faults.fifo; sleep 1'
check "an unsynced file loses its data" "" \
    "$(docker exec barrel-e2e-peer-a cat /lazyfs/mnt/probe-unsynced)"
check "a synced file keeps its data" kept \
    "$(docker exec barrel-e2e-peer-a cat /lazyfs/mnt/probe-synced)"

echo "--- 1. synced writes survive a power cut"
DB=pf
create_db a $DB
on a "[{ok, _} = barrel_docdb:put_doc(<<\"$DB\">>, #{<<\"id\">> => <<\"s\", (integer_to_binary(I))/binary>>}, #{sync => true}) || I <- lists:seq(1, 50)], ok." >/dev/null
bulk a $DB 1 500
power_cut a
SYNCED=$(curl -fsS "$(url a)/db/$DB" >/dev/null; on a "{ok, N} = barrel_docdb:fold_docs(<<\"$DB\">>, fun(#{<<\"id\">> := <<\"s\", _/binary>>}, A) -> {ok, A + 1}; (_, A) -> {ok, A} end, 0), N." | tr -d '\r\n')
check "all 50 synced writes are back" 50 "$SYNCED"
echo "  $(( $(count a $DB) - SYNCED )) of 500 unsynced writes survived"

echo "--- 2. the target loses a batch it acknowledged"
DB=pr
create_db a $DB
create_db b $DB
bulk a $DB 1 500
push a b $DB
check "the batch reached the target" 500 "$(count b $DB)"
power_cut b
LEFT=$(count b $DB)
echo "  the target kept $LEFT of 500"
check "the power cut lost part of the batch (the test is meaningful)" yes \
    "$([ "$LEFT" -lt 500 ] && echo yes || echo no)"
push a b $DB
check "the next run sends the lost documents again" 500 "$(count b $DB)"
for id in d1 d250 d500; do same_everywhere $DB $id a b; done

echo "--- 3. the target of a continuous task loses what it received"
DB=pc
create_db a $DB
create_db b $DB
T3=$(start_task $DB push)
bulk a $DB 1 500
wait_until 120 has_count b $DB 500 || true
check "the task delivered every document" 500 "$(count b $DB)"
power_cut b
echo "  the target kept $(count b $DB) of 500"
wait_until 180 has_count b $DB 500 || true
check "the task delivers the lost documents again" 500 "$(count b $DB)"
on a "barrel_rep_tasks:stop_task(<<\"$T3\">>)." >/dev/null 2>&1 || true

echo "--- 4. a peer loses writes that already reached the other peer"
DB=po
create_db a $DB
create_db b $DB
T4=$(start_task $DB both)
bulk a $DB 1 500
wait_until 120 has_count b $DB 500 || true
check "the writes reached the other peer" 500 "$(count b $DB)"
power_cut a
echo "  the origin kept $(count a $DB) of 500"
wait_until 180 has_count a $DB 500 || true
check "the two-way task brings the lost writes back" 500 "$(count a $DB)"
for id in d1 d250 d500; do same_everywhere $DB $id a b; done
on a "barrel_rep_tasks:stop_task(<<\"$T4\">>)." >/dev/null 2>&1 || true

echo "--- 5. attachments a run copied survive a power cut on the target"
DB=pa
create_db a $DB
create_db b $DB
put_atts a $DB 200
check "the run copied every attachment" 200 "$(att_push a b $DB)"
power_cut b
check "the target kept every attachment" 200 "$(att_count b $DB 200)"
check "the next run sends nothing again" 0 "$(att_push a b $DB)"

echo "--- 6. best effort: the target may lose them, the next run resends"
DB=pe
create_db a $DB
create_db b $DB
put_atts a $DB 200
check "the run copied every attachment" 200 \
    "$(att_push a b $DB ', att_durability => best_effort')"
power_cut b
LEFT=$(att_count b $DB 200)
echo "  the target kept $LEFT of 200"
check "the power cut lost part of them (the test is meaningful)" yes \
    "$([ "$LEFT" -lt 200 ] && echo yes || echo no)"
SENT=$(att_push a b $DB ', att_durability => best_effort')
check "the next run sends the lost attachments again" $((200 - LEFT)) "$SENT"
check "the target has every attachment" 200 "$(att_count b $DB 200)"

finish
