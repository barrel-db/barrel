# Helpers shared by the e2e scripts. Peers are named a, b, c (compose
# services peer-a/b/c) and old (a container from the previous release).
# Source after setting DIR.

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

finish() {
    echo
    echo "=== $pass passed, $fail failed"
    [ "$fail" -eq 0 ]
}

url() {
    case "$1" in a) echo http://127.0.0.1:8091 ;;
                 b) echo http://127.0.0.1:8092 ;;
                 c) echo http://127.0.0.1:8093 ;;
                 old) echo http://127.0.0.1:8094 ;; esac
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

# Evaluate an Erlang expression on the running node of <peer>.
on() { docker exec -i "barrel-e2e-peer-$1" barrel_server eval "$2"; }

create_db() { curl -fsS -X PUT "$(url "$1")/db/$2" >/dev/null; }

put() {  # put <peer> <db> <id> <json>
    curl -fsS -X PUT "$(url "$1")/db/$2/doc/$3" \
        -H 'content-type: application/json' -d "$4" >/dev/null
}

del() { curl -fsS -X DELETE "$(url "$1")/db/$2/doc/$3" >/dev/null; }

doc() { curl -sS "$(url "$1")/db/$2/doc/$3?conflicts=true"; }
rev() { doc "$@" | jq -r '._rev'; }
field() { doc "$1" "$2" "$3" | jq -r ".$4"; }
nconf() { doc "$@" | jq '(._conflicts // []) | length'; }
code() { curl -s -o /dev/null -w '%{http_code}' "$(url "$1")/db/$2/doc/$3"; }
# live documents, counted on the node (the GET opens the database)
count() {
    curl -fsS "$(url "$1")/db/$2" >/dev/null
    on "$1" "{ok, N} = barrel_docdb:fold_docs(<<\"$2\">>, fun(_, A) -> {ok, A + 1} end, 0), N." \
        | tr -d '\r\n'
}

state() { curl -fsS "$(url "$1")/db/$2/_sync/state/$3"; }
digest() { state "$@" | jq -r '.digest'; }
# the live bodies' "v" fields, sorted, one line
values() { state "$@" | jq -c '[.bodies[] | objects | select(has("v")) | .v] | sort'; }

endpoint() { echo "barrel_rep_transport_http:endpoint(<<\"http://peer-$1:8080/db/$2\">>)"; }

# push/pull/sync run the replicator inside <from>, against <to> over HTTP.
push() {  # push <from> <to> <db> [extra rep opts]
    on "$1" "{ok, _} = barrel_rep:replicate(<<\"$3\">>, $(endpoint "$2" "$3"), #{target_transport => barrel_rep_transport_http${4:-}}), ok." >/dev/null \
        || { echo "  FAIL push $1 -> $2 ($3)"; fail=$((fail + 1)); }
}
pull() {  # pull <into> <from> <db> [extra rep opts]
    on "$1" "{ok, _} = barrel_rep:replicate($(endpoint "$2" "$3"), <<\"$3\">>, #{source_transport => barrel_rep_transport_http${4:-}}), ok." >/dev/null \
        || { echo "  FAIL pull $2 -> $1 ($3)"; fail=$((fail + 1)); }
}
sync() { push "$@"; pull "$@"; }

same_everywhere() {  # same_everywhere <db> <id> <peers...>
    local db=$1 id=$2; shift 2
    local first; first=$(digest "$1" "$db" "$id")
    for p in "$@"; do
        check "$id: peer-$p has the same state as peer-$1" "$first" "$(digest "$p" "$db" "$id")"
    done
}

wait_until() {  # wait_until <seconds> <command...>: poll until it succeeds
    local n=$1; shift
    for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 1; done
    return 1
}
