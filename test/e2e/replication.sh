#!/usr/bin/env bash
#
# End-to-end replication test across two containerised barrel_server instances.
#
# peer-a and peer-b are separate OS processes on a real network. We write to one,
# trigger replication over HTTP, and assert the other converged. Replication is
# initiated inside a peer with `barrel_server eval`, which evaluates against the
# running node, so it runs the real barrel_rep algorithm against the other peer's
# _sync endpoints (livery + hackney over the wire).
#
# Usage: test/e2e/replication.sh   (from the umbrella root, or anywhere)
# Requires: docker, docker compose. Exit 0 = pass, non-zero = fail.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="docker compose -f $DIR/docker-compose.yml"
A=http://127.0.0.1:8091          # peer-a, published
B=http://127.0.0.1:8092          # peer-b, published
DB=notes

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

echo "--- building and starting two peers"
$COMPOSE up -d --build

wait_healthy() {  # wait_healthy <base-url> <name>
    for _ in $(seq 1 40); do
        if curl -fsS "$1/health" >/dev/null 2>&1; then
            echo "  $2 is up"
            return 0
        fi
        sleep 2
    done
    echo "  $2 did not become healthy"; return 1
}
wait_healthy "$A" peer-a
wait_healthy "$B" peer-b

# barrel_server eval, run inside a peer, to push/pull against the other peer's
# _sync endpoints. Peers reach each other by service name on the compose network.
replicate() {  # replicate <peer-service> <erlang-expr>
    $COMPOSE exec -T "$1" barrel_server eval "$2"
}

PUSH_A_TO_B='E = barrel_rep_transport_http:endpoint(<<"http://peer-b:8080/db/notes">>), {ok, R} = barrel_rep:replicate(<<"notes">>, E, #{target_transport => barrel_rep_transport_http}), io:format("~p~n", [R]).'
PULL_B_TO_A='E = barrel_rep_transport_http:endpoint(<<"http://peer-b:8080/db/notes">>), {ok, R} = barrel_rep:replicate(E, <<"notes">>, #{source_transport => barrel_rep_transport_http}), io:format("~p~n", [R]).'

echo "--- creating the database on both peers"
curl -fsS -X PUT "$A/db/$DB" >/dev/null
curl -fsS -X PUT "$B/db/$DB" >/dev/null

echo "--- writing three docs to peer-a"
for i in 1 2 3; do
    curl -fsS -X PUT "$A/db/$DB/doc/doc$i" \
        -H 'content-type: application/json' \
        -d "{\"n\":$i,\"from\":\"a\"}" >/dev/null
done

echo "--- push peer-a -> peer-b"
replicate peer-a "$PUSH_A_TO_B" || { echo "  replication call failed"; fail=$((fail+1)); }

echo "--- assert peer-b converged"
for i in 1 2 3; do
    code=$(curl -s -o /dev/null -w '%{http_code}' "$B/db/$DB/doc/doc$i")
    check "peer-b has doc$i" 200 "$code"
done
n=$(curl -fsS "$B/db/$DB/doc/doc2" | grep -o '"n":2' | head -1)
check "peer-b doc2 body replicated" '"n":2' "$n"

echo "--- write a doc to peer-b, pull peer-b -> peer-a"
curl -fsS -X PUT "$B/db/$DB/doc/docB" \
    -H 'content-type: application/json' -d '{"from":"b"}' >/dev/null
replicate peer-a "$PULL_B_TO_A" || { echo "  replication call failed"; fail=$((fail+1)); }
code=$(curl -s -o /dev/null -w '%{http_code}' "$A/db/$DB/doc/docB")
check "peer-a pulled docB from peer-b" 200 "$code"

echo "--- deleting doc1 on peer-a, re-push, assert peer-b sees the delete"
curl -fsS -X DELETE "$A/db/$DB/doc/doc1" >/dev/null
replicate peer-a "$PUSH_A_TO_B" || { echo "  replication call failed"; fail=$((fail+1)); }
code=$(curl -s -o /dev/null -w '%{http_code}' "$B/db/$DB/doc/doc1")
check "peer-b reflects the delete of doc1" 404 "$code"

echo
echo "=== $pass passed, $fail failed"
[ "$fail" -eq 0 ]
