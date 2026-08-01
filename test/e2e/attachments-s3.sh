#!/usr/bin/env bash
#
# End-to-end test for the S3 attachment storage backend (barrel_att_s3),
# against real MinIO and Garage containers and a real barrel_server release
# built with the S3 backend included (the `s3_server` rebar3 profile) --
# not CT against the backend module directly (that's
# apps/barrel_att_s3/test/barrel_att_s3_SUITE.erl), and not a mock store.
#
# peer-a keeps the default (RocksDB) attachment backend; peer-b's databases
# point their att_opts at MinIO or Garage. Both run the same release image.
#
# Databases are created via `barrel_server eval` (barrel_docdb:create_db/2),
# not a plain HTTP PUT /db/:name with a JSON body: barrel_server_http's
# create_db/1 reads only the :db path binding and never parses a request
# body at all, so att_opts (which backend, which bucket) has no HTTP
# surface yet -- confirmed by reading the handler, not assumed. Once a
# database exists, its normal doc/attachment HTTP routes work exactly as
# usual, since they operate on the already-configured db by name.
#
# Usage: test/e2e/attachments-s3.sh   (from the umbrella root, or anywhere)
# Requires: docker, docker compose. Exit 0 = pass, non-zero = fail.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="docker compose -f $DIR/docker-compose.attachments-s3.yml"
A=http://127.0.0.1:8093   # peer-a, published
B=http://127.0.0.1:8094   # peer-b, published

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
check_match() {  # check_match <description> <regex> <actual>
    if echo "$3" | grep -qE "$2"; then
        echo "  ok   $1"
        pass=$((pass + 1))
    else
        echo "  FAIL $1: pattern [$2] not found in [$3]"
        fail=$((fail + 1))
    fi
}

cleanup() {
    echo "--- tearing down"
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
    rm -f "$WORKDIR"/*.bin
}
WORKDIR=$(mktemp -d)
trap cleanup EXIT

echo "--- building peer image and starting the stack"
$COMPOSE build peer-a
echo "--- provisioning minio + garage"
GARAGE_ENV=$("$DIR"/attachments-s3-setup.sh)
eval "$GARAGE_ENV"

echo "--- starting the peers"
$COMPOSE up -d peer-a peer-b

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

eval_b() {  # eval_b <erlang-expr>   -- runs inside peer-b, network-adjacent to minio/garage
    $COMPOSE exec -T peer-b barrel_server eval "$1"
}
eval_a() {  # eval_a <erlang-expr>
    $COMPOSE exec -T peer-a barrel_server eval "$1"
}

# --- MinIO-backed database: whole-blob round trip (small + multipart-crossing) ---

MINIO_ATT_OPTS='#{backend => s3, s3 => #{bucket => <<"barrel-att-s3-test">>, endpoint => <<"http://minio:9000">>, region => <<"us-east-1">>, access_key_id => <<"minioadmin">>, secret_access_key => <<"minioadmin">>}}'

echo "--- creating minio-backed db on peer-b"
eval_b "barrel_docdb:create_db(<<\"s3db\">>, #{att_opts => $MINIO_ATT_OPTS})." >/dev/null
curl -fsS -X PUT "$B/db/s3db/doc/doc1" -H 'content-type: application/json' -d '{"n":1}' >/dev/null

echo "--- small attachment round trip (minio)"
curl -fsS -X PUT "$B/db/s3db/doc/doc1/att/small.txt" \
    -H 'content-type: text/plain' -d 'hello from the s3 backend e2e test' >/dev/null
body=$(curl -fsS "$B/db/s3db/doc/doc1/att/small.txt")
check "minio small attachment content" 'hello from the s3 backend e2e test' "$body"

echo "--- large (multipart-crossing) attachment round trip (minio)"
head -c 6000000 /dev/urandom > "$WORKDIR/large.bin"
sum_before=$(shasum -a 256 "$WORKDIR/large.bin" | awk '{print $1}')
curl -fsS -X PUT "$B/db/s3db/doc/doc1/att/large.bin" \
    -H 'content-type: application/octet-stream' --data-binary "@$WORKDIR/large.bin" >/dev/null
curl -fsS "$B/db/s3db/doc/doc1/att/large.bin" -o "$WORKDIR/large.roundtrip.bin"
sum_after=$(shasum -a 256 "$WORKDIR/large.roundtrip.bin" | awk '{print $1}')
check "minio large attachment digest" "$sum_before" "$sum_after"

echo "--- delete + 404 (minio)"
curl -fsS -X DELETE "$B/db/s3db/doc/doc1/att/small.txt" >/dev/null
code=$(curl -s -o /dev/null -w '%{http_code}' "$B/db/s3db/doc/doc1/att/small.txt")
check "minio attachment gone after delete" 404 "$code"

# --- Garage-backed database: same whole-blob round trip ---

GARAGE_ATT_OPTS="#{backend => s3, s3 => #{bucket => <<\"$GARAGE_S3_TEST_BUCKET\">>, endpoint => <<\"http://garage:3900\">>, region => <<\"garage\">>, access_key_id => <<\"$GARAGE_S3_TEST_ACCESS_KEY\">>, secret_access_key => <<\"$GARAGE_S3_TEST_SECRET_KEY\">>}}"

echo "--- creating garage-backed db on peer-b"
eval_b "barrel_docdb:create_db(<<\"garagedb\">>, #{att_opts => $GARAGE_ATT_OPTS})." >/dev/null
curl -fsS -X PUT "$B/db/garagedb/doc/doc1" -H 'content-type: application/json' -d '{"n":1}' >/dev/null

echo "--- attachment round trip (garage)"
curl -fsS -X PUT "$B/db/garagedb/doc/doc1/att/note.txt" \
    -H 'content-type: text/plain' -d 'hello from garage' >/dev/null
body=$(curl -fsS "$B/db/garagedb/doc/doc1/att/note.txt")
check "garage attachment content" 'hello from garage' "$body"
curl -fsS -X DELETE "$B/db/garagedb/doc/doc1/att/note.txt" >/dev/null
code=$(curl -s -o /dev/null -w '%{http_code}' "$B/db/garagedb/doc/doc1/att/note.txt")
check "garage attachment gone after delete" 404 "$code"

# --- Write-conflict detection: real behavior, not the doc's claim ---
#
# There is no HTTP-level If-Match/If-None-Match wiring yet (create_only/
# expected_etag are Erlang-API-only as of M1), so this drives them the same
# way replication is driven below: barrel_server eval against the running
# node, not raw HTTP headers.

echo "--- create_only conflict detection (minio: verifiably enforced)"
# `eval` echoes the final expression's own return value, not anything an
# inner io:format prints (io:format/2 itself returns `ok` -- see
# replication.sh's own note on this), so the conflicting put must be the
# trailing expression, unwrapped.
MINIO_CONFLICT='{ok, _} = barrel_docdb:put_attachment(<<"s3db">>, <<"doc1">>, <<"conf.txt">>, <<"first">>, #{create_only => true}), barrel_docdb:put_attachment(<<"s3db">>, <<"doc1">>, <<"conf.txt">>, <<"second">>, #{create_only => true}).'
out=$(eval_b "$MINIO_CONFLICT" | tr -d '\r\n ')
check_match "minio create_only conflict detected" '^\{error,\{conflict,' "$out"

echo "--- create_only fails fast (garage: not structurally supported, by design)"
GARAGE_CONFLICT='barrel_docdb:put_attachment(<<"garagedb">>, <<"doc1">>, <<"conf.txt">>, <<"first">>, #{create_only => true}).'
out=$(eval_b "$GARAGE_CONFLICT" | tr -d '\r\n ')
check "garage create_only refuses rather than pretends to protect" \
    '{error,conditional_writes_unsupported}' "$out"

# --- Replication asymmetry: RocksDB source -> S3(minio) target ---
#
# Mirrors replication.sh's push pattern, over the real HTTP transport
# between two separate containers, landing on an S3-backed target this
# time (barrel_rep_att_SUITE covers this at the CT level with a stub
# backend; this confirms it live against a real store).

echo "--- creating repdb (default backend) on peer-a, (s3/minio backend) on peer-b"
curl -fsS -X PUT "$A/db/repdb" >/dev/null
eval_b "barrel_docdb:create_db(<<\"repdb\">>, #{att_opts => $MINIO_ATT_OPTS})." >/dev/null

curl -fsS -X PUT "$A/db/repdb/doc/doc1" -H 'content-type: application/json' -d '{"n":1}' >/dev/null
curl -fsS -X PUT "$A/db/repdb/doc/doc1/att/note.txt" -H 'content-type: text/plain' -d 'replicated to s3' >/dev/null

echo "--- push peer-a -> peer-b"
PUSH='E = barrel_rep_transport_http:endpoint(<<"http://peer-b:8080/db/repdb">>), {ok, R} = barrel_rep:replicate(<<"repdb">>, E, #{target_transport => barrel_rep_transport_http}), io:format("~p~n", [R]).'
eval_a "$PUSH" || { echo "  replication call failed"; fail=$((fail+1)); }

code=$(curl -s -o /dev/null -w '%{http_code}' "$B/db/repdb/doc/doc1")
check "peer-b (s3-backed) received doc1" 200 "$code"
body=$(curl -fsS "$B/db/repdb/doc/doc1/att/note.txt" 2>/dev/null || true)
check "peer-b (s3-backed) received the attachment via replication" 'replicated to s3' "$body"

echo
echo "=== $pass passed, $fail failed"
[ "$fail" -eq 0 ]
