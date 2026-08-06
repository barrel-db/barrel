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
# Most databases below are still created via `barrel_server eval`
# (barrel_docdb:create_db/2), for brevity in scenarios where HTTP db
# creation isn't itself what's under test. `PUT /db/:name` also accepts
# `att_opts` in a JSON body now (see the "backend selection over HTTP"
# scenario) -- one database is created that way specifically, to prove the
# HTTP surface itself works, not just the Erlang API underneath it.
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
check_bool() {  # check_bool <description> <exit-code-already-captured>
    # The condition itself must be evaluated by the CALLER (typically an
    # `if ...; then rc=0; else rc=1; fi`), not inline here: under
    # `set -e`, a non-zero exit from a plain statement kills the script
    # before this function would ever see it.
    if [ "$2" -eq 0 ]; then
        echo "  ok   $1"
        pass=$((pass + 1))
    else
        echo "  FAIL $1"
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

# --- Backend selection over HTTP: PUT /db/:name with att_opts in the body ---
#
# create_db/1 now parses an optional JSON body -- this creates an S3
# (minio)-backed database purely over HTTP, no barrel_server eval
# involved, and confirms the write actually landed in the minio bucket
# (not silently RocksDB, which would round-trip the content identically
# and so not be caught by a content check alone).

echo "--- creating s3httpdb (minio) via PUT /db/:name with att_opts in the body"
MINIO_ATT_OPTS_JSON='{"att_opts":{"backend":"s3","s3":{"bucket":"barrel-att-s3-test","endpoint":"http://minio:9000","region":"us-east-1","access_key_id":"minioadmin","secret_access_key":"minioadmin"}}}'
create_resp=$(curl -fsS -X PUT "$B/db/s3httpdb" -H 'content-type: application/json' -d "$MINIO_ATT_OPTS_JSON")
check_match "create_db with att_opts over HTTP succeeds" '"ok":true' "$create_resp"

curl -fsS -X PUT "$B/db/s3httpdb/doc/doc1" -H 'content-type: application/json' -d '{"n":1}' >/dev/null
curl -fsS -X PUT "$B/db/s3httpdb/doc/doc1/att/note.txt" \
    -H 'content-type: text/plain' -d 'created via http att_opts' >/dev/null
body=$(curl -fsS "$B/db/s3httpdb/doc/doc1/att/note.txt")
check "s3httpdb attachment round trip" 'created via http att_opts' "$body"

# hex("doc1") = 646f6331; the prefix is the db's own name on first open
# (see barrel_att_s3_store's "Key scheme" moduledoc section). The `docker
# run` itself must be the `if` condition, not a separate statement: under
# `set -e`, a non-zero exit from a plain statement kills the script before
# a later `[ $? -eq 0 ]` ever runs.
if docker run --rm --network container:barrel-att-s3-minio \
    -e MC_HOST_local="http://minioadmin:minioadmin@127.0.0.1:9000" \
    minio/mc stat "local/barrel-att-s3-test/s3httpdb/646f6331/note.txt" >/dev/null 2>&1
then rc=0; else rc=1; fi
check_bool "s3httpdb object exists in the minio bucket (att_opts over HTTP genuinely selected S3)" "$rc"

# --- Write-conflict detection: real behavior, not the doc's claim ---
#
# Two paths now: the original Erlang-API one (barrel_server eval, kept for
# coverage of barrel_docdb:put_attachment/5 itself) and real HTTP headers
# (If-None-Match/If-Match, now wired on the attachment PUT route).

echo "--- create_only conflict detection (minio: verifiably enforced)"
# `eval` echoes the final expression's own return value, not anything an
# inner io:format prints (io:format/2 itself returns `ok` -- see
# replication.sh's own note on this), so the conflicting put must be the
# trailing expression, unwrapped.
MINIO_CONFLICT='{ok, _} = barrel_docdb:put_attachment(<<"s3db">>, <<"doc1">>, <<"conf.txt">>, <<"first">>, #{create_only => true}), barrel_docdb:put_attachment(<<"s3db">>, <<"doc1">>, <<"conf.txt">>, <<"second">>, #{create_only => true}).'
out=$(eval_b "$MINIO_CONFLICT" | tr -d '\r\n ')
check_match "minio create_only conflict detected (erlang API)" '^\{error,\{conflict,' "$out"

echo "--- create_only fails fast (garage: not structurally supported, by design)"
GARAGE_CONFLICT='barrel_docdb:put_attachment(<<"garagedb">>, <<"doc1">>, <<"conf.txt">>, <<"first">>, #{create_only => true}).'
out=$(eval_b "$GARAGE_CONFLICT" | tr -d '\r\n ')
check "garage create_only refuses rather than pretends to protect (erlang API)" \
    '{error,conditional_writes_unsupported}' "$out"

echo "--- create_only conflict detection over real HTTP headers (minio)"
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$B/db/s3httpdb/doc/doc1/att/http-conf.txt" \
    -H 'If-None-Match: *' -H 'content-type: text/plain' -d 'first')
check "minio If-None-Match: * succeeds on a fresh key" 201 "$code"
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$B/db/s3httpdb/doc/doc1/att/http-conf.txt" \
    -H 'If-None-Match: *' -H 'content-type: text/plain' -d 'second')
check "minio If-None-Match: * conflicts on an existing key" 409 "$code"

echo "--- create_only fails fast over real HTTP headers (garage) -- must not hang or retry"
t0=$(date +%s)
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$B/db/garagedb/doc/doc1/att/http-conf.txt" \
    -H 'If-None-Match: *' -H 'content-type: text/plain' -d 'first')
elapsed=$(( $(date +%s) - t0 ))
check "garage If-None-Match: * fails fast (501)" 501 "$code"
if [ "$elapsed" -le 5 ]; then rc=0; else rc=1; fi
check_bool "garage conditional-write rejection was fast (${elapsed}s)" "$rc"

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

# --- Bidirectional replication: a real conflict, not just first delivery ---
#
# The section above only ever delivers a doc/attachment that never existed
# on the target -- it doesn't exercise the S3 target's LWW guard. M1 could
# not replicate INTO S3 with real conflict resolution at all (no feed); M2
# adds one. This drives an actual concurrent write on both sides through
# real HTTP replication (mirrors barrel_rep_att_SUITE's
# bidirectional_lww_convergence_rocksdb_and_s3, over the wire instead of
# the local transport CT uses) and checks convergence, not just delivery.

echo "--- creating bidirdb: default backend on peer-a, minio-backed on peer-b"
curl -fsS -X PUT "$A/db/bidirdb" >/dev/null
eval_b "barrel_docdb:create_db(<<\"bidirdb\">>, #{att_opts => $MINIO_ATT_OPTS})." >/dev/null
curl -fsS -X PUT "$A/db/bidirdb/doc/doc1" -H 'content-type: application/json' -d '{"n":1}' >/dev/null
curl -fsS -X PUT "$B/db/bidirdb/doc/doc1" -H 'content-type: application/json' -d '{"n":1}' >/dev/null

echo "--- both sides write the same attachment concurrently"
curl -fsS -X PUT "$A/db/bidirdb/doc/doc1/att/f.txt" \
    -H 'content-type: text/plain' -d 'from peer-a (rocksdb)' >/dev/null
curl -fsS -X PUT "$B/db/bidirdb/doc/doc1/att/f.txt" \
    -H 'content-type: text/plain' -d 'from peer-b (s3)' >/dev/null

PUSH_A_TO_B='E = barrel_rep_transport_http:endpoint(<<"http://peer-b:8080/db/bidirdb">>), {ok, R} = barrel_rep:replicate(<<"bidirdb">>, E, #{target_transport => barrel_rep_transport_http}), io:format("~p~n", [R]).'
PUSH_B_TO_A='E = barrel_rep_transport_http:endpoint(<<"http://peer-a:8080/db/bidirdb">>), {ok, R} = barrel_rep:replicate(<<"bidirdb">>, E, #{target_transport => barrel_rep_transport_http}), io:format("~p~n", [R]).'

echo "--- syncing both ways twice"
eval_a "$PUSH_A_TO_B" >/dev/null || { echo "  push a->b failed"; fail=$((fail+1)); }
eval_b "$PUSH_B_TO_A" >/dev/null || { echo "  push b->a failed"; fail=$((fail+1)); }
eval_a "$PUSH_A_TO_B" >/dev/null || { echo "  push a->b failed"; fail=$((fail+1)); }
eval_b "$PUSH_B_TO_A" >/dev/null || { echo "  push b->a failed"; fail=$((fail+1)); }

body_a=$(curl -fsS "$A/db/bidirdb/doc/doc1/att/f.txt")
body_b=$(curl -fsS "$B/db/bidirdb/doc/doc1/att/f.txt")
check "bidirectional convergence: both peers agree" "$body_a" "$body_b"

echo "--- a further sync moves nothing (stable convergence)"
IDLE_CHECK='E = barrel_rep_transport_http:endpoint(<<"http://peer-b:8080/db/bidirdb">>), {ok, R} = barrel_rep:replicate(<<"bidirdb">>, E, #{target_transport => barrel_rep_transport_http}), maps:get(att_sync, R).'
out=$(eval_a "$IDLE_CHECK" | tr -d '\r\n ')
check_match "idle round: nothing left to write" 'atts_written=>0' "$out"

# --- Branching: fork an S3(minio)-backed database ---
#
# barrel_docdb:branch_db/3 has a real HTTP route (unlike att_opts on
# create_db), so this drives it the same way a client would:
# POST /db/:db/_timeline/branch. checkpoint/2 only does the cheap local
# part synchronously; several attachments give the background copy sweep
# actual work, so branch_db returning fast is a meaningful check, not
# trivially true because there was nothing to copy.

echo "--- creating s3branchparent (minio) with several attachments on peer-b"
eval_b "barrel_docdb:create_db(<<\"s3branchparent\">>, #{att_opts => $MINIO_ATT_OPTS})." >/dev/null
curl -fsS -X PUT "$B/db/s3branchparent/doc/doc1" -H 'content-type: application/json' -d '{"n":1}' >/dev/null
for i in $(seq 1 15); do
    curl -fsS -X PUT "$B/db/s3branchparent/doc/doc1/att/f$i.txt" \
        -H 'content-type: text/plain' -d "attachment number $i" >/dev/null
done

echo "--- forking s3branchparent -> s3branchchild"
branch_resp=$(curl -fsS -X POST "$B/db/s3branchparent/_timeline/branch" \
    -H 'content-type: application/json' -d '{"name":"s3branchchild"}')
check_match "branch_db over HTTP succeeds" '"ok":true' "$branch_resp"

echo "--- reading an inherited attachment on the branch (may be 503 briefly while the copy sweep catches up)"
body=""
for _ in $(seq 1 40); do
    code=$(curl -s -o "$WORKDIR/branch_att.txt" -w '%{http_code}' "$B/db/s3branchchild/doc/doc1/att/f1.txt")
    if [ "$code" = "200" ]; then
        body=$(cat "$WORKDIR/branch_att.txt")
        break
    fi
    sleep 1
done
check "branch attachment converges to parent's content" 'attachment number 1' "$body"

echo "--- branch write is independent of the parent"
curl -fsS -X PUT "$B/db/s3branchchild/doc/doc1/att/branch-only.txt" \
    -H 'content-type: text/plain' -d 'only on the branch' >/dev/null
code=$(curl -s -o /dev/null -w '%{http_code}' "$B/db/s3branchparent/doc/doc1/att/branch-only.txt")
check "parent does not see the branch's own write" 404 "$code"

echo
echo "=== $pass passed, $fail failed"
[ "$fail" -eq 0 ]
