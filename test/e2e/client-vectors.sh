#!/usr/bin/env bash
#
# Client-supplied vectors across containerised barrel_server peers.
#
# Two peers write the same document concurrently, each with its own
# vector. Every peer must keep the vector of every version: the winner's,
# the losing version's, and past versions', over HTTP replication. A
# resolution back to the losing version, or a new resolution carrying its
# own vector, reaches every peer with that vector. A peer on the previous
# release (OLD_REF) keeps replicating; it drops the vectors of versions
# that are not current, and the next direct sync from an upgraded peer
# brings them back.
#
# Usage: test/e2e/client-vectors.sh
# Requires: docker, docker compose, curl, jq, git, python3. Exit 0 = pass.
# OLD_PORT (default 8094) is the host port of the previous-release peer.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
. "$DIR/lib.sh"

OLD_REF="${OLD_REF:-barrel_server-v1.14.2}"
OLD_IMAGE=barrel-server-e2e-vec-old
OLD_SRC="$(mktemp -d)"

cleanup() {
    echo "--- tearing down"
    docker rm -f barrel-e2e-peer-old >/dev/null 2>&1 || true
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
    git -C "$ROOT" worktree remove --force "$OLD_SRC" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# base64 of the float32 little-endian vector, the REST wire form
b64() {
    python3 -c 'import base64, struct, sys; v = [float(x) for x in sys.argv[1:]]; print(base64.b64encode(struct.pack("<%df" % len(v), *v)).decode())' "$@"
}
vec() {  # vec <peer> <db> <id>: the current version's vector
    curl -sS "$(url "$1")/db/$2/doc/$3?include_embedding=true" | jq -r '._embedding.vector // "none"'
}
pastvec() {  # pastvec <peer> <db> <id> <rev>: a version's vector
    curl -sS "$(url "$1")/db/$2/doc/$3/_versions/$4?include_embedding=true" | jq -r '._embedding.vector // "none"'
}
resolve_choose() {  # resolve_choose <peer> <db> <id> <current> <chosen>
    on "$1" "{ok, _} = barrel_docdb:resolve_conflict(<<\"$2\">>, <<\"$3\">>, <<\"$4\">>, {choose, <<\"$5\">>}), ok." >/dev/null
}

VA=$(b64 0.5 0.25 0.125)
VB=$(b64 0.75 1.0 0.0625)
VC=$(b64 0.375 2.0 0.5)

echo "--- building the previous release from $OLD_REF"
git -C "$ROOT" worktree add -f --detach "$OLD_SRC" "$OLD_REF" >/dev/null
docker build -q -t $OLD_IMAGE -f "$OLD_SRC/test/e2e/Dockerfile" "$OLD_SRC" >/dev/null

echo "--- starting three upgraded peers and the old peer"
$COMPOSE up -d --build
docker run -d --name barrel-e2e-peer-old --network barrel-e2e_default \
    --network-alias peer-old -p "${OLD_PORT:-8094}:8080" $OLD_IMAGE >/dev/null
for p in a b c old; do wait_healthy $p; done

DB=vec
for p in a b c old; do create_db $p $DB; done

echo "--- 1. concurrent writes with their own vectors, synced"
put a $DB m '{"text":"a","_embedding":[0.5,0.25,0.125]}'
RA=$(rev a $DB m)
sleep 0.01
put b $DB m '{"text":"b","_embedding":[0.75,1.0,0.0625]}'
RB=$(rev b $DB m)
sync a b $DB
same_everywhere $DB m a b
for p in a b; do
    check "m: peer-$p indexes the winner's vector" "$VB" "$(vec $p $DB m)"
    check "m: peer-$p keeps the losing version's vector" "$VA" "$(pastvec $p $DB m "$RA")"
done

echo "--- 2. a third peer gets both vectors from one sync"
pull c b $DB
check "m: peer-c current vector" "$VB" "$(vec c $DB m)"
check "m: peer-c losing version's vector" "$VA" "$(pastvec c $DB m "$RA")"

echo "--- 3. resolving to the losing version keeps its vector everywhere"
resolve_choose c $DB m "$RB" "$RA"
check "m: peer-c current vector after resolution" "$VA" "$(vec c $DB m)"
push c a $DB
push c b $DB
same_everywhere $DB m a b c
for p in a b c; do
    check "m: peer-$p indexes the chosen version's vector" "$VA" "$(vec $p $DB m)"
    check "m: peer-$p keeps the replaced winner's vector" "$VB" "$(pastvec $p $DB m "$RB")"
done

echo "--- 4. a resolution over HTTP carries its own vector"
put a $DB r '{"text":"a"}'
sync a b $DB
put a $DB r "{\"text\":\"a2\",\"_rev\":\"$(rev a $DB r)\",\"_embedding\":[0.5,0.25,0.125]}"
put b $DB r "{\"text\":\"b2\",\"_rev\":\"$(rev b $DB r)\",\"_embedding\":[0.75,1.0,0.0625]}"
sync a b $DB
OBS=$(doc a $DB r | jq -c '._observed')
put a $DB r "{\"text\":\"merged\",\"_observed\":$OBS,\"_embedding\":[0.375,2.0,0.5]}"
sync a b $DB
pull c a $DB
same_everywhere $DB r a b c
for p in a b c; do
    check "r: peer-$p no conflict" 0 "$(nconf $p $DB r)"
    check "r: peer-$p indexes the resolution's vector" "$VC" "$(vec $p $DB r)"
done

echo "--- 5. the previous release replicates, the next direct sync restores vectors"
put a $DB o '{"text":"a","_embedding":[0.5,0.25,0.125]}'
RO=$(rev a $DB o)
sleep 0.01
put b $DB o '{"text":"b","_embedding":[0.75,1.0,0.0625]}'
sync a b $DB
push a old $DB
check "o: the old peer has the winner's vector" "$VB" "$(vec old $DB o)"
pull old a $DB
check "o: the old peer replicates back without loss" "$(digest a $DB o)" "$(digest old $DB o)"
DB2=relay
for p in a b c old; do create_db $p $DB2; done
put a $DB2 x '{"text":"a","_embedding":[0.5,0.25,0.125]}'
RX=$(rev a $DB2 x)
sleep 0.01
put b $DB2 x '{"text":"b","_embedding":[0.75,1.0,0.0625]}'
sync a b $DB2
push a old $DB2
push old c $DB2
check "x: relayed through the old peer, the winner's vector arrives" "$VB" "$(vec c $DB2 x)"
check "x: the old peer dropped the losing version's vector" none "$(pastvec c $DB2 x "$RX")"
pull c a $DB2
check "x: a direct sync from an upgraded peer restores it" "$VA" "$(pastvec c $DB2 x "$RX")"

finish
