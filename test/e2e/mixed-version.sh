#!/usr/bin/env bash
#
# Replication between upgraded peers and a peer still on the previous
# release, both ways, with the replicator running on either side.
#
# An old peer sends and receives one version at a time. Upgraded peers
# never let it remove anything: its edits and deletes arrive as new
# versions next to the ones they replaced, so an edit made on the old
# peer shows as a conflict on upgraded peers until someone resolves it.
# The old peer receives each document's winner.
#
# The old image is built from OLD_REF (default: b272, barrel_server 1.11.1, the
# last release before whole-state replication).
#
# Usage: test/e2e/mixed-version.sh
# Requires: docker, docker compose, curl, jq, git. Exit 0 = pass.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
. "$DIR/lib.sh"

OLD_REF="${OLD_REF:-b272}"   # the bundle of barrel_server 1.11.1
OLD_IMAGE=barrel-server-e2e-old
OLD_SRC="$(mktemp -d)"

cleanup() {
    echo "--- tearing down"
    docker rm -f barrel-e2e-peer-old >/dev/null 2>&1 || true
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
    git -C "$ROOT" worktree remove --force "$OLD_SRC" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "--- building the old peer from $OLD_REF"
git -C "$ROOT" worktree add -f --detach "$OLD_SRC" "$OLD_REF" >/dev/null
docker build -q -t $OLD_IMAGE -f "$OLD_SRC/test/e2e/Dockerfile" "$OLD_SRC" >/dev/null

echo "--- starting two upgraded peers and the old peer"
$COMPOSE up -d --build peer-a peer-b
docker run -d --name barrel-e2e-peer-old --network barrel-e2e_default \
    --network-alias peer-old -p 8094:8080 $OLD_IMAGE >/dev/null
for p in a b old; do wait_healthy $p; done

DB=mx
for p in a b old; do create_db $p $DB; done

echo "--- 1. documents flow both ways, whichever side replicates"
put old $DB o1 '{"v":"o1"}'
push old a $DB                      # old replicator writes to a new server
check "o1: old -> new, old pushing" o1 "$(field a $DB o1 v)"
check "o1: same version on both" "$(rev old $DB o1)" "$(rev a $DB o1)"
put a $DB n1 '{"v":"n1"}'
push a old $DB                      # new replicator writes to an old server
check "n1: new -> old, new pushing" n1 "$(field old $DB n1 v)"
check "n1: same version on both" "$(rev a $DB n1)" "$(rev old $DB n1)"
put old $DB o2 '{"v":"o2"}'
pull a old $DB                      # new replicator reads an old server
check "o2: old -> new, new pulling" o2 "$(field a $DB o2 v)"
put a $DB n2 '{"v":"n2"}'
pull old a $DB                      # old replicator reads a new server
check "n2: new -> old, old pulling" n2 "$(field old $DB n2 v)"

echo "--- 2. an edit on the old peer shows as a conflict, nothing is lost"
put old $DB n1 "{\"v\":\"n1-old-edit\",\"_rev\":\"$(rev old $DB n1)\"}"
pull a old $DB
check "n1: the old peer's edit is the winner" n1-old-edit "$(field a $DB n1 v)"
check "n1: the version it edited is kept" '["n1","n1-old-edit"]' "$(values a $DB n1)"
OBS=$(doc a $DB n1 | jq -c '._observed')
put a $DB n1 "{\"v\":\"n1-resolved\",\"_observed\":$OBS}"
push a old $DB
check "n1: resolved on a" 0 "$(nconf a $DB n1)"
check "n1: the old peer takes the resolution" n1-resolved "$(field old $DB n1 v)"
pull a old $DB
check "n1: the resolution is not undone by the old peer" '["n1-resolved"]' "$(values a $DB n1)"

echo "--- 3. a conflict between upgraded peers reaches the old peer as its winner"
put a $DB c1 '{"v":"0"}'
sync a b $DB
put a $DB c1 "{\"v\":\"a\",\"_rev\":\"$(rev a $DB c1)\"}"
put b $DB c1 "{\"v\":\"b\",\"_rev\":\"$(rev b $DB c1)\"}"
sync a b $DB
push a old $DB
check "c1: the old peer shows the same winner" "$(field a $DB c1 v)" "$(field old $DB c1 v)"
pull a old $DB
check "c1: the conflict stays on the upgraded peer" '["a","b"]' "$(values a $DB c1)"

echo "--- 4. a delete on the old peer keeps the version it deleted"
del old $DB n2
pull a old $DB
check "n2: the delete wins" 404 "$(code a $DB n2)"
check "n2: the deleted version is kept as a conflict" '["n2"]' "$(values a $DB n2)"

echo "--- 5. the upgraded peers converge after the old peer's changes"
sync a b $DB
for id in o1 o2 n1 n2 c1; do same_everywhere $DB $id a b; done

finish
