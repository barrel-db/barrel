#!/usr/bin/env bash
#
# Brings up MinIO + Garage (docker-compose.attachments-s3.yml) and provisions
# both: barrel_att_s3_store's open/2 never creates a bucket itself (confirmed
# by reading it -- it only checks that `bucket` was configured), so nothing
# put through this backend works until the bucket exists. Garage additionally
# needs a one-time layout assignment before it serves any S3 request at all,
# and (unlike MinIO) a Garage key can't create its own bucket either way.
#
# Usage:
#   test/e2e/attachments-s3-setup.sh              # start + provision, print exports
#   eval "$(test/e2e/attachments-s3-setup.sh)"    # ... and load them into the shell
#
# Idempotent for MinIO (bucket creation is `--ignore-existing`). NOT
# idempotent for Garage past the first run: Garage never reveals a key's
# secret again after creation, so re-running against an already-provisioned
# volume fails loudly rather than silently reusing a key whose secret this
# script can no longer print. Run `docker compose -f
# docker-compose.attachments-s3.yml down -v` first to start clean.
#
# On success, prints `export FOO=bar` lines for the Garage credentials to
# stdout ONLY -- all progress/log output goes to stderr, so the stdout stream
# stays safe to eval. Exit 0 = both stores ready, non-zero = failed.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="docker compose -f $DIR/docker-compose.attachments-s3.yml"
BUCKET=barrel-att-s3-test
KEY_NAME=barrel-att-s3-key

log() { echo "$@" >&2; }

log "--- starting minio + garage"
$COMPOSE up -d minio garage

log "--- waiting for minio"
minio_up=0
for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:19000/minio/health/live >/dev/null 2>&1; then
        minio_up=1; break
    fi
    sleep 1
done
[ "$minio_up" -eq 1 ] || { log "  minio did not become healthy"; exit 1; }
log "  minio is up"

# --network container:<name> shares the minio container's own network
# namespace, so this reaches it on 127.0.0.1 regardless of the compose
# project's network name (which depends on the directory this repo is
# checked out into and isn't worth depending on here).
log "--- ensuring minio bucket $BUCKET"
docker run --rm --network container:barrel-att-s3-minio \
    -e MC_HOST_local="http://minioadmin:minioadmin@127.0.0.1:9000" \
    minio/mc mb --ignore-existing "local/$BUCKET" >&2

log "--- waiting for garage rpc"
garage_up=0
for _ in $(seq 1 30); do
    if $COMPOSE exec -T garage /garage node id -q >/dev/null 2>&1; then
        garage_up=1; break
    fi
    sleep 1
done
[ "$garage_up" -eq 1 ] || { log "  garage did not become reachable"; exit 1; }
log "  garage is up"

NODE_ID=$($COMPOSE exec -T garage /garage node id -q 2>/dev/null | tr -d '\r\n')
NODE_ID="${NODE_ID%%@*}"
# `layout show`'s ID column prints only the first 16 hex chars of the full
# node id; assign accepts any unambiguous prefix, so use the short form
# consistently for both the membership check and the assign call itself.
NODE_ID="${NODE_ID:0:16}"

if $COMPOSE exec -T garage /garage layout show 2>/dev/null | grep -q "$NODE_ID"; then
    log "--- garage layout already assigned"
else
    log "--- assigning garage layout"
    $COMPOSE exec -T garage /garage layout assign -z dc1 -c 1G "$NODE_ID" >&2
    VERSION=$($COMPOSE exec -T garage /garage layout show 2>/dev/null \
        | grep -oE 'layout version: [0-9]+' | grep -oE '[0-9]+')
    NEXT_VERSION=$((VERSION + 1))
    $COMPOSE exec -T garage /garage layout apply --version "$NEXT_VERSION" >&2
fi

if $COMPOSE exec -T garage /garage bucket list 2>/dev/null | awk '{print $1}' | grep -qx "$BUCKET"; then
    log "--- garage bucket $BUCKET already exists"
else
    log "--- creating garage bucket $BUCKET"
    $COMPOSE exec -T garage /garage bucket create "$BUCKET" >&2
fi

if $COMPOSE exec -T garage /garage key list 2>/dev/null | awk '{print $2}' | grep -qx "$KEY_NAME"; then
    log "!!! key $KEY_NAME already exists and its secret cannot be recovered"
    log "!!! run '$COMPOSE down -v' to start from a clean volume, then retry"
    exit 1
fi

log "--- creating garage key $KEY_NAME"
KEY_OUT=$($COMPOSE exec -T garage /garage key create "$KEY_NAME" 2>/dev/null)
ACCESS_KEY=$(echo "$KEY_OUT" | sed -n 's/^Key ID: //p' | tr -d '\r')
SECRET_KEY=$(echo "$KEY_OUT" | sed -n 's/^Secret key: //p' | tr -d '\r')
[ -n "$ACCESS_KEY" ] && [ -n "$SECRET_KEY" ] || { log "!!! could not parse garage key output"; exit 1; }

log "--- authorizing $KEY_NAME on $BUCKET"
$COMPOSE exec -T garage /garage bucket allow "$BUCKET" --key "$KEY_NAME" --read --write >&2

log "--- ready"
echo "export GARAGE_S3_TEST_ACCESS_KEY=$ACCESS_KEY"
echo "export GARAGE_S3_TEST_SECRET_KEY=$SECRET_KEY"
echo "export GARAGE_S3_TEST_BUCKET=$BUCKET"
