#!/usr/bin/env bash
#
# Brings up RustFS + Garage (docker-compose.attachments-s3.yml) and
# provisions both: barrel_att_s3_store:open/2 never creates a bucket, Garage
# also needs a one-time layout assignment before it serves any S3 request,
# and a Garage key cannot create its own bucket.
#
# Usage:
#   test/e2e/attachments-s3-setup.sh              # start + provision, print exports
#   eval "$(test/e2e/attachments-s3-setup.sh)"    # ... and load them into the shell
#
# Idempotent for RustFS. NOT idempotent for Garage past the first run: Garage
# never reveals a key's secret again, so re-running against a provisioned
# volume fails. Run `docker compose -f docker-compose.attachments-s3.yml
# down -v` first to start clean.
#
# Prints `export FOO=bar` lines on stdout only once both stores answer a
# signed request on their bucket; logs go to stderr. Any failed pull, start
# or provisioning step exits non-zero before anything is printed.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE="docker compose -f $DIR/docker-compose.attachments-s3.yml"
BUCKET=barrel-att-s3-test
KEY_NAME=barrel-att-s3-key
# Must match the rustfs service in the compose file.
RUSTFS_ENDPOINT=http://127.0.0.1:19000
RUSTFS_ACCESS_KEY=s3testadmin
RUSTFS_SECRET_KEY=s3testsecret
RUSTFS_REGION=us-east-1
GARAGE_ENDPOINT=http://127.0.0.1:13900

log() { echo "$@" >&2; }
die() { log "!!! $*"; exit 1; }
trap 'log "!!! attachments-s3-setup.sh failed at line $LINENO"' ERR

# s3_head <endpoint> <region> <access> <secret>: signed HEAD on the bucket.
s3_head() {
    curl -fsS -o /dev/null --aws-sigv4 "aws:amz:$2:s3" --user "$3:$4" \
        -I "$1/$BUCKET"
}

log "--- pulling rustfs + garage"
$COMPOSE pull rustfs garage >&2

log "--- starting rustfs + garage"
$COMPOSE up -d rustfs garage >&2

log "--- waiting for rustfs"
rustfs_up=0
for _ in $(seq 1 30); do
    if curl -fsS "$RUSTFS_ENDPOINT/health" >/dev/null 2>&1; then
        rustfs_up=1; break
    fi
    sleep 1
done
[ "$rustfs_up" -eq 1 ] || die "rustfs did not become healthy"
log "  rustfs is up"

# Signed PUT Bucket; RustFS answers 200 for a bucket it already owns, and
# 503 for a few seconds after /health first reports ready.
log "--- ensuring rustfs bucket $BUCKET"
rustfs_bucket=0
for _ in $(seq 1 30); do
    if curl -fsS -o /dev/null --aws-sigv4 "aws:amz:$RUSTFS_REGION:s3" \
        --user "$RUSTFS_ACCESS_KEY:$RUSTFS_SECRET_KEY" \
        -X PUT "$RUSTFS_ENDPOINT/$BUCKET" 2>/dev/null; then
        rustfs_bucket=1; break
    fi
    sleep 1
done
[ "$rustfs_bucket" -eq 1 ] || die "could not create rustfs bucket $BUCKET"
s3_head "$RUSTFS_ENDPOINT" "$RUSTFS_REGION" "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY" \
    || die "rustfs bucket $BUCKET not reachable with the test credentials"

log "--- waiting for garage rpc"
garage_up=0
for _ in $(seq 1 30); do
    if $COMPOSE exec -T garage /garage node id -q >/dev/null 2>&1; then
        garage_up=1; break
    fi
    sleep 1
done
[ "$garage_up" -eq 1 ] || die "garage did not become reachable"
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
    die "run '$COMPOSE down -v' to start from a clean volume, then retry"
fi

log "--- creating garage key $KEY_NAME"
KEY_OUT=$($COMPOSE exec -T garage /garage key create "$KEY_NAME" 2>/dev/null)
ACCESS_KEY=$(echo "$KEY_OUT" | sed -n 's/^Key ID: //p' | tr -d '\r')
SECRET_KEY=$(echo "$KEY_OUT" | sed -n 's/^Secret key: //p' | tr -d '\r')
[ -n "$ACCESS_KEY" ] && [ -n "$SECRET_KEY" ] || die "could not parse garage key output"

log "--- authorizing $KEY_NAME on $BUCKET"
$COMPOSE exec -T garage /garage bucket allow "$BUCKET" --key "$KEY_NAME" --read --write >&2

log "--- checking the garage key can reach $BUCKET"
garage_ok=0
for _ in $(seq 1 15); do
    if s3_head "$GARAGE_ENDPOINT" garage "$ACCESS_KEY" "$SECRET_KEY" 2>/dev/null; then
        garage_ok=1; break
    fi
    sleep 1
done
[ "$garage_ok" -eq 1 ] || die "garage bucket $BUCKET not reachable with the new key"

log "--- ready"
echo "export RUSTFS_S3_TEST_ENDPOINT=$RUSTFS_ENDPOINT"
echo "export RUSTFS_S3_TEST_ACCESS_KEY=$RUSTFS_ACCESS_KEY"
echo "export RUSTFS_S3_TEST_SECRET_KEY=$RUSTFS_SECRET_KEY"
echo "export RUSTFS_S3_TEST_REGION=$RUSTFS_REGION"
echo "export RUSTFS_S3_TEST_BUCKET=$BUCKET"
echo "export GARAGE_S3_TEST_ACCESS_KEY=$ACCESS_KEY"
echo "export GARAGE_S3_TEST_SECRET_KEY=$SECRET_KEY"
echo "export GARAGE_S3_TEST_BUCKET=$BUCKET"
