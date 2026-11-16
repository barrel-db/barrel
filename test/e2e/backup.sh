#!/usr/bin/env bash
# barrel-backup against a running barrel-server image: create, list,
# restore under a new name, prune, and an interrupted backup left behind.
#
#   DIST=tcp  test/e2e/backup.sh   the image's default distribution
#   DIST=quic test/e2e/backup.sh   QUIC distribution (erlang_quic quic_dist),
#                                  with verify_none then verify_peer
#
# The QUIC run mounts vm.args and sys.config into /etc/barrel, with
# certificates from a test CA, and also checks that the release's own
# `eval` cannot reach that node: the script brings its own client.
#
# Requires: docker, curl, openssl (QUIC only). Exit 0 = pass.
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DIST="${DIST:-tcp}"
IMAGE="${IMAGE:-barrel-server-e2e-backup}"
NAME="barrel-e2e-backup"
PORT="${PORT:-18090}"
WORK="$(mktemp -d)"
pass=0; fail=0
ok() { echo "  ok   $1"; pass=$((pass + 1)); }
ko() { echo "  FAIL $1"; fail=$((fail + 1)); }
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1; rm -rf "$WORK"; }
trap cleanup EXIT

backup() { docker exec "$NAME" barrel backup "$@"; }
api() { curl -fsS -X "$1" "127.0.0.1:$PORT$2" -H 'content-type: application/json' ${3:+-d "$3"}; }

echo "--- building $IMAGE"
docker build -q -t "$IMAGE" "$ROOT" >/dev/null || { echo "build failed"; exit 1; }

# A CA and one node certificate that works as a TLS server and client
# and names the node's host (127.0.0.1), as verify_peer checks.
make_certs() {
    local d="$WORK/etc"
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=barrel-e2e-ca" \
        -keyout "$d/ca.key" -out "$d/ca.pem" 2>/dev/null
    openssl req -newkey rsa:2048 -nodes -subj "/CN=127.0.0.1" \
        -keyout "$d/key.pem" -out "$d/node.csr" 2>/dev/null
    printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature,keyEncipherment\n' > "$d/ext"
    openssl x509 -req -in "$d/node.csr" -CA "$d/ca.pem" -CAkey "$d/ca.key" -CAcreateserial \
        -days 1 -out "$d/cert.pem" -extfile "$d/ext" 2>/dev/null
    chmod 644 "$d"/*.pem
}

quic_config() {  # quic_config verify_none|verify_peer
    local d="$WORK/etc"
    cat > "$d/vm.args" <<EOF
-name barrel@127.0.0.1
-setcookie e2e_backup_cookie
-proto_dist quic
-epmd_module quic_epmd
-start_epmd false
-quic_dist_port 4433
-quic_dist_cert /etc/barrel/cert.pem
-quic_dist_key /etc/barrel/key.pem
EOF
    cat > "$d/sys.config" <<EOF
[{barrel_server, [{http_port, 8080}, {data_dir, "/data/docdb"}]},
 {barrel_docdb, [{data_dir, "/data/docdb"}]},
 {barrel_ngram, [{data_dir, "/data/ngram"}]},
 {quic, [{dist, [{cert_file, "/etc/barrel/cert.pem"},
                 {key_file, "/etc/barrel/key.pem"},
                 {cacert_file, "/etc/barrel/ca.pem"},
                 {verify, $1},
                 {discovery_module, quic_discovery_static},
                 {nodes, [{'barrel@127.0.0.1', {"127.0.0.1", 4433}}]}]}]},
 {kernel, [{logger_level, notice}]}].
EOF
}

start() {  # start [docker run options]
    docker rm -f "$NAME" >/dev/null 2>&1
    docker run -d --name "$NAME" -p "$PORT:8080" "$@" "$IMAGE" >/dev/null
    for _ in $(seq 60); do
        curl -fsS "127.0.0.1:$PORT/ready" >/dev/null 2>&1 && return 0
        sleep 1
    done
    docker logs "$NAME" | tail -20
    return 1
}

run_cases() {  # run_cases <label>
    local label="$1"
    api PUT /db/notes >/dev/null && api PUT /db/notes/doc/a '{"v":1}' >/dev/null \
        && ok "$label: database written" || ko "$label: database written"

    if backup create /data/backups --all >/dev/null; then ok "$label: create --all"; else ko "$label: create --all"; fi
    sleep 1
    backup create /data/backups notes >/dev/null
    local n
    n=$(backup list /data/backups notes | wc -l | tr -d ' ')
    [ "$n" = 2 ] && ok "$label: list shows 2 backups" || ko "$label: list shows $n backups"

    local latest
    latest=$(backup list /data/backups notes | head -1 | awk '{print $2}')
    if backup restore "/data/backups/notes/$latest" notes_copy >/dev/null \
       && [ "$(api GET /db/notes_copy/doc/a | grep -o '"v":1')" = '"v":1' ]; then
        ok "$label: restore under a new name answers over REST"
    else
        ko "$label: restore under a new name"
    fi
    if backup restore "/data/backups/notes/$latest" notes >/dev/null 2>&1; then
        ko "$label: restore over an existing name refused"
    else
        ok "$label: restore over an existing name refused"
    fi

    # an interrupted backup: a directory without its top manifest
    docker exec "$NAME" sh -c "cp -r /data/backups/notes/$latest /data/backups/notes/$latest.partial-dead && rm -f /data/backups/notes/$latest.partial-dead/MANIFEST.json /data/backups/notes/$latest.partial-dead/BARREL.json"
    n=$(backup list /data/backups notes | wc -l | tr -d ' ')
    [ "$n" = 2 ] && ok "$label: an interrupted backup is not listed" || ko "$label: interrupted backup listed ($n)"
    if backup restore "/data/backups/notes/$latest.partial-dead" notes_bad >/dev/null 2>&1; then
        ko "$label: an interrupted backup cannot be restored"
    else
        ok "$label: an interrupted backup cannot be restored"
    fi

    # vectors: the server's databases are barrel databases, their
    # backups carry the vector store (768 dimensions by default)
    local va vb
    va="[1$(printf ',0%.0s' $(seq 767))]"
    vb="[0,1$(printf ',0%.0s' $(seq 766))]"
    api PUT /db/vecs >/dev/null
    api POST /db/vecs/vector "{\"id\":\"a\",\"text\":\"alpha\",\"vector\":$va}" >/dev/null
    api POST /db/vecs/vector "{\"id\":\"b\",\"text\":\"beta\",\"vector\":$vb}" >/dev/null
    backup create /data/vbackups vecs >/dev/null
    local vlatest
    vlatest=$(backup list /data/vbackups vecs | head -1 | awk '{print $2}')
    if backup restore "/data/vbackups/vecs/$vlatest" vecs_copy >/dev/null \
       && api POST /db/vecs_copy/search/vector "{\"vector\":$vb,\"k\":1}" | grep -q '"key":"b"'; then
        ok "$label: a restored database keeps its vectors"
    else
        ko "$label: a restored database keeps its vectors"
    fi

    backup prune /data/backups --keep 1 >/dev/null
    n=$(backup list /data/backups notes | wc -l | tr -d ' ')
    [ "$n" = 1 ] && ok "$label: prune --keep 1" || ko "$label: prune left $n"
}

case "$DIST" in
    tcp)
        echo "--- TCP distribution"
        start || { echo "node did not start"; exit 1; }
        run_cases tcp
        ;;
    quic)
        mkdir -p "$WORK/etc"
        make_certs
        for verify in verify_none verify_peer; do
            echo "--- QUIC distribution, $verify"
            quic_config "$verify"
            start -v "$WORK/etc:/etc/barrel:ro" || { ko "quic $verify: node started"; continue; }
            if docker exec "$NAME" sh -c 'grep -q "proto_dist quic" /etc/barrel/vm.args'; then
                ok "quic $verify: node runs -proto_dist quic"
            else
                ko "quic $verify: node runs -proto_dist quic"
            fi
            if docker exec "$NAME" barrel eval '1.' >/dev/null 2>&1; then
                ko "quic $verify: release eval cannot reach a QUIC node"
            else
                ok "quic $verify: release eval cannot reach a QUIC node"
            fi
            run_cases "quic $verify"
        done
        ;;
    *)
        echo "DIST must be tcp or quic"; exit 2 ;;
esac

echo "=== $pass passed, $fail failed"
[ "$fail" -eq 0 ]
