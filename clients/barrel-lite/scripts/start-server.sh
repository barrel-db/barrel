#!/usr/bin/env bash
#
# Boot barrel_server headless for barrel-lite integration tests: auth
# on (one token), CORS open. Prints the base URL and token, then blocks
# until killed. Compile with `rebar3 as server compile` first.
#
# Env: BARREL_PORT (default 18080), BARREL_TOKEN (default itest-token),
#      BARREL_DATA_DIR (default a fresh mktemp -d).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PORT="${BARREL_PORT:-18080}"
TOKEN="${BARREL_TOKEN:-itest-token}"
DATA="${BARREL_DATA_DIR:-$(mktemp -d)}"
CFG="$(mktemp -d)"

cat > "$CFG/sys.config" <<EOF
[{barrel_server, [
  {http_port, ${PORT}},
  {data_dir, "${DATA}"},
  {auth, #{tokens => [<<"${TOKEN}">>]}},
  {cors, #{origins => '*'}}
]}].
EOF

# rebar3's path command omits some transitive deps (e.g. h1); glob every
# built ebin instead.
exec erl -noshell -noinput \
  -config "$CFG/sys" \
  -pa "$ROOT"/_build/server/lib/*/ebin \
  -eval 'application:ensure_all_started(barrel_server).'
