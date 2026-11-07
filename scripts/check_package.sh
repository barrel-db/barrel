#!/usr/bin/env bash
#
# Build an app's docs and Hex package the way `rebar3 hex publish` does,
# so a broken doc or a dependency missing from the package shows up before
# publishing (which cannot be undone).
#
#   1. Docs, against the in-repo sibling apps: a copy of the app gets them
#      in _checkouts, so this runs even when a sibling's new version is not
#      on Hex yet.
#   2. The package, with dependencies from Hex, then check_hex_requirements.py.
#      A dependency version not on Hex yet means the siblings must be
#      published first: that is reported as a warning, not a failure.
#
# Usage: scripts/check_package.sh <app>

set -euo pipefail

APP=$1
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIR="$ROOT/apps/$APP"
[ -f "$DIR/rebar.config" ] || { echo "no app $APP"; exit 2; }

# In-repo apps the app depends on, directly or through another in-repo app.
siblings() {
    python3 -I - "$ROOT" "$APP" <<'EOF'
import os, re, sys
root, app = sys.argv[1], sys.argv[2]
def deps(a):
    p = os.path.join(root, "apps", a, "rebar.config")
    t = re.sub(r"^\s*%.*$", "", open(p).read(), flags=re.M)
    m = re.search(r"^\{deps,\s*\[(.*?)\]\}\s*\.", t, re.S | re.M)
    names = re.findall(r"\{\s*([a-z_][a-z_0-9]*)\s*,", m.group(1)) if m else []
    return [n for n in names if os.path.isfile(os.path.join(root, "apps", n, "rebar.config"))]
seen, todo = set(), deps(app)
while todo:
    n = todo.pop()
    if n not in seen:
        seen.add(n); todo += deps(n)
print(" ".join(sorted(seen)))
EOF
}

copy_app() {  # copy_app <app> <dest>: the sources, without build output
    # or a local lock, so dependencies resolve as in a fresh checkout
    mkdir -p "$2"
    tar -C "$ROOT/apps/$1" --exclude=_build --exclude=_checkouts --exclude=doc \
        --exclude=rebar.lock -cf - . | tar -C "$2" -xf -
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "--- $APP: docs"
copy_app "$APP" "$WORK/$APP"
for s in $(siblings); do
    copy_app "$s" "$WORK/$APP/_checkouts/$s"
done
(cd "$WORK/$APP" && rebar3 ex_doc)

echo "--- $APP: Hex package"
PKG="$WORK/pkg/$APP"
copy_app "$APP" "$PKG"
LOG="$WORK/hex-build.log"
if ! (cd "$PKG" && rebar3 hex build) >"$LOG" 2>&1; then
    if grep -q "Package not found in any repo" "$LOG"; then
        grep "Package not found" "$LOG"
        echo "::warning title=$APP package not checked::$(grep -o 'Package not found in any repo: .*' "$LOG" | head -1). Publish that sibling first, then rerun."
        exit 0
    fi
    cat "$LOG"
    exit 1
fi
python3 -I "$ROOT/scripts/check_hex_requirements.py" "$PKG"
