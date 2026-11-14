#!/usr/bin/env bash
# One-time backfill of the Barrel bundles before the release workflow. A
# commit on main becomes a bundle when it changed the version of an app and
# that version was really released, that is published on Hex. It is named
# YYYY.MM.N (tag vYYYY.MM.N) from the commit's month, N counting the bundles
# of that month from 0, with a GitHub release whose notes come from
# scripts/bundle_notes.sh against the previous bundle. Notes only, no
# tarballs or image, never marked latest. Existing tags are kept.
#
# Usage: scripts/bundle_backfill.sh           list what it would create
#        scripts/bundle_backfill.sh --apply   create the tags and releases
set -euo pipefail

APPLY=false
[ "${1:-}" = "--apply" ] && APPLY=true

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
git fetch -q --tags origin main

# app@version of every release on Hex
PUBLISHED="$(mktemp)"
trap 'rm -f "$PUBLISHED"' EXIT
for app in $(git ls-tree --name-only origin/main apps/ | sed 's|apps/||'); do
    curl -fsS "https://hex.pm/api/packages/$app" 2>/dev/null \
        | jq -r --arg a "$app" '.releases[].version | "\($a)@\(.)"' >> "$PUBLISHED" || true
done

vsn() {
    git show "$1:apps/$2/src/$2.app.src" 2>/dev/null \
        | sed -n 's/.*{vsn, *"\([^"]*\)"}.*/\1/p' | head -1 || true
}

prev=-
month=""
n=0
git log --reverse --format=%H origin/main -G'\{vsn,' -- 'apps/*/src/*.app.src' |
while read -r sha; do
    subject="$(git log -1 --format='%cs %s' "$sha")"
    released=""
    for app in $(scripts/bundle_notes.sh --changed "$prev" "$sha"); do
        v="$(vsn "$sha" "$app")"
        if grep -qx "$app@$v" "$PUBLISHED"; then released="$released $app@$v"; fi
    done
    if [ -z "$released" ]; then
        echo "skip            $subject (no released version)"
        continue
    fi
    ym="$(TZ=UTC git log -1 --date=format-local:%Y.%m --format=%cd "$sha")"
    if [ "$ym" = "$month" ]; then n=$((n + 1)); else month="$ym"; n=0; fi
    bundle="$ym.$n"
    if git ls-remote --exit-code --tags origin "refs/tags/v$bundle" >/dev/null; then
        echo "keep    $bundle  $subject"
    elif $APPLY; then
        notes="$(mktemp)"
        scripts/bundle_notes.sh "$prev" "$sha" > "$notes"
        git tag -a "v$bundle" -m "Barrel $bundle" "$sha"
        git push -q origin "v$bundle"
        gh release create "v$bundle" --title "Barrel $bundle" \
            --notes-file "$notes" --latest=false >/dev/null
        rm -f "$notes"
        echo "created $bundle  $subject"
    else
        echo "would   $bundle  $subject ($released )"
    fi
    prev="$sha"
done
