#!/usr/bin/env bash
# One-time backfill of the Barrel bundles before the release workflow: each
# commit on main that changed an app version becomes b<N> (N = commits on main
# at that commit), with a GitHub release whose notes come from
# scripts/bundle_notes.sh against the previous point. Notes only, no
# tarballs or image, never marked latest. Existing tags and releases are kept.
#
# Usage: scripts/bundle_backfill.sh           list what it would create
#        scripts/bundle_backfill.sh --apply   create the tags and releases
set -euo pipefail

APPLY=false
[ "${1:-}" = "--apply" ] && APPLY=true

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
git fetch -q --tags origin main

prev=-
git log --reverse --format=%H origin/main -G'\{vsn,' -- 'apps/*/src/*.app.src' |
while read -r sha; do
    bundle="b$(git rev-list --count "$sha")"
    subject="$(git log -1 --format='%cs %s' "$sha")"
    if git ls-remote --exit-code --tags origin "refs/tags/$bundle" >/dev/null; then
        echo "keep    $bundle  $subject"
    elif $APPLY; then
        notes="$(mktemp)"
        scripts/bundle_notes.sh "$prev" "$sha" > "$notes"
        git tag -a "$bundle" -m "Barrel $bundle" "$sha"
        git push -q origin "$bundle"
        gh release create "$bundle" --title "Barrel $bundle" \
            --notes-file "$notes" --latest=false >/dev/null
        rm -f "$notes"
        echo "created $bundle  $subject"
    else
        echo "would   $bundle  $subject"
    fi
    prev="$sha"
done
