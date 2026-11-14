#!/usr/bin/env bash
# Release notes of a Barrel bundle: every app's version at TO, and the
# CHANGELOG section of each app whose version changed since FROM.
#
# Usage: scripts/bundle_notes.sh [--changed] FROM TO
#   FROM       the previous bundle (a tag or a commit), or - for none: the
#              apps whose version TO itself changed are listed as changed
#   TO         the commit of the bundle
#   --changed  print only the apps whose version changed, one per line
#              (nothing when no app was bumped: no bundle to release)
#
# Reads git objects only, so it works on any commit without a checkout.
set -euo pipefail

ONLY_CHANGED=false
if [ "${1:-}" = "--changed" ]; then ONLY_CHANGED=true; shift; fi
FROM="$1"
TO="$2"
[ "$FROM" = "-" ] && FROM="$(git rev-parse --verify --quiet "$TO^" || true)"

# The version of an app at a ref, empty when the app does not exist there.
vsn() {
    git show "$1:apps/$2/src/$2.app.src" 2>/dev/null \
        | sed -n 's/.*{vsn, *"\([^"]*\)"}.*/\1/p' | head -1 || true
}

# The CHANGELOG section of one version, without its heading.
section() {
    git show "$1:apps/$2/CHANGELOG.md" 2>/dev/null | awk -v v="$3" '
        index($0, "## [" v "]") == 1 { on = 1; next }
        on && /^## / { exit }
        on { print }
    ' | sed -e '/./,$!d' || true
}

apps="$(git ls-tree --name-only "$TO" apps/ | sed 's|apps/||' | sort)"
sha="$(git rev-parse "$TO")"

if $ONLY_CHANGED; then
    for app in $apps; do
        new="$(vsn "$TO" "$app")"
        [ -z "$new" ] && continue
        old=""
        [ -n "$FROM" ] && old="$(vsn "$FROM" "$app")"
        [ "$new" != "$old" ] && echo "$app"
    done
    exit 0
fi

echo "Commit \`${sha:0:12}\`, $(git log -1 --format=%cs "$TO")."
echo
echo "| App | Version |"
echo "|---|---|"
changed=()
for app in $apps; do
    new="$(vsn "$TO" "$app")"
    [ -z "$new" ] && continue
    old=""
    [ -n "$FROM" ] && old="$(vsn "$FROM" "$app")"
    if [ "$new" != "$old" ]; then
        changed+=("$app")
        if [ -n "$old" ]; then
            echo "| $app | $new (was $old) |"
        else
            echo "| $app | $new (new) |"
        fi
    else
        echo "| $app | $new |"
    fi
done

if [ ${#changed[@]} -eq 0 ]; then
    echo
    echo "No app version changed."
    exit 0
fi

for app in "${changed[@]}"; do
    new="$(vsn "$TO" "$app")"
    echo
    echo "## $app $new"
    echo
    body="$(section "$TO" "$app" "$new")"
    if [ -n "$body" ]; then
        echo "$body"
    else
        echo "See apps/$app/CHANGELOG.md."
    fi
done
