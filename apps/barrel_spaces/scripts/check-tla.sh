#!/bin/bash
# Model-check the barrel_spaces document layout with TLC
# (spec/SpacesLayout.tla).
#
# Usage: ./scripts/check-tla.sh
#
# Needs Java and tla2tools.jar. Set TLA2TOOLS to the jar, or put it at
# _build/tla/tla2tools.jar (https://github.com/tlaplus/tlaplus/releases).
#
# grant, session and handoff must pass. TLC must find a violation in the
# others, which shows the model catches the bugs 2.0 fixes: oldgrant and
# oldsession model the layout before 2.0 (lost revocation, lost data
# key), opengrant and opensession the 2.0 layout without its resolver
# (replicas that agree but keep two live versions).
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="${TLA2TOOLS:-$ROOT/_build/tla/tla2tools.jar}"
OUT="$ROOT/_build/tla/states"

if [ ! -f "$JAR" ]; then
    echo "tla2tools.jar not found at $JAR (set TLA2TOOLS)" >&2
    exit 2
fi
mkdir -p "$OUT"

run() {
    java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -workers auto -deadlock \
         -metadir "$OUT/$1" -config "$ROOT/spec/$1.cfg" \
         "$ROOT/spec/SpacesLayout.tla"
}

FAILED=0
for model in grant session handoff; do
    echo "== $model"
    if run "$model" > "$OUT/$model.log" 2>&1; then
        grep -E "states generated" "$OUT/$model.log" | tail -1
    else
        echo "FAILED, see $OUT/$model.log"
        FAILED=1
    fi
done

for model in oldgrant oldsession opengrant opensession; do
    echo "== $model (must fail)"
    if run "$model" > "$OUT/$model.log" 2>&1; then
        echo "FAILED: no violation found, see $OUT/$model.log"
        FAILED=1
    else
        grep -E "is violated" "$OUT/$model.log" | head -1
    fi
done

exit $FAILED
