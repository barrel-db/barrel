#!/usr/bin/env bash
# Check the replication proofs: TLC (bounded, exhaustive), Ivy
# (inductive, unbounded), Lean (algebra). Every negative control must
# fail; the script exits non-zero otherwise.
set -u
cd "$(dirname "$0")"

JAVA=${JAVA:-java}
TLA2TOOLS=${TLA2TOOLS:-$HOME/tools/tla2tools.jar}
LEAN=${LEAN:-lean}
IVY_IMAGE=${IVY_IMAGE:-barrel-ivy}
META=$(mktemp -d)
status=0

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; status=1; }

tlc() {
    "$JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC -workers auto \
        -metadir "$META/$1" -config "tla/$1.cfg" tla/DocState.tla 2>&1
}

# TLC: properties hold on the full bounded state space
for cfg in DocState Resolver MixedFns Resolver_Sends4; do
    if tlc "$cfg" | grep -q "No error has been found"; then pass "tlc $cfg"
    else fail "tlc $cfg"; fi
done
# TLC negative controls: each must break the invariant it targets
for pair in NC_NaiveMerge:NoResurrection NC_PlusOne:ResolutionAbove \
            NC_NoStop:ResolutionAbove NC_OwnClock:OneVersion; do
    cfg=${pair%%:*}; inv=${pair##*:}
    if tlc "$cfg" | grep -q "Invariant $inv is violated"; then
        pass "tlc $cfg ($inv violated)"
    else
        fail "tlc $cfg ($inv not violated: model too weak)"
    fi
done

# Ivy: inductive invariants, any number of replicas, writers, versions
docker image inspect "$IVY_IMAGE" >/dev/null 2>&1 \
    || docker build -q -t "$IVY_IMAGE" ivy >/dev/null
ivy() {
    docker run --rm --platform linux/amd64 -v "$PWD/ivy":/proofs "$IVY_IMAGE" "$1" 2>&1
}
if ivy live.ivy | grep -q "^OK"; then pass "ivy live"; else fail "ivy live"; fi
nc_ivy() {  # name, target invariant, text, replacement
    local name=$1 target=$2
    python3 -I - "$name" "$3" "$4" <<'PY'
import sys
name, a, b = sys.argv[1:]
t = open("ivy/live.ivy").read()
assert t.count(a) == 1, name
open(f"ivy/nc_{name}.ivy", "w").write(t.replace(a, b))
PY
    # it must break the invariant it targets
    if ivy "nc_$name.ivy" | grep -q "$target ... FAIL"; then
        pass "ivy nc_$name ($target fails)"
    else
        fail "ivy nc_$name ($target holds: model too weak)"
    fi
    rm -f "ivy/nc_$name.ivy"
}
nc_ivy legacy_raises_mark only_replaced_leave "        cloud(r,t) := true;
    }" "        cloud(r,t) := true;
        mark(r, w(t), H) := mark(r, w(t), H) | le(H, h(t));
    }"
nc_ivy merge_ignores_seen no_resurrection "    live(r,T) := (live(r,T) & live(s,T))
               | (live(r,T) & ~(cloud(s,T) | mark(s, w(T), h(T))))
               | (live(s,T) & ~(cloud(r,T) | mark(r, w(T), h(T))));" \
    "    live(r,T) := live(r,T) | live(s,T);"
nc_ivy writer_reused own_handled "    require ~legacy_w(nw) & forall R. ~owner(nw, R);
    require forall T. ~authored(T) | w(T) ~= nw;
    require forall R, H. ~mark(R, nw, H);" "    require ~legacy_w(nw);"

# Lean: the join is a semilattice and drops what the other side replaced
if "$LEAN" lean/Live.lean >/dev/null 2>&1; then pass "lean Live"; else fail "lean Live"; fi
# negative controls: each one-rule change to the merge must fail the
# property named after it
lean_nc() {  # name, live join body, seen join body, theorem
    local f="$META/Nc_$1.lean"
    cat > "$f" <<LEAN
def lj (la sa lb sb : Bool) : Bool := $2
def sj (sa sb : Bool) : Bool := $3
theorem nc : $4 := by decide
LEAN
    # it must fail because decide refutes the property, not on a typo
    if "$LEAN" "$f" 2>&1 | grep -q "proved that the proposition"; then
        pass "lean nc_$1 (refuted)"
    else
        fail "lean nc_$1 (not refuted)"
    fi
}
LIVE='(la && lb) || (la && !sb) || (lb && !sa)'
COMM='∀ la sa lb sb : Bool, lj la sa lb sb = lj lb sb la sa'
ASSOC='∀ la sa lb sb lc sc : Bool, lj (lj la sa lb sb) (sj sa sb) lc sc = lj la sa (lj lb sb lc sc) (sj sb sc)'
DROPS='∀ la sa lb sb : Bool, sb = true → lb = false → lj la sa lb sb = false'
KEEPS='∀ la sa lb sb : Bool, la = true → sb = false → lj la sa lb sb = true'
GROWS='∀ sa sb : Bool, sa = true → sj sa sb = true'
lean_nc one_side_term '(la && lb) || (la && !sb)' 'sa || sb' "$COMM"
lean_nc union_keeps_replaced 'la || lb' 'sa || sb' "$DROPS"
lean_nc keeps_own_dropped '(la && lb) || la || (lb && !sa)' 'sa || sb' "$DROPS"
lean_nc intersection_loses_write 'la && lb' 'sa || sb' "$KEEPS"
lean_nc seen_shrinks "$LIVE" 'sa && sb' "$GROWS"
lean_nc seen_shrinks_assoc "$LIVE" 'sa && sb' "$ASSOC"
lean_nc seen_ignored_by_right '(la && lb) || la || (lb && !sa)' 'sa || sb' "$COMM"

rm -rf "$META"
[ "$status" -eq 0 ] && echo "PROOFS PASS" || echo "PROOFS FAIL"
exit "$status"
