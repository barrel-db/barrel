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
# TLC negative controls: each must report a violation
for cfg in NC_NaiveMerge NC_PlusOne NC_NoStop NC_OwnClock; do
    if tlc "$cfg" | grep -q "is violated"; then pass "tlc $cfg (violation found)"
    else fail "tlc $cfg (no violation: model too weak)"; fi
done

# Ivy: inductive invariants, any number of replicas, writers, versions
docker image inspect "$IVY_IMAGE" >/dev/null 2>&1 \
    || docker build -q -t "$IVY_IMAGE" ivy >/dev/null
ivy() {
    docker run --rm --platform linux/amd64 -v "$PWD/ivy":/proofs "$IVY_IMAGE" "$1" 2>&1
}
if ivy live.ivy | grep -q "^OK"; then pass "ivy live"; else fail "ivy live"; fi
nc_ivy() {
    python3 -I - "$1" "$2" "$3" <<'PY'
import sys
name, a, b = sys.argv[1:]
t = open("ivy/live.ivy").read()
assert t.count(a) == 1, name
open(f"ivy/nc_{name}.ivy", "w").write(t.replace(a, b))
PY
    if ivy "nc_$1.ivy" | grep -q "error: failed checks"; then pass "ivy nc_$1 (fails)"
    else fail "ivy nc_$1 (proved: model too weak)"; fi
    rm -f "ivy/nc_$1.ivy"
}
nc_ivy legacy_raises_mark "        cloud(r,t) := true;
    }" "        cloud(r,t) := true;
        mark(r, w(t), H) := mark(r, w(t), H) | le(H, h(t));
    }"
nc_ivy merge_ignores_seen "    live(r,T) := (live(r,T) & live(s,T))
               | (live(r,T) & ~(cloud(s,T) | mark(s, w(T), h(T))))
               | (live(s,T) & ~(cloud(r,T) | mark(r, w(T), h(T))));" \
    "    live(r,T) := live(r,T) | live(s,T);"
nc_ivy writer_reused "    require ~legacy_w(nw) & forall R. ~owner(nw, R);
    require forall T. ~authored(T) | w(T) ~= nw;
    require forall R, H. ~mark(R, nw, H);" "    require ~legacy_w(nw);"

# Lean: the join is a semilattice and drops what the other side replaced
if "$LEAN" lean/Live.lean >/dev/null 2>&1; then pass "lean Live"; else fail "lean Live"; fi
# negative control: a union merge (ignoring what was seen) must not
# satisfy the drop theorem
NC="$META/NcUnion.lean"
cat > "$NC" <<'LEAN'
def unionJoin (la _sa lb _sb : Bool) : Bool := la || lb
theorem nc_union_drops : ∀ la sa lb sb : Bool,
    sb = true → lb = false → unionJoin la sa lb sb = false := by decide
LEAN
if "$LEAN" "$NC" >/dev/null 2>&1; then fail "lean nc_union_drops (proved)"
else pass "lean nc_union_drops (rejected)"; fi

rm -rf "$META"
[ "$status" -eq 0 ] && echo "PROOFS PASS" || echo "PROOFS FAIL"
exit "$status"
