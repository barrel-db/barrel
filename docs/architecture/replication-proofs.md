# Replication proofs

This page states the properties the document replication protocol relies on and argues why each holds. It covers the per-document state in `barrel_live` (`apps/barrel_docdb/src/barrel_live.erl`) and the rules around it. Read it when you change the merge, how writes pick what they replace, automatic resolution, eviction, or the upgrade path. The executable gate is `barrel_live_SUITE`; each section names the tests that exercise it.

## Model

A **token** is `(HLC, writer)`, ordered by HLC then writer. A user write's writer is the database's writer id, minted on every open. An automatic resolution's writer is `auto:<hash>`.

A **state** `S` has three parts:

- `live`: the current versions, each with a deleted flag, a vector `vv` (for old peers) and, for an automatic resolution, its `base` and `result` hash.
- `obs`: per writer, the highest HLC of that writer this state has handled. Only the writer's own writes raise it.
- `cloud`: exact tokens this state has seen (old-peer versions, automatic resolutions, evicted and converted versions).

`seen(S, t)` holds when `t` is in `cloud`, or `obs[writer(t)] >= hlc(t)`.

**Invariant I1.** Every live token is seen.

**Invariant I2 (closure).** If `obs[w] >= h`, every token of writer `w` with HLC at most `h` that was ever authored has been handled by `S`: it is live in `S`, or a write whose effect reached `S` replaced it.

I2 holds because:

1. One writer process authors every version of a database (enforced at runtime: `alloc_hlc/1` fails outside it).
2. A writer's HLCs strictly increase, and every write takes a token above all live tokens and above the writer's own mark (`author/4` asserts both).
3. A writer id is used by one open only, so a crash or a restored copy can't author under an id whose earlier versions it lost.
4. Nothing but the writer's own write raises `obs[w]`. Old-peer arrivals, conversion and automatic resolutions only add exact tokens to `cloud`.

Rule 4 is what the third review's relay counterexample broke (an old peer relaying `a2` raised `obs[a]` and swallowed `a1`). It is pinned by `relayed_version_keeps_sibling` and `conversion_keeps_unseen_sibling`, whose negative controls raise the mark and lose the write.

## T1. Merge is a join-semilattice

`merge/2` keeps a version both sides hold, or one the other side never saw; contexts join by union (`cloud`) and pointwise max (`obs`); the info of a token held on both sides joins pointwise (`vv` max, `base` union with a missing base as bottom).

This is the `DotFun` join of the causal CRDT framework with lattice values (Almeida, Shoker, Baquero 2018, section 7.4). Its laws hold on states whose contexts are causally consistent, which I1 and I2 give. The departures from the paper are that dots are HLC tokens compacted by I2 rather than by counter contiguity, and that `cloud` never compacts; neither changes the set equations.

Gate: `join_laws_on_reachable_states` checks commutativity, idempotence and associativity on 373,438 triples of states from the same execution (a token names one write only within an execution).

## T2. Closure

Argued above (I2). Gate: the two relay and conversion scenarios with their negative controls, and every search run (a broken closure shows up as a lost write).

## T3. A document that exists always has a live version

Take the highest token `p` across `live(A) ∪ live(B)`, say `p ∈ live(A)`. The merge drops `p` only if `p` is seen by `B` and not live there. Something in `B`'s history removed it, and the only removals are:

- a user write, whose token is above every live token, so above `p`;
- an automatic resolution, whose token is above everything it replaces (HLC = highest in its base plus the size of the base, so strictly above any input, including other resolutions);
- eviction, which never removes the winner, so a version above `p` was live.

In each case a token above `p` was live in `B`, and by the same argument applied to that token, some token above `p` is still live in `B`. That contradicts `p` being the highest. Old-peer arrivals and conversion remove nothing.

Gate: `empty` is checked in every search run; `auto_sorts_above_replaced` pins the `+ size of base` rule, and its negative control (`+ 1`) sorts a resolution below what it replaced.

## T4. Convergence

Once writes stop, merging over a connected set of replication paths reaches the join of all states (T1). With a resolver, each resolution covers strictly more original writes than any automatic version it replaces (T10), so resolution steps are finite and the fixpoint is reached. Eviction removes versions but is deterministic on the state, so it does not break convergence.

Gate: `diverge` and `stuck` in every search run, which settles all pairs to a fixpoint.

## T5. A write removes only what it observed

`replaces/3` returns, in order of precedence:

1. the explicit observed set intersected with `live`;
2. when the winner is a delete, every live delete, after checking `Rev` if one is given (the read showed a deleted document, which observed every delete);
3. otherwise `[Rev]`, and only if `Rev` is the winner.

`author/4` removes exactly that list.

Exceptions:

- eviction;
- the compatibility call `resolve_conflict(Db, Id, BaseRev, Resolution)`, which replaces every current version and can remove one that arrived after the read.

Gate: `edit_rules`.

## T6. A stub merges like the deleted document

A stub stores the same `live`, `obs` and `cloud` and drops only bodies, history and index entries, which are not part of the state. `digest/1` and `merge/2` read the state only, so they can't tell the two apart. (Stubs land with the document state; this holds by construction there.)

## T7. Purge off: a replaced version never comes back

A replaced token is seen by the replica that replaced it. Seen only grows: merge unions contexts, writes and evictions add to them, and conversion happens once per document. A merge never keeps a token the other side has seen and dropped, and an old-peer arrival of a seen token is ignored. So once any replica replaces `t`, every replica that later merges with it, directly or through others, drops `t` and never admits it again, whatever the delay, duplication or reordering of states.

A crash that rolls back unsynced writes can lose a replacement together with the write that made it. That replacement never reached anyone, so nothing saw `t` replaced. If it did reach a peer, the next merge drops `t` again.

Gate: `resurrected` in every search run (stale deliveries of past states included); `pre_upgrade_delete_stays_deleted` and its negative control (no conversion seed).

## T8. Purge on

T7 holds for anything younger than `tombstone_purge_after`. A replica, backup, stored state or message older than that can bring a purged document back. This is the documented relaxation, as with Cassandra's `gc_grace_seconds`; there is nothing further to prove.

## T9. Filters

A state with no live version passes every filter; otherwise it passes when some live body matches. A replica receives every matching document and every delete that leaves no live version, for a document it holds, provided some path of replication jobs carries the document. Because the context is per document, not receiving a document never marks it seen, so a filter change can't make a replica skip a document it later comes to match (the new scope is replayed from the start). Checked in the Python models of the simplified design; the Erlang gate gains filter cases when `passes/2` lands.

## T10. Automatic resolution

A resolution's base is the union of its inputs' bases (a user write covers itself). Its token is computed from the base and the result.

- **One function gives one version.** An order-independent function gives the same result for the same base, so every replica that resolves the same original writes, in any grouping, writes the same token.
- **It terminates.** The no-replace stop forbids replacing an automatic version that has the same base and a different result. An automatic version is therefore replaced only by one covering strictly more original writes, or by itself. Bases are subsets of a finite set of writes.
- **It never empties a document:** T3, with the `+ size of base` rule.
- **It loses no user write:** a resolution replaces only the live versions it was given, all of which it covers.
- **Different functions** on different replicas still terminate, never empty the document and lose no write, but which result survives depends on sync order. That is a misconfiguration and is documented as such.

Gate:

- `overlapping_auto_one_version`; its negative control (a resolution named by the resolver's own clock) leaves two versions;
- `different_functions_never_empty`; its negative control (no stop) empties the document;
- `search_resolver`.

## What the gate covers and what it does not

`barrel_live_SUITE` runs bounded exhaustive searches over one document: a line of three replicas and a triangle, up to three user writes (edit, edit with observed set, delete), one writer rotation, stale delivery of past states, and the resolver on and off. Every search runs to completion within its bounds. `search_negative_control` shows the search catches a merge that ignores the context.

Not covered here:

- several documents, eviction inside the search, and old peers inside the search (they are covered by directed scenarios);
- filters, stubs and purge, which land later;
- clock skew and liveness under continuous writes.

Mechanized checks live in `proofs/` (see its README): TLC explores every reachable state of three replicas, including crashes with rollback and automatic resolution; Ivy proves closure (T2), T5 and T7 as inductive invariants for any number of replicas and writers; Lean proves the join laws (T1), T7 at one merge and T6. Each has negative controls that must fail.
