---------------------------- MODULE DocState ----------------------------
(* One document replicated across replicas, as in barrel_live: each
   replica holds the current versions (live) and every version it has
   seen. Messages carry whole states and can be delayed, duplicated,
   reordered or never delivered; a crash rolls a replica back to a
   state it sent earlier and starts a new writer id.

   seen is kept as an exact set of tokens: the per-writer high-water
   marks of the implementation are a compression of it, proved sound
   separately (closure, Ivy).

   Ghost state: gone[r] holds the versions replaced by a write or a
   resolution whose effect reached r; written the user writes; bad
   flags a resolution that would sort at or below one of its inputs. *)
EXTENDS Naturals, FiniteSets

CONSTANTS Replicas, MaxOps, MaxSends, MaxCrashes,
          Resolver,     \* TRUE: resolve automatically with max
          NaiveMerge,   \* negative control: ignore what the other side saw
          PlusOne,      \* negative control: resolution sorts one step above
          NoStop        \* negative control: no no-replace stop (with MixedFns)
CONSTANT MixedFns       \* TRUE: replicas run different functions
CONSTANT OwnClock       \* negative control: a resolution named by its replica

VARIABLES live, seen, gone, del, written, hlc, epoch, ops, sends, crashes,
          msgs, bad

vars == <<live, seen, gone, del, written, hlc, epoch, ops, sends, crashes,
          msgs, bad>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

(* User writes take HLCs that are multiples of 10; a resolution sorts a
   few steps above the newest write it covers, never at a user HLC. *)
UserTok(h, r, e) == [h |-> h, w |-> <<r, e>>, base |-> {}, res |-> 0]
AutoTok(h, b, v) == [h |-> h, w |-> <<"auto", 0>>, base |-> b, res |-> v]
IsAuto(t) == t.w[1] = "auto"
Base(t) == IF IsAuto(t) THEN t.base ELSE {t}
Val(t) == IF IsAuto(t) THEN t.res ELSE t.h

Winner(L) == CHOOSE t \in L : \A u \in L : u.h <= t.h

Join(a, b) ==
  LET L == IF NaiveMerge THEN a.live \cup b.live
           ELSE (a.live \cap b.live) \cup (a.live \ b.seen)
                                     \cup (b.live \ a.seen)
  IN [live |-> L, seen |-> a.seen \cup b.seen, gone |-> a.gone \cup b.gone]

State(r) == [live |-> live[r], seen |-> seen[r], gone |-> gone[r]]

Fn(r) == IF MixedFns /\ r # CHOOSE x \in Replicas : TRUE THEN "min" ELSE "max"

(* One automatic resolution step over a state; returns the new state and
   whether its token failed to sort above every input. *)
Resolve(S, r) ==
  IF ~Resolver \/ Cardinality(S.live) <= 1 THEN [s |-> S, bad |-> FALSE]
  ELSE
    LET B   == UNION {Base(t) : t \in S.live}
        Vs  == {Val(t) : t \in S.live}
        v   == IF Fn(r) = "max" THEN Max(Vs) ELSE Min(Vs)
        mh  == Max({b.h : b \in B})
        h   == IF PlusOne THEN mh + 1 ELSE mh + Cardinality(B)
        t   == IF OwnClock THEN [AutoTok(h, B, v) EXCEPT !.w = <<"auto", r>>]
               ELSE AutoTok(h, B, v)
        Clash == \E u \in S.live : IsAuto(u) /\ u.base = B /\ u.res # v
    IN IF Clash /\ ~NoStop THEN [s |-> S, bad |-> FALSE]
       ELSE [s |-> [live |-> {t}, seen |-> S.seen \cup {t},
                    gone |-> S.gone \cup (S.live \ {t})],
             bad |-> \E u \in S.live : u # t /\ u.h >= h]

Set(r, S) ==
  /\ live' = [live EXCEPT ![r] = S.live]
  /\ seen' = [seen EXCEPT ![r] = S.seen]
  /\ gone' = [gone EXCEPT ![r] = S.gone]

Init ==
  /\ live = [r \in Replicas |-> {}]
  /\ seen = [r \in Replicas |-> {}]
  /\ gone = [r \in Replicas |-> {}]
  /\ del = {} /\ written = {} /\ hlc = 10
  /\ epoch = [r \in Replicas |-> 0]
  /\ ops = 0 /\ sends = 0 /\ crashes = 0 /\ msgs = {} /\ bad = FALSE

(* A write by r: what it replaces follows barrel_live:replaces/3. An
   edit of the winner replaces only the winner; an edit with the
   observed set replaces every live version; over a delete, every
   delete. *)
Write(r) ==
  /\ ops < MaxOps
  /\ \E observed \in BOOLEAN, d \in BOOLEAN :
       LET L == live[r]
           R == IF L = {} THEN {}
                ELSE IF Winner(L) \in del THEN {u \in L : u \in del}
                ELSE IF observed THEN L ELSE {Winner(L)}
           t == UserTok(hlc, r, epoch[r])
       IN /\ Set(r, [live |-> (L \ R) \cup {t}, seen |-> seen[r] \cup {t},
                     gone |-> gone[r] \cup R])
          /\ del' = IF d THEN del \cup {t} ELSE del
          /\ written' = written \cup {t}
  /\ hlc' = hlc + 10
  /\ ops' = ops + 1
  /\ UNCHANGED <<epoch, sends, crashes, msgs, bad>>

Send(r) ==
  /\ sends < MaxSends
  /\ msgs' = msgs \cup {[from |-> r, live |-> live[r], seen |-> seen[r],
                             gone |-> gone[r]]}
  /\ sends' = sends + 1
  /\ UNCHANGED <<live, seen, gone, del, written, hlc, epoch, ops, crashes, bad>>

Receive(r) ==
  \E m \in msgs :
    /\ m.from # r
    /\ LET J == Join(State(r), [live |-> m.live, seen |-> m.seen, gone |-> m.gone])
           R == Resolve(J, r)
       IN /\ Set(r, R.s)
          /\ bad' = (bad \/ R.bad)
          /\ del' = IF R.s.live # J.live /\ J.live \subseteq del
                    THEN del \cup R.s.live ELSE del
    /\ UNCHANGED <<written, hlc, epoch, ops, sends, crashes, msgs>>

(* A crash rolls r back to a state it sent earlier (or keeps its state)
   and starts a new writer id. *)
Crash(r) ==
  /\ crashes < MaxCrashes
  /\ \/ \E m \in msgs : m.from = r /\ Set(r, [live |-> m.live, seen |-> m.seen,
                                              gone |-> m.gone])
     \/ UNCHANGED <<live, seen, gone>>
  /\ epoch' = [epoch EXCEPT ![r] = @ + 1]
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<del, written, hlc, ops, sends, msgs, bad>>

Next == \E r \in Replicas : Write(r) \/ Send(r) \/ Receive(r) \/ Crash(r)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
States == {State(r) : r \in Replicas}
          \cup {[live |-> m.live, seen |-> m.seen, gone |-> m.gone] : m \in msgs}

TypeOK == \A r \in Replicas : live[r] \subseteq seen[r]

(* T7: a version replaced anywhere that a replica has heard of is never
   live there. *)
NoResurrection == \A r \in Replicas : live[r] \cap gone[r] = {}

(* T5: a version leaves the live set only through a write or resolution
   that replaced it. *)
OnlyReplacedLeave == \A r \in Replicas : (seen[r] \ live[r]) \subseteq gone[r]

(* T3: a replica that has a version always has a live one. *)
NonEmpty == \A r \in Replicas : seen[r] # {} => live[r] # {}

(* T1 on reachable states: the join is commutative, idempotent and
   associative. *)
JoinLaws ==
  \A a, b \in States :
    /\ Join(a, b).live = Join(b, a).live
    /\ Join(a, a).live = a.live
    /\ \A c \in States : Join(Join(a, b), c).live = Join(a, Join(b, c)).live

(* T10: a resolution sorts above everything it replaces. *)
ResolutionAbove == ~bad

(* T10: with one order-independent function, replicas that have seen
   the same versions agree on a single version. *)
OneVersion ==
  Resolver /\ ~MixedFns =>
    \A r, s \in Replicas :
      (r # s /\ seen[r] = seen[s] /\ seen[r] # {})
        => live[r] = live[s] /\ Cardinality(live[r]) = 1
=============================================================================
