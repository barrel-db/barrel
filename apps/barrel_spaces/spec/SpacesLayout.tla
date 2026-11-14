---------------------------- MODULE SpacesLayout ----------------------------
(***************************************************************************)
(* barrel_spaces documents replicated on several nodes with barrel_docdb's *)
(* default behaviour and no conflict resolver.                             *)
(*                                                                         *)
(* Each replica holds, per document, the live versions and every version   *)
(* it has seen (barrel_live). Versions are totally ordered by their token; *)
(* a replica shows its highest live version. A replace (a write carrying   *)
(* every version the replica shows) leaves one live version; a create      *)
(* replaces only deleted versions; a removal writes a tombstone over every *)
(* live version and is a no-op when the replica shows nothing.             *)
(* Replication joins two replicas: a version stays live unless one side    *)
(* has seen it and no longer holds it.                                     *)
(*                                                                         *)
(* Family selects the documents and calls modelled:                        *)
(*   "grant"      grant:Id (rewritten by old nodes) and revoke:Id          *)
(*   "session"    one data key, one pin, one count document per replica   *)
(*   "handoff"    one accept document per agent and the complete document  *)
(*   "oldgrant"   before 2.0: revocation is a field of the grant document  *)
(*   "oldsession" before 2.0: data keys are fields of the session document *)
(* The two "old" families must fail: TLC finds a lost revocation and a     *)
(* lost data key, which shows the model can catch the bug 2.0 fixes.       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS Replicas, Agents, Keys, MaxWrites, Family

VARIABLES live, seen, clock, revoked, rank, incs, last

vars == <<live, seen, clock, revoked, rank, incs, last>>

----------------------------------------------------------------------------
(* Documents and versions *)

Docs ==
    CASE Family = "grant" -> {<<"grant", "_">>, <<"revoke", "_">>}
      [] Family = "session" -> {<<"data", "_">>, <<"pin", "_">>} \cup {<<"count", r>> : r \in Replicas}
      [] Family = "handoff" -> {<<"complete", "_">>} \cup {<<"accept", a>> : a \in Agents}
      [] Family = "oldgrant" -> {<<"grant", "_">>}
      [] Family = "oldsession" -> {<<"session", "_">>}

\* val is a number for every family but oldsession, where it is the set of
\* <<key, write>> pairs the session document holds.
None == [t |-> 0, val |-> 0, del |-> TRUE]

Winner(S) == IF S = {} THEN None ELSE CHOOSE v \in S : \A w \in S : w.t <= v.t

Top(r, d) == Winner(live[r][d])

Shows(r, d) == ~Top(r, d).del

Val(r, d) == Top(r, d).val

New(v, isDel) == [t |-> clock + 1, val |-> v, del |-> isDel]

Write(r, d, S) ==
    /\ clock < MaxWrites
    /\ live' = [live EXCEPT ![r][d] = S]
    /\ clock' = clock + 1

\* A write over every version the replica shows (barrel_spaces_docs:replace).
Replace(r, d, v, isDel) ==
    /\ Write(r, d, {New(v, isDel)})
    /\ seen' = [seen EXCEPT ![r][d] = @ \cup {New(v, isDel)}]

\* A write without a rev: allowed when the replica shows nothing.
Create(r, d, v) ==
    /\ ~Shows(r, d)
    /\ Write(r, d, {x \in live[r][d] : ~x.del} \cup {New(v, FALSE)})
    /\ seen' = [seen EXCEPT ![r][d] = @ \cup {New(v, FALSE)}]

Remove(r, d) ==
    /\ Shows(r, d)
    /\ Replace(r, d, 0, TRUE)

Join(a, b, d) ==
    {x \in live[a][d] \cup live[b][d] :
        /\ (x \in live[a][d] \/ x \notin seen[a][d])
        /\ (x \in live[b][d] \/ x \notin seen[b][d])}

Sync(a, b) ==
    /\ a # b
    /\ live' = [live EXCEPT ![b] = [d \in Docs |-> Join(a, b, d)]]
    /\ seen' = [seen EXCEPT ![b] = [d \in Docs |-> seen[a][d] \cup seen[b][d]]]
    /\ UNCHANGED clock

----------------------------------------------------------------------------
(* Answers *)

IsRevoked(r) ==
    CASE Family = "oldgrant" -> Shows(r, <<"grant", "_">>) /\ Val(r, <<"grant", "_">>) = 1
      [] Family = "grant" -> Shows(r, <<"revoke", "_">>)
      [] OTHER -> FALSE

Accepted(r) ==
    IF Family = "handoff" THEN {a \in Agents : Shows(r, <<"accept", a>>)}
    ELSE {}

Status(r) ==
    IF Family # "handoff" THEN 0
    ELSE IF Shows(r, <<"complete", "_">>) THEN 2
    ELSE IF Accepted(r) # {} THEN 1
    ELSE 0

\* The earliest accept, then the agent name.
Owner(r) ==
    CHOOSE a \in Accepted(r) :
        \A b \in Accepted(r) : Top(r, <<"accept", a>>).val <= Top(r, <<"accept", b>>).val

RECURSIVE SumOver(_, _)
SumOver(r, S) ==
    IF S = {} THEN 0
    ELSE LET o == CHOOSE x \in S : TRUE
         IN (IF Shows(r, <<"count", o>>) THEN Val(r, <<"count", o>>) ELSE 0)
            + SumOver(r, S \ {o})

Count(r) == SumOver(r, Replicas)

OldData(r) == IF Shows(r, <<"session", "_">>) THEN Val(r, <<"session", "_">>) ELSE {}

----------------------------------------------------------------------------
(* Calls *)

GrantNext ==
    \E r \in Replicas :
        \/ /\ ~IsRevoked(r)                       \* barrel_caps:revoke/1
           /\ Create(r, <<"revoke", "_">>, 1)
        \/ /\ Shows(r, <<"grant", "_">>)                   \* an old node edits the grant
           /\ Replace(r, <<"grant", "_">>, clock + 1, FALSE)

OldGrantNext ==
    \E r \in Replicas :
        \/ Replace(r, <<"grant", "_">>, 1, FALSE)          \* revoke: revoked_at > 0
        \/ Replace(r, <<"grant", "_">>, 0, FALSE)          \* any other edit, revoked_at 0

SessionNext ==
    \E r \in Replicas :
        \/ /\ \/ Replace(r, <<"data", "_">>, clock + 1, FALSE) \* set_data
              \/ Remove(r, <<"data", "_">>)                    \* delete_data
              \/ Create(r, <<"pin", "_">>, clock + 1)          \* pin_context
              \/ Remove(r, <<"pin", "_">>)                     \* unpin_context
           /\ UNCHANGED incs
        \/ /\ Replace(r, <<"count", r>>,          \* incr_counter, own document
                      (IF Shows(r, <<"count", r>>) THEN Val(r, <<"count", r>>) ELSE 0) + 1,
                      FALSE)
           /\ incs' = incs + 1

OldSessionNext ==
    \E r \in Replicas, k \in Keys :
        /\ Replace(r, <<"session", "_">>,
                   {p \in OldData(r) : p[1] # k} \cup {<<k, clock + 1>>}, FALSE)
        /\ last' = [last EXCEPT ![k] = clock + 1]

HandoffNext ==
    \E r \in Replicas :
        \/ \E a \in Agents :                      \* accept/2
               /\ Status(r) = 0
               /\ Create(r, <<"accept", a>>, clock + 1)
        \/ /\ Status(r) < 2                       \* complete/2
           /\ Create(r, <<"complete", "_">>, clock + 1)

Step ==
    CASE Family = "grant" -> GrantNext /\ UNCHANGED <<incs, last>>
      [] Family = "oldgrant" -> OldGrantNext /\ UNCHANGED <<incs, last>>
      [] Family = "session" -> SessionNext /\ UNCHANGED last
      [] Family = "oldsession" -> OldSessionNext /\ UNCHANGED incs
      [] Family = "handoff" -> HandoffNext /\ UNCHANGED <<incs, last>>

Init ==
    /\ live = [r \in Replicas |-> [d \in Docs |->
                 IF d = <<"grant", "_">> THEN {[t |-> 0, val |-> 0, del |-> FALSE]}
                 ELSE {}]]
    /\ seen = live
    /\ clock = 0
    /\ revoked = [r \in Replicas |-> FALSE]
    /\ rank = [r \in Replicas |-> 0]
    /\ incs = 0
    /\ last = [k \in Keys |-> 0]

\* What each replica has ever answered, for the "never goes back" checks.
Track ==
    /\ revoked' = [r \in Replicas |-> revoked[r] \/ IsRevoked(r)']
    /\ rank' = [r \in Replicas |-> IF Status(r)' > rank[r] THEN Status(r)' ELSE rank[r]]

Next ==
    /\ \/ Step
       \/ /\ \E a, b \in Replicas : Sync(a, b)
          /\ UNCHANGED <<incs, last>>
    /\ Track

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* Properties *)

Converged ==
    \A a, b \in Replicas : live[a] = live[b] /\ seen[a] = seen[b]

\* Every version ever written, and the newest of them per document.
AllOf(d) == UNION {seen[r][d] : r \in Replicas}

\* A revocation, once a replica answers it, is never undone there.
RevocationKept == \A r \in Replicas : revoked[r] => IsRevoked(r)

\* No handoff status ever goes back on any replica.
StatusKept == \A r \in Replicas : Status(r) = rank[r]

\* Once replicas agree, each shows the newest write of every document:
\* the later write wins and a removal stays.
LatestWins ==
    Converged => \A r \in Replicas, d \in Docs : Top(r, d) = Winner(AllOf(d))

\* Counts are exact once replicas agree, and a count document, written by
\* one replica only, never holds a conflict.
CountsExact == Converged => \A r \in Replicas : Count(r) = incs

CountNoConflict ==
    \A r \in Replicas, o \in Replicas : Cardinality(live[r][<<"count", o>>]) <= 1

\* Every replica names the same owner once they agree.
OneOwner ==
    Converged => \A a, b \in Replicas :
        Accepted(a) # {} => Owner(a) = Owner(b)

\* Before 2.0: once replicas agree, every key's last write is in the
\* session document. TLC finds a violation.
NoLostKey ==
    Converged => \A r \in Replicas, k \in Keys :
        last[k] > 0 => <<k, last[k]>> \in OldData(r)

=============================================================================
