/-!
# barrel_live: the join is a semilattice (T1), a stub merges like the
deleted document (T6)

A state over versions of type `α`: `live` the current versions, `seen`
every version the state has seen (the per-writer marks of the
implementation compress `seen`; that compression is proved sound in the
Ivy model). Well formed: every live version is seen.
-/

structure State (α : Type) where
  live : α → Bool
  seen : α → Bool

def WF {α : Type} (a : State α) : Prop := ∀ x, a.live x = true → a.seen x = true

/-- Keep a version both sides hold, or one the other side never saw. -/
def liveJoin (la sa lb sb : Bool) : Bool :=
  (la && lb) || (la && !sb) || (lb && !sa)

def join {α : Type} (a b : State α) : State α :=
  ⟨fun x => liveJoin (a.live x) (a.seen x) (b.live x) (b.seen x),
   fun x => a.seen x || b.seen x⟩

theorem liveJoin_comm : ∀ la sa lb sb : Bool,
    liveJoin la sa lb sb = liveJoin lb sb la sa := by decide

theorem liveJoin_idem : ∀ la sa : Bool, liveJoin la sa la sa = la := by decide

theorem liveJoin_assoc : ∀ la sa lb sb lc sc : Bool,
    liveJoin (liveJoin la sa lb sb) (sa || sb) lc sc
      = liveJoin la sa (liveJoin lb sb lc sc) (sb || sc) := by decide

/-- T7 at one merge: a version the other side has seen and no longer
holds is not kept. -/
theorem liveJoin_drops_replaced : ∀ la sa lb sb : Bool,
    sb = true → lb = false → liveJoin la sa lb sb = false := by decide

theorem liveJoin_seen : ∀ la sa lb sb : Bool,
    (la = true → sa = true) → (lb = true → sb = true) →
    liveJoin la sa lb sb = true → (sa || sb) = true := by decide

theorem join_comm {α : Type} (a b : State α) : join a b = join b a := by
  unfold join
  congr 1
  · funext x; exact liveJoin_comm _ _ _ _
  · funext x; exact Bool.or_comm _ _

theorem join_idem {α : Type} (a : State α) : join a a = a := by
  cases a with
  | mk l s =>
    unfold join
    congr 1
    · funext x; exact liveJoin_idem _ _
    · funext x; exact Bool.or_self _

theorem join_wf {α : Type} (a b : State α) (ha : WF a) (hb : WF b) :
    WF (join a b) := by
  intro x h
  exact liveJoin_seen _ _ _ _ (ha x) (hb x) h

theorem join_assoc {α : Type} (a b c : State α) :
    join (join a b) c = join a (join b c) := by
  unfold join
  congr 1
  · funext x; exact liveJoin_assoc _ _ _ _ _ _
  · funext x; exact Bool.or_assoc _ _ _

/-- A stub keeps the whole replicated state of the deleted document and
drops only what is not part of it (bodies, history, index entries). -/
def stub {α : Type} (a : State α) : State α := ⟨a.live, a.seen⟩

theorem join_drops_replaced {α : Type} (a b : State α) (x : α)
    (hs : b.seen x = true) (hl : b.live x = false) : (join a b).live x = false :=
  liveJoin_drops_replaced _ _ _ _ hs hl

theorem stub_join {α : Type} (a b : State α) : join (stub a) b = join a b := rfl
