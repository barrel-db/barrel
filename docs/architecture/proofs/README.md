# Replication proofs

Machine-checked versions of the arguments in `../replication-proofs.md`. Run them when you change the document merge, how writes pick what they replace, automatic resolution, or old-peer arrivals. Each tool checks a different part, and each comes with negative controls: a one-rule change that must make the check fail, so a passing check is known to have teeth.

| Tool | Checks | File |
|---|---|---|
| TLC | T1, T3, T5, T7, T10 over every reachable state of three replicas (writes, deletes, re-creates, delayed/duplicated/reordered messages, crashes with rollback and a new writer id, automatic resolution) | `tla/DocState.tla` |
| Ivy | T2 closure, T5, T7 as inductive invariants, for any number of replicas, writers and versions, with the per-writer marks explicit | `ivy/live.ivy` |
| Lean 4 | T1 (the join is commutative, idempotent, associative), T7 at one merge, T6 | `lean/Live.lean` |

## Install

```
brew install openjdk@21
mkdir -p ~/tools && curl -L -o ~/tools/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
curl https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh -sSf | sh -s -- -y
docker build -t barrel-ivy ivy     # Ivy runs in Docker (Linux x86_64 wheel only)
```

## Run

```
JAVA=/opt/homebrew/opt/openjdk@21/bin/java LEAN=~/.elan/bin/lean ./run.sh
```

It prints one line per check and ends with `PROOFS PASS` or `PROOFS FAIL` (non-zero exit). A TLC run takes under a minute per configuration.

## Notes

- TLC keeps the set of seen versions exact; Ivy proves the high-water-mark compression of it sound.
- Apalache is not used: a resolution's base is a set of tokens of the same record type, a recursive type it cannot express, and TLC already exhausts the bounded model.
- Not covered: eviction (a stated exception to "no lost write"), filters, purge, and clock skew.
