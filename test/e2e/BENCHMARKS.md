# Benchmark run

A reference run of the in-tree benchmark harnesses. Numbers are from a single
developer machine (Apple Silicon, macOS, OTP 29) and are meant as a shape, not an
absolute: throughput moves with hardware, dataset, and RocksDB tuning. Re-run on
your target to get numbers that mean anything for you.

## How to run

Document layer (`barrel_docdb`), CRUD / query / changes:

```console
$ cd apps/barrel_docdb/bench && ./run_bench.sh 1000 1000
```

Vector layer (`barrel_vectordb`), HNSW backend:

```console
$ cd apps/barrel_vectordb && ./scripts/run_backend_bench.sh --quick
```

FAISS shows as N/A unless the system FAISS library is installed and `barrel_faiss`
is on the path.

## barrel_docdb (1000 docs, 1000 iterations)

CRUD, ops/sec:

| op | ops/sec | p50 | p99 |
|----|--------:|----:|----:|
| read   | 30,758 | 4us  | 20us  |
| update | 41,054 | 5us  | 18us  |
| delete |  5,620 | 5us  | 184us |
| insert |  5,144 | 140us| 270us |

Query, a selection (ops/sec, p50):

| query | ops/sec | p50 |
|-------|--------:|----:|
| prefix_limit    | 17,073 | 55us   |
| simple_eq_limit | 16,850 | 54us   |
| order_by_limit  | 14,335 | 64us   |
| prefix          |  1,408 | 681us  |
| simple_eq       |    547 | 1,567us|
| or_condition    |    130 | 7,362us|
| range           |    170 | 5,569us|

Indexed, limited queries run in tens of microseconds; unindexed multi-condition
and range scans over the full set are milliseconds. Add `LIMIT` and lean on the
path index for the fast path.

Changes:

| workload | ops/sec | p50 |
|----------|--------:|----:|
| subscription | 12,642 | 65us   |
| incremental  |  4,752 | 206us  |
| full_scan    |    152 | 6,597us|

## barrel_vectordb (dim 128, 500-vector index, HNSW)

| op | ops/sec | p95 |
|----|--------:|----:|
| delete_single   | 24,430 | 0.06ms  |
| insert_single   | 13,193 | 0.14ms  |
| insert_batch_100|  5,280 | 19.4ms  |
| search_k1       |  3,358 | 0.35ms  |
| search_k50      |  3,039 | 0.37ms  |
| search_k10      |  2,678 | 0.42ms  |
| index_build_1k  |  1,272 | 807ms   |

HNSW search stays sub-millisecond at p95 across k. Batch insert amortizes the
graph updates; a full 1k index build is the one heavy operation.
