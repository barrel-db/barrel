# Read-only opens: RocksDB read-write against OpenForReadOnly

An imported generation is served with `read_only => true`. Before this
change the stores refused every write at the API, but RocksDB still opened
read-write: WAL recovery flushed new SST files, and each open wrote a new
MANIFEST, OPTIONS file, WAL and info LOG, so the copied files stopped
matching the manifest checksums and a second node could not open the same
directory (the LOCK file). This note records what opening every RocksDB
behind a read-only database with `DB::OpenForReadOnly` changes (docdb
store, blob attachments, vector store, `bm25.ids`, DiskANN `diskann_ids`),
with the BM25 and DiskANN flat files opened without write access.

## How to run

```sh
bench/rocksdb_readonly/run.sh CORPUS.jsonl [WORK_DIR]
```

`BASE_REF` (default `origin/ctx/10-demo-guide-bench`) is the baseline: the
modules the change touches are compiled from it and put first in the code
path. Each measured run is one BEAM, never two at once, the side order
alternates, `ITER` (default 5) runs per side, and the report gives the
median. The corpus is the OTP module corpus of the demo (1078 documents,
128-dimension vectors, disk BM25).

Cases: `clean` is an export of a closed source (no WAL to replay); `wal`
is a copy taken while the source was open, so the open replays the WAL.
Concurrent reader: a second BEAM opens the directory while the first holds
it. Legacy: an import whose `bm25.ids` lacks the column families added in
barrel_vectordb 2.4.1.

## Results

Apple M4 Pro, measured under load 62 to 79 (other sessions running).
Medians of 5.

| case | side | open ms | bytes written | files +/~/- | checksums valid | bm25 p50 us | get_doc p50 us |
|---|---|---|---|---|---|---|---|
| clean | baseline (read-write) | 405.5 | 714,209 | 16/8/12 | 0/5 | 176 | 28 |
| clean | OpenForReadOnly | 311.5 | 0 | 0/0/0 | 5/5 | 111 | 19 |
| wal | baseline (read-write) | 1444.3 | 33,448,621 | 32/8/8 | 0/5 | 136 | 25 |
| wal | OpenForReadOnly | 1105.0 | 0 | 0/0/0 | 5/5 | 98 | 12 |

| check | baseline (read-write) | OpenForReadOnly |
|---|---|---|
| second reader on the same directory | fails on the RocksDB LOCK | opens, reads 1078 docs, BM25 answers |
| files changed by the holder | 17 created, 8 modified, 12 deleted (734 KB) | none |
| legacy `bm25.ids` | opens and upgrades the copy in place (78 MB written) | refused with `read_only_upgrade_needed`, nothing written |

Opens are 20 to 25% faster (no WAL flush, no MANIFEST or OPTIONS rewrite).
Query latencies and memory do not move beyond the noise of a loaded
machine. A store that needs an upgrade is upgraded at its source: export
opens it writable once under the hold before copying.
