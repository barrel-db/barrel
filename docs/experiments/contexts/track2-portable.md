# Track 2 results: portability and local materialization (M2 core)

Branch `exp/ctx-portable`, based on `docs/contexts-action-plan` (9bbb51d2).
Erlang API only; REST, MCP and the federated executor are another track.
Contract: `docs/architecture/contexts-action-plan.md` (3.1, 3.3, 3.5,
3.8, 3.9, 4, 6.1 to 6.3, B13 to B17, S3, S4).

In the PR stack built on main, `bm25_rebuild`, `bm25_load` and
`barrel_record_bm25` are gone: barrel_vectordb 2.4.1 makes disk BM25
durable and rebuilds a memory index at open (from the text column family
or the docstore), so imports keep their source's BM25 backend and answer
with the same scores.

## Built

| Item | Status | Where |
|---|---|---|
| B15 read-only open (docdb, vectordb) | done | `barrel_db_server`, `barrel_docdb`, `barrel_vectordb_server` |
| B15 read-only and stored-policy open (barrel) | done | `barrel:open/2` (`read_only`, `embedding => stored`), `barrel_record_bm25` |
| B14 quiesced export | done (blob and none attachment backends) | `barrel_ctx_export:export/3`, `barrel_ctx_manifest`, `barrel_dbs:hold/2` |
| S3 import identity | done, mechanism chosen and implemented | TIMELINE `kind => import` (`barrel_keyspace`) |
| B15 import | done (verify, resume, atomic seal, read-only open) | `barrel_ctx_export:import/2` |
| B13 working sets | done | `barrel_ctx_ws` |
| B16 retrieved-set slices | done (local and remote sources) | `barrel_ctx_slice`, `barrel_ctx_remote` |
| B17 offline coverage | done (pure decision module) | `barrel_ctx_coverage` |
| S4 bench | script committed, smoke run only | `scripts/bench_ctx_snapshot.escript` |

## Tests

| Command | Result |
|---|---|
| `rebar3 ct --dir apps/barrel/test` | 124 passed (all barrel suites, including the 2 new ones) |
| `rebar3 ct --dir apps/barrel/test --suite barrel_ctx_export_SUITE,barrel_ctx_ws_SUITE` | 22 passed |
| `rebar3 eunit --app barrel` | 26 passed (includes `barrel_ctx_coverage_tests`) |
| `rebar3 ct --dir apps/barrel_docdb/test --suite barrel_read_only_SUITE,barrel_timeline_SUITE,barrel_timeline_merge_SUITE,barrel_timeline_pitr_SUITE,barrel_encryption_SUITE,barrel_att_SUITE,barrel_att_store_none_SUITE,barrel_att_feed_SUITE,barrel_retention_SUITE,barrel_docdb_ttl_SUITE,barrel_compaction_SUITE,barrel_outbox_SUITE,barrel_rep_SUITE,barrel_delete_db_SUITE,barrel_doc_SUITE` | 206 passed |
| `rebar3 eunit --module barrel_keyspace_tests,barrel_vectordb_read_only_tests` | 8 passed |
| `rebar3 eunit --app barrel_vectordb` | 274 passed |
| `rebar3 ct --dir apps/barrel_spaces/test` (barrel_dbs consumer) | 40 passed |

Test data: the OTP corpus named by `BARREL_CTX_CORPUS` (the suites fall
back to a synthetic corpus when it is not set). Suites use the `tools`, `sasl` and `eunit` apps (46 modules).
Vectors are 64-dim feature-hashed token counts through a meck stub of
`barrel_embed`; no embedding service is called. Data dirs live in the CT
`priv_dir`.

Required acceptance checks and where they are:

- Export, import under a new name, same answers (find, `vector_top_k`,
  `bm25_top_k`): `plain_same_answers`, `encrypted_same_answers`,
  `closed_db_export` compare exact rows and scores (ties ordered by id).
  `record_same_answers` compares find and vector rows exactly, BM25 by
  matching ids (see deviations). Every query is asserted non-empty first.
- Imported ANN graph loads: `index_origin = loaded` asserted for plain and
  record imports.
- Writes to an imported generation fail: `imported_writes_refused`
  (put, delete, attachment, vector add and delete), docdb
  `barrel_read_only_SUITE`, `barrel_vectordb_read_only_tests`.
- Interrupted import never opens partial data and resumes:
  `interrupted_import_resumes` (two interruptions, 3 then 2 files, then
  a resume that reuses 5 verified files); `corrupt_artifact_rejected`.
- Slice answers like the source restricted to its ids:
  `slice_plain_local`, `slice_record_local`, `slice_remote` (find exact,
  vector ranking exact with scores within 1e-6, BM25 matching ids).
- Over-budget materialize writes nothing: `slice_over_budget_writes_nothing`
  (working-set bytes and request `max_bytes`), `remote_limits` (response
  cap, deadline), `snapshot_over_budget_writes_nothing`.
- Attach opens nothing: `attach_opens_nothing` (`barrel_dbs:list/0` and
  `barrel_docdb:list_dbs/0` unchanged after attaching remote, snapshot and
  local members and resolving them).
- Provenance survives a restart: `provenance_survives_restart` (barrel,
  vectordb and docdb applications stopped and restarted).

## xref and dialyzer

- `rebar3 xref`: only the 14 `barrel_faiss` warnings of the default build
  (baseline, FAISS is not in the default app set).
- `rebar3 dialyzer`: 11 warnings, all baseline (`barrel_att_store_none`
  callback spec, `barrel_vectordb_index_faiss` unknown functions). None in
  touched modules.

## Spike S3: import identity

Naive import (`s3_naive_copy_breaks`, `s3_same_name_reuses_source_id`):
the docdb directory copied under a new name and opened normally.

1. Every storage key embeds the source name: every document is
   invisible (`get_doc` is `not_found`, `find` returns nothing).
2. Local docs are invisible too, including the record-mode policy
   `_barrel/embedding`, so the copy cannot reopen in record mode from
   what it carries.
3. The vector store is not inside the docdb directory: copying the docdb
   directory brings no vectors, and the default vector path is relative to
   the working directory and recorded nowhere, so the copy opens an empty
   store wherever the caller points it.
4. Identity: a new name mints a new source id (the lookup is keyed by the
   logical name). Restoring under the same name on another data dir
   reuses the source id: two live authors under one id.

Chosen mechanism: the branch keyspace indirection, extended. The import
writes a TIMELINE sidecar with `kind => import` and
`keyspace => <source keyspace>` (from the manifest). The docdb server reads
it before RocksDB opens, builds keys with the source name, and records no
timeline parent (so the copy is not listed as a branch, cannot be merged
into a same-named local database, and `db_info` shows no `parent`). The
source id is looked up under the new logical name, so it is fresh. The
vector store directory travels in the export and is opened at an explicit
path recorded in the import record.

Restrictions:

- An import cannot be branched (`cannot_branch_a_branch`): the keyspace
  indirection stays single level.
- Importing under the source's own name or keyspace is refused
  (`same_name_as_source`): it would reuse the source id.
- An encrypted generation resolves its key through the importer's
  keyprovider under the source keyspace name; the importer's provider must
  know that name.
- Channel config is by logical name and is not carried; channel feeds of
  the source are inert in the copy.
- The read-only docdb still persists a freshly minted source id under the
  new name on first open (one metadata key). It never authors versions.
- Re-exporting an import under the same `wsnap_` name elsewhere would carry
  that stored source id; harmless while imports stay read only.

## Deviations from the contract

- Manifest artifacts keep their natural relative paths
  (`docdb/docs/000012.sst`), not `objects/sha256-<hex>`; parts are
  content-addressed (`parts/sha256-<hex>.json`, at most 1000 artifacts).
  Content addressing of artifacts belongs to publication (B18).
- The manifest carries the vector store config an importer needs to load
  the graph (dimension, index backend and its config, BM25 settings) as a
  base64 Erlang term (`layout.vector_config_etf`), not JSON. It holds no
  key, embedder, or docstore.
- Imports and slices serve BM25 from an in-memory index rebuilt at open
  (plain: from the vector store's text column family, `bm25_rebuild`;
  record: from the documents through the policy, `barrel_record_bm25`),
  because the disk BM25 backend does not survive a reopen (bug below) and a
  read-only store cannot write one. Consequence: a record-mode source that
  serves BM25 from its disk backend and its import agree on matching ids
  but not on scores (different BM25 implementations). Plain sources on the
  memory backend match exactly.
- Slices: "created under a temporary name then renamed" is implemented as
  a temporary directory holding a database already under its final name,
  renamed into place when complete, with the name held in `barrel_dbs`
  during the build. Renaming a database would need the keyspace
  indirection (S3); a directory rename does not.
- Slices from a remote source can carry vectors only when the source is in
  record mode (vectors live in documents); a plain source's vectors live
  only in its vector store. Remote record slices need the `embedding`
  policy in the request (the source's policy is not on the wire).
- Slice BM25 scores differ from the source's (IDF is corpus-relative);
  tests compare matching ids. Slice vector scores differ from the live
  source by float32 rounding (< 1e-6).
- Remote slices record `observed` as null: `GET /db/:db` reports neither
  instance id nor last seq yet (B2 adds them). The code keeps whatever the
  route reports.
- The Erlang surface is `barrel_ctx_ws`, `barrel_ctx_export`,
  `barrel_ctx_slice`, `barrel_ctx_coverage`; the `barrel_ctx` facade of 3.6
  (`attach/3`, `detach/2`, `materialize/2`) is left to the integration so
  it can sit next to the executor.
- Attach enforces the `contexts` budget (8) at attach time so a whole
  working set never exceeds it; `open_dbs` is recorded but enforced by the
  executor and `barrel_dbs`.

## Bugs and hazards found in existing code

1. **Disk BM25 does not survive a close/reopen** (`barrel_vectordb_bm25_disk`):
   `close/1` does not flush the hot layer (up to `hot_max_size *
   threshold` documents, 40000 by default); a compaction writes only the
   hot layer's postings, replacing the previous disk segment; doc stats
   loading is a TODO. Reproduced: 20 docs, stop, reopen, `search_bm25`
   returns `[]`. Record mode defaults to this backend "so it survives
   restarts". Memory BM25 is never rebuilt at open either. Every live
   database loses BM25 on restart today.
2. **Record-mode exports carry the embedder config**: the policy persisted
   in `_barrel/embedding` includes the embedder config, which may hold an
   API key (`barrel_embed_openai` accepts `api_key`). An export copies it.
   `persist_policy` should drop secrets; until then, export a record
   database only when its policy names no inline credential.
3. **A read-write RocksDB open replays the WAL into new SST files**, so a
   read-only open still changes the directory (existing data files stay
   untouched). Checksums are therefore verified before the first open and
   an imported directory is not byte-identical after it.
4. `barrel_dbs` stores no open options; a manager-held database could not
   be reopened with its options after being closed. `hold/2` needed them,
   so entries now keep them.

## Surprises

- The persisted HNSW graph loaded in every import, plain and record,
  encrypted or not, with identical vector scores to the live source.
- Cold open of an import is dominated by the in-memory BM25 rebuild when
  the text is large (full module bodies): about 80 ms without it, 3 to 5 s
  with it at 700 to 1078 documents.
- hackney 4 ignores `with_body` and always returns the whole body; the
  response byte cap needs `{async, once}`.

## Smoke numbers (non-authoritative)

One run on a developer laptop while other work ran; not a campaign.

| apps | vectors | docs | export ms | bytes | import ms | cold open ms | open w/o BM25 rebuild ms | index_origin |
|---|---|---|---|---|---|---|---|---|
| 1 | no | 104 | 22 | 4.8 MB | 21 | 82 | 87 | loaded |
| 1 | yes | 104 | 19 | 6.1 MB | 21 | 319 | 88 | loaded |
| 10 | no | 695 | 36 | 30.9 MB | 41 | 82 | 85 | loaded |
| 10 | yes | 695 | 48 | 39.8 MB | 48 | 2999 | 94 | loaded |
| all (33) | no | 1078 | 54 | 47.0 MB | 57 | 81 | 84 | loaded |
| all (33) | yes | 1078 | 65 | 60.8 MB | 67 | 5318 | 88 | loaded |

"vectors: no" means no vector store content, hence nothing for BM25 to
rebuild in plain mode.

## Bench command (S4)

```sh
rebar3 compile
BARREL_CTX_CORPUS=/path/to/corpus.jsonl \
  escript scripts/bench_ctx_snapshot.escript --apps 1,10,all --vectors both \
  --runs 5 --dir _build/bench_ctx --out s4.json
```

Options: `--apps` (comma list of counts or `all`; apps ranked by document
count), `--vectors both|yes|no`, `--runs N`, `--dir` (wiped at start),
`--out` (JSON rows). It prints a markdown table.

## What remains before PR-ready

- Split into PRs in dependency order: docdb read-only plus sidecar kind,
  vectordb read-only plus BM25 load, barrel read-only open, `barrel_dbs`
  hold, export/import, working sets plus slices, coverage. The B13 commit
  references `barrel_ctx_slice` (added in the B16 commit): reorder or
  squash those two when splitting.
- Fix or replace the disk BM25 persistence (bug 1), then decide whether
  imports keep the in-memory rebuild.
- Strip secrets from the persisted record policy (hazard 2).
- Export downtime: the database is closed for the whole copy. A docdb
  `checkpoint_to` (hard links) plus a vector store checkpoint-to-directory
  would shorten it to the flush.
- Pending record-mode outbox entries at export (async mode) are copied but
  never indexed by a read-only import; the manifest should count them and
  coverage should report a partial index.
- Export of a branch (TIMELINE present) is expected to work (the import
  sidecar replaces it with the source keyspace) but is not tested.
- `hold/2` closes manager entries with no owner tag; `barrel_dbs` cannot
  tell whether another caller is using such a handle.
- Partial import directories stay until resumed or `remove_import/1`; no
  GC.
- Slice attachments (`include.attachments`) are refused.
- Docs: a guide for export/import and working sets.

## Erlang API for the integrator

```erlang
%% Working sets (B13)
barrel_ctx_ws:create(#{owner => term(), budget => map(), id => binary()})
    -> {ok, WsId} | {error, already_exists}.
barrel_ctx_ws:get(WsId) -> {ok, Ws} | {error, not_found}.
%% Ws :: #{id, owner, created_at, budget := #{bytes, contexts, open_dbs,
%%        remote_parallel, transfer_bytes, deadline_ms},
%%        members := [Member], usage := #{bytes}}
barrel_ctx_ws:list() -> [WsId].
barrel_ctx_ws:delete(WsId) -> ok | {error, term()}.
barrel_ctx_ws:attach(WsId, Member) -> {ok, Ws} | {error, Reason}.
%% Member :: #{context := binary(), mode := local | remote | snapshot
%%             | retrieved_set, local_db, location := #{endpoint, db},
%%             credential_ref, generation, predicate, derived, bytes}
%% Reason :: {already_attached, Ctx} | {over_budget, contexts, N}
%%         | {over_budget, bytes, #{needed, available}} | {invalid_member, M}
barrel_ctx_ws:detach(WsId, Ctx) -> {ok, Ws} | {error, {not_attached, Ctx}}.
barrel_ctx_ws:detach(WsId, Ctx, #{keep_slice => boolean()}) -> same.
barrel_ctx_ws:members(WsId) -> {ok, [Resolved]} | {error, not_found}.
%% Resolved :: Member#{coverage := live | complete_generation | retrieved_set,
%%             version := #{kind := live} | #{kind := generation, generation}
%%                      | #{kind := retrieved_set, observed},
%%             available := boolean(), open_opts => map()}
barrel_ctx_ws:open_member(Resolved) -> {ok, barrel:db()} | {error, term()}.
barrel_ctx_ws:import_snapshot(WsId, SrcDir, ImportOpts) -> {ok, Ws} | {error, term()}.
barrel_ctx_ws:materialize(WsId, Request) -> see barrel_ctx_slice:materialize/2.

%% Export and import (B14, B15)
barrel_ctx_export:export(DbName, DestDir, #{owner, open_opts, context,
                                            generation, parent_generation})
    -> {ok, #{dest, context, generation, artifacts, bytes, elapsed_ms,
              manifest}}
     | {error, pinned | {owned_by, O} | open_outside_manager | {held, H}
              | {dest_not_empty, D} | {unsupported_att_backend, B}
              | {open_failed, R}}.
barrel_ctx_export:import(SrcDir, #{name, encryption, embedding, vectordb,
                                   open, owner, stop_after})
    -> {ok, #{name, path, context, version := #{kind := generation,
              generation}, copied, reused, db => barrel:db()}}
     | {error, {checksum_mismatch, Path} | {part_checksum_mismatch, Ref}
              | {interrupted, {Copied, Reused}} | {same_name_as_source, N}
              | {name_taken, N, Other} | term()}.
barrel_ctx_export:open(Name) -> {ok, barrel:db()} | {error, not_imported | term()}.
barrel_ctx_export:open_opts(Name) -> {ok, map()} | {error, not_imported}.
barrel_ctx_export:import_info(Name) -> {ok, map()} | {error, not_imported}.
barrel_ctx_export:list_imports() -> [binary()].
barrel_ctx_export:remove_import(Name) -> ok.
barrel_ctx_manifest:read(Dir) -> {ok, Root, [#{path, sha256, bytes}]} | {error, term()}.

%% Slices (B16)
barrel_ctx_slice:materialize(WsId, #{context := Ctx,
        source := {local, DbName | barrel:db()}
                | {remote, #{endpoint, db, credential_ref}},
        ids := [binary()], include => #{embeddings => boolean()},
        max_bytes => integer(), embedding => Policy,
        source_open_opts => map(), timeout => ms})
    -> {ok, #{working_set, slices := [#{context, local_db, docs, bytes,
              status := complete, derived}], usage := #{bytes, budget_bytes}}}
     | {error, {over_budget, bytes | transfer_bytes, map()}
              | {already_attached, Ctx} | {source_unavailable, R}
              | {unsupported, attachments}}.
%% derived :: #{<<"from">>, <<"observed">> := #{<<"instance_id">>,
%%             <<"last_seq">>}, <<"selection">>, <<"ids_hash">>,
%%             <<"docs">>, <<"missing">>, <<"source">>, <<"bytes">>,
%%             <<"created_at">>}
barrel_ctx_slice:open_opts(Name) -> {ok, map()} | {error, not_materialized}.
barrel_ctx_slice:provenance(Name) -> {ok, Derived} | {error, not_materialized}.
barrel_ctx_remote:bulk_get(Location, Ids, #{include_embedding, timeout,
                                           max_bytes})
    -> {ok, [{ok, Doc} | {error, R}], Bytes}
     | {error, timeout | response_too_large | {http_status, S, B} | term()}.

%% Offline coverage (B17), pure
barrel_ctx_coverage:member(Resolved, #{offline := boolean(),
                                       available => boolean(),
                                       where => [Cond] | unknown})
    -> #{context, status := ok | pending | skipped_offline | error,
         membership => live | complete_generation | complete_predicate
                     | predicate_overlap | retrieved_set,
         version => map(), note => binary(), error => map()}.
barrel_ctx_coverage:summarize(Sources, explicit | discovered)
    -> #{execution := complete | partial,
         coverage := #{requested, answered, failed, skipped, pending,
                       missing, scope_origin}}.

%% Lower layers
barrel:open(Name, #{read_only => true, embedding => stored | Policy, ...}).
barrel_dbs:hold(Name, #{owner => O}) -> {ok, #{was_open, opts}} | {error, _}.
barrel_dbs:release(Name) -> ok.
barrel_dbs:lookup(Name) -> {ok, #{pinned, owner, opts}} | {error, not_open}.
barrel_docdb:create_db(Name, #{read_only => true, ...}).   %% {error, read_only} on writes
barrel_vectordb:start_link(#{read_only => true, bm25_rebuild => true, ...}).
barrel_vectordb:bm25_load(Store, [{Id, Text}]) -> ok | {error, bm25_not_memory}.
```

Configuration: `barrel` env `ctx_dir` (imports, slices and the working-set
database; default `<barrel_docdb data_dir>/_ctx`) and `ctx_credentials`
(map from credential reference or endpoint to a bearer token).
