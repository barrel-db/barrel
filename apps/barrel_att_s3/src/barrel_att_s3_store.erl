%%%-------------------------------------------------------------------
%%% @doc S3-compatible attachment storage backend.
%%%
%%% Implements `barrel_att_backend' against an S3-compatible object store via
%%% `livery_s3', including the attachment change feed (`att_changes/4',
%%% `att_floor/2', `sweep_att_feed/3') on top of the local feed store this
%%% module opens (see "Local feed store" below) -- thin delegation to
%%% `barrel_att_feed', the same way `barrel_att_store_blob' does -- plus
%%% `rebuild_feed/2', which recovers feed state directly from S3 object
%%% metadata if the local feed is ever lost (see "Attachment change feed"
%%% below), and `checkpoint/2', which makes `barrel_docdb:branch_db/3' work
%%% against an S3-backed database (see "Branching" below). Every optional
%%% callback `barrel_att_backend' defines is implemented.
%%% `delete/5' is nominally optional too but called unconditionally by
%%% `barrel_att_store:delete/5', so it must exist regardless.
%%%
%%% == Config ==
%%% `att_opts => #{backend => s3, s3 => #{bucket, endpoint, region,
%%% access_key_id, secret_access_key, ...}}' -- every key in the nested `s3'
%%% map except `bucket' and `part_size' passes straight to `livery_s3:new/1'.
%%% `att_opts.db_name' (set by `barrel_db_server' from the database's
%%% resolved keyspace name, not user-facing config) seeds the S3 key prefix
%%% the first time a given `Path' is ever opened -- see "Key scheme" below.
%%%
%%% == Local feed store ==
%%% `open/2' also opens a small, local, metadata-only RocksDB instance at
%%% `Path/feed.db' (`Path' is always `<DbPath>/attachments', a real local
%%% directory even though the attachment bytes themselves live in S3 --
%%% confirmed via `barrel_db_server:init/8'). `barrel_att_feed' is not
%%% actually storage-agnostic: it hardcodes `rocksdb:*' calls against
%%% whatever handle it's given, so this is where those calls land for this
%%% backend, exactly the way `barrel_att_store_blob' points the same module
%%% at its own blob-data handle. The feed handle is stored in `att_ref' as
%%% `feed_ref'.
%%%
%%% == Key scheme ==
%%% `Prefix/hex(DocId)/AttName'. `DocId' is arbitrary bytes with no length
%%% cap, so it is hex-encoded; `AttName' is already validated to exclude
%%% NUL/`/'/`\'. A defensive length check rejects keys that would exceed S3's
%%% 1024-byte cap.
%%%
%%% `Prefix' is NOT the per-call `DbName' argument (every `barrel_att_backend'
%%% callback still receives one, the contract requires it, but this backend
%%% stops trusting it for key construction) -- it's read once at `open/2'
%%% from a small local marker file, `Path/s3_prefix':
%%%
%%% <ul>
%%%   <li>If the marker exists, its content is the prefix, unconditionally
%%%       -- covers both a reopened store and a freshly checkpointed
%%%       branch.</li>
%%%   <li>If it doesn't (first-ever open of a fresh `Path'), the prefix is
%%%       derived from `att_opts.db_name' (identical to the scheme's
%%%       original, pre-persisted-marker behavior) and the marker is
%%%       written so every future open agrees. This is also why an existing
%%%       database's first post-upgrade open doesn't move its keys: it
%%%       derives and persists the exact prefix it already had.</li>
%%% </ul>
%%%
%%% Branching needs the prefix decoupled from `DbName' entirely, since
%%% `barrel_keyspace:resolve/1' maps a branch's name back to its parent's
%%% -- see "Branching" below.
%%%
%%% == Branching ==
%%% `checkpoint/2' does only the cheap synchronous part: a RocksDB
%%% checkpoint of the local feed, a fresh random prefix, and a
%%% `Path/fork_pending' marker recording `{Bucket, OldPrefix}'. It returns
%%% immediately -- `branch_db/3' does not block on attachment count.
%%%
%%% The actual S3 copy (`run_copy_sweep/7') is spawned by the branch's own
%%% first `open/2', not by `checkpoint/2' -- `fork/6' always opens the
%%% branch right after checkpointing it, so spawning from both would race
%%% a redundant sweep. This also means a fork that fails before ever
%%% reaching `open/2' leaves nothing spawned, and crash recovery is just
%%% `open/2' finding the marker again on a later open.
%%%
%%% The sweep skips a destination key that already exists (a branch write
%%% wins regardless of ordering, since it always lands eventually) and any
%%% key the branch's own feed shows as deleted since the fork -- rechecked
%%% right after the copy lands too, since a delete (unlike a put) does not
%%% self-heal a lost race: nothing else would remove an object the sweep
%%% just created. The marker only clears once every key succeeds; a
%%% partial failure retries on the next open.
%%%
%%% Reads (`get/4', `get_stream/4', `get_info/4') check the local feed
%%% first: a row whose object isn't copied yet returns
%%% `{error, {att_sync_pending, {DocId, AttName}}}' instead of blocking or
%%% returning stale data. `fold/5' is not feed-checked and only reflects
%%% what has been copied so far.
%%%
%%% Forking a still-syncing source is refused with
%%% `{error, {fork_sync_pending, retry}}' -- unreachable today since
%%% `barrel_timeline' already rejects forking a branch, kept as a safety
%%% net regardless.
%%%
%%% `destroy/2' can race an ALREADY-RUNNING sweep for the same store the
%%% same way: `Options.resume_fork_sync => false' stops `open/2' from
%%% spawning a NEW one, but there is no live-process registry here to find
%%% and join/kill a sweep some EARLIER, unrelated open already started.
%%% Deleting a still-syncing branch while it happens to be open can still
%%% leak the objects that sweep writes after `destroy/2''s own listing
%%% snapshot -- narrow, not closed here, same class of accepted
%%% check-then-act race as the ones above. See `docs/limitations.md`'s
%%% "Deleting a database" section.
%%%
%%% == Whole-blob vs. streaming ==
%%% The caller already has the full binary in memory for `put/5,6', so it is
%%% always a single `put_object' (S3 single-PUT covers up to 5GB) -- no
%%% multipart there. Streaming (`put_stream'/`write_chunk'/`finish_stream',
%%% the path every *replicated* attachment actually goes through) buffers in
%%% memory and only starts a multipart upload once the buffer crosses
%%% `part_size'; `finish_stream' does a single `put_object' if that never
%%% happens (the common case for small/replicated attachments).
%%%
%%% == Write-conflict detection ==
%%% Opt-in optimistic concurrency via S3 conditional writes: `create_only
%%% => true' (`If-None-Match: "*"') and `expected_etag => Etag' (`If-Match:
%%% Etag') on `put/5,6' and `finish_stream/1'. Default (neither given)
%%% stays unconditional, matching the RocksDB backend. A failed precondition
%%% surfaces as `{error, {conflict, CurrentInfo}}' (what's actually there
%%% now), not just `precondition_failed'.
%%%
%%% Not every S3-compatible store can be trusted to enforce this: MinIO has
%%% since 2023, AWS since 2024, but Garage cannot at all, by its own
%%% documented design (no consensus algorithm) -- and a store that silently
%%% accepts the headers without enforcing them is a worse failure mode than
%%% one that rejects them outright, since a caller would believe it's
%%% protected when it is not. `open/2' runs a capability probe (a
%%% `create_only' put to a throwaway key, then a deliberate second one that
%%% must be rejected) and records the verified result; `create_only'/
%%% `expected_etag' fail fast with `{error, conditional_writes_unsupported}'
%%% if the store didn't verifiably enforce it, rather than silently
%%% proceeding unprotected.
%%%
%%% `create_only'/`expected_etag' apply uniformly whether a write ends up a
%%% single `put_object' or a multipart upload: `livery_s3' >= 0.2.0 supports
%%% conditional writes on `complete_multipart_upload' too (the object is
%%% created there, not at `create_multipart_upload', so completion is the
%%% only place the guard can apply). A losing conditional completion
%%% (`precondition_failed' 412, `conditional_request_conflict' 409 -- the
%%% upload id is dead either way for our purposes, a one-shot `finish_stream'
%%% never retries it -- or `not_found' 404 when an `if_match' completion
%%% lost to a concurrent delete) is reported the same
%%% `{error, {conflict, CurrentInfo}}' shape as the single-put path.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_store).
-behaviour(barrel_att_backend).

-export([open/2, close/1, checkpoint/2, destroy/2]).
-export([put/5, put/6, get/4, delete/4, delete/5]).
-export([delete_all/3]).
-export([fold/5]).
-export([get_info/4]).
-export([put_stream/5, put_stream/6]).
-export([write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, read_chunk/1, close_stream/1]).
-export([att_changes/4, att_floor/2, sweep_att_feed/3, rebuild_feed/2]).

-define(DEFAULT_PART_SIZE, 8 * 1024 * 1024). %% 8 MiB; used starting Step 3
-define(MAX_KEY_SIZE, 1024).
-define(META_DIGEST, <<"digest">>).
-define(META_ORIGIN, <<"origin-hlc">>).
-define(FORK_PENDING, "fork_pending").
-define(DELETE_BATCH, 1000). %% S3 DeleteObjects cap per request

-spec open(string(), map()) -> {ok, map()} | {error, term()}.
open(Path, Options) ->
    S3Opts = maps:get(s3, Options, #{}),
    case maps:find(bucket, S3Opts) of
        error ->
            {error, missing_bucket};
        {ok, Bucket} ->
            _ = application:ensure_all_started(livery_s3),
            ClientOpts = maps:without([bucket, part_size], S3Opts),
            Client = livery_s3:new(ClientOpts),
            PartSize = maps:get(part_size, S3Opts, ?DEFAULT_PART_SIZE),
            ConditionalWrites = probe_conditional_writes(Client, Bucket),
            case open_feed_and_prefix(Path, Options) of
                {ok, FeedRef, Prefix} ->
                    AttRef = #{client => Client, bucket => Bucket, part_size => PartSize,
                              conditional_writes => ConditionalWrites,
                              feed_ref => FeedRef, prefix => Prefix, path => Path},
                    ok = maybe_resume_fork_sync(AttRef, Path, Options),
                    {ok, AttRef};
                {error, _} = Err ->
                    Err
            end
    end.

%% @private Resumes a pending fork sync (fresh fork or crash recovery --
%% the sweep is idempotent, so both just re-run it). The only place the
%% sweep is spawned from; checkpoint/2 only writes the marker.
%%
%% `Options.resume_fork_sync => false' skips spawning it even if a marker
%% is present -- for a caller about to destroy/2 this store immediately
%% anyway (see barrel_docdb:maybe_destroy_closed_att_store/3), where
%% spawning a sweep just to abandon it is pure waste, and skipping it
%% removes one whole path by which destroy/2 could race a sweep still
%% writing into the prefix it is about to delete. Defaults to `true'.
maybe_resume_fork_sync(AttRef, Path, Options) ->
    case maps:get(resume_fork_sync, Options, true) of
        false -> ok;
        true -> do_maybe_resume_fork_sync(AttRef, Path, Options)
    end.

do_maybe_resume_fork_sync(#{client := Client, bucket := Bucket, prefix := Prefix,
                            feed_ref := FeedRef}, Path, Options) ->
    case read_fork_pending(Path) of
        {ok, {_MarkerBucket, OldPrefix}} ->
            case maps:get(db_name, Options, undefined) of
                undefined ->
                    logger:warning(
                        "attachment fork sync at ~s cannot resume: no "
                        "db_name given to open/2", [Path]),
                    ok;
                DbName ->
                    _ = spawn(fun() ->
                        run_copy_sweep(Path, FeedRef, DbName, Client, Bucket,
                                       OldPrefix, Prefix)
                    end),
                    ok
            end;
        not_found ->
            ok;
        {error, Reason} ->
            logger:warning("attachment fork_pending marker at ~s unreadable: ~p",
                           [Path, Reason]),
            ok
    end.

%% @private The local feed metadata store: opened at Path/feed.db regardless
%% of whether feed callbacks are wired up yet (Step 1 only lays the
%% groundwork), so its lifecycle (create-if-missing, checkpoint-ability) is
%% established once and doesn't change shape when att_changes/4 etc. land.
open_feed_and_prefix(Path, Options) ->
    FeedPath = filename:join(Path, "feed.db"),
    ok = filelib:ensure_dir(FeedPath ++ "/"),
    case rocksdb:open(FeedPath, [{create_if_missing, true}]) of
        {ok, FeedRef} ->
            case read_or_derive_prefix(Path, Options) of
                {ok, Prefix} ->
                    {ok, FeedRef, Prefix};
                {error, _} = Err ->
                    _ = rocksdb:close(FeedRef),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @private See the moduledoc's "Key scheme" section for why this exists.
read_or_derive_prefix(Path, Options) ->
    PrefixFile = filename:join(Path, "s3_prefix"),
    case file:read_file(PrefixFile) of
        {ok, Prefix} ->
            {ok, Prefix};
        {error, enoent} ->
            case maps:find(db_name, Options) of
                {ok, DbName} ->
                    case file:write_file(PrefixFile, DbName) of
                        ok -> {ok, DbName};
                        {error, _} = Err -> Err
                    end;
                error ->
                    {error, missing_db_name}
            end;
        {error, _} = Err ->
            Err
    end.

%% @private A store either verifiably enforces If-None-Match (the second,
%% deliberately-colliding put is rejected) or it's treated as unsupported --
%% including on any probe hiccup unrelated to the property being tested,
%% since this only gates an opt-in safety net, not ordinary operation:
%% failing closed here just means create_only/expected_etag refuse clearly
%% instead of silently proceeding unprotected.
probe_conditional_writes(Client, Bucket) ->
    ProbeKey = <<".barrel_att_s3_probe/",
                (binary:encode_hex(crypto:strong_rand_bytes(8), lowercase))/binary>>,
    Result = case livery_s3:put_object(Client, Bucket, ProbeKey, <<"probe">>,
                                       #{if_none_match => <<"*">>}) of
        {ok, _} ->
            case livery_s3:put_object(Client, Bucket, ProbeKey, <<"probe2">>,
                                      #{if_none_match => <<"*">>}) of
                {error, precondition_failed} -> supported;
                _ -> unsupported
            end;
        _ ->
            unsupported
    end,
    _ = livery_s3:delete_object(Client, Bucket, ProbeKey),
    Result.

-spec close(map()) -> ok.
close(#{feed_ref := FeedRef}) ->
    rocksdb:close(FeedRef).

%%====================================================================
%% Branching: non-blocking eager copy
%%====================================================================
%%
%% See the moduledoc's "Branching" section for the full design.

%% @doc Refuses a source still mid-sync from its own fork (checked via
%% `path', not `BranchPath') -- unreachable via barrel_timeline today,
%% kept as a safety net.
-spec checkpoint(map(), string()) -> ok | {error, term()}.
checkpoint(#{feed_ref := FeedRef, bucket := Bucket, prefix := OldPrefix,
            path := SourcePath}, BranchPath) ->
    case read_fork_pending(SourcePath) of
        {ok, _} ->
            {error, {fork_sync_pending, retry}};
        not_found ->
            do_checkpoint(FeedRef, Bucket, OldPrefix, BranchPath);
        {error, _} = Err ->
            Err
    end.

%% ensure_path(BranchPath), not ensure_dir(FeedPath ++ "/"): checkpoint's
%% target must not already exist, unlike rocksdb:open's create_if_missing.
do_checkpoint(FeedRef, Bucket, OldPrefix, BranchPath) ->
    ok = filelib:ensure_path(BranchPath),
    FeedPath = filename:join(BranchPath, "feed.db"),
    case rocksdb:checkpoint(FeedRef, FeedPath) of
        ok ->
            NewPrefix = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
            case file:write_file(filename:join(BranchPath, "s3_prefix"), NewPrefix) of
                ok -> write_fork_pending(BranchPath, Bucket, OldPrefix);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @private {Bucket, OldPrefix} is all a crash/restart needs to resume.
write_fork_pending(Path, Bucket, OldPrefix) ->
    File = fork_pending_path(Path),
    Tmp = File ++ ".tmp",
    Data = io_lib:format("~p.~n", [#{bucket => Bucket, old_prefix => OldPrefix}]),
    case file:write_file(Tmp, Data) of
        ok -> file:rename(Tmp, File);
        {error, _} = Err -> Err
    end.

read_fork_pending(Path) ->
    case file:consult(fork_pending_path(Path)) of
        {ok, [#{bucket := Bucket, old_prefix := OldPrefix}]} ->
            {ok, {Bucket, OldPrefix}};
        {error, enoent} ->
            not_found;
        _ ->
            {error, corrupt_fork_pending}
    end.

clear_fork_pending(Path) ->
    case file:delete(fork_pending_path(Path)) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, _} = Err -> Err
    end.

fork_pending_path(Path) ->
    filename:join(Path, ?FORK_PENDING).

%% @doc Copies every object under OldPrefix to NewPrefix, skipping keys
%% already there (branch write or a prior partial run) or deleted since
%% the fork. Clears fork_pending only once every key succeeds.
run_copy_sweep(Path, FeedRef, DbName, Client, Bucket, OldPrefix, NewPrefix) ->
    ListPrefix = <<OldPrefix/binary, "/">>,
    case livery_s3:list_objects_all(Client, Bucket, #{prefix => ListPrefix}) of
        {ok, #{objects := Objects}} ->
            Results = [copy_one(FeedRef, DbName, Client, Bucket, OldPrefix, NewPrefix, Key)
                      || #{key := Key} <- Objects],
            case lists:all(fun(Ok) -> Ok end, Results) of
                true ->
                    ok = clear_fork_pending(Path);
                false ->
                    logger:warning(
                        "attachment fork copy sweep for ~s incomplete; "
                        "will retry on next open", [Path])
            end;
        {error, Reason} ->
            logger:warning(
                "attachment fork copy sweep for ~s failed to list ~s: ~p; "
                "will retry on next open", [Path, OldPrefix, Reason])
    end.

copy_one(FeedRef, DbName, Client, Bucket, OldPrefix, NewPrefix, SrcKey) ->
    case key_to_doc_att(OldPrefix, SrcKey) of
        {ok, DocId, AttName} ->
            case deleted_since_fork(FeedRef, DbName, DocId, AttName) of
                true ->
                    true;
                false ->
                    DstKey = swap_prefix(OldPrefix, NewPrefix, SrcKey),
                    copy_if_absent(FeedRef, DbName, DocId, AttName, Client, Bucket,
                                   SrcKey, DstKey)
            end;
        error ->
            true
    end.

deleted_since_fork(FeedRef, DbName, DocId, AttName) ->
    case barrel_att_feed:index_get(FeedRef, DbName, DocId, AttName) of
        {ok, #{op := delete}} -> true;
        _ -> false
    end.

%% @private Rechecks for a delete right after the copy lands and undoes
%% it if one raced in -- unlike a put, a delete does not self-heal a lost
%% race (delete_object on an absent key is a no-op).
copy_if_absent(FeedRef, DbName, DocId, AttName, Client, Bucket, SrcKey, DstKey) ->
    case livery_s3:head_object(Client, Bucket, DstKey) of
        {ok, _} ->
            true;
        {error, not_found} ->
            case livery_s3:copy_object(Client, Bucket, SrcKey, Bucket, DstKey) of
                {ok, _} ->
                    case deleted_since_fork(FeedRef, DbName, DocId, AttName) of
                        true -> _ = livery_s3:delete_object(Client, Bucket, DstKey);
                        false -> ok
                    end,
                    true;
                {error, Reason} ->
                    logger:warning("attachment fork copy ~s -> ~s failed: ~p",
                                   [SrcKey, DstKey, Reason]),
                    false
            end;
        {error, Reason} ->
            logger:warning("attachment fork copy HEAD ~s failed: ~p", [DstKey, Reason]),
            false
    end.

swap_prefix(OldPrefix, NewPrefix, Key) ->
    OldLen = byte_size(OldPrefix),
    <<OldPrefix:OldLen/binary, Rest/binary>> = Key,
    <<NewPrefix/binary, Rest/binary>>.

%% @private Distinguishes a genuinely absent attachment from one whose
%% bytes just haven't been copied by an in-progress fork sync yet.
not_found_or_sync_pending(#{feed_ref := FeedRef, path := Path}, DbName, DocId, AttName) ->
    case barrel_att_feed:index_get(FeedRef, DbName, DocId, AttName) of
        {ok, #{op := put}} ->
            case read_fork_pending(Path) of
                {ok, _} -> {error, {att_sync_pending, {DocId, AttName}}};
                _ -> {error, not_found}
            end;
        _ ->
            {error, not_found}
    end.

%% @doc Erase every S3 object under this store's own prefix, called from
%% delete_db before the local directory is removed. Clears fork_pending
%% first (best-effort -- an in-flight background sweep isn't cancelled,
%% just no longer resumed on a future open that will never happen).
-spec destroy(map(), binary()) -> ok | {error, term()}.
destroy(#{client := Client, bucket := Bucket, prefix := Prefix, path := Path}, _DbName) ->
    _ = clear_fork_pending(Path),
    case livery_s3:list_objects_all(Client, Bucket, #{prefix => <<Prefix/binary, "/">>}) of
        {ok, #{objects := Objects}} ->
            delete_in_batches(Client, Bucket, [maps:get(key, O) || O <- Objects]);
        {error, _} = Err ->
            Err
    end.

%% S3's DeleteObjects caps a single request at 1000 keys.
delete_in_batches(_Client, _Bucket, []) ->
    ok;
delete_in_batches(Client, Bucket, Keys) ->
    {Batch, Rest} = case Keys of
        [_ | _] = All when length(All) > ?DELETE_BATCH ->
            lists:split(?DELETE_BATCH, All);
        All ->
            {All, []}
    end,
    case livery_s3:delete_objects(Client, Bucket, Batch) of
        {ok, _} -> delete_in_batches(Client, Bucket, Rest);
        {error, _} = Err -> Err
    end.

-spec put(map(), binary(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data) ->
    put(AttRef, DbName, DocId, AttName, Data, #{}).

-spec put(map(), binary(), binary(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
put(#{prefix := Prefix} = AttRef, DbName, DocId, AttName, Data, Opts) when is_binary(Data) ->
    with_key(Prefix, DocId, AttName, fun(Key) ->
        do_put(AttRef, DbName, DocId, Key, AttName, Data, Opts)
    end).

do_put(#{client := Client, bucket := Bucket, conditional_writes := CW,
         feed_ref := FeedRef} = AttRef, DbName, DocId, Key, AttName, Data, Opts) ->
    Digest = compute_digest(Data),
    case check_expected_digest(Digest, Opts) of
        {error, _} = Err ->
            Err;
        ok ->
            OriginOpt = maps:get(origin_hlc, Opts, undefined),
            case resolve_origin(FeedRef, DbName, DocId, AttName, OriginOpt, Digest) of
                ignored ->
                    {ok, ignored};
                {apply, OriginHlc} ->
                    case conditional_headers(Opts, CW) of
                        {error, _} = Err ->
                            Err;
                        {ok, CondHeaders} ->
                            ContentType = maps:get(content_type, Opts, mimerl:filename(AttName)),
                            PutOpts = maps:merge(#{
                                content_type => ContentType,
                                metadata => #{?META_DIGEST => Digest,
                                              ?META_ORIGIN => encode_origin(OriginHlc)}
                            }, CondHeaders),
                            case livery_s3:put_object(Client, Bucket, Key, Data, PutOpts) of
                                {ok, _} ->
                                    commit_feed_put(FeedRef, DbName, DocId, AttName,
                                                    OriginHlc, Digest, byte_size(Data),
                                                    ContentType),
                                    {ok, #{name => AttName, content_type => ContentType,
                                           length => byte_size(Data), digest => Digest,
                                           chunked => false}};
                                {error, precondition_failed} ->
                                    conflict_error(AttRef, Key, AttName);
                                {error, _} = Err ->
                                    Err
                            end
                    end
            end
    end.

%% @private expected_digest is a data-integrity check on the incoming bytes
%% (mirrors barrel_att_store_blob's put/6), distinct from the
%% write-conflict-detection Opts (create_only/expected_etag) below.
check_expected_digest(Digest, Opts) ->
    case maps:get(expected_digest, Opts, undefined) of
        undefined -> ok;
        Digest -> ok;
        _Other -> {error, digest_mismatch}
    end.

%% @private Local writes (no origin_hlc) always mint a fresh origin and
%% proceed unconditionally -- there is no "other version" to compare
%% against for a direct, non-replicated write. Replicated writes (an
%% origin_hlc supplied) run the last-write-wins guard against the local
%% feed, exactly mirroring barrel_att_store_blob:resolve_origin/6, just
%% against this backend's own local feed_ref instead of its blob handle.
resolve_origin(_FeedRef, _DbName, _DocId, _AttName, undefined, _Digest) ->
    {apply, barrel_hlc:new_hlc()};
resolve_origin(FeedRef, DbName, DocId, AttName, OriginHlc, Digest) ->
    case barrel_att_feed:check(FeedRef, DbName, DocId, AttName, OriginHlc, Digest) of
        apply -> {apply, OriginHlc};
        ignored -> ignored
    end.

%% @private Moves the (DocId, AttName) feed row to a fresh local HLC,
%% committed as its own local RocksDB batch -- NOT atomic with the S3 write
%% that already succeeded by the time this runs (S3 and RocksDB are two
%% different systems; there is no cross-system transaction to have). A
%% failure here is logged, not propagated: the attachment itself was
%% already durably written, so returning an error here would be
%% misleading. The residual window this opens (S3 succeeded, the feed
%% commit didn't) is the same class of gap rebuild_feed/2 (Step 4) and
%% barrel_rep_att's not-found tolerance already absorb elsewhere -- not a
%% new risk this introduces.
commit_feed_put(FeedRef, DbName, DocId, AttName, OriginHlc, Digest, Length, ContentType) ->
    FeedOps = barrel_att_feed:ops(FeedRef, DbName, DocId, AttName, put, OriginHlc,
                                  #{digest => Digest, length => Length,
                                    content_type => ContentType}),
    commit_feed(FeedRef, DbName, DocId, AttName, FeedOps).

commit_feed(FeedRef, DbName, DocId, AttName, FeedOps) ->
    {ok, Batch} = rocksdb:batch(),
    try
        lists:foreach(
            fun({put, K, V}) -> ok = rocksdb:batch_put(Batch, K, V);
               ({delete, K}) -> ok = rocksdb:batch_delete(Batch, K)
            end,
            FeedOps),
        case rocksdb:write_batch(FeedRef, Batch, []) of
            ok -> ok;
            {error, Reason} ->
                logger:warning("attachment feed commit for ~s/~s/~s failed: ~p",
                               [DbName, DocId, AttName, Reason]),
                ok
        end
    after
        rocksdb:release_batch(Batch)
    end.

%% @private Translates create_only/expected_etag into the S3 headers
%% livery_s3 expects, gated on the store having verifiably enforced them at
%% open/2 -- {ok, #{}} (no headers) when the caller asked for neither, so
%% ordinary unconditional writes never even consult ConditionalWrites.
-spec conditional_headers(map(), supported | unsupported) ->
    {ok, map()} | {error, conditional_writes_unsupported}.
conditional_headers(Opts, ConditionalWrites) ->
    CreateOnly = maps:get(create_only, Opts, false),
    ExpectedEtag = maps:get(expected_etag, Opts, undefined),
    case CreateOnly =:= false andalso ExpectedEtag =:= undefined of
        true ->
            {ok, #{}};
        false ->
            case ConditionalWrites of
                unsupported ->
                    {error, conditional_writes_unsupported};
                supported ->
                    Headers0 = case CreateOnly of
                        true -> #{if_none_match => <<"*">>};
                        false -> #{}
                    end,
                    Headers = case ExpectedEtag of
                        undefined -> Headers0;
                        Etag -> Headers0#{if_match => Etag}
                    end,
                    {ok, Headers}
            end
    end.

%% @private A failed precondition reports what's actually at Key now, so
%% the caller can decide: retry against the new baseline, surface it, or
%% force an overwrite by retrying without the conditional options.
conflict_error(#{client := Client, bucket := Bucket}, Key, AttName) ->
    case livery_s3:head_object(Client, Bucket, Key) of
        {ok, Meta} ->
            Metadata = maps:get(metadata, Meta, #{}),
            {error, {conflict, #{
                name => AttName,
                content_type => maps:get(content_type, Meta,
                                         mimerl:filename(AttName)),
                length => maps:get(content_length, Meta, 0),
                digest => maps:get(?META_DIGEST, Metadata, undefined),
                etag => maps:get(etag, Meta, undefined)
            }}};
        {error, not_found} ->
            %% Raced with a delete between our rejected write and this
            %% HEAD -- report the conflict without current info rather
            %% than pretending the write actually landed.
            {error, {conflict, undefined}};
        {error, _} = Err ->
            Err
    end.

-spec get(map(), binary(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
get(#{prefix := Prefix} = AttRef, DbName, DocId, AttName) ->
    with_key(Prefix, DocId, AttName, fun(Key) ->
        do_get(AttRef, DbName, DocId, AttName, Key)
    end).

do_get(#{client := Client, bucket := Bucket} = AttRef, DbName, DocId, AttName, Key) ->
    case livery_s3:get_object(Client, Bucket, Key) of
        {ok, #{body := Body}} -> {ok, Body};
        {error, {s3, _Code, _Msg, #{status := 404}}} ->
            not_found_or_sync_pending(AttRef, DbName, DocId, AttName);
        {error, _} = Err -> Err
    end.

-spec delete(map(), binary(), binary(), binary()) -> ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName) ->
    delete(AttRef, DbName, DocId, AttName, #{}).

%% @doc `Opts.origin_hlc', for a replicated delete, runs the same
%% last-write-wins guard put/6 does. Unlike barrel_att_store_blob, this
%% always writes a delete tombstone to the feed rather than first checking
%% whether the attachment (or an index row) already existed -- that check
%% would cost an extra S3 round trip here, and skipping it only costs a
%% slightly larger feed for deletes of things that were never there, not a
%% correctness gap.
-spec delete(map(), binary(), binary(), binary(), map()) ->
    ok | {error, term()}.
delete(#{prefix := Prefix} = AttRef, DbName, DocId, AttName, Opts) ->
    with_key(Prefix, DocId, AttName, fun(Key) ->
        do_delete(AttRef, DbName, DocId, Key, AttName, Opts)
    end).

do_delete(#{client := Client, bucket := Bucket, feed_ref := FeedRef},
          DbName, DocId, Key, AttName, Opts) ->
    OriginOpt = maps:get(origin_hlc, Opts, undefined),
    case resolve_origin(FeedRef, DbName, DocId, AttName, OriginOpt, <<>>) of
        ignored ->
            ok;
        {apply, OriginHlc} ->
            case livery_s3:delete_object(Client, Bucket, Key) of
                ok ->
                    FeedOps = barrel_att_feed:ops(FeedRef, DbName, DocId, AttName,
                                                  delete, OriginHlc, #{}),
                    commit_feed(FeedRef, DbName, DocId, AttName, FeedOps);
                {error, _} = Err ->
                    Err
            end
    end.

-spec delete_all(map(), binary(), binary()) -> ok | {error, term()}.
delete_all(#{client := Client, bucket := Bucket, prefix := Prefix}, _DbName, DocId) ->
    case doc_prefix(Prefix, DocId) of
        {error, _} = Err ->
            Err;
        {ok, DocPrefix} ->
            case livery_s3:list_objects_all(Client, Bucket, #{prefix => DocPrefix}) of
                {ok, #{objects := []}} ->
                    ok;
                {ok, #{objects := Objects}} ->
                    Keys = [maps:get(key, O) || O <- Objects],
                    case livery_s3:delete_objects(Client, Bucket, Keys) of
                        {ok, _} -> ok;
                        {error, _} = Err -> Err
                    end;
                {error, _} = Err ->
                    Err
            end
    end.

%% @doc Enumerates attachment names under a document. Does not fetch object
%% bodies: the sole caller in this codebase (`barrel_att:list_attachments/3'
%% via `barrel_docdb:list_attachments/2') discards `Data' entirely, and
%% fetching bytes (or even just metadata) per key here would cost N extra S3
%% round trips for a value nothing reads -- `Data' is `undefined'. Revisit if
%% a real consumer of fold's Data argument appears.
-spec fold(map(), binary(), binary(), fun(), term()) -> term().
fold(#{client := Client, bucket := Bucket, prefix := StorePrefix}, _DbName, DocId, Fun, Acc) ->
    case doc_prefix(StorePrefix, DocId) of
        {error, _} ->
            Acc;
        {ok, Prefix} ->
            case livery_s3:list_objects_all(Client, Bucket, #{prefix => Prefix}) of
                {ok, #{objects := Objects}} ->
                    fold_objects(Objects, Prefix, Fun, Acc);
                {error, _} ->
                    Acc
            end
    end.

fold_objects([], _Prefix, _Fun, Acc) ->
    Acc;
fold_objects([#{key := Key} | Rest], Prefix, Fun, Acc) ->
    AttName = att_name_from_key(Prefix, Key),
    case Fun(AttName, undefined, Acc) of
        {ok, Acc1} -> fold_objects(Rest, Prefix, Fun, Acc1);
        {stop, Acc1} -> Acc1;
        stop -> Acc
    end.

-spec get_info(map(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
get_info(#{prefix := Prefix} = AttRef, DbName, DocId, AttName) ->
    with_key(Prefix, DocId, AttName, fun(Key) ->
        do_get_info(AttRef, DbName, DocId, AttName, Key)
    end).

do_get_info(#{client := Client, bucket := Bucket} = AttRef, DbName, DocId, AttName, Key) ->
    case livery_s3:head_object(Client, Bucket, Key) of
        {ok, Meta} ->
            Metadata = maps:get(metadata, Meta, #{}),
            {ok, #{
                name => AttName,
                content_type => maps:get(content_type, Meta,
                                         mimerl:filename(AttName)),
                length => maps:get(content_length, Meta, 0),
                digest => maps:get(?META_DIGEST, Metadata, undefined),
                chunked => false
            }};
        {error, not_found} ->
            not_found_or_sync_pending(AttRef, DbName, DocId, AttName);
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Attachment change feed
%%====================================================================
%%
%% Thin delegation to barrel_att_feed against feed_ref, exactly mirroring
%% barrel_att_store_blob's own one-liners (apps/barrel_docdb/src/
%% barrel_att_store_blob.erl:589-604) -- the module's key encoding, codec,
%% and read-side folding are storage-agnostic, and this backend's local
%% feed store follows the same key layout blob's own RocksDB does, just in
%% a dedicated instance rather than sharing one with blob data.

-spec att_changes(map(), binary(), barrel_hlc:timestamp() | first, map()) ->
    {ok, [barrel_att_feed:entry()], barrel_hlc:timestamp() | first}.
att_changes(#{feed_ref := FeedRef}, DbName, Since, Opts) ->
    barrel_att_feed:att_changes(FeedRef, DbName, Since, Opts).

-spec att_floor(map(), binary()) -> barrel_hlc:timestamp() | undefined.
att_floor(#{feed_ref := FeedRef}, DbName) ->
    barrel_att_feed:att_floor(FeedRef, DbName).

-spec sweep_att_feed(map(), binary(), barrel_hlc:timestamp()) ->
    {ok, #{tombstones_swept := non_neg_integer()}}.
sweep_att_feed(#{feed_ref := FeedRef}, DbName, Cutoff) ->
    barrel_att_feed:sweep(FeedRef, DbName, Cutoff).

%% @doc Maintenance escape hatch: resynthesize feed rows for every
%% attachment currently present in the bucket, for when the local feed is
%% lost or corrupted -- unlike barrel_att_store_blob:rebuild_feed/2, which
%% has no separate record of a write's original origin once its feed is
%% gone and so always falls back to the MINIMUM origin, this backend can
%% usually do better: `?META_ORIGIN' rides in each object's own S3 custom
%% metadata, independent of the local feed entirely, so the real origin
%% survives a feed loss and is recovered here. Only an object written
%% before `?META_ORIGIN' existed (pre-M2) has none to recover, and falls
%% back to `barrel_hlc:min()' the same way blob's rebuild always does --
%% so any real write, local or remote, still wins the LWW race against it.
%% Only rebuilds `put' rows for attachments that currently exist: a delete
%% tombstone for something already gone from the bucket is never
%% reconstructed, matching blob's own rebuild scope (a fresh put's LWW
%% check finds no index row either way, and applies unconditionally, the
%% same outcome a tombstone would have produced). Safe to re-run.
-spec rebuild_feed(map(), binary()) -> {ok, #{rows := non_neg_integer()}} | {error, term()}.
rebuild_feed(#{client := Client, bucket := Bucket, prefix := Prefix,
               feed_ref := FeedRef}, DbName) ->
    ListPrefix = <<Prefix/binary, "/">>,
    case livery_s3:list_objects_all(Client, Bucket, #{prefix => ListPrefix}) of
        {ok, #{objects := Objects}} ->
            N = lists:foldl(
                fun(#{key := Key}, Count) ->
                    case rebuild_one(Client, Bucket, Prefix, FeedRef, DbName, Key) of
                        true -> Count + 1;
                        false -> Count
                    end
                end,
                0,
                Objects),
            {ok, #{rows => N}};
        {error, _} = Err ->
            Err
    end.

%% @private HEADs the object for its digest/origin/content-type (a listing
%% alone carries none of those) and writes one feed row for it. A 404 here
%% means it raced with a delete between the listing and this HEAD -- skip
%% it rather than fabricating a row for something no longer there.
rebuild_one(Client, Bucket, Prefix, FeedRef, DbName, Key) ->
    case key_to_doc_att(Prefix, Key) of
        {ok, DocId, AttName} ->
            case livery_s3:head_object(Client, Bucket, Key) of
                {ok, Meta} ->
                    Metadata = maps:get(metadata, Meta, #{}),
                    Digest = maps:get(?META_DIGEST, Metadata, <<>>),
                    ContentType = maps:get(content_type, Meta,
                                           mimerl:filename(AttName)),
                    Length = maps:get(content_length, Meta, 0),
                    OriginHlc = decode_origin(maps:get(?META_ORIGIN, Metadata, undefined)),
                    Ops = barrel_att_feed:ops(FeedRef, DbName, DocId, AttName, put,
                                              OriginHlc,
                                              #{digest => Digest, length => Length,
                                                content_type => ContentType}),
                    ok = commit_feed(FeedRef, DbName, DocId, AttName, Ops),
                    true;
                {error, _} ->
                    false
            end;
        error ->
            false
    end.

%% @private Inverse of object_key/3: `Prefix/hex(DocId)/AttName' -> the
%% original DocId bytes and AttName. AttName is validated elsewhere to
%% exclude "/", so splitting Rest on the first "/" cleanly separates the
%% hex DocId from it.
key_to_doc_att(Prefix, Key) ->
    Offset = byte_size(Prefix) + 1,
    case Offset =< byte_size(Key) of
        true ->
            Rest = binary:part(Key, Offset, byte_size(Key) - Offset),
            case binary:split(Rest, <<"/">>) of
                [HexDocId, AttName] when AttName =/= <<>> ->
                    try
                        {ok, binary:decode_hex(HexDocId), AttName}
                    catch
                        _:_ -> error
                    end;
                _ ->
                    error
            end;
        false ->
            error
    end.

%%====================================================================
%% Key scheme
%%====================================================================

%% @private Resolve the object key, defensively rejecting anything that
%% would exceed S3's 1024-byte key cap, then run Fun against it. `Prefix' is
%% the store's own persisted key prefix (see the moduledoc), not a DbName.
with_key(Prefix, DocId, AttName, Fun) ->
    case object_key(Prefix, DocId, AttName) of
        {error, _} = Err -> Err;
        {ok, Key} -> Fun(Key)
    end.

-spec object_key(binary(), binary(), binary()) ->
    {ok, binary()} | {error, key_too_long}.
object_key(Prefix, DocId, AttName) ->
    case doc_prefix(Prefix, DocId) of
        {error, _} = Err -> Err;
        {ok, DocPrefix} -> bounded_key(<<DocPrefix/binary, AttName/binary>>)
    end.

-spec doc_prefix(binary(), binary()) -> {ok, binary()} | {error, key_too_long}.
doc_prefix(Prefix, DocId) ->
    bounded_key(<<Prefix/binary, "/",
                  (binary:encode_hex(DocId, lowercase))/binary, "/">>).

bounded_key(Key) ->
    case byte_size(Key) =< ?MAX_KEY_SIZE of
        true -> {ok, Key};
        false -> {error, key_too_long}
    end.

att_name_from_key(Prefix, Key) ->
    PrefixLen = byte_size(Prefix),
    binary:part(Key, PrefixLen, byte_size(Key) - PrefixLen).

compute_digest(Data) ->
    Digest = crypto:hash(sha256, Data),
    <<"sha256-", (binary:encode_hex(Digest, lowercase))/binary>>.

%% @private barrel_hlc:encode/1's raw 12-byte binary isn't safe as an HTTP
%% header value as-is; hex-encode it the same way the digest already is.
encode_origin(OriginHlc) ->
    binary:encode_hex(barrel_hlc:encode(OriginHlc), lowercase).

%% @private Inverse of encode_origin/1, used by rebuild_feed/2. Missing or
%% unparseable (a corrupt/foreign value should not crash a recovery pass)
%% falls back to barrel_hlc:min() -- the same "any real write wins" floor
%% blob's own rebuild_feed/2 always uses.
decode_origin(undefined) ->
    barrel_hlc:min();
decode_origin(HexOrigin) ->
    try
        barrel_hlc:decode(binary:decode_hex(HexOrigin))
    catch
        _:_ -> barrel_hlc:min()
    end.

%%====================================================================
%% Streaming API
%%====================================================================
%%
%% This is the path every *replicated* attachment actually goes through
%% (barrel_rep_att:transfer/6 always calls get_attachment_stream/
%% put_attachment, never the whole-blob API). write_chunk buffers in memory
%% and does NOT start a multipart upload eagerly: a real S3 call only
%% happens once the buffer crosses part_size, and finish_stream does a
%% single put_object if that threshold was never crossed (the common case
%% for small/replicated attachments) instead of a needless 3-call
%% create+upload_part+complete sequence.
%%
%% Stream maps thread through write_chunk/read_chunk exactly like the
%% RocksDB backend's: each call returns the map to pass to the next call,
%% and on {error, _} the caller's last-known-good map (from the previous
%% successful call) is what abort_stream/1 must be given -- it alone
%% accurately reflects what has actually been sent to S3 so far.

-spec put_stream(map(), binary(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType) ->
    put_stream(AttRef, DbName, DocId, AttName, ContentType, #{}).

-spec put_stream(map(), binary(), binary(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
put_stream(#{prefix := Prefix} = AttRef, DbName, DocId, AttName, ContentType, Opts) ->
    with_key(Prefix, DocId, AttName, fun(Key) ->
        {ok, #{
            type => write,
            att_ref => AttRef,
            key => Key,
            db_name => DbName,
            doc_id => DocId,
            att_name => AttName,
            content_type => ContentType,
            origin_hlc => maps:get(origin_hlc, Opts, undefined),
            expected_digest => maps:get(expected_digest, Opts, undefined),
            create_only => maps:get(create_only, Opts, false),
            expected_etag => maps:get(expected_etag, Opts, undefined),
            buffer => <<>>,
            digest_ctx => crypto:hash_init(sha256),
            length => 0,
            multipart => undefined
        }}
    end).

-spec write_chunk(map(), binary()) -> {ok, map()} | {error, term()}.
write_chunk(#{type := write, buffer := Buffer, digest_ctx := Ctx,
              length := Length} = Stream, Data) ->
    NewBuffer = <<Buffer/binary, Data/binary>>,
    Stream1 = Stream#{
        buffer => NewBuffer,
        digest_ctx => crypto:hash_update(Ctx, Data),
        length => Length + byte_size(Data)
    },
    case byte_size(NewBuffer) >= part_size(Stream1) of
        true -> flush_part(Stream1, maps:get(multipart, Stream1) =:= undefined);
        false -> {ok, Stream1}
    end.

part_size(#{att_ref := #{part_size := PartSize}}) -> PartSize.

%% @private Upload the buffer as one multipart part, creating the upload
%% first if none exists yet. `WasUndefined' is whether `multipart' was
%% `undefined' before THIS write_chunk call started: if the upload was just
%% created in this same call and the part upload then fails, the caller's
%% retained (pre-call) stream has no upload_id at all to abort_stream later
%% -- self-abort here instead of orphaning it. If the upload already
%% existed from a prior successful call, the caller's own stream already
%% carries it, so abort_stream on that is sufficient; no self-abort here.
%%
%% create_only/expected_etag are NOT applied to create_multipart_upload:
%% the object is created at completion, not here (see finish_stream/1),
%% consistent with where livery_s3 itself applies conditional writes for
%% multipart uploads.
flush_part(#{multipart := undefined} = Stream, WasUndefined) ->
    #{att_ref := #{client := Client, bucket := Bucket}, key := Key,
      content_type := ContentType} = Stream,
    case livery_s3:create_multipart_upload(Client, Bucket, Key,
                                           #{content_type => ContentType}) of
        {ok, UploadId} ->
            Stream1 = Stream#{multipart => #{upload_id => UploadId,
                                             parts => [], part_number => 1}},
            flush_part(Stream1, WasUndefined);
        {error, _} = Err ->
            Err
    end;
flush_part(#{multipart := MP, buffer := Buffer} = Stream, WasUndefined) ->
    #{att_ref := #{client := Client, bucket := Bucket}, key := Key} = Stream,
    #{upload_id := UploadId, parts := Parts, part_number := PartNumber} = MP,
    case livery_s3:upload_part(Client, Bucket, Key, UploadId, PartNumber, Buffer) of
        {ok, #{etag := ETag}} ->
            NewMP = MP#{parts => Parts ++ [{PartNumber, ETag}],
                       part_number => PartNumber + 1},
            {ok, Stream#{multipart => NewMP, buffer => <<>>}};
        {error, Reason} ->
            case WasUndefined of
                true -> _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId);
                false -> ok
            end,
            {error, Reason}
    end.

%% @doc Finish a write stream: never crossed the multipart threshold ->
%% single put_object with everything buffered (digest and origin checked
%% BEFORE sending anything, since nothing has been written yet); otherwise
%% upload the remainder as the final part (allowed under the size minimum
%% since it's last) and complete -- a digest mismatch, a losing LWW check,
%% or a completion failure aborts the whole multipart upload rather than
%% leaving it dangling. The origin check runs against the local feed the
%% same way put/6's does: local writes (no origin_hlc) always proceed,
%% replicated ones are subject to the last-write-wins guard.
-spec finish_stream(map()) -> {ok, map()} | {error, term()}.
finish_stream(#{type := write, multipart := undefined} = Stream) ->
    #{att_ref := #{client := Client, bucket := Bucket, feed_ref := FeedRef,
                  conditional_writes := CW} = AttRef,
      key := Key, db_name := DbName, doc_id := DocId, att_name := AttName,
      content_type := ContentType,
      buffer := Buffer, digest_ctx := Ctx,
      origin_hlc := OriginOpt,
      expected_digest := ExpectedDigest} = Stream,
    Digest = finalize_digest(Ctx),
    case digest_ok(Digest, ExpectedDigest) of
        {error, _} = Err ->
            Err;
        ok ->
            case resolve_origin(FeedRef, DbName, DocId, AttName, OriginOpt, Digest) of
                ignored ->
                    {ok, ignored};
                {apply, OriginHlc} ->
                    case conditional_headers(Stream, CW) of
                        {error, _} = Err ->
                            Err;
                        {ok, CondHeaders} ->
                            PutOpts = maps:merge(#{content_type => ContentType,
                                                   metadata => #{?META_DIGEST => Digest,
                                                                 ?META_ORIGIN => encode_origin(OriginHlc)}},
                                                CondHeaders),
                            case livery_s3:put_object(Client, Bucket, Key, Buffer, PutOpts) of
                                {ok, _} ->
                                    commit_feed_put(FeedRef, DbName, DocId, AttName,
                                                    OriginHlc, Digest, byte_size(Buffer),
                                                    ContentType),
                                    {ok, #{name => AttName, content_type => ContentType,
                                           length => byte_size(Buffer), digest => Digest,
                                           chunked => false}};
                                {error, precondition_failed} ->
                                    conflict_error(AttRef, Key, AttName);
                                {error, _} = Err ->
                                    Err
                            end
                    end
            end
    end;
finish_stream(#{type := write, multipart := MP} = Stream) ->
    #{att_ref := #{client := Client, bucket := Bucket, feed_ref := FeedRef,
                  conditional_writes := CW} = AttRef,
      key := Key, db_name := DbName, doc_id := DocId, att_name := AttName,
      content_type := ContentType,
      buffer := Buffer, length := Length, digest_ctx := Ctx,
      origin_hlc := OriginOpt,
      expected_digest := ExpectedDigest} = Stream,
    #{upload_id := UploadId, parts := Parts, part_number := PartNumber} = MP,
    FinalParts = case Buffer of
        <<>> ->
            {ok, Parts};
        _ ->
            case livery_s3:upload_part(Client, Bucket, Key, UploadId,
                                       PartNumber, Buffer) of
                {ok, #{etag := ETag}} -> {ok, Parts ++ [{PartNumber, ETag}]};
                {error, _} = UploadErr -> UploadErr
            end
    end,
    case FinalParts of
        {error, Reason} ->
            _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
            {error, Reason};
        {ok, AllParts} ->
            Digest = finalize_digest(Ctx),
            case digest_ok(Digest, ExpectedDigest) of
                {error, _} = DigestErr ->
                    _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
                    DigestErr;
                ok ->
                    case resolve_origin(FeedRef, DbName, DocId, AttName, OriginOpt, Digest) of
                        ignored ->
                            _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
                            {ok, ignored};
                        {apply, OriginHlc} ->
                            case conditional_headers(Stream, CW) of
                                {error, _} = Err ->
                                    _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
                                    Err;
                                {ok, CondHeaders} ->
                                    complete(AttRef, Key, UploadId, AllParts, CondHeaders,
                                             DbName, DocId, AttName, ContentType, Length,
                                             Digest, OriginHlc)
                            end
                    end
            end
    end.

%% @private The object is created here, not at create_multipart_upload, so
%% this is where a losing conditional write shows up. precondition_failed
%% (412), conditional_request_conflict (409, a concurrent writer won an
%% if_none_match race), and not_found (404, an if_match completion lost to
%% a concurrent delete) all mean the same thing to our caller regardless of
%% which one the upload_id's exact state maps to -- finish_stream is a
%% one-shot terminal call either way, nothing here retries the same
%% upload_id -- so all three report the same {conflict, CurrentInfo} shape
%% as the single-put path. The abort attempt is harmless best-effort
%% cleanup even when the upload id is already dead (409/404).
%%
%% ?META_DIGEST/?META_ORIGIN can't be set on create_multipart_upload (the
%% digest isn't known until every part has been seen, and S3 only accepts
%% custom object metadata at creation, never at completion), so a
%% multipart-uploaded object is created here with none. A self-copy with a
%% REPLACE metadata directive attaches it immediately after, a well-worn S3
%% pattern for exactly this (set/refresh metadata without re-uploading the
%% data). If that copy fails, it's logged and NOT propagated as an error --
%% the attachment itself was already durably completed; a repair (or a
%% future rebuild_feed/2 pass, which falls back to barrel_hlc:min() for
%% whatever metadata still didn't make it) covers the gap either way.
complete(#{client := Client, bucket := Bucket, feed_ref := FeedRef} = AttRef, Key,
         UploadId, AllParts, CondHeaders, DbName, DocId, AttName, ContentType,
         Length, Digest, OriginHlc) ->
    case livery_s3:complete_multipart_upload(Client, Bucket, Key, UploadId,
                                             AllParts, CondHeaders) of
        {ok, _} ->
            attach_multipart_metadata(Client, Bucket, Key, ContentType, Digest, OriginHlc),
            commit_feed_put(FeedRef, DbName, DocId, AttName, OriginHlc, Digest,
                            Length, ContentType),
            {ok, #{name => AttName, content_type => ContentType,
                   length => Length, digest => Digest, chunked => true}};
        {error, Reason} when Reason =:= precondition_failed;
                            Reason =:= conditional_request_conflict;
                            Reason =:= not_found ->
            _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
            conflict_error(AttRef, Key, AttName);
        {error, _} = CompleteErr ->
            _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
            CompleteErr
    end.

attach_multipart_metadata(Client, Bucket, Key, ContentType, Digest, OriginHlc) ->
    Opts = #{content_type => ContentType,
             metadata => #{?META_DIGEST => Digest,
                           ?META_ORIGIN => encode_origin(OriginHlc)}},
    case livery_s3:copy_object(Client, Bucket, Key, Bucket, Key, Opts) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            logger:warning("attaching metadata to multipart object ~s failed: ~p",
                           [Key, Reason]),
            ok
    end.

finalize_digest(Ctx) ->
    DigestBin = crypto:hash_final(Ctx),
    <<"sha256-", (binary:encode_hex(DigestBin, lowercase))/binary>>.

digest_ok(_Digest, undefined) -> ok;
digest_ok(Digest, Digest) -> ok;
digest_ok(_Digest, _Expected) -> {error, digest_mismatch}.

%% @doc Cleans up only what was actually sent to S3: nothing, if the
%% multipart threshold was never crossed (finish_stream is the only place
%% that would have made anything visible); the in-progress multipart
%% upload otherwise.
-spec abort_stream(map()) -> ok.
abort_stream(#{type := write, multipart := #{upload_id := UploadId}} = Stream) ->
    #{att_ref := #{client := Client, bucket := Bucket}, key := Key} = Stream,
    _ = livery_s3:abort_multipart_upload(Client, Bucket, Key, UploadId),
    ok;
abort_stream(_Stream) ->
    ok.

-spec get_stream(map(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
get_stream(#{prefix := Prefix} = AttRef, DbName, DocId, AttName) ->
    with_key(Prefix, DocId, AttName, fun(Key) ->
        do_get_stream(AttRef, DbName, DocId, AttName, Key)
    end).

do_get_stream(#{client := Client, bucket := Bucket} = AttRef, DbName, DocId, AttName, Key) ->
    case livery_s3:get_object(Client, Bucket, Key, #{stream => true}) of
        {ok, #{body := {stream, Reader}}} ->
            {ok, #{type => read, att_ref => AttRef, key => Key,
                   att_name => AttName, reader => Reader}};
        {error, {s3, _Code, _Msg, #{status := 404}}} ->
            not_found_or_sync_pending(AttRef, DbName, DocId, AttName);
        {error, _} = Err ->
            Err
    end.

%% @doc Pulls from the `livery_client' reader `get_object' handed back
%% (`stream => true'); `{done, _}' -> `eof' per the behaviour contract, not
%% a 3-tuple, since there is no more data to hand the caller.
-spec read_chunk(map()) -> {ok, binary(), map()} | eof | {error, term()}.
read_chunk(#{type := read, reader := Reader} = Stream) ->
    case livery_client:read(Reader, 30000) of
        {ok, Data, NextReader} -> {ok, Data, Stream#{reader => NextReader}};
        {done, _NextReader} -> eof;
        {error, _} = Err -> Err
    end.

%% @doc No-op, matching the RocksDB backend: an abandoned (not fully
%% drained) read stream's underlying connection is reclaimed by hackney's
%% own pool/timeout machinery, not explicitly cancelled here -- livery_client
%% exposes no cancel for a pull-style reader (only for its separate
%% flow => manual push-stream mechanism, which get_object's `stream => true`
%% option does not use).
-spec close_stream(map()) -> ok.
close_stream(_Stream) ->
    ok.
