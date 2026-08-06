%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_att_s3_store: the whole-blob API (Step 2 --
%%% open/close, put/get/delete/delete_all/fold, get_info, key scheme,
%%% expected_digest) and the streaming/multipart API (Step 3 --
%%% put_stream/write_chunk/finish_stream/abort_stream, get_stream/
%%% read_chunk, the buffer-until-threshold single-put-vs-multipart
%%% decision).
%%%
%%% Multipart tests use a 5 MiB part_size, not a tiny synthetic value:
%%% real S3 (and MinIO) reject non-final parts under 5 MiB with
%%% EntityTooSmall regardless of what part_size the *client* chooses to
%%% buffer to, so a genuine multipart round trip needs real S3-sized data.
%%% Garage is more lenient (a separate test below uses a 1 MiB part
%%% against Garage only, to exercise that the part_size knob genuinely
%%% changes behavior on a store that allows it).
%%%
%%% Runs against both MinIO and Garage (same test bodies, one per group),
%%% since the two stores have deliberately different capability profiles
%%% (see the plan) -- what's covered here is the part that must behave
%%% identically everywhere. Connection details come from OS env vars; a
%%% group skips cleanly if its store isn't configured/reachable, so this
%%% suite is safe to run without either service present.
%%%
%%% Local setup this suite was developed against:
%%%   MinIO:  docker run -p 19000:9000 -e MINIO_ROOT_USER=minioadmin \
%%%           -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data
%%%   Garage: see apps/barrel_att_s3/test/README.md (bucket + key must be
%%%           pre-provisioned; Garage keys can't create buckets themselves).
%%%
%%% Env vars (all optional for MinIO, defaults match the setup above;
%%% GARAGE_S3_TEST_ACCESS_KEY/_SECRET_KEY have no default -- the garage
%%% group skips without them):
%%%   MINIO_S3_TEST_ENDPOINT/_ACCESS_KEY/_SECRET_KEY/_REGION/_BUCKET
%%%   GARAGE_S3_TEST_ENDPOINT/_ACCESS_KEY/_SECRET_KEY/_REGION/_BUCKET
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

-export([
    open_missing_bucket/1,
    put_get_roundtrip/1,
    get_returns_not_found_for_missing/1,
    get_info_returns_metadata/1,
    get_info_not_found/1,
    content_type_override_persists/1,
    delete_removes_attachment/1,
    delete_missing_is_ok/1,
    fold_lists_attachment_names/1,
    fold_stop_early/1,
    delete_all_removes_every_attachment_for_doc/1,
    delete_all_does_not_affect_other_docs/1,
    expected_digest_match_succeeds/1,
    expected_digest_mismatch_rejected/1,
    key_too_long_rejected/1,
    stream_small_single_put/1,
    stream_large_multipart/1,
    stream_abort_mid_multipart/1,
    stream_digest_mismatch_small/1,
    stream_digest_mismatch_multipart_aborts_upload/1,
    stream_read_roundtrip/1,
    stream_read_not_found/1,
    stream_small_part_size_accepted_by_garage/1,
    conditional_writes_capability_reflects_store/1,
    default_put_stays_unconditional/1,
    create_only_succeeds_on_fresh_key/1,
    create_only_conflicts_on_existing_key/1,
    expected_etag_match_succeeds/1,
    expected_etag_stale_returns_conflict/1,
    stream_create_only_conflicts_small_path/1,
    stream_create_only_conflicts_multipart_path/1,
    garage_create_only_fails_fast/1,
    garage_expected_etag_fails_fast/1,
    prefix_derived_from_db_name_on_first_open/1,
    prefix_persists_across_reopen/1,
    open_missing_db_name_on_fresh_path/1,
    origin_hlc_newer_put_applies/1,
    origin_hlc_stale_put_ignored/1,
    origin_hlc_stale_delete_ignored/1,
    origin_hlc_stream_stale_ignored_single_put/1,
    origin_hlc_stream_stale_ignored_multipart/1,
    att_changes_reflects_puts_and_deletes/1,
    att_changes_pagination_and_since/1,
    att_floor_and_sweep/1,
    rebuild_feed_on_empty_store_returns_zero_rows/1,
    rebuild_feed_recovers_lost_feed/1,
    rebuild_feed_missing_origin_falls_back_to_min/1,
    checkpoint_returns_fast_without_copying_bytes/1,
    checkpoint_open_sweeps_and_converges/1,
    checkpoint_branch_write_not_clobbered_by_sweep/1,
    checkpoint_branch_delete_not_resurrected_by_sweep/1,
    checkpoint_refuses_still_syncing_source/1,
    destroy_removes_all_objects_under_prefix/1,
    destroy_clears_fork_pending_marker/1,
    resume_fork_sync_false_does_not_spawn_sweep/1
]).

-define(CASES, [
    open_missing_bucket,
    put_get_roundtrip,
    get_returns_not_found_for_missing,
    get_info_returns_metadata,
    get_info_not_found,
    content_type_override_persists,
    delete_removes_attachment,
    delete_missing_is_ok,
    fold_lists_attachment_names,
    fold_stop_early,
    delete_all_removes_every_attachment_for_doc,
    delete_all_does_not_affect_other_docs,
    expected_digest_match_succeeds,
    expected_digest_mismatch_rejected,
    key_too_long_rejected,
    stream_small_single_put,
    stream_large_multipart,
    stream_abort_mid_multipart,
    stream_digest_mismatch_small,
    stream_digest_mismatch_multipart_aborts_upload,
    stream_read_roundtrip,
    stream_read_not_found,
    conditional_writes_capability_reflects_store,
    default_put_stays_unconditional,
    prefix_derived_from_db_name_on_first_open,
    prefix_persists_across_reopen,
    open_missing_db_name_on_fresh_path,
    origin_hlc_newer_put_applies,
    origin_hlc_stale_put_ignored,
    origin_hlc_stale_delete_ignored,
    origin_hlc_stream_stale_ignored_single_put,
    origin_hlc_stream_stale_ignored_multipart,
    att_changes_reflects_puts_and_deletes,
    att_changes_pagination_and_since,
    att_floor_and_sweep,
    rebuild_feed_on_empty_store_returns_zero_rows,
    rebuild_feed_recovers_lost_feed,
    rebuild_feed_missing_origin_falls_back_to_min,
    checkpoint_returns_fast_without_copying_bytes,
    checkpoint_open_sweeps_and_converges,
    checkpoint_branch_write_not_clobbered_by_sweep,
    checkpoint_branch_delete_not_resurrected_by_sweep,
    checkpoint_refuses_still_syncing_source,
    destroy_removes_all_objects_under_prefix,
    destroy_clears_fork_pending_marker,
    resume_fork_sync_false_does_not_spawn_sweep
]).

%% MinIO has verifiably enforced If-Match/If-None-Match since 2023; Garage
%% cannot at all, by its own documented design. Run the actual
%% conflict-detection assertions only where they mean something.
-define(MINIO_ONLY_CASES, [
    create_only_succeeds_on_fresh_key,
    create_only_conflicts_on_existing_key,
    expected_etag_match_succeeds,
    expected_etag_stale_returns_conflict,
    stream_create_only_conflicts_small_path,
    stream_create_only_conflicts_multipart_path
]).

-define(GARAGE_ONLY_CASES, [
    stream_small_part_size_accepted_by_garage,
    garage_create_only_fails_fast,
    garage_expected_etag_fails_fast
]).

%% 5 MiB: the real S3/MinIO minimum for a non-final multipart part,
%% independent of whatever part_size the client buffers to.
-define(PART_SIZE, 5 * 1024 * 1024).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, minio}, {group, garage}].

groups() ->
    [
        {minio, [sequence], ?CASES ++ ?MINIO_ONLY_CASES},
        {garage, [sequence], ?CASES ++ ?GARAGE_ONLY_CASES}
    ].

init_per_suite(Config) ->
    %% barrel_att_s3 now depends on barrel_docdb at the application level
    %% (barrel_hlc:new_hlc/0, used for feed origin timestamps, needs
    %% barrel_docdb's own supervisor-started clock process), so
    %% ensure_all_started(barrel_att_s3) already covers this -- kept
    %% explicit since this suite exercises the S3 backend on its own,
    %% never through a running barrel_db_server.
    {ok, _} = application:ensure_all_started(barrel_att_s3),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(minio, Config) ->
    S3Opts = barrel_att_s3_test_support:minio_opts(),
    case barrel_att_s3_test_support:reachable(S3Opts) of
        true ->
            Client = livery_s3:new(maps:without([bucket], S3Opts)),
            Bucket = maps:get(bucket, S3Opts),
            case livery_s3:create_bucket(Client, Bucket) of
                ok -> ok;
                {error, {s3, <<"BucketAlreadyOwnedByYou">>, _, _}} -> ok;
                {error, {s3, <<"BucketAlreadyExists">>, _, _}} -> ok;
                {error, Reason} -> ct:fail({minio_bucket_setup_failed, Reason})
            end,
            [{store, minio}, {s3_opts, S3Opts} | Config];
        false ->
            {skip, {minio_not_reachable, maps:get(endpoint, S3Opts)}}
    end;
init_per_group(garage, Config) ->
    case barrel_att_s3_test_support:garage_opts() of
        undefined ->
            {skip, garage_credentials_not_configured};
        S3Opts ->
            case barrel_att_s3_test_support:reachable(S3Opts) of
                true -> [{store, garage}, {s3_opts, S3Opts} | Config];
                false -> {skip, {garage_not_reachable, maps:get(endpoint, S3Opts)}}
            end
    end.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    S3Opts = ?config(s3_opts, Config),
    %% priv_dir is shared across the whole suite run, not per-group, and
    %% both groups run the same-named test cases -- without the store
    %% prefix, minio.foo and garage.foo would open the same local feed.db
    %% path and silently see each other's feed state (a real bug this
    %% caught: att_floor_and_sweep saw a floor already set, left over from
    %% the other group's earlier run of the same-named test).
    Store = atom_to_binary(?config(store, Config), utf8),
    DbName = <<Store/binary, "-", (atom_to_binary(TestCase, utf8))/binary>>,
    Path = filename:join(?config(priv_dir, Config), binary_to_list(DbName)),
    {ok, AttRef} = barrel_att_s3_store:open(Path,
                                            #{s3 => S3Opts#{part_size => ?PART_SIZE},
                                              db_name => DbName}),
    [{att_ref, AttRef}, {db_name, DbName}, {path, Path} | Config].

end_per_testcase(_TestCase, Config) ->
    ok = barrel_att_s3_store:close(?config(att_ref, Config)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Polls Fun (a 0-arity predicate) every 50ms until it returns true or
%% TimeoutMs elapses, at which point the test fails with a clear reason
%% rather than a plain assertion mismatch on whatever the last poll saw.
wait_until(Fun, TimeoutMs) ->
    wait_until(Fun, TimeoutMs, 50).

wait_until(Fun, TimeoutMs, _IntervalMs) when TimeoutMs =< 0 ->
    case Fun() of
        true -> ok;
        false -> ct:fail(wait_until_timeout)
    end;
wait_until(Fun, TimeoutMs, IntervalMs) ->
    case Fun() of
        true -> ok;
        false ->
            timer:sleep(IntervalMs),
            wait_until(Fun, TimeoutMs - IntervalMs, IntervalMs)
    end.

%% @private Builds a branch's att_ref by hand from checkpoint/2's own
%% on-disk output (the persisted s3_prefix marker, a raw rocksdb:open of
%% the checkpointed feed.db) instead of a real open/2 -- deliberately, so
%% no sweep is ever spawned for BranchPath and a test can exercise its
%% pre-sweep, pre-open state deterministically. Returns the feed handle
%% too, so the caller can rocksdb:close/1 it -- always inside try/after,
%% since an assertion failure between opening it and closing it would
%% otherwise leak the handle for the rest of the CT run.
open_branch_att_ref_raw(SourceAttRef, BranchPath) ->
    {ok, PrefixBin} = file:read_file(filename:join(BranchPath, "s3_prefix")),
    {ok, FeedRef} = rocksdb:open(filename:join(BranchPath, "feed.db"), []),
    BranchAttRef = SourceAttRef#{feed_ref => FeedRef, prefix => PrefixBin,
                                 path => BranchPath},
    {BranchAttRef, FeedRef}.

%%====================================================================
%% Test Cases
%%====================================================================

open_missing_bucket(_Config) ->
    ?assertEqual({error, missing_bucket},
                 barrel_att_s3_store:open("/tmp/unused", #{s3 => #{}})).

%% The per-testcase att_ref from init_per_testcase already IS a first-ever
%% open of a fresh path, with db_name set to the testcase's own name -- this
%% just asserts the prefix open/2 derived matches it exactly, since every
%% other test in this suite implicitly depends on that being true.
prefix_derived_from_db_name_on_first_open(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual(DbName, maps:get(prefix, AttRef)).

%% Opens its own dedicated path/att_ref rather than reusing the per-testcase
%% one from init_per_testcase (this test needs to close and reopen it, and
%% the per-testcase one is end_per_testcase's to close). Reopens the same
%% path with a DIFFERENT db_name than the original open used, and confirms
%% the prefix -- and so the actual S3 keys an attachment put through it
%% lands under -- is still the ORIGINAL one, read from the persisted marker
%% rather than re-derived. This is the property M2's branching design
%% depends on (a checkpointed branch's marker is what makes its prefix
%% stick across opens), and also what keeps an existing M1-created store's
%% keys from moving on its first M2-code open.
prefix_persists_across_reopen(Config) ->
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config) ++ "-persist",
    S3Opts = ?config(s3_opts, Config),
    {ok, AttRef} = barrel_att_s3_store:open(Path,
        #{s3 => S3Opts#{part_size => ?PART_SIZE}, db_name => DbName}),
    DocId = <<"doc1">>,
    Data = <<"persists across reopen">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"note.txt">>, Data),
    ok = barrel_att_s3_store:close(AttRef),

    DifferentDbName = <<"not-", DbName/binary>>,
    {ok, Reopened} = barrel_att_s3_store:open(Path,
        #{s3 => S3Opts#{part_size => ?PART_SIZE}, db_name => DifferentDbName}),
    ?assertEqual(DbName, maps:get(prefix, Reopened)),
    ?assertEqual({ok, Data},
                 barrel_att_s3_store:get(Reopened, DifferentDbName, DocId, <<"note.txt">>)),
    ok = barrel_att_s3_store:delete(Reopened, DifferentDbName, DocId, <<"note.txt">>),
    ok = barrel_att_s3_store:close(Reopened).

open_missing_db_name_on_fresh_path(Config) ->
    S3Opts = ?config(s3_opts, Config),
    FreshPath = ?config(path, Config) ++ "-no-db-name",
    ?assertEqual({error, missing_db_name},
                 barrel_att_s3_store:open(FreshPath, #{s3 => S3Opts#{part_size => ?PART_SIZE}})).

%% barrel_hlc:min/0 as the "old" origin and barrel_hlc:new_hlc/0 as "new" --
%% trivially ordered, no need to construct anything more elaborate to
%% exercise the guard. index_get/4 reaches into the local feed directly
%% (feed_ref is exposed on att_ref): Step 3 hasn't landed att_changes/4 yet,
%% so this is the only way to confirm what actually got committed.
origin_hlc_newer_put_applies(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    OldOrigin = barrel_hlc:min(),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"origin.txt">>,
                                      <<"first">>, #{origin_hlc => OldOrigin}),
    NewOrigin = barrel_hlc:new_hlc(),
    {ok, Info} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"origin.txt">>,
                                        <<"second">>, #{origin_hlc => NewOrigin}),
    ?assertMatch(#{length := 6}, Info),
    ?assertEqual({ok, <<"second">>},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"origin.txt">>)),
    FeedRef = maps:get(feed_ref, AttRef),
    {ok, IdxEntry} = barrel_att_feed:index_get(FeedRef, DbName, DocId, <<"origin.txt">>),
    ?assertEqual(NewOrigin, maps:get(origin, IdxEntry)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"origin.txt">>).

origin_hlc_stale_put_ignored(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    NewOrigin = barrel_hlc:new_hlc(),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"stale.txt">>,
                                      <<"second">>, #{origin_hlc => NewOrigin}),
    OldOrigin = barrel_hlc:min(),
    Result = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"stale.txt">>,
                                     <<"first">>, #{origin_hlc => OldOrigin}),
    ?assertEqual({ok, ignored}, Result),
    ?assertEqual({ok, <<"second">>},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"stale.txt">>)),
    FeedRef = maps:get(feed_ref, AttRef),
    {ok, IdxEntry} = barrel_att_feed:index_get(FeedRef, DbName, DocId, <<"stale.txt">>),
    ?assertEqual(NewOrigin, maps:get(origin, IdxEntry)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"stale.txt">>).

origin_hlc_stale_delete_ignored(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    NewOrigin = barrel_hlc:new_hlc(),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"del.txt">>,
                                      <<"data">>, #{origin_hlc => NewOrigin}),
    OldOrigin = barrel_hlc:min(),
    Result = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"del.txt">>,
                                        #{origin_hlc => OldOrigin}),
    ?assertEqual(ok, Result),
    ?assertEqual({ok, <<"data">>},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"del.txt">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"del.txt">>).

origin_hlc_stream_stale_ignored_single_put(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    NewOrigin = barrel_hlc:new_hlc(),
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
        <<"stream-stale.txt">>, <<"text/plain">>, #{origin_hlc => NewOrigin}),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, <<"second">>),
    {ok, _} = barrel_att_s3_store:finish_stream(S2),

    OldOrigin = barrel_hlc:min(),
    {ok, T1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
        <<"stream-stale.txt">>, <<"text/plain">>, #{origin_hlc => OldOrigin}),
    {ok, T2} = barrel_att_s3_store:write_chunk(T1, <<"first">>),
    Result = barrel_att_s3_store:finish_stream(T2),
    ?assertEqual({ok, ignored}, Result),
    ?assertEqual({ok, <<"second">>},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"stream-stale.txt">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"stream-stale.txt">>).

%% Also confirms the multipart metadata self-copy (Step 2's
%% attach_multipart_metadata/6) actually lands: get_info's digest wouldn't
%% be populated otherwise, since custom metadata can't be set at
%% create_multipart_upload time (the digest isn't known yet) and S3 has no
%% way to attach it at completion either.
origin_hlc_stream_stale_ignored_multipart(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    NewOrigin = barrel_hlc:new_hlc(),
    Part1 = binary:copy(<<"a">>, ?PART_SIZE),
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
        <<"stream-stale-mp.bin">>, <<"application/octet-stream">>,
        #{origin_hlc => NewOrigin}),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, Part1),
    ?assertNotEqual(undefined, maps:get(multipart, S2)),
    {ok, S3} = barrel_att_s3_store:write_chunk(S2, <<"tail">>),
    {ok, Info} = barrel_att_s3_store:finish_stream(S3),
    ?assertMatch(#{chunked := true}, Info),
    {ok, GotInfo} = barrel_att_s3_store:get_info(AttRef, DbName, DocId,
                                                 <<"stream-stale-mp.bin">>),
    ?assertNotEqual(undefined, maps:get(digest, GotInfo)),

    OldOrigin = barrel_hlc:min(),
    {ok, T1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
        <<"stream-stale-mp.bin">>, <<"application/octet-stream">>,
        #{origin_hlc => OldOrigin}),
    {ok, T2} = barrel_att_s3_store:write_chunk(T1, binary:copy(<<"b">>, ?PART_SIZE)),
    Result = barrel_att_s3_store:finish_stream(T2),
    ?assertEqual({ok, ignored}, Result),
    Expected = <<Part1/binary, "tail">>,
    ?assertEqual({ok, Expected},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"stream-stale-mp.bin">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"stream-stale-mp.bin">>).

%% barrel_att_feed itself (key encoding, LWW tie-breaking, pagination
%% internals, sweep mechanics) has its own dedicated suite
%% (barrel_att_feed_SUITE, in barrel_docdb). These just confirm the S3
%% backend's delegation wiring -- feed_ref/DbName threaded through
%% correctly -- against real puts/deletes, not barrel_att_feed's own logic.
att_changes_reflects_puts_and_deletes(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.txt">>, <<"aa">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"b.txt">>, <<"bb">>),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"a.txt">>),

    {ok, Entries, _LastSeq} = barrel_att_s3_store:att_changes(AttRef, DbName, first, #{}),
    ByName = [{maps:get(name, E), maps:get(op, E)} || E <- Entries],
    ?assert(lists:member({<<"a.txt">>, delete}, ByName)),
    ?assert(lists:member({<<"b.txt">>, put}, ByName)),

    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"b.txt">>).

att_changes_pagination_and_since(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.txt">>, <<"aa">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"b.txt">>, <<"bb">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"c.txt">>, <<"cc">>),

    {ok, Batch1, LastSeq1} = barrel_att_s3_store:att_changes(AttRef, DbName, first,
                                                             #{limit => 2}),
    ?assertEqual(2, length(Batch1)),
    {ok, Batch2, _LastSeq2} = barrel_att_s3_store:att_changes(AttRef, DbName, LastSeq1, #{}),
    ?assertEqual(1, length(Batch2)),

    Names1 = [maps:get(name, E) || E <- Batch1],
    Names2 = [maps:get(name, E) || E <- Batch2],
    ?assertEqual([], Names1 -- (Names1 -- Names2)),  %% no overlap between batches
    ?assertEqual([<<"a.txt">>, <<"b.txt">>, <<"c.txt">>], lists:sort(Names1 ++ Names2)),

    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"a.txt">>),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"b.txt">>),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"c.txt">>).

att_floor_and_sweep(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    ?assertEqual(undefined, barrel_att_s3_store:att_floor(AttRef, DbName)),

    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.txt">>, <<"aa">>),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"a.txt">>),

    Cutoff = barrel_hlc:new_hlc(),
    {ok, #{tombstones_swept := Swept}} =
        barrel_att_s3_store:sweep_att_feed(AttRef, DbName, Cutoff),
    ?assertEqual(1, Swept),
    ?assertEqual(Cutoff, barrel_att_s3_store:att_floor(AttRef, DbName)),

    %% put rows are never swept, only delete tombstones -- the a.txt put
    %% was already replaced by its own delete's tombstone (one feed row
    %% per (doc, attname), moved on every write), so sweeping that
    %% tombstone leaves nothing in the feed at all.
    {ok, Entries, _} = barrel_att_s3_store:att_changes(AttRef, DbName, first, #{}),
    ?assertEqual([], Entries).

rebuild_feed_on_empty_store_returns_zero_rows(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual({ok, #{rows => 0}}, barrel_att_s3_store:rebuild_feed(AttRef, DbName)).

%% Opens its own dedicated path (same DbName as the per-testcase config,
%% but a fresh directory -- avoids double-closing the shared att_ref from
%% init_per_testcase, same reasoning as prefix_persists_across_reopen).
%% Puts two attachments, deletes one, then wipes ONLY feed.db (not the
%% s3_prefix marker) to simulate a lost/corrupted local feed while leaving
%% the store's actual identity and S3 objects untouched. rebuild_feed/2
%% must recover the surviving attachment -- with its REAL origin, read
%% back from the object's own ?META_ORIGIN, not the barrel_hlc:min()
%% fallback -- and must not resurrect the deleted one (its object is gone,
%% so there is nothing left to rebuild a row from).
rebuild_feed_recovers_lost_feed(Config) ->
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config) ++ "-rebuild",
    S3Opts = ?config(s3_opts, Config),
    OpenOpts = #{s3 => S3Opts#{part_size => ?PART_SIZE}, db_name => DbName},
    {ok, AttRef} = barrel_att_s3_store:open(Path, OpenOpts),
    DocId = <<"doc1">>,
    KeepOrigin = barrel_hlc:new_hlc(),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"keep.txt">>,
                                      <<"keep-me">>, #{origin_hlc => KeepOrigin}),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"gone.txt">>,
                                      <<"gone">>, #{origin_hlc => barrel_hlc:new_hlc()}),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"gone.txt">>),
    ok = barrel_att_s3_store:close(AttRef),

    ok = file:del_dir_r(filename:join(Path, "feed.db")),
    {ok, Reopened} = barrel_att_s3_store:open(Path, OpenOpts),
    ?assertEqual(undefined, barrel_att_s3_store:att_floor(Reopened, DbName)),
    ?assertEqual({ok, [], first},
                 barrel_att_s3_store:att_changes(Reopened, DbName, first, #{})),

    ?assertEqual({ok, #{rows => 1}},
                 barrel_att_s3_store:rebuild_feed(Reopened, DbName)),

    {ok, Entries, _} = barrel_att_s3_store:att_changes(Reopened, DbName, first, #{}),
    ?assertMatch([#{name := <<"keep.txt">>, op := put, length := 7}], Entries),
    FeedRef = maps:get(feed_ref, Reopened),
    {ok, IdxEntry} = barrel_att_feed:index_get(FeedRef, DbName, DocId, <<"keep.txt">>),
    ?assertEqual(KeepOrigin, maps:get(origin, IdxEntry)),
    ?assertEqual(not_found,
                 barrel_att_feed:index_get(FeedRef, DbName, DocId, <<"gone.txt">>)),

    %% Safe to re-run: rebuilding again over the same bucket state doesn't
    %% duplicate or otherwise disturb the recovered row.
    ?assertEqual({ok, #{rows => 1}},
                 barrel_att_s3_store:rebuild_feed(Reopened, DbName)),
    {ok, Entries2, _} = barrel_att_s3_store:att_changes(Reopened, DbName, first, #{}),
    ?assertEqual(1, length(Entries2)),

    ok = barrel_att_s3_store:delete(Reopened, DbName, DocId, <<"keep.txt">>),
    ok = barrel_att_s3_store:close(Reopened).

%% Writes an object directly via livery_s3 (bypassing the store API
%% entirely), with a digest but no ?META_ORIGIN -- standing in for a
%% pre-M2 object that predates that metadata key. rebuild_feed/2 must
%% still recover a row for it, falling back to barrel_hlc:min() as its
%% origin (so any real write, local or remote, wins the LWW race against
%% it), the same escape hatch blob's own rebuild_feed/2 always uses.
rebuild_feed_missing_origin_falls_back_to_min(Config) ->
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config) ++ "-rebuild-legacy",
    S3Opts = ?config(s3_opts, Config),
    OpenOpts = #{s3 => S3Opts#{part_size => ?PART_SIZE}, db_name => DbName},
    {ok, AttRef} = barrel_att_s3_store:open(Path, OpenOpts),
    #{client := Client, bucket := Bucket, prefix := Prefix} = AttRef,
    DocId = <<"doc1">>,
    AttName = <<"legacy.bin">>,
    Key = <<Prefix/binary, "/", (binary:encode_hex(DocId, lowercase))/binary, "/",
            AttName/binary>>,
    Data = <<"pre-M2 object, no origin metadata">>,
    Digest = <<"sha256-", (binary:encode_hex(crypto:hash(sha256, Data), lowercase))/binary>>,
    {ok, _} = livery_s3:put_object(Client, Bucket, Key, Data,
                                   #{content_type => <<"application/octet-stream">>,
                                     metadata => #{<<"digest">> => Digest}}),

    ?assertEqual({ok, #{rows => 1}}, barrel_att_s3_store:rebuild_feed(AttRef, DbName)),
    FeedRef = maps:get(feed_ref, AttRef),
    {ok, IdxEntry} = barrel_att_feed:index_get(FeedRef, DbName, DocId, AttName),
    ?assertEqual(barrel_hlc:min(), maps:get(origin, IdxEntry)),
    ?assertEqual(Digest, maps:get(digest, IdxEntry)),

    ok = livery_s3:delete_object(Client, Bucket, Key),
    ok = barrel_att_s3_store:close(AttRef).

%%====================================================================
%% checkpoint/2: non-blocking eager-copy branching
%%====================================================================
%%
%% checkpoint/2 is called directly here (not through barrel_docdb:
%% branch_db/3 / barrel_timeline:fork/6), mirroring how barrel_db_server's
%% checkpoint_to/3 actually calls it: on the SOURCE's already-open att_ref,
%% targeting the branch's brand-new (not yet existing) directory.

%% Deterministic (no timing dependency): checkpoint/2 itself only does the
%% cheap local part -- it never spawns the copy sweep (see the moduledoc's
%% "Branching" section), so as long as this test never calls open/2 on
%% BranchPath, NOTHING copies any bytes, no matter how long the test takes
%% to get around to checking. The branch att_ref here is therefore built by
%% hand from the checkpoint's own on-disk output (the persisted s3_prefix
%% marker, a raw rocksdb:open of the checkpointed feed.db) rather than via
%% open/2, specifically to avoid triggering the real sweep.
checkpoint_returns_fast_without_copying_bytes(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.txt">>, <<"alpha">>),

    BranchPath = Path ++ "-branch-fast",
    ?assertEqual(ok, barrel_att_s3_store:checkpoint(AttRef, BranchPath)),

    %% the new prefix is already persisted and differs from the parent's
    {ok, OldPrefixBin} = file:read_file(filename:join(Path, "s3_prefix")),
    {ok, NewPrefixBin} = file:read_file(filename:join(BranchPath, "s3_prefix")),
    ?assertNotEqual(OldPrefixBin, NewPrefixBin),

    %% the copied feed rows are keyed under the SOURCE's DbName (a straight
    %% byte-for-byte checkpoint) -- exactly matching a real branch, whose
    %% keyspace always resolves to its parent's name (barrel_keyspace), so
    %% every call against it, including on the branch side, uses DbName too
    %% (not some new name of the branch's own).
    {BranchAttRef, BranchFeedRef} = open_branch_att_ref_raw(AttRef, BranchPath),
    try
        %% the branch's local feed already knows about a.txt (a straight
        %% RocksDB checkpoint), but the bytes have not been copied -- get/4,
        %% get_info/4, and get_stream/4 all report the distinguishable pending
        %% error rather than not_found or blocking
        ?assertEqual({error, {att_sync_pending, {DocId, <<"a.txt">>}}},
                     barrel_att_s3_store:get(BranchAttRef, DbName, DocId, <<"a.txt">>)),
        ?assertEqual({error, {att_sync_pending, {DocId, <<"a.txt">>}}},
                     barrel_att_s3_store:get_info(BranchAttRef, DbName, DocId, <<"a.txt">>)),
        ?assertEqual({error, {att_sync_pending, {DocId, <<"a.txt">>}}},
                     barrel_att_s3_store:get_stream(BranchAttRef, DbName, DocId, <<"a.txt">>)),

        %% an attachment that genuinely never existed is still plain not_found,
        %% not pending -- the feed has no row for it at all
        ?assertEqual({error, not_found},
                     barrel_att_s3_store:get(BranchAttRef, DbName, DocId, <<"never.txt">>))
    after
        ok = rocksdb:close(BranchFeedRef)
    end,
    ok = file:del_dir_r(BranchPath),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"a.txt">>).

%% The real end-to-end path: checkpoint/2, then a real open/2 on the
%% branch (as barrel_timeline:fork/6 always does immediately afterward),
%% which spawns the copy sweep. Not a hard timing assertion -- polls with
%% a generous bound until every attachment converges, proving the sweep
%% actually finishes and the branch ends up byte-identical to the parent
%% for everything neither side touched after the fork.
checkpoint_open_sweeps_and_converges(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config),
    S3Opts = ?config(s3_opts, Config),
    DocId = <<"doc1">>,
    Names = [integer_to_binary(N) || N <- lists:seq(1, 8)],
    lists:foreach(
        fun(Name) ->
            {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, Name,
                                              <<"data-", Name/binary>>)
        end,
        Names),

    BranchPath = Path ++ "-branch-converge",
    ?assertEqual(ok, barrel_att_s3_store:checkpoint(AttRef, BranchPath)),

    %% db_name here only ever seeds a fresh prefix on a marker-less open --
    %% BranchPath already carries checkpoint/2's marker, so this value is
    %% unused for that purpose; every per-call DbName below is the SOURCE's
    %% own name, matching a real branch's keyspace (see barrel_keyspace).
    {ok, Branch} = barrel_att_s3_store:open(BranchPath,
        #{s3 => S3Opts#{part_size => ?PART_SIZE}, db_name => DbName}),

    ok = wait_until(
        fun() ->
            lists:all(
                fun(Name) ->
                    barrel_att_s3_store:get(Branch, DbName, DocId, Name) =:=
                        {ok, <<"data-", Name/binary>>}
                end,
                Names)
        end,
        10000),

    ok = barrel_att_s3_store:close(Branch),
    ok = file:del_dir_r(BranchPath),
    lists:foreach(
        fun(Name) -> ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, Name) end,
        Names).

%% A write on the branch to a key the sweep has not reached yet must
%% survive: the sweep's head_object-before-copy check is exactly what
%% prevents it from later overwriting this newer, independent write with
%% the stale parent copy.
checkpoint_branch_write_not_clobbered_by_sweep(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config),
    S3Opts = ?config(s3_opts, Config),
    DocId = <<"doc1">>,
    Names = [integer_to_binary(N) || N <- lists:seq(1, 8)],
    lists:foreach(
        fun(Name) ->
            {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, Name,
                                              <<"parent-", Name/binary>>)
        end,
        Names),

    BranchPath = Path ++ "-branch-write",
    ?assertEqual(ok, barrel_att_s3_store:checkpoint(AttRef, BranchPath)),

    {ok, Branch} = barrel_att_s3_store:open(BranchPath,
        #{s3 => S3Opts#{part_size => ?PART_SIZE}, db_name => DbName}),

    %% overwrite one attachment on the branch immediately -- races the
    %% sweep on purpose
    Target = lists:nth(1, Names),
    {ok, _} = barrel_att_s3_store:put(Branch, DbName, DocId, Target,
                                      <<"branch-owns-this">>),

    ok = wait_until(
        fun() ->
            lists:all(
                fun(Name) when Name =:= Target ->
                        barrel_att_s3_store:get(Branch, DbName, DocId, Name) =:=
                            {ok, <<"branch-owns-this">>};
                   (Name) ->
                        barrel_att_s3_store:get(Branch, DbName, DocId, Name) =:=
                            {ok, <<"parent-", Name/binary>>}
                end,
                Names)
        end,
        10000),

    %% and it is still the branch's value, not clobbered after convergence
    ?assertEqual({ok, <<"branch-owns-this">>},
                 barrel_att_s3_store:get(Branch, DbName, DocId, Target)),

    ok = barrel_att_s3_store:close(Branch),
    ok = file:del_dir_r(BranchPath),
    lists:foreach(
        fun(Name) -> ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, Name) end,
        Names).

%% A delete on the branch for a key the sweep has not reached yet must
%% also survive: the sweep consults the branch's own feed (not just a
%% bare existence check) before copying, so it does not resurrect
%% something the branch has already tombstoned.
checkpoint_branch_delete_not_resurrected_by_sweep(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config),
    S3Opts = ?config(s3_opts, Config),
    DocId = <<"doc1">>,
    Names = [integer_to_binary(N) || N <- lists:seq(1, 8)],
    lists:foreach(
        fun(Name) ->
            {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, Name,
                                              <<"parent-", Name/binary>>)
        end,
        Names),

    BranchPath = Path ++ "-branch-delete",
    ?assertEqual(ok, barrel_att_s3_store:checkpoint(AttRef, BranchPath)),

    {ok, Branch} = barrel_att_s3_store:open(BranchPath,
        #{s3 => S3Opts#{part_size => ?PART_SIZE}, db_name => DbName}),

    Target = lists:nth(1, Names),
    ok = barrel_att_s3_store:delete(Branch, DbName, DocId, Target),

    ok = wait_until(
        fun() ->
            lists:all(
                fun(Name) when Name =:= Target ->
                        barrel_att_s3_store:get(Branch, DbName, DocId, Name) =:=
                            {error, not_found};
                   (Name) ->
                        barrel_att_s3_store:get(Branch, DbName, DocId, Name) =:=
                            {ok, <<"parent-", Name/binary>>}
                end,
                Names)
        end,
        10000),

    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(Branch, DbName, DocId, Target)),

    ok = barrel_att_s3_store:close(Branch),
    ok = file:del_dir_r(BranchPath),
    lists:foreach(
        fun(Name) when Name =:= Target -> ok;
           (Name) -> ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, Name)
        end,
        Names).

%% Forking a source that is itself still mid-sync (its own fork_pending
%% marker still present) is refused, not silently allowed to produce an
%% incomplete copy -- v1 lineage never actually reaches this via
%% barrel_docdb:branch_db/3 (branching a branch is rejected earlier), but
%% checkpoint/2 enforces it directly regardless, as a safety net.
checkpoint_refuses_still_syncing_source(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.txt">>, <<"alpha">>),

    BranchPath = Path ++ "-branch-source",
    ?assertEqual(ok, barrel_att_s3_store:checkpoint(AttRef, BranchPath)),

    %% BranchPath now carries its own fork_pending marker (from the
    %% checkpoint above) -- built by hand so no sweep is spawned and the
    %% marker is still guaranteed present
    {BranchAttRef, BranchFeedRef} = open_branch_att_ref_raw(AttRef, BranchPath),
    try
        GrandBranchPath = Path ++ "-branch-source-grandchild",
        ?assertEqual({error, {fork_sync_pending, retry}},
                     barrel_att_s3_store:checkpoint(BranchAttRef, GrandBranchPath))
    after
        ok = rocksdb:close(BranchFeedRef)
    end,
    ok = file:del_dir_r(BranchPath),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"a.txt">>).

%%====================================================================
%% destroy/2: S3 object cleanup on delete_db
%%====================================================================

destroy_removes_all_objects_under_prefix(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    #{client := Client, bucket := Bucket, prefix := Prefix} = AttRef,
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.txt">>, <<"alpha">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"b.txt">>, <<"beta">>),

    ?assertEqual(ok, barrel_att_s3_store:destroy(AttRef, DbName)),

    {ok, #{objects := Objects}} =
        livery_s3:list_objects_all(Client, Bucket, #{prefix => <<Prefix/binary, "/">>}),
    ?assertEqual([], Objects).

%% Built by hand (same reasoning as the checkpoint tests): a real open/2
%% on BranchPath would spawn the copy sweep, which is not what this test
%% is about. Writes directly to the branch's own prefix first so destroy
%% has a real object to remove, not just an empty marker to clear.
destroy_clears_fork_pending_marker(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.txt">>, <<"alpha">>),

    BranchPath = Path ++ "-branch-destroy",
    ?assertEqual(ok, barrel_att_s3_store:checkpoint(AttRef, BranchPath)),
    ?assert(filelib:is_regular(filename:join(BranchPath, "fork_pending"))),

    {BranchAttRef, BranchFeedRef} = open_branch_att_ref_raw(AttRef, BranchPath),
    try
        {ok, _} = barrel_att_s3_store:put(BranchAttRef, DbName, DocId, <<"branch-own.txt">>,
                                          <<"own-data">>),

        ?assertEqual(ok, barrel_att_s3_store:destroy(BranchAttRef, DbName)),
        ?assertNot(filelib:is_regular(filename:join(BranchPath, "fork_pending"))),

        #{client := Client, bucket := Bucket, prefix := BranchPrefixBin} = BranchAttRef,
        {ok, #{objects := Objects}} =
            livery_s3:list_objects_all(Client, Bucket, #{prefix => <<BranchPrefixBin/binary, "/">>}),
        ?assertEqual([], Objects)
    after
        ok = rocksdb:close(BranchFeedRef)
    end,
    ok = file:del_dir_r(BranchPath),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"a.txt">>).

%% Options.resume_fork_sync => false (used by
%% barrel_docdb:maybe_destroy_closed_att_store/3 right before an
%% immediate destroy/2, so a sweep is never spawned just to be abandoned)
%% must not spawn the copy sweep even though a fork_pending marker is
%% present -- proven by: the marker is still there after open (a real
%% sweep would eventually clear it), and the inherited attachment is
%% still sync_pending (a real sweep would eventually copy it).
resume_fork_sync_false_does_not_spawn_sweep(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config),
    S3Opts = ?config(s3_opts, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.txt">>, <<"alpha">>),

    BranchPath = Path ++ "-branch-no-resume",
    ?assertEqual(ok, barrel_att_s3_store:checkpoint(AttRef, BranchPath)),
    ?assert(filelib:is_regular(filename:join(BranchPath, "fork_pending"))),

    {ok, Branch} = barrel_att_s3_store:open(BranchPath,
        #{s3 => S3Opts#{part_size => ?PART_SIZE}, db_name => DbName,
          resume_fork_sync => false}),

    %% a real sweep is idempotent but not instant -- give one a bounded
    %% moment to have done SOMETHING if it had wrongly been spawned, then
    %% confirm nothing changed
    timer:sleep(500),
    ?assert(filelib:is_regular(filename:join(BranchPath, "fork_pending"))),
    ?assertEqual({error, {att_sync_pending, {DocId, <<"a.txt">>}}},
                 barrel_att_s3_store:get(Branch, DbName, DocId, <<"a.txt">>)),

    ok = barrel_att_s3_store:close(Branch),
    ok = file:del_dir_r(BranchPath),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"a.txt">>).

put_get_roundtrip(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Data = <<"hello world">>,
    {ok, Info} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"note.txt">>, Data),
    ?assertMatch(#{name := <<"note.txt">>, length := 11, chunked := false}, Info),
    ?assertEqual({ok, Data},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"note.txt">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"note.txt">>).

get_returns_not_found_for_missing(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, <<"doc1">>, <<"nope.txt">>)).

get_info_returns_metadata(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Data = <<"some bytes">>,
    {ok, #{digest := Digest}} =
        barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a.bin">>, Data),
    {ok, InfoResult} = barrel_att_s3_store:get_info(AttRef, DbName, DocId, <<"a.bin">>),
    ?assertMatch(#{name := <<"a.bin">>, length := 10, digest := Digest},
                 InfoResult),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"a.bin">>).

get_info_not_found(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get_info(AttRef, DbName, <<"doc1">>, <<"nope">>)).

%% barrel_att_store_blob's get_info recomputes content-type from the
%% filename for small attachments, ignoring any override the caller gave
%% put/6 -- this backend persists it as a real S3 header, so an override
%% survives the round trip. Documented divergence, not a bug.
content_type_override_persists(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"data.bin">>,
                                      <<"x">>, #{content_type => <<"application/custom">>}),
    {ok, #{content_type := <<"application/custom">>}} =
        barrel_att_s3_store:get_info(AttRef, DbName, DocId, <<"data.bin">>),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"data.bin">>).

delete_removes_attachment(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v">>),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)).

delete_missing_is_ok(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual(ok, barrel_att_s3_store:delete(AttRef, DbName, <<"doc1">>, <<"nope">>)).

fold_lists_attachment_names(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a">>, <<"1">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"b">>, <<"2">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"c">>, <<"3">>),
    Names = barrel_att_s3_store:fold(AttRef, DbName, DocId,
                                     fun(Name, _Data, Acc) -> {ok, [Name | Acc]} end, []),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], lists:sort(Names)),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, DocId).

fold_stop_early(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a">>, <<"1">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"b">>, <<"2">>),
    Result = barrel_att_s3_store:fold(AttRef, DbName, DocId,
                                      fun(Name, _Data, Acc) -> {stop, [Name | Acc]} end, []),
    ?assertEqual(1, length(Result)),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, DocId).

delete_all_removes_every_attachment_for_doc(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"a">>, <<"1">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"b">>, <<"2">>),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, DocId),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"a">>)),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"b">>)).

%% Two different DocIds sharing an AttName must not collide -- proves the
%% hex-encoded-DocId key scheme actually isolates documents.
delete_all_does_not_affect_other_docs(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, <<"docA">>, <<"f">>, <<"a">>),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, <<"docB">>, <<"f">>, <<"b">>),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, <<"docA">>),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, <<"docA">>, <<"f">>)),
    ?assertEqual({ok, <<"b">>},
                 barrel_att_s3_store:get(AttRef, DbName, <<"docB">>, <<"f">>)),
    ok = barrel_att_s3_store:delete_all(AttRef, DbName, <<"docB">>).

expected_digest_match_succeeds(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Data = <<"payload">>,
    Digest = <<"sha256-", (binary:encode_hex(crypto:hash(sha256, Data), lowercase))/binary>>,
    ?assertMatch({ok, _},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, Data,
                                        #{expected_digest => Digest})),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

expected_digest_mismatch_rejected(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    ?assertEqual({error, digest_mismatch},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"payload">>,
                                        #{expected_digest => <<"sha256-wrong">>})),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)).

key_too_long_rejected(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    HugeDocId = binary:copy(<<"a">>, 2048),
    ?assertEqual({error, key_too_long},
                 barrel_att_s3_store:put(AttRef, DbName, HugeDocId, <<"f">>, <<"v">>)),
    ?assertEqual({error, key_too_long},
                 barrel_att_s3_store:get(AttRef, DbName, HugeDocId, <<"f">>)).

%%====================================================================
%% Streaming / multipart test cases
%%====================================================================

stream_small_single_put(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
                                              <<"small.txt">>, <<"text/plain">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, <<"hello">>),
    {ok, Info} = barrel_att_s3_store:finish_stream(S2),
    ?assertMatch(#{chunked := false, length := 5}, Info),
    ?assertEqual({ok, <<"hello">>},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"small.txt">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"small.txt">>).

stream_large_multipart(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Part1 = binary:copy(<<"a">>, ?PART_SIZE),
    Part2 = binary:copy(<<"b">>, 100),
    Expected = <<Part1/binary, Part2/binary>>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
                                              <<"large.bin">>, <<"application/octet-stream">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, Part1),
    ?assertNotEqual(undefined, maps:get(multipart, S2)),
    {ok, S3} = barrel_att_s3_store:write_chunk(S2, Part2),
    {ok, Info} = barrel_att_s3_store:finish_stream(S3),
    ?assertMatch(#{chunked := true, length := 5242980}, Info),
    ?assertEqual({ok, Expected},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"large.bin">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"large.bin">>).

stream_abort_mid_multipart(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId,
                                              <<"aborted.bin">>, <<"application/octet-stream">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, binary:copy(<<"x">>, ?PART_SIZE)),
    ?assertNotEqual(undefined, maps:get(multipart, S2)),
    ?assertEqual(ok, barrel_att_s3_store:abort_stream(S2)),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"aborted.bin">>)).

stream_digest_mismatch_small(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"bad.txt">>,
                                              <<"text/plain">>,
                                              #{expected_digest => <<"sha256-wrong">>}),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, <<"hello">>),
    ?assertEqual({error, digest_mismatch}, barrel_att_s3_store:finish_stream(S2)),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"bad.txt">>)).

%% A mismatch discovered only at finish time (the digest can't be known
%% until every part is in) must abort the whole multipart upload, not just
%% report an error -- confirmed here by asserting no in-progress upload is
%% left behind for anyone to accidentally complete later.
stream_digest_mismatch_multipart_aborts_upload(Config) ->
    AttRef = #{client := Client, bucket := Bucket} = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"bad2.bin">>,
                                              <<"application/octet-stream">>,
                                              #{expected_digest => <<"sha256-wrong">>}),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, binary:copy(<<"z">>, ?PART_SIZE)),
    {ok, S3} = barrel_att_s3_store:write_chunk(S2, binary:copy(<<"z">>, 100)),
    ?assertEqual({error, digest_mismatch}, barrel_att_s3_store:finish_stream(S3)),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"bad2.bin">>)),
    {ok, #{uploads := Uploads}} = livery_s3:list_multipart_uploads(Client, Bucket),
    ?assertNot(lists:any(fun(#{key := K}) -> K =:= maps:get(key, S3) end, Uploads)).

stream_read_roundtrip(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Part1 = binary:copy(<<"q">>, ?PART_SIZE),
    Expected = <<Part1/binary, "tail">>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"stream.bin">>,
                                              <<"application/octet-stream">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, Part1),
    {ok, S3} = barrel_att_s3_store:write_chunk(S2, <<"tail">>),
    {ok, _} = barrel_att_s3_store:finish_stream(S3),
    {ok, R1} = barrel_att_s3_store:get_stream(AttRef, DbName, DocId, <<"stream.bin">>),
    Got = drain(R1, []),
    ?assertEqual(Expected, Got),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"stream.bin">>).

stream_read_not_found(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    ?assertEqual({error, not_found},
                 barrel_att_s3_store:get_stream(AttRef, DbName, <<"doc1">>, <<"nope">>)).

%% Garage-only: unlike AWS/MinIO's strict 5 MiB minimum for a non-final
%% part, Garage accepts a much smaller one (confirmed empirically at 1 MiB
%% this session) -- exercises that the part_size config knob genuinely
%% changes upload behavior on a store lenient enough to allow it, rather
%% than a knob that's accepted but ignored.
stream_small_part_size_accepted_by_garage(Config) ->
    S3Opts = ?config(s3_opts, Config),
    SmallPartSize = 1024 * 1024,
    DbName = ?config(db_name, Config),
    Path = ?config(path, Config) ++ "-small-part",
    {ok, AttRef} = barrel_att_s3_store:open(Path,
                                            #{s3 => S3Opts#{part_size => SmallPartSize},
                                              db_name => DbName}),
    DocId = <<"doc1">>,
    Part1 = binary:copy(<<"a">>, SmallPartSize),
    Part2 = binary:copy(<<"b">>, 50),
    Expected = <<Part1/binary, Part2/binary>>,
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"garage-small.bin">>,
                                              <<"application/octet-stream">>),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, Part1),
    ?assertNotEqual(undefined, maps:get(multipart, S2)),
    {ok, S3} = barrel_att_s3_store:write_chunk(S2, Part2),
    {ok, Info} = barrel_att_s3_store:finish_stream(S3),
    ?assertMatch(#{chunked := true}, Info),
    ?assertEqual({ok, Expected},
                 barrel_att_s3_store:get(AttRef, DbName, DocId, <<"garage-small.bin">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"garage-small.bin">>),
    ok = barrel_att_s3_store:close(AttRef).

%%====================================================================
%% Streaming helpers
%%====================================================================

drain(Stream, Acc) ->
    case barrel_att_s3_store:read_chunk(Stream) of
        {ok, Data, Next} -> drain(Next, [Data | Acc]);
        eof -> iolist_to_binary(lists:reverse(Acc))
    end.

%%====================================================================
%% Write-conflict detection test cases
%%====================================================================

conditional_writes_capability_reflects_store(Config) ->
    AttRef = ?config(att_ref, Config),
    Expected = case ?config(store, Config) of
        minio -> supported;
        garage -> unsupported
    end,
    ?assertEqual(Expected, maps:get(conditional_writes, AttRef)).

default_put_stays_unconditional(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    ?assertMatch({ok, _}, barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>)),
    ?assertEqual({ok, <<"v2">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

create_only_succeeds_on_fresh_key(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    ?assertMatch({ok, _},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>,
                                        #{create_only => true})),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

create_only_conflicts_on_existing_key(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>,
                                      #{create_only => true}),
    Result = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>,
                                     #{create_only => true}),
    ?assertMatch({error, {conflict, #{name := <<"f">>, length := 2}}}, Result),
    %% the rejected write never landed
    ?assertEqual({ok, <<"v1">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

expected_etag_match_succeeds(Config) ->
    #{client := Client, bucket := Bucket} = AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    Key = s3_key(DbName, DocId, <<"f">>),
    {ok, #{etag := Etag}} = livery_s3:head_object(Client, Bucket, Key),
    ?assertMatch({ok, _},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>,
                                        #{expected_etag => Etag})),
    ?assertEqual({ok, <<"v2">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

expected_etag_stale_returns_conflict(Config) ->
    #{client := Client, bucket := Bucket} = AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    Key = s3_key(DbName, DocId, <<"f">>),
    {ok, #{etag := StaleEtag}} = livery_s3:head_object(Client, Bucket, Key),
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>),
    Result = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v3">>,
                                     #{expected_etag => StaleEtag}),
    ?assertMatch({error, {conflict, #{name := <<"f">>}}}, Result),
    ?assertEqual({ok, <<"v2">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

stream_create_only_conflicts_small_path(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    {ok, S1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"f">>,
                                              <<"text/plain">>, #{create_only => true}),
    {ok, S2} = barrel_att_s3_store:write_chunk(S1, <<"v2">>),
    Result = barrel_att_s3_store:finish_stream(S2),
    ?assertMatch({error, {conflict, #{name := <<"f">>}}}, Result),
    ?assertEqual({ok, <<"v1">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

%% create_only applies at completion, not create_multipart_upload (see the
%% barrel_att_s3_store module doc): a doc already carrying "big.bin" makes
%% the *second* stream's completion lose an If-None-Match race, not its
%% first write_chunk -- unlike the single-put path, the multipart upload
%% itself succeeds right up to completion before the conflict surfaces.
stream_create_only_conflicts_multipart_path(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    Part1 = binary:copy(<<"a">>, ?PART_SIZE),
    {ok, First1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"big.bin">>,
                                                  <<"application/octet-stream">>,
                                                  #{create_only => true}),
    {ok, First2} = barrel_att_s3_store:write_chunk(First1, Part1),
    {ok, _} = barrel_att_s3_store:finish_stream(First2),

    {ok, Second1} = barrel_att_s3_store:put_stream(AttRef, DbName, DocId, <<"big.bin">>,
                                                   <<"application/octet-stream">>,
                                                   #{create_only => true}),
    {ok, Second2} = barrel_att_s3_store:write_chunk(Second1, Part1),
    Result = barrel_att_s3_store:finish_stream(Second2),
    ?assertMatch({error, {conflict, #{name := <<"big.bin">>}}}, Result),
    ?assertEqual({ok, Part1}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"big.bin">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"big.bin">>).

garage_create_only_fails_fast(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    ?assertEqual({error, conditional_writes_unsupported},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>,
                                        #{create_only => true})),
    ?assertEqual({error, not_found}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)).

garage_expected_etag_fails_fast(Config) ->
    AttRef = ?config(att_ref, Config),
    DbName = ?config(db_name, Config),
    DocId = <<"doc1">>,
    {ok, _} = barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v1">>),
    ?assertEqual({error, conditional_writes_unsupported},
                 barrel_att_s3_store:put(AttRef, DbName, DocId, <<"f">>, <<"v2">>,
                                        #{expected_etag => <<"whatever">>})),
    ?assertEqual({ok, <<"v1">>}, barrel_att_s3_store:get(AttRef, DbName, DocId, <<"f">>)),
    ok = barrel_att_s3_store:delete(AttRef, DbName, DocId, <<"f">>).

s3_key(DbName, DocId, AttName) ->
    <<DbName/binary, "/", (binary:encode_hex(DocId, lowercase))/binary, "/", AttName/binary>>.
