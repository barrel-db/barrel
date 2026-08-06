%%%-------------------------------------------------------------------
%%% @doc Attachment replication: the second phase of a replication run
%%% ships blobs content-addressed with LWW convergence, independent
%%% checkpoints and floor-guarded resync.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_att_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([basic_sync_and_digest_skip/1,
         update_propagates/1,
         delete_propagates/1,
         bidirectional_lww_convergence/1,
         checkpoint_resume/1,
         off_switch/1,
         mixed_docs_and_attachments/1,
         floor_forces_resync/1,
         source_lacks_feed_reports_skipped/1,
         target_lacks_feed_puts_land_without_lww/1,
         s3_target_receives_puts_and_deletes/1,
         s3_target_rejects_stale_replicated_write_minio/1,
         s3_target_rejects_stale_replicated_write_garage/1,
         bidirectional_lww_convergence_rocksdb_and_s3/1,
         bidirectional_lww_convergence_s3_to_s3/1]).

all() ->
    [basic_sync_and_digest_skip,
     update_propagates,
     delete_propagates,
     bidirectional_lww_convergence,
     checkpoint_resume,
     off_switch,
     mixed_docs_and_attachments,
     floor_forces_resync,
     source_lacks_feed_reports_skipped,
     target_lacks_feed_puts_land_without_lww,
     s3_target_receives_puts_and_deletes,
     s3_target_rejects_stale_replicated_write_minio,
     s3_target_rejects_stale_replicated_write_garage,
     bidirectional_lww_convergence_rocksdb_and_s3,
     bidirectional_lww_convergence_s3_to_s3].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_rep_att_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Src = <<(atom_to_binary(TC, utf8))/binary, "_src">>,
    Tgt = <<(atom_to_binary(TC, utf8))/binary, "_tgt">>,
    Dir = ?config(dir, Config),
    {ok, _} = barrel_docdb:create_db(Src, #{data_dir => Dir}),
    {ok, _} = barrel_docdb:create_db(Tgt, #{data_dir => Dir}),
    [{src, Src}, {tgt, Tgt} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(src, Config)) catch _:_ -> ok end,
    try barrel_docdb:delete_db(?config(tgt, Config)) catch _:_ -> ok end,
    ok.

att_stats(Result) ->
    maps:get(att_sync, Result).

%%====================================================================
%% Cases
%%====================================================================

basic_sync_and_digest_skip(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d1">>, <<"a.txt">>,
                                          <<"alpha">>),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d2">>, <<"b.bin">>,
                                          binary:copy(<<"z">>, 200000)),
    {ok, R1} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 2, atts_skipped := 0}, att_stats(R1)),
    {ok, <<"alpha">>} = barrel_docdb:get_attachment(Tgt, <<"d1">>,
                                                    <<"a.txt">>),
    {ok, Big} = barrel_docdb:get_attachment(Tgt, <<"d2">>, <<"b.bin">>),
    ?assertEqual(200000, byte_size(Big)),
    %% content types survive the trip
    {ok, SrcInfo} = barrel_docdb:get_attachment_info(Src, <<"d1">>,
                                                     <<"a.txt">>),
    {ok, TgtInfo} = barrel_docdb:get_attachment_info(Tgt, <<"d1">>,
                                                     <<"a.txt">>),
    ?assertEqual(maps:get(content_type, SrcInfo),
                 maps:get(content_type, TgtInfo)),
    ?assertEqual(maps:get(digest, SrcInfo), maps:get(digest, TgtInfo)),
    %% second run: nothing to do (checkpointed, zero re-transfer)
    {ok, R2} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 0}, att_stats(R2)).

update_propagates(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"v1">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"v2">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_written := 1}, att_stats(R)),
    {ok, <<"v2">>} = barrel_docdb:get_attachment(Tgt, <<"d">>, <<"f">>).

delete_propagates(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"v1">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    ok = barrel_docdb:delete_attachment(Src, <<"d">>, <<"f">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{atts_deleted := 1}, att_stats(R)),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Tgt, <<"d">>, <<"f">>)),
    %% redelivery of the whole feed is harmless
    {ok, _} = barrel_rep:replicate(Src, Tgt).

bidirectional_lww_convergence(Config) ->
    A = ?config(src, Config),
    B = ?config(tgt, Config),
    %% both sides write the same attachment concurrently
    {ok, _} = barrel_docdb:put_attachment(A, <<"d">>, <<"f">>, <<"from a">>),
    {ok, _} = barrel_docdb:put_attachment(B, <<"d">>, <<"f">>, <<"from b">>),
    %% sync both ways twice: no oscillation, both converge to one value
    {ok, _} = barrel_rep:replicate(A, B),
    {ok, _} = barrel_rep:replicate(B, A),
    {ok, _} = barrel_rep:replicate(A, B),
    {ok, _} = barrel_rep:replicate(B, A),
    {ok, VA} = barrel_docdb:get_attachment(A, <<"d">>, <<"f">>),
    {ok, VB} = barrel_docdb:get_attachment(B, <<"d">>, <<"f">>),
    ?assertEqual(VA, VB),
    ?assert(lists:member(VA, [<<"from a">>, <<"from b">>])),
    %% and a further round moves nothing
    {ok, R} = barrel_rep:replicate(A, B),
    ?assertMatch(#{atts_written := 0, atts_ignored := 0}, att_stats(R)).

checkpoint_resume(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d1">>, <<"a">>, <<"1">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d2">>, <<"b">>, <<"2">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    %% only the delta ships; the first attachment is not even offered
    ?assertMatch(#{atts_written := 1, atts_skipped := 0}, att_stats(R)).

off_switch(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"v">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt, #{attachments => false}),
    ?assertEqual(disabled, att_stats(R)),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Tgt, <<"d">>, <<"f">>)).

mixed_docs_and_attachments(Config) ->
    Src = ?config(src, Config),
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:put_doc(Src, #{<<"id">> => <<"d">>,
                                          <<"kind">> => <<"report">>}),
    {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"body.pdf">>,
                                          <<"pdf bytes">>),
    {ok, R} = barrel_rep:replicate(Src, Tgt),
    ?assertMatch(#{docs_written := 1}, R),
    ?assertMatch(#{atts_written := 1}, att_stats(R)),
    {ok, _} = barrel_docdb:get_doc(Tgt, <<"d">>),
    {ok, <<"pdf bytes">>} =
        barrel_docdb:get_attachment(Tgt, <<"d">>, <<"body.pdf">>).

floor_forces_resync(Config) ->
    Dir = ?config(dir, Config),
    Src = <<"floor_src2">>,
    Tgt = ?config(tgt, Config),
    {ok, _} = barrel_docdb:create_db(Src, #{data_dir => Dir,
                                            retention_period => 1}),
    try
        {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"keep">>,
                                              <<"kept">>),
        {ok, _} = barrel_rep:replicate(Src, Tgt),
        %% a delete swept past the window moves the floor above the
        %% target's checkpoint
        {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"gone">>,
                                              <<"bye">>),
        ok = barrel_docdb:delete_attachment(Src, <<"d">>, <<"gone">>),
        timer:sleep(1200),
        {ok, _} = barrel_docdb:sweep_retention(Src),
        ?assertNotEqual(undefined, barrel_docdb:att_floor(Src)),
        %% replication restarts from first and still converges
        {ok, R} = barrel_rep:replicate(Src, Tgt),
        ?assertMatch(#{atts_written := 0}, att_stats(R)),
        {ok, <<"kept">>} = barrel_docdb:get_attachment(Tgt, <<"d">>,
                                                       <<"keep">>)
    after
        _ = barrel_docdb:delete_db(Src)
    end.

%%====================================================================
%% Replication asymmetry: barrel_rep_att's "skipped" degrade only checks
%% the SOURCE's feed support (barrel_rep_att:supports/1 gates on the
%% transport, not the backend; the actual skip comes from the source's
%% att_changes/4 call failing at runtime) -- nothing checks the target.
%% Uses its own src/tgt pair (not the suite's shared per-testcase ones)
%% since these need a non-default att_opts.backend.
%%====================================================================

source_lacks_feed_reports_skipped(Config) ->
    Dir = ?config(dir, Config),
    Src = <<"asym_nofeed_src">>,
    Tgt = <<"asym_nofeed_tgt">>,
    {ok, _} = barrel_docdb:create_db(Src, #{
        data_dir => Dir,
        att_opts => #{backend => barrel_docdb_test_att_backend_minimal}
    }),
    {ok, _} = barrel_docdb:create_db(Tgt, #{data_dir => Dir}),
    try
        {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"v">>),
        {ok, R} = barrel_rep:replicate(Src, Tgt),
        ?assertEqual(skipped, att_stats(R)),
        %% the attachment never replicates: with no feed to enumerate
        %% changes from, the sync phase has nothing to walk, even though
        %% put/get on Src itself works fine
        ?assertEqual({error, not_found},
                     barrel_docdb:get_attachment(Tgt, <<"d">>, <<"f">>))
    after
        _ = barrel_docdb:delete_db(Src),
        _ = barrel_docdb:delete_db(Tgt)
    end.

%% Puts/deletes are required callbacks, not feed-gated, so replicating
%% INTO a feedless target still moves bytes -- but with no feed there to
%% check origin_hlc against, an older value from the source can clobber
%% a newer one already on the target, unlike RocksDB<->RocksDB (where the
%% target's own feed would reject the stale write via barrel_att_feed:check/6).
target_lacks_feed_puts_land_without_lww(Config) ->
    Dir = ?config(dir, Config),
    Src = <<"asym_target_nofeed_src">>,
    Tgt = <<"asym_target_nofeed_tgt">>,
    {ok, _} = barrel_docdb:create_db(Src, #{data_dir => Dir}),
    {ok, _} = barrel_docdb:create_db(Tgt, #{
        data_dir => Dir,
        att_opts => #{backend => barrel_docdb_test_att_backend_minimal}
    }),
    try
        %% the target's own, more recent state
        {ok, _} = barrel_docdb:put_attachment(Tgt, <<"d">>, <<"f">>, <<"new">>),
        %% the source has an older value for the same attachment
        {ok, _} = barrel_docdb:put_attachment(Src, <<"d">>, <<"f">>, <<"old">>),
        {ok, R} = barrel_rep:replicate(Src, Tgt),
        ?assertMatch(#{atts_written := 1}, att_stats(R)),
        ?assertEqual({ok, <<"old">>},
                     barrel_docdb:get_attachment(Tgt, <<"d">>, <<"f">>))
    after
        _ = barrel_docdb:delete_db(Src),
        _ = barrel_docdb:delete_db(Tgt)
    end.

%%====================================================================
%% M2: barrel_att_s3 as a replication TARGET. barrel_att_s3_store's own
%% suite (barrel_att_s3_SUITE) already unit-tests the LWW guard directly
%% against the backend (origin_hlc_stale_put_ignored and friends); these
%% prove the same guard is correctly wired end-to-end through
%% barrel_rep_att/barrel_rep_transport_local when the target itself is
%% S3-backed -- retiring the M1 "S3 should only be a replication source"
%% limitation (barrel_att_s3 had no feed at all, so a target using it
%% could never enforce LWW; see target_lacks_feed_puts_land_without_lww
%% above for the general feedless case this used to fall into).
%%
%% barrel_docdb already references barrel_att_s3_store by bare atom (see
%% barrel_att_store:backend_module/1) without a hard app dependency --
%% is_available/1 probes for it at runtime -- so these tests need no
%% special build wiring beyond running under a build that also has
%% barrel_att_s3 (and so livery_s3) on the code path, i.e. `rebar3 as s3
%% ct'. Skip cleanly (not fail) when that isn't the case, or when the
%% real MinIO/Garage fixture isn't reachable -- same reasoning as
%% barrel_att_s3_SUITE's own group skips.
%%====================================================================

s3_target_receives_puts_and_deletes(Config) ->
    with_minio(fun(S3Opts) ->
        run_s3_target_receives_puts_and_deletes(Config, S3Opts)
    end).

run_s3_target_receives_puts_and_deletes(Config, S3Opts) ->
    Dir = ?config(dir, Config),
    Src = <<"s3_basic_src">>,
    Tgt = <<"s3_basic_tgt_minio">>,
    {ok, _} = barrel_docdb:create_db(Src, #{data_dir => Dir}),
    {ok, _} = barrel_docdb:create_db(Tgt, #{
        data_dir => Dir,
        att_opts => #{backend => s3, s3 => S3Opts}
    }),
    try
        {ok, _} = barrel_docdb:put_attachment(Src, <<"d1">>, <<"a.txt">>, <<"alpha">>),
        {ok, _} = barrel_docdb:put_attachment(Src, <<"d2">>, <<"b.bin">>,
                                              binary:copy(<<"z">>, 200000)),
        {ok, R1} = barrel_rep:replicate(Src, Tgt),
        ?assertMatch(#{atts_written := 2, atts_skipped := 0}, att_stats(R1)),
        {ok, <<"alpha">>} = barrel_docdb:get_attachment(Tgt, <<"d1">>, <<"a.txt">>),
        {ok, Big} = barrel_docdb:get_attachment(Tgt, <<"d2">>, <<"b.bin">>),
        ?assertEqual(200000, byte_size(Big)),
        %% second run: nothing to do (checkpointed, zero re-transfer) --
        %% exactly basic_sync_and_digest_skip's proof, now with an S3 target
        {ok, R2} = barrel_rep:replicate(Src, Tgt),
        ?assertMatch(#{atts_written := 0}, att_stats(R2)),

        ok = barrel_docdb:delete_attachment(Src, <<"d1">>, <<"a.txt">>),
        {ok, R3} = barrel_rep:replicate(Src, Tgt),
        ?assertMatch(#{atts_deleted := 1}, att_stats(R3)),
        ?assertEqual({error, not_found},
                     barrel_docdb:get_attachment(Tgt, <<"d1">>, <<"a.txt">>)),

        ok = barrel_docdb:delete_attachment(Src, <<"d2">>, <<"b.bin">>),
        {ok, _} = barrel_rep:replicate(Src, Tgt)
    after
        _ = barrel_docdb:delete_db(Src),
        _ = barrel_docdb:delete_db(Tgt)
    end.

s3_target_rejects_stale_replicated_write_minio(Config) ->
    with_minio(fun(S3Opts) ->
        run_s3_target_rejects_stale_replicated_write(Config, S3Opts,
                                                     <<"s3_lww_src_minio">>,
                                                     <<"s3_lww_tgt_minio">>)
    end).

s3_target_rejects_stale_replicated_write_garage(Config) ->
    with_garage(fun(S3Opts) ->
        run_s3_target_rejects_stale_replicated_write(Config, S3Opts,
                                                     <<"s3_lww_src_garage">>,
                                                     <<"s3_lww_tgt_garage">>)
    end).

%% A (RocksDB) writes first -- its own feed row gets the EARLIER origin,
%% since barrel_hlc:new_hlc/0 draws from the single node-global clock and
%% only ever advances. B (S3) writes second, so its own row is genuinely,
%% provably LATER. Replicating A -> B therefore offers B a real (not
%% contrived) stale write for the same attachment: a target with a working
%% feed must reject it (atts_ignored, not atts_written) and keep its own
%% newer value -- the exact behavior
%% target_lacks_feed_puts_land_without_lww shows a feedless target cannot
%% provide.
run_s3_target_rejects_stale_replicated_write(Config, S3Opts, SrcName, TgtName) ->
    Dir = ?config(dir, Config),
    {ok, _} = barrel_docdb:create_db(SrcName, #{data_dir => Dir}),
    {ok, _} = barrel_docdb:create_db(TgtName, #{
        data_dir => Dir,
        att_opts => #{backend => s3, s3 => S3Opts}
    }),
    try
        {ok, _} = barrel_docdb:put_attachment(SrcName, <<"d">>, <<"f">>, <<"from a">>),
        {ok, _} = barrel_docdb:put_attachment(TgtName, <<"d">>, <<"f">>, <<"from b">>),
        {ok, R} = barrel_rep:replicate(SrcName, TgtName),
        ?assertMatch(#{atts_written := 0, atts_ignored := 1}, att_stats(R)),
        ?assertEqual({ok, <<"from b">>},
                     barrel_docdb:get_attachment(TgtName, <<"d">>, <<"f">>)),
        ok = barrel_docdb:delete_attachment(TgtName, <<"d">>, <<"f">>)
    after
        _ = barrel_docdb:delete_db(SrcName),
        _ = barrel_docdb:delete_db(TgtName)
    end.

%% Mirrors bidirectional_lww_convergence (M1, RocksDB<->RocksDB) with B
%% now S3-backed: both sides write the same attachment concurrently, sync
%% both ways twice, and confirm both converge to the SAME value with a
%% final idle round moving nothing -- the property that only holds if the
%% S3 side's own feed is correctly guarding against oscillation.
bidirectional_lww_convergence_rocksdb_and_s3(Config) ->
    with_minio(fun(S3Opts) ->
        run_bidirectional_lww_convergence(Config, undefined, S3Opts,
                                          <<"s3_conv_a">>, <<"s3_conv_b_minio">>)
    end).

%% The genuine "S3 to S3" case: both peers are real, but different,
%% S3-compatible stores (MinIO and Garage) -- proves convergence doesn't
%% depend on anything specific to one implementation.
bidirectional_lww_convergence_s3_to_s3(Config) ->
    with_minio(fun(MinioOpts) ->
        with_garage(fun(GarageOpts) ->
            run_bidirectional_lww_convergence(Config, MinioOpts, GarageOpts,
                                              <<"s3_conv_minio">>, <<"s3_conv_garage">>)
        end)
    end).

run_bidirectional_lww_convergence(Config, AOpts, BOpts, AName, BName) ->
    Dir = ?config(dir, Config),
    AAttOpts = case AOpts of
        undefined -> #{};
        _ -> #{att_opts => #{backend => s3, s3 => AOpts}}
    end,
    BAttOpts = case BOpts of
        undefined -> #{};
        _ -> #{att_opts => #{backend => s3, s3 => BOpts}}
    end,
    {ok, _} = barrel_docdb:create_db(AName, maps:merge(#{data_dir => Dir}, AAttOpts)),
    {ok, _} = barrel_docdb:create_db(BName, maps:merge(#{data_dir => Dir}, BAttOpts)),
    try
        {ok, _} = barrel_docdb:put_attachment(AName, <<"d">>, <<"f">>, <<"from a">>),
        {ok, _} = barrel_docdb:put_attachment(BName, <<"d">>, <<"f">>, <<"from b">>),
        {ok, _} = barrel_rep:replicate(AName, BName),
        {ok, _} = barrel_rep:replicate(BName, AName),
        {ok, _} = barrel_rep:replicate(AName, BName),
        {ok, _} = barrel_rep:replicate(BName, AName),
        {ok, VA} = barrel_docdb:get_attachment(AName, <<"d">>, <<"f">>),
        {ok, VB} = barrel_docdb:get_attachment(BName, <<"d">>, <<"f">>),
        ?assertEqual(VA, VB),
        ?assert(lists:member(VA, [<<"from a">>, <<"from b">>])),
        {ok, R} = barrel_rep:replicate(AName, BName),
        ?assertMatch(#{atts_written := 0, atts_ignored := 0}, att_stats(R)),
        ok = barrel_docdb:delete_attachment(AName, <<"d">>, <<"f">>)
    after
        _ = barrel_docdb:delete_db(AName),
        _ = barrel_docdb:delete_db(BName)
    end.

%%====================================================================
%% S3 fixture helpers (real MinIO/Garage, same conventions and defaults
%% as apps/barrel_att_s3/test/barrel_att_s3_SUITE.erl -- see
%% test/e2e/attachments-s3-setup.sh)
%%====================================================================

with_minio(Fun) ->
    case s3_backend_available() andalso barrel_att_s3_test_support:minio_opts() of
        false ->
            {skip, s3_backend_not_available};
        S3Opts ->
            case s3_reachable(S3Opts) of
                true -> Fun(S3Opts);
                false -> {skip, {minio_not_reachable, maps:get(endpoint, S3Opts)}}
            end
    end.

with_garage(Fun) ->
    case s3_backend_available() andalso barrel_att_s3_test_support:garage_opts() of
        false ->
            {skip, s3_backend_not_available};
        undefined ->
            {skip, garage_credentials_not_configured};
        S3Opts ->
            case s3_reachable(S3Opts) of
                true -> Fun(S3Opts);
                false -> {skip, {garage_not_reachable, maps:get(endpoint, S3Opts)}}
            end
    end.

%% barrel_att_s3 (and so livery_s3) is only on the code path under
%% `rebar3 as s3 ...' -- outside it, barrel_att_store:is_available(s3)
%% already returns false the same way, but checking livery_s3 directly
%% here avoids a hard compile-time dependency on barrel_att_store's
%% internal helper staying exported for this exact purpose.
s3_backend_available() ->
    case code:ensure_loaded(livery_s3) of
        {module, livery_s3} -> true;
        {error, _} -> false
    end.

%% Same probe as barrel_att_s3_test_support:reachable/1, plus (MinIO only)
%% making sure the bucket itself exists -- Garage buckets/keys are
%% provisioned externally (see test/e2e/attachments-s3-setup.sh) and can't
%% be created by a scoped key, matching barrel_att_s3_SUITE's own
%% init_per_group split.
s3_reachable(#{bucket := Bucket} = S3Opts) ->
    case barrel_att_s3_test_support:reachable(S3Opts) of
        true ->
            Client = livery_s3:new(maps:without([bucket], S3Opts)),
            _ = livery_s3:create_bucket(Client, Bucket),
            true;
        false ->
            false
    end.
