%%%-------------------------------------------------------------------
%%% @doc barrel_att_s3 as a replication participant: proves the feed's
%%% LWW guard is correctly wired end-to-end through barrel_rep/
%%% barrel_rep_transport_local when an S3-backed database is a
%%% replication source or target, against real MinIO and Garage.
%%%
%%% Lives here (not in apps/barrel_docdb/test) so barrel_docdb's test
%%% suite never references barrel_att_s3/livery_s3 -- barrel_att_s3
%%% already depends on barrel_docdb, not the other way around.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_rep_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([s3_target_receives_puts_and_deletes/1,
         s3_target_rejects_stale_replicated_write_minio/1,
         s3_target_rejects_stale_replicated_write_garage/1,
         bidirectional_lww_convergence_rocksdb_and_s3/1,
         bidirectional_lww_convergence_s3_to_s3/1]).

all() ->
    [s3_target_receives_puts_and_deletes,
     s3_target_rejects_stale_replicated_write_minio,
     s3_target_rejects_stale_replicated_write_garage,
     bidirectional_lww_convergence_rocksdb_and_s3,
     bidirectional_lww_convergence_s3_to_s3].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_att_s3_rep_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(_TC, Config) ->
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

att_stats(Result) ->
    maps:get(att_sync, Result).

%%====================================================================
%% Cases
%%
%% barrel_att_s3_store's own suite (barrel_att_s3_SUITE) already
%% unit-tests the LWW guard directly against the backend
%% (origin_hlc_stale_put_ignored and friends); these prove the same
%% guard is correctly wired end-to-end through barrel_rep/
%% barrel_rep_transport_local when the target itself is S3-backed.
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
        %% second run: nothing to do (checkpointed, zero re-transfer)
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
%% newer value.
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

%% Both sides write the same attachment concurrently, sync both ways
%% twice, and confirm both converge to the SAME value with a final idle
%% round moving nothing -- the property that only holds if the S3 side's
%% own feed is correctly guarding against oscillation.
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
%% as barrel_att_s3_SUITE.erl -- see test/e2e/attachments-s3-setup.sh)
%%====================================================================

with_minio(Fun) ->
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
            Fun(S3Opts);
        false ->
            {skip, {minio_not_reachable, maps:get(endpoint, S3Opts)}}
    end.

with_garage(Fun) ->
    case barrel_att_s3_test_support:garage_opts() of
        undefined ->
            {skip, garage_credentials_not_configured};
        S3Opts ->
            case barrel_att_s3_test_support:reachable(S3Opts) of
                true -> Fun(S3Opts);
                false -> {skip, {garage_not_reachable, maps:get(endpoint, S3Opts)}}
            end
    end.
