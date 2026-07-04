%%%-------------------------------------------------------------------
%%% @doc CT suite for record-mode building blocks: the read-through
%%% docstore adapter (barrel_record_docstore) against a real docdb.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_record_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([adapter_get/1,
         adapter_multi_get/1,
         adapter_missing_and_deleted/1,
         adapter_is_read_only/1,
         adapter_never_touches_doc/1,
         record_open_tags_writes/1,
         record_user_tags_preserved/1,
         record_policy_persisted/1,
         record_sync_mode_rejected/1,
         record_dimension_mismatch/1,
         record_plain_open_untagged/1,
         indexer_async_indexing/1,
         indexer_update_reembeds/1,
         indexer_delete_removes_vector/1,
         indexer_nonmatching_acked/1,
         indexer_crash_restart/1,
         indexer_poison_parked/1]).

all() ->
    [adapter_get,
     adapter_multi_get,
     adapter_missing_and_deleted,
     adapter_is_read_only,
     adapter_never_touches_doc,
     record_open_tags_writes,
     record_user_tags_preserved,
     record_policy_persisted,
     record_sync_mode_rejected,
     record_dimension_mismatch,
     record_plain_open_untagged,
     indexer_async_indexing,
     indexer_update_reembeds,
     indexer_delete_removes_vector,
     indexer_nonmatching_acked,
     indexer_crash_restart,
     indexer_poison_parked].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel),
    Dir = "/tmp/barrel_record_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    case lists:member(TC, meck_cases()) of
        true -> mock_embed();
        false -> ok
    end,
    Db = atom_to_binary(TC, utf8),
    {ok, _Pid} = barrel_docdb:create_db(Db, #{data_dir => ?config(dir, Config)}),
    {ok, Policy} = barrel_embedding_policy:validate(#{
        fields => [<<"title">>, <<"body">>],
        metadata_fields => [<<"kind">>]
    }),
    {ok, Ctx} = barrel_record_docstore:init(
        binary_to_atom(Db, utf8), #{db => Db, policy => Policy}),
    [{db, Db}, {ctx, Ctx} | Config].

end_per_testcase(TC, Config) ->
    case lists:member(TC, meck_cases()) of
        true -> try meck:unload(barrel_embed) catch _:_ -> ok end;
        false -> ok
    end,
    try barrel_docdb:delete_db(?config(db, Config)) catch _:_ -> ok end,
    ok.

meck_cases() ->
    [indexer_async_indexing,
     indexer_update_reembeds,
     indexer_delete_removes_vector,
     indexer_nonmatching_acked,
     indexer_crash_restart,
     indexer_poison_parked].

%% Deterministic 3-dim embedder; texts containing "poison" fail.
mock_embed() ->
    _ = try meck:unload(barrel_embed) catch _:_ -> ok end,
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, embed, fun(Text, _State) ->
        case binary:match(Text, <<"poison">>) of
            nomatch -> {ok, mock_vec(Text)};
            _ -> {error, poison}
        end
    end),
    meck:expect(barrel_embed, embed_batch, fun(Texts, _State) ->
        case lists:any(fun(T) -> binary:match(T, <<"poison">>) =/= nomatch end,
                       Texts) of
            true -> {error, poison};
            false -> {ok, [mock_vec(T) || T <- Texts]}
        end
    end).

mock_vec(Text) ->
    Hash = erlang:phash2(Text, 1000000),
    [Hash / 1000000.0, (Hash rem 1000) / 1000.0, (Hash rem 100) / 100.0].

%%====================================================================
%% Test cases
%%====================================================================

adapter_get(Config) ->
    Db = ?config(db, Config),
    Ctx = ?config(ctx, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"a">>, <<"title">> => <<"Hello">>,
        <<"body">> => <<"World">>, <<"kind">> => <<"note">>,
        <<"noise">> => 42}),
    {ok, Text, Meta} = barrel_record_docstore:get(Ctx, <<"a">>),
    ?assertEqual(<<"Hello\nWorld">>, Text),
    %% metadata_fields projection applies
    ?assertEqual(#{<<"kind">> => <<"note">>}, Meta).

adapter_multi_get(Config) ->
    Db = ?config(db, Config),
    Ctx = ?config(ctx, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"a">>, <<"title">> => <<"A">>, <<"kind">> => <<"x">>}),
    {ok, _} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"b">>, <<"title">> => <<"B">>, <<"kind">> => <<"y">>}),
    Results = barrel_record_docstore:multi_get(Ctx, [<<"b">>, <<"gone">>, <<"a">>]),
    [{ok, <<"B">>, #{<<"kind">> := <<"y">>}},
     not_found,
     {ok, <<"A">>, #{<<"kind">> := <<"x">>}}] = Results,
    ok.

adapter_missing_and_deleted(Config) ->
    Db = ?config(db, Config),
    Ctx = ?config(ctx, Config),
    ?assertEqual(not_found, barrel_record_docstore:get(Ctx, <<"nope">>)),
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"d">>, <<"title">> => <<"T">>}),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"d">>, #{rev => Rev}),
    %% Deleted documents read as missing through the adapter
    ?assertEqual(not_found, barrel_record_docstore:get(Ctx, <<"d">>)).

adapter_is_read_only(Config) ->
    Ctx = ?config(ctx, Config),
    ?assertEqual({error, read_only},
                 barrel_record_docstore:put(Ctx, <<"a">>, <<"t">>, #{})),
    ?assertEqual({error, read_only},
                 barrel_record_docstore:multi_put(Ctx, [{<<"a">>, <<"t">>, #{}}])).

adapter_never_touches_doc(Config) ->
    Db = ?config(db, Config),
    Ctx = ?config(ctx, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{
        <<"id">> => <<"keep">>, <<"title">> => <<"T">>}),
    %% delete on the adapter is a vector-side no-op: the doc survives
    ?assertEqual(ok, barrel_record_docstore:delete(Ctx, <<"keep">>)),
    {ok, _Doc} = barrel_docdb:get_doc(Db, <<"keep">>),
    %% terminate never closes the database
    ?assertEqual(ok, barrel_record_docstore:terminate(Ctx)),
    {ok, _Doc2} = barrel_docdb:get_doc(Db, <<"keep">>),
    ok.

%%====================================================================
%% Test cases: record-mode open + write tagging
%%====================================================================

record_open_tags_writes(Config) ->
    {ok, Db} = open_record(record_tags_db, Config, #{fields => [<<"title">>]}),
    %% Freeze the indexer so pending-entry assertions cannot race it
    ok = sys:suspend(erlang:whereis(barrel_record_indexer:name(record_tags_db))),
    DbBin = <<"record_tags_db">>,
    {ok, #{<<"rev">> := Rev}} = barrel:put_doc(Db, #{
        <<"id">> => <<"a">>, <<"title">> => <<"hello">>}),
    [Entry] = pending(DbBin),
    ?assertEqual(<<"a">>, maps:get(id, Entry)),
    ?assertEqual(false, maps:get(deleted, Entry)),
    %% Tagged delete replaces the entry with a deleted one
    {ok, _} = barrel:delete_doc(Db, <<"a">>),
    [Entry2] = pending(DbBin),
    ?assertEqual(true, maps:get(deleted, Entry2)),
    ?assertNotEqual(Rev, maps:get(rev, Entry2)),
    %% Batch writes tag every doc
    [_, _] = [R || {ok, _} = R <- barrel:put_docs(Db, [
        #{<<"id">> => <<"b">>, <<"title">> => <<"B">>},
        #{<<"id">> => <<"c">>, <<"title">> => <<"C">>}])],
    ?assertEqual(3, length(pending(DbBin))),
    ok = barrel:close(Db).

record_user_tags_preserved(Config) ->
    {ok, Db} = open_record(record_user_tags_db, Config,
                           #{fields => [<<"title">>]}),
    ok = sys:suspend(
        erlang:whereis(barrel_record_indexer:name(record_user_tags_db))),
    DbBin = <<"record_user_tags_db">>,
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"title">> => <<"T">>},
                             #{outbox => [<<"audit">>]}),
    ?assertEqual(1, length(pending(DbBin))),
    [AuditEntry] = lists:reverse(barrel_docdb:outbox_fold(
        DbBin, <<"audit">>, fun(E, Acc) -> {ok, [E | Acc]} end, [])),
    ?assertEqual(<<"a">>, maps:get(id, AuditEntry)),
    ok = barrel:close(Db).

record_policy_persisted(Config) ->
    Policy1 = #{fields => [<<"title">>]},
    {ok, Db} = open_record(record_policy_db, Config, Policy1),
    DbBin = <<"record_policy_db">>,
    {ok, #{<<"policy">> := Stored1}} =
        barrel_docdb:get_local_doc(DbBin, <<"_barrel/embedding">>),
    ?assertMatch(#{fields := [[<<"title">>]]}, binary_to_term(Stored1)),
    ok = barrel:close(Db),
    %% Reopen with a different policy: overwritten (a warning is logged)
    {ok, Db2} = open_record(record_policy_db, Config,
                            #{fields => [<<"body">>]}),
    {ok, #{<<"policy">> := Stored2}} =
        barrel_docdb:get_local_doc(DbBin, <<"_barrel/embedding">>),
    ?assertMatch(#{fields := [[<<"body">>]]}, binary_to_term(Stored2)),
    ok = barrel:close(Db2).

record_sync_mode_rejected(Config) ->
    ?assertEqual({error, {unsupported, sync_mode}},
                 open_record(record_sync_db, Config,
                             #{fields => [<<"t">>], mode => sync})).

record_dimension_mismatch(Config) ->
    Dir = ?config(dir, Config),
    ?assertEqual({error, {dimension_mismatch, 3, 4}},
                 barrel:open(record_dim_db, #{
                     embedding => #{fields => [<<"t">>], dimensions => 3},
                     docdb => #{data_dir => Dir},
                     vectordb => #{dimension => 4,
                                   db_path => Dir ++ "/record_dim_vec"}})).

record_plain_open_untagged(Config) ->
    Dir = ?config(dir, Config),
    {ok, Db} = barrel:open(record_plain_db, #{
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3, db_path => Dir ++ "/record_plain_vec",
                      bm25_backend => memory}}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"title">> => <<"T">>}),
    ?assertEqual([], pending(<<"record_plain_db">>)),
    ok = barrel:close(Db).

%%====================================================================
%% Test cases: the record indexer (async vector indexing)
%%====================================================================

indexer_async_indexing(Config) ->
    {ok, Db} = open_record(idx_async_db, Config, #{fields => [<<"title">>]}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"hello world">>}),
    ok = wait_until(fun() ->
        case barrel:search_vector(Db, mock_vec(<<"hello world">>), #{k => 1}) of
            {ok, [#{key := <<"a">>}]} -> true;
            _ -> false
        end
    end),
    %% Outbox drained
    ok = wait_until(fun() -> pending(<<"idx_async_db">>) =:= [] end),
    ok = barrel:close(Db).

indexer_update_reembeds(Config) ->
    {ok, Db} = open_record(idx_update_db, Config, #{fields => [<<"title">>]}),
    {ok, #{<<"rev">> := Rev}} = barrel:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"title">> => <<"first">>}),
    ok = wait_until(fun() -> hit(Db, <<"first">>, <<"a">>) end),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"second">>,
                                   <<"_rev">> => Rev}),
    ok = wait_until(fun() -> hit(Db, <<"second">>, <<"a">>) end),
    ok = barrel:close(Db).

indexer_delete_removes_vector(Config) ->
    {ok, Db} = open_record(idx_delete_db, Config, #{fields => [<<"title">>]}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"to delete">>}),
    ok = wait_until(fun() -> barrel_vectordb:count(idx_delete_db) =:= 1 end),
    {ok, _} = barrel:delete_doc(Db, <<"a">>),
    ok = wait_until(fun() -> barrel_vectordb:count(idx_delete_db) =:= 0 end),
    ok = wait_until(fun() -> pending(<<"idx_delete_db">>) =:= [] end),
    ok = barrel:close(Db).

indexer_nonmatching_acked(Config) ->
    {ok, Db} = open_record(idx_nomatch_db, Config, #{fields => [<<"title">>]}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>, <<"other">> => <<"x">>}),
    ok = wait_until(fun() -> pending(<<"idx_nomatch_db">>) =:= [] end),
    ?assertEqual(0, barrel_vectordb:count(idx_nomatch_db)),
    ok = barrel:close(Db).

indexer_crash_restart(Config) ->
    {ok, Db} = open_record(idx_crash_db, Config, #{fields => [<<"title">>]}),
    IndexerName = barrel_record_indexer:name(idx_crash_db),
    Pid0 = erlang:whereis(IndexerName),
    ?assert(is_pid(Pid0)),
    %% Freeze, enqueue work, kill mid-flight: the supervisor restarts the
    %% indexer and the pending entry converges.
    ok = sys:suspend(Pid0),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"a">>,
                                   <<"title">> => <<"survives">>}),
    exit(Pid0, kill),
    ok = wait_until(fun() ->
        case erlang:whereis(IndexerName) of
            undefined -> false;
            Pid -> Pid =/= Pid0
        end
    end),
    ok = wait_until(fun() -> hit(Db, <<"survives">>, <<"a">>) end),
    ok = barrel:close(Db).

indexer_poison_parked(Config) ->
    {ok, Db} = open_record(idx_poison_db, Config, #{fields => [<<"title">>]}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"bad">>,
                                   <<"title">> => <<"poison pill">>}),
    {ok, _} = barrel:put_doc(Db, #{<<"id">> => <<"good">>,
                                   <<"title">> => <<"healthy doc">>}),
    %% The good doc indexes despite the poison one failing every round
    ok = wait_until(fun() -> hit(Db, <<"healthy doc">>, <<"good">>) end),
    %% The poison entry stays pending (parked, visible)
    ok = wait_until(fun() ->
        [E || #{id := Id} = E <- pending(<<"idx_poison_db">>),
              Id =:= <<"bad">>] =/= []
    end),
    ?assertEqual(1, barrel_vectordb:count(idx_poison_db)),
    ok = barrel:close(Db).

%%====================================================================
%% Helpers
%%====================================================================

hit(Db, Text, ExpectedId) ->
    case barrel:search_vector(Db, mock_vec(Text), #{k => 1}) of
        {ok, [#{key := Key} | _]} -> Key =:= ExpectedId;
        _ -> false
    end.

wait_until(Fun) ->
    wait_until(Fun, 100).

wait_until(_Fun, 0) ->
    {error, timeout};
wait_until(Fun, Retries) ->
    case Fun() of
        true -> ok;
        false ->
            timer:sleep(50),
            wait_until(Fun, Retries - 1)
    end.

open_record(Name, Config, PolicyMap) ->
    Dir = ?config(dir, Config),
    barrel:open(Name, #{
        embedding => PolicyMap,
        docdb => #{data_dir => Dir},
        vectordb => #{dimension => 3,
                      db_path => Dir ++ "/" ++ atom_to_list(Name) ++ "_vec"}}).

pending(DbBin) ->
    lists:reverse(barrel_docdb:outbox_fold(
        DbBin, <<"embed">>, fun(E, Acc) -> {ok, [E | Acc]} end, [])).
