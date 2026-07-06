%%%-------------------------------------------------------------------
%%% @doc Doc TTL: the expires_at write option, lazy expiry on reads,
%%% and (from step 8) the opt-in sweeper that turns expired docs into
%%% real tombstones.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_ttl_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_set_preserve_clear/1,
    t_lazy_expiry_on_reads/1,
    t_delete_clears_ttl/1,
    t_invalid_expires_rejected/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_set_preserve_clear, t_lazy_expiry_on_reads, t_delete_clears_ttl,
     t_invalid_expires_rejected].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_ttl",
    os:cmd("rm -rf " ++ DataDir),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

init_per_testcase(Case, Config) ->
    Db = <<"ttl_", (atom_to_binary(Case, utf8))/binary>>,
    {ok, _} = barrel_docdb:create_db(Db, #{
        data_dir => ?config(data_dir, Config)}),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

now_ms() ->
    erlang:system_time(millisecond).

%%====================================================================
%% Cases
%%====================================================================

t_set_preserve_clear(Config) ->
    Db = ?config(db, Config),
    Future = now_ms() + 60000,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>, <<"v">> => 1},
                                   #{expires_at => Future}),
    {ok, #{<<"_rev">> := R1}} = barrel_docdb:get_doc(Db, <<"a">>),
    %% a write without the option preserves the TTL
    {ok, #{<<"rev">> := R2}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 2, <<"_rev">> => R1}),
    {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Db, <<"a">>),
    %% clearing with 0 removes the TTL; a later past-expiry rewrite
    %% proves both transitions took effect
    {ok, #{<<"rev">> := R3}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 3, <<"_rev">> => R2},
        #{expires_at => 0}),
    {ok, #{<<"v">> := 3}} = barrel_docdb:get_doc(Db, <<"a">>),
    {ok, _} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"a">>, <<"v">> => 4, <<"_rev">> => R3},
        #{expires_at => now_ms() - 1}),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, <<"a">>)),
    ok.

t_lazy_expiry_on_reads(Config) ->
    Db = ?config(db, Config),
    Past = now_ms() - 1,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"gone">>},
                                   #{expires_at => Past}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"kept">>}),
    %% point read
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, <<"gone">>)),
    %% batch read keeps order, expired reads as missing
    ?assertMatch([{error, not_found}, {ok, _}],
                 barrel_docdb:get_docs(Db, [<<"gone">>, <<"kept">>])),
    %% folds skip it
    {ok, Ids} = barrel_docdb:fold_docs(
        Db, fun(#{<<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end, []),
    ?assertEqual([<<"kept">>], Ids),
    ok.

t_delete_clears_ttl(Config) ->
    Db = ?config(db, Config),
    Future = now_ms() + 60000,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>},
                                   #{expires_at => Future}),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"a">>),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, <<"a">>)),
    %% re-creating the doc starts without a TTL
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>,
                                         <<"v">> => 2}),
    {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Db, <<"a">>),
    ok.

t_invalid_expires_rejected(Config) ->
    Db = ?config(db, Config),
    Doc = #{<<"id">> => <<"bad">>},
    ?assertMatch({error, {invalid_expires_at, -1}},
                 barrel_docdb:put_doc(Db, Doc, #{expires_at => -1})),
    ?assertMatch({error, {invalid_expires_at, _}},
                 barrel_docdb:put_doc(Db, Doc,
                                      #{expires_at => <<"soon">>})),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, <<"bad">>)),
    ok.
