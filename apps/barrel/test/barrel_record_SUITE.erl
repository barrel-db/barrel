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
         adapter_never_touches_doc/1]).

all() ->
    [adapter_get,
     adapter_multi_get,
     adapter_missing_and_deleted,
     adapter_is_read_only,
     adapter_never_touches_doc].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_record_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_binary(TC, utf8),
    {ok, _Pid} = barrel_docdb:create_db(Db, #{data_dir => ?config(dir, Config)}),
    {ok, Policy} = barrel_embedding_policy:validate(#{
        fields => [<<"title">>, <<"body">>],
        metadata_fields => [<<"kind">>]
    }),
    {ok, Ctx} = barrel_record_docstore:init(
        binary_to_atom(Db, utf8), #{db => Db, policy => Policy}),
    [{db, Db}, {ctx, Ctx} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(db, Config)) catch _:_ -> ok end,
    ok.

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
