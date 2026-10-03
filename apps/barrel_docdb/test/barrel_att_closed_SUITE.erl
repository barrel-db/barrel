%%%-------------------------------------------------------------------
%%% @doc An attachment call that races its database closing answers
%%% `{error, not_found}' instead of crashing on the closed store.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_closed_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([read_during_close/1, badarg_on_open_db_propagates/1]).

-define(DB, <<"att_closed">>).

all() ->
    [read_during_close, badarg_on_open_db_propagates].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TC, Config) ->
    {ok, _} = barrel_docdb:create_db(?DB, #{data_dir => ?config(priv_dir, Config)}),
    {ok, _} = barrel_docdb:put_doc(?DB, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:put_attachment(?DB, <<"d">>, <<"a.txt">>, <<"data">>),
    ok = meck:new(barrel_att_store, [passthrough, no_link]),
    Config.

end_per_testcase(_TC, _Config) ->
    meck:unload(barrel_att_store),
    _ = barrel_docdb:delete_db(?DB),
    ok.

%% The database closes after with_att took its attachment store and
%% before the read: the read meets a closed RocksDB handle.
read_during_close(_Config) ->
    meck:expect(barrel_att_store, att_changes,
                fun(Ref, Name, Since, Opts) ->
                    ok = barrel_docdb:close_db(?DB),
                    meck:passthrough([Ref, Name, Since, Opts])
                end),
    ?assertEqual({error, not_found}, barrel_docdb:att_changes(?DB, first)).

%% A badarg on an open database is a bug, not a close: it propagates.
badarg_on_open_db_propagates(_Config) ->
    meck:expect(barrel_att_store, att_changes,
                fun(_Ref, _Name, _Since, _Opts) -> error(badarg) end),
    ?assertError(badarg, barrel_docdb:att_changes(?DB, first)).
