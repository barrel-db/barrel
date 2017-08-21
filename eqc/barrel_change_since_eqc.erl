%%%-------------------------------------------------------------------
%%% @author Zachary Kessin <>
%%% @copyright (C) 2017, Zachary Kessin
%%% @doc
%%%
%%% @end
%%% Created :  7 Aug 2017 by Zachary Kessin <>
%%%-------------------------------------------------------------------
-module(barrel_change_since_eqc).

-compile(export_all).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(DB, <<"testdb1">>).
init_db()->
    {ok, _} = application:ensure_all_started(barrel),
    barrel:create_database(#{ <<"database_id">> => ?DB }),
    fun delete_db/0.

delete_db() ->
    ok = barrel:delete_database(?DB),
    ok.


prop_rpc_listen() ->
    ?SETUP(fun init_db/0,
           ?FORALL({Id,Doc},
                   {non_empty(utf8(8)),
                    non_empty(map(non_empty(utf8()), non_empty(utf8())))},
                   begin
                       Listen = fun (Change, Acc) ->
                                        {ok, [{maps:get(<<"seq">>, Change), maps:get(<<"doc">>, Change)} |Acc]}
                                end,
                       Doc1         = maps:put(<<"id">>, Id, Doc),
                       {ok, Id, _RevId} = barrel:post(?DB, Doc1, #{}),
                       {ok, Doc1, _} = barrel:get(?DB, Id, #{}),
                       [Change|_] = barrel:changes_since(?DB, 0, Listen, [], #{include_doc => true}),
                       {_, Doc1} = Change,
                       true
                   end)).
