%%%-------------------------------------------------------------------
%%% @author Zachary Kessin <>
%%% @copyright (C) 2017, Zachary Kessin
%%% @doc
%%%
%%% @end
%%% Created : 21 Aug 2017 by Zachary Kessin <>
%%%-------------------------------------------------------------------
-module(run_tests).

-include_lib("eunit/include/eunit.hrl").

run_quickcheck_test_() ->
    application:stop(lager),
    Mods =[
           barrel_change_since_eqc,
           barrel_rpc_events_eqc,
           create_delete_database_eqc,
           barrel_rpc_eqc],
    
    {timeout, 36000,
     [?_assertEqual([], eqc:module({numtests,30} , M)) || M <-Mods
     ]}.
