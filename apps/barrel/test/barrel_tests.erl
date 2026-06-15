%%%-------------------------------------------------------------------
%%% @doc Unit tests for pure facade logic (no layers started).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_tests).

-include_lib("eunit/include/eunit.hrl").

%% A mixed-shape vector batch is rejected before any store call, so this needs
%% no running store.
mixed_batch_rejected_test() ->
    Db = #{name => t, docdb => <<"t">>, vstore => no_store},
    Batch = [{<<"a">>, <<"t">>, #{}}, {<<"b">>, <<"t">>, #{}, [0.1]}],
    ?assertEqual({error, mixed_batch}, barrel:vector_add_batch(Db, Batch)).
