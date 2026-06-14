%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_embed_provider module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_provider_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

provider_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
       {"call_embed success", fun test_call_embed_success/0},
       {"call_embed provider error", fun test_call_embed_error/0},
       {"call_embed provider not found", fun test_call_embed_not_found/0},
       {"call_embed_batch success", fun test_call_embed_batch_success/0},
       {"call_embed_batch error", fun test_call_embed_batch_error/0},
       {"check_available true", fun test_check_available_true/0},
       {"check_available false", fun test_check_available_false/0},
       {"check_available not implemented", fun test_check_available_default/0},
       {"check_available crashes", fun test_check_available_crash/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    meck:new(mock_provider, [non_strict]),
    ok.

cleanup(_) ->
    meck:unload(mock_provider),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_call_embed_success() ->
    meck:expect(mock_provider, embed, fun(<<"hello">>, _Config) ->
        {ok, [1.0, 2.0, 3.0]}
    end),
    Result = barrel_embed_provider:call_embed(mock_provider, <<"hello">>, #{}),
    ?assertEqual({ok, [1.0, 2.0, 3.0]}, Result).

test_call_embed_error() ->
    meck:expect(mock_provider, embed, fun(_, _) ->
        {error, api_failed}
    end),
    Result = barrel_embed_provider:call_embed(mock_provider, <<"test">>, #{}),
    ?assertEqual({error, api_failed}, Result).

test_call_embed_not_found() ->
    Result = barrel_embed_provider:call_embed(nonexistent_module, <<"test">>, #{}),
    ?assertMatch({error, {provider_not_found, nonexistent_module}}, Result).

test_call_embed_batch_success() ->
    meck:expect(mock_provider, embed_batch, fun(Texts, _Config) ->
        Vecs = [[float(I)] || I <- lists:seq(1, length(Texts))],
        {ok, Vecs}
    end),
    Result = barrel_embed_provider:call_embed_batch(mock_provider, [<<"a">>, <<"b">>], #{}),
    ?assertEqual({ok, [[1.0], [2.0]]}, Result).

test_call_embed_batch_error() ->
    meck:expect(mock_provider, embed_batch, fun(_, _) ->
        {error, batch_too_large}
    end),
    Result = barrel_embed_provider:call_embed_batch(mock_provider, [<<"a">>], #{}),
    ?assertEqual({error, batch_too_large}, Result).

test_check_available_true() ->
    meck:expect(mock_provider, available, fun(_Config) -> true end),
    ?assertEqual(true, barrel_embed_provider:check_available(mock_provider, #{})).

test_check_available_false() ->
    meck:expect(mock_provider, available, fun(_Config) -> false end),
    ?assertEqual(false, barrel_embed_provider:check_available(mock_provider, #{})).

test_check_available_default() ->
    %% Provider without available/1 should return true
    meck:new(simple_provider, [non_strict]),
    meck:expect(simple_provider, embed, fun(_, _) -> {ok, []} end),
    Result = barrel_embed_provider:check_available(simple_provider, #{}),
    ?assertEqual(true, Result),
    meck:unload(simple_provider).

test_check_available_crash() ->
    meck:expect(mock_provider, available, fun(_Config) ->
        error(crash)
    end),
    ?assertEqual(false, barrel_embed_provider:check_available(mock_provider, #{})).
