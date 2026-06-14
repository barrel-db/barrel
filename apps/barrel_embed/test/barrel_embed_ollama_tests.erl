%%%-------------------------------------------------------------------
%%% @doc Integration tests for barrel_embed_ollama module
%%%
%%% These tests run against a real Ollama instance.
%%% Tests are skipped if Ollama is not available.
%%%
%%% To run: ensure Ollama is running with nomic-embed-text model:
%%%   ollama pull nomic-embed-text
%%%   ollama serve
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_ollama_tests).

-include_lib("eunit/include/eunit.hrl").

-define(OLLAMA_URL, <<"http://localhost:11434">>).
-define(TEST_MODEL, <<"nomic-embed-text">>).
-define(TIMEOUT, 30000).

%%====================================================================
%% Test Generators
%%====================================================================

ollama_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {with, [
       fun test_available/1,
       fun test_init/1,
       fun test_embed_single/1,
       fun test_embed_batch/1,
       fun test_embed_batch_sequential/1,
       fun test_dimension/1
     ]}
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(hackney),
    case check_ollama_available() of
        true ->
            {ok, Config} = barrel_embed_ollama:init(#{
                url => ?OLLAMA_URL,
                model => ?TEST_MODEL,
                timeout => ?TIMEOUT
            }),
            {available, Config};
        false ->
            unavailable
    end.

cleanup(_) ->
    ok.

check_ollama_available() ->
    ApiUrl = <<?OLLAMA_URL/binary, "/api/tags">>,
    case hackney:request(get, ApiUrl, [], <<>>, [{recv_timeout, 5000}, {with_body, true}]) of
        {ok, 200, _, _} -> true;
        _ -> false
    end.

%%====================================================================
%% Test Cases
%%====================================================================

test_available(unavailable) ->
    {skip, "Ollama not available"};
test_available({available, Config}) ->
    ?assertEqual(true, barrel_embed_ollama:available(Config)).

test_init(unavailable) ->
    {skip, "Ollama not available"};
test_init({available, _Config}) ->
    {ok, Config} = barrel_embed_ollama:init(#{
        url => ?OLLAMA_URL,
        model => ?TEST_MODEL
    }),
    ?assertEqual(?OLLAMA_URL, maps:get(url, Config)),
    ?assertEqual(?TEST_MODEL, maps:get(model, Config)),
    ?assert(maps:is_key(timeout, Config)),
    ?assert(maps:is_key(dimension, Config)).

test_embed_single(unavailable) ->
    {skip, "Ollama not available"};
test_embed_single({available, Config}) ->
    {ok, Vector} = barrel_embed_ollama:embed(<<"Hello, world!">>, Config),
    ?assert(is_list(Vector)),
    ?assert(length(Vector) > 0),
    ?assert(is_float(hd(Vector))).

test_embed_batch(unavailable) ->
    {skip, "Ollama not available"};
test_embed_batch({available, Config}) ->
    Texts = [
        <<"The quick brown fox">>,
        <<"jumps over the lazy dog">>,
        <<"Hello world">>
    ],
    {ok, Vectors} = barrel_embed_ollama:embed_batch(Texts, Config),
    ?assertEqual(3, length(Vectors)),
    lists:foreach(fun(Vec) ->
        ?assert(is_list(Vec)),
        ?assert(length(Vec) > 0),
        ?assert(is_float(hd(Vec)))
    end, Vectors).

test_embed_batch_sequential(unavailable) ->
    {skip, "Ollama not available"};
test_embed_batch_sequential({available, Config}) ->
    %% Test with old API fallback by using embeddings endpoint
    Texts = [<<"One">>, <<"Two">>],
    {ok, Vectors} = barrel_embed_ollama:embed_batch(Texts, Config),
    ?assertEqual(2, length(Vectors)).

test_dimension(unavailable) ->
    {skip, "Ollama not available"};
test_dimension({available, Config}) ->
    Dim = barrel_embed_ollama:dimension(Config),
    ?assert(is_integer(Dim)),
    ?assert(Dim > 0),
    %% nomic-embed-text has 768 dimensions
    ?assertEqual(768, Dim).

%%====================================================================
%% Additional Edge Case Tests
%%====================================================================

error_handling_test_() ->
    {setup,
     fun() -> application:ensure_all_started(hackney) end,
     fun(_) -> ok end,
     [
       {"unavailable returns false for bad url", fun test_unavailable_bad_url/0},
       {"embed fails with bad url", fun test_embed_bad_url/0}
     ]
    }.

test_unavailable_bad_url() ->
    Config = #{
        url => <<"http://localhost:99999">>,
        model => ?TEST_MODEL,
        timeout => 1000
    },
    ?assertEqual(false, barrel_embed_ollama:available(Config)).

test_embed_bad_url() ->
    {ok, Config} = barrel_embed_ollama:init(#{
        url => <<"http://localhost:99999">>,
        model => ?TEST_MODEL,
        timeout => 1000
    }),
    Result = barrel_embed_ollama:embed(<<"test">>, Config),
    ?assertMatch({error, _}, Result).
