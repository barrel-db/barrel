%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_embed module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

embed_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
       {"init without embedder returns undefined", fun test_init_no_embedder/0},
       {"init with embedder", fun test_init_with_embedder/0},
       {"init with custom dimension", fun test_init_dimension/0},
       {"init with provider chain", fun test_init_chain/0},
       {"init fails when provider fails", fun test_init_provider_fails/0},
       {"embed single text", fun test_embed/0},
       {"embed with string input", fun test_embed_string/0},
       {"embed without embedder returns error", fun test_embed_no_embedder/0},
       {"embed_batch multiple texts", fun test_embed_batch/0},
       {"embed_batch with custom batch size", fun test_embed_batch_size/0},
       {"embed_batch chunking", fun test_embed_batch_chunking/0},
       {"dimension returns configured value", fun test_dimension/0},
       {"dimension returns undefined when no embedder", fun test_dimension_no_embedder/0},
       {"info returns provider details", fun test_info/0},
       {"info returns not configured when no embedder", fun test_info_no_embedder/0},
       {"fallback to next provider", fun test_fallback/0},
       {"all providers fail", fun test_all_fail/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    %% Make sure all meck is clean
    catch meck:unload(barrel_embed_local),
    catch meck:unload(barrel_embed_ollama),

    %% Mock the local provider
    meck:new(barrel_embed_local, [passthrough]),
    meck:expect(barrel_embed_local, init, fun(Config) ->
        {ok, Config#{initialized => true}}
    end),
    meck:expect(barrel_embed_local, name, fun() -> local end),
    meck:expect(barrel_embed_local, available, fun(_) -> true end),
    meck:expect(barrel_embed_local, embed, fun(Text, _Config) ->
        Vec = make_vector(Text, 768),
        {ok, Vec}
    end),
    meck:expect(barrel_embed_local, embed_batch, fun(Texts, _Config) ->
        Vecs = [make_vector(T, 768) || T <- Texts],
        {ok, Vecs}
    end),
    ok.

cleanup(_) ->
    catch meck:unload(barrel_embed_local),
    catch meck:unload(barrel_embed_ollama),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_init_no_embedder() ->
    %% No embedder config -> returns undefined
    {ok, State} = barrel_embed:init(#{}),
    ?assertEqual(undefined, State).

test_init_with_embedder() ->
    {ok, State} = barrel_embed:init(#{embedder => {local, #{}}}),
    ?assertEqual(768, maps:get(dimension, State)),
    ?assertEqual(32, maps:get(batch_size, State)),
    ?assert(length(maps:get(providers, State)) > 0).

test_init_dimension() ->
    {ok, State} = barrel_embed:init(#{
        embedder => {local, #{}},
        dimensions => 384
    }),
    ?assertEqual(384, maps:get(dimension, State)).

test_init_chain() ->
    Config = #{
        embedder => [
            {local, #{model => <<"test">>}},
            {local, #{model => <<"fallback">>}}
        ]
    },
    {ok, State} = barrel_embed:init(Config),
    ?assertEqual(2, length(maps:get(providers, State))).

test_init_provider_fails() ->
    %% Mock init to fail
    meck:expect(barrel_embed_local, init, fun(_) ->
        {error, not_available}
    end),
    Result = barrel_embed:init(#{embedder => {local, #{}}}),
    ?assertEqual({error, no_providers_available}, Result),
    %% Restore
    meck:expect(barrel_embed_local, init, fun(Config) ->
        {ok, Config#{initialized => true}}
    end).

test_embed() ->
    {ok, State} = barrel_embed:init(#{embedder => {local, #{}}}),
    {ok, Vec} = barrel_embed:embed(<<"hello world">>, State),
    ?assertEqual(768, length(Vec)),
    ?assert(is_float(hd(Vec))).

test_embed_string() ->
    {ok, State} = barrel_embed:init(#{embedder => {local, #{}}}),
    {ok, Vec} = barrel_embed:embed("hello world", State),
    ?assertEqual(768, length(Vec)).

test_embed_no_embedder() ->
    {ok, State} = barrel_embed:init(#{}),
    Result = barrel_embed:embed(<<"hello">>, State),
    ?assertEqual({error, embedder_not_configured}, Result).

test_embed_batch() ->
    {ok, State} = barrel_embed:init(#{embedder => {local, #{}}}),
    Texts = [<<"one">>, <<"two">>, <<"three">>],
    {ok, Vecs} = barrel_embed:embed_batch(Texts, State),
    ?assertEqual(3, length(Vecs)),
    lists:foreach(fun(V) ->
        ?assertEqual(768, length(V))
    end, Vecs).

test_embed_batch_size() ->
    {ok, State} = barrel_embed:init(#{
        embedder => {local, #{}},
        batch_size => 10
    }),
    ?assertEqual(10, maps:get(batch_size, State)).

test_embed_batch_chunking() ->
    %% Track batch calls
    Self = self(),
    meck:expect(barrel_embed_local, embed_batch, fun(Texts, _Config) ->
        Self ! {batch_call, length(Texts)},
        Vecs = [make_vector(T, 768) || T <- Texts],
        {ok, Vecs}
    end),

    {ok, State} = barrel_embed:init(#{
        embedder => {local, #{}},
        batch_size => 2
    }),
    Texts = [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>],
    {ok, Vecs} = barrel_embed:embed_batch(Texts, State),

    ?assertEqual(5, length(Vecs)),

    %% Should have made 3 batch calls: 2, 2, 1
    Sizes = collect_messages(),
    ?assertEqual([2, 2, 1], Sizes).

test_dimension() ->
    {ok, State} = barrel_embed:init(#{
        embedder => {local, #{}},
        dimensions => 512
    }),
    ?assertEqual(512, barrel_embed:dimension(State)).

test_dimension_no_embedder() ->
    {ok, State} = barrel_embed:init(#{}),
    ?assertEqual(undefined, barrel_embed:dimension(State)).

test_info() ->
    {ok, State} = barrel_embed:init(#{embedder => {local, #{}}}),
    Info = barrel_embed:info(State),
    ?assertEqual(true, maps:get(configured, Info)),
    ?assert(maps:is_key(providers, Info)),
    ?assert(maps:is_key(dimension, Info)),
    [Provider | _] = maps:get(providers, Info),
    ?assertEqual(local, maps:get(name, Provider)).

test_info_no_embedder() ->
    {ok, State} = barrel_embed:init(#{}),
    Info = barrel_embed:info(State),
    ?assertEqual(false, maps:get(configured, Info)).

test_fallback() ->
    %% Create a mock failing provider
    meck:new(barrel_embed_ollama, [passthrough]),
    meck:expect(barrel_embed_ollama, init, fun(Config) ->
        {ok, Config}
    end),
    meck:expect(barrel_embed_ollama, name, fun() -> ollama end),
    meck:expect(barrel_embed_ollama, available, fun(_) -> true end),
    meck:expect(barrel_embed_ollama, embed, fun(_, _) ->
        {error, connection_failed}
    end),

    Config = #{
        embedder => [
            {ollama, #{}},
            {local, #{}}
        ]
    },
    {ok, State} = barrel_embed:init(Config),

    %% Should fall back to local after ollama fails
    {ok, Vec} = barrel_embed:embed(<<"test">>, State),
    ?assertEqual(768, length(Vec)).

test_all_fail() ->
    meck:expect(barrel_embed_local, embed, fun(_, _) ->
        {error, failed}
    end),

    {ok, State} = barrel_embed:init(#{embedder => {local, #{}}}),
    Result = barrel_embed:embed(<<"test">>, State),
    ?assertEqual({error, all_providers_failed}, Result).

%%====================================================================
%% Helpers
%%====================================================================

%% Generate deterministic vector from text
make_vector(Text, Dim) ->
    Hash = erlang:phash2(Text, 1000000),
    [((Hash + I) rem 1000) / 1000.0 || I <- lists:seq(1, Dim)].

%% Collect batch_call messages
collect_messages() ->
    collect_messages([]).

collect_messages(Acc) ->
    receive
        {batch_call, Size} -> collect_messages([Size | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.
