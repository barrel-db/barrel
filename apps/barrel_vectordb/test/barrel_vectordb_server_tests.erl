%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_server write paths, currently
%%% the index-only path (add_index_only/4, add_index_only_batch/2).
%%%
%%% Named after the server module so app-scoped eunit discovers it:
%%% rebar3 eunit --app only runs test modules paired with a source
%%% module by the _tests suffix.
%%%
%%% Index-only writes store the vector (authoritative for rebuild) and
%%% feed BM25 transiently, without persisting text/metadata. Used by
%%% callers that own document storage elsewhere.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_server_tests).

-include_lib("eunit/include/eunit.hrl").

-define(STORE, index_only_store).

%%====================================================================
%% Test Generators
%%====================================================================

index_only_test_() ->
    {foreach,
     fun setup_test/0,
     fun cleanup_test/1,
     [
       {"index_only vector is searchable", fun test_search_hit/0},
       {"get on index-only id reports incomplete data", fun test_get_incomplete/0},
       {"re-add same id is an upsert", fun test_upsert/0},
       {"delete removes an index-only entry", fun test_delete/0},
       {"stale text/meta from a full add are cleared", fun test_clears_stale_docdata/0},
       {"dimension mismatch is rejected", fun test_dimension_mismatch/0},
       {"batch form indexes all entries", fun test_batch/0},
       {"bm25 finds index-only text", fun test_bm25/0}
     ]
    }.

restart_rebuild_test() ->
    %% Standalone (owns its dir): the ANN index rebuilds from the vectors
    %% CF on restart, which includes index-only rows.
    TestDir = mk_dir(),
    mock_embed(),
    try
        {ok, _} = start_store(TestDir),
        ok = barrel_vectordb:add_index_only(
            ?STORE, <<"a">>, <<"text">>, [1.0, 0.0, 0.0]),
        ok = barrel_vectordb:stop(?STORE),
        timer:sleep(50),
        {ok, _} = start_store(TestDir),
        {ok, [#{key := <<"a">>}]} = barrel_vectordb:search_vector(
            ?STORE, [1.0, 0.0, 0.0], #{k => 1}),
        ok
    after
        catch barrel_vectordb:stop(?STORE),
        timer:sleep(50),
        unmock_embed(),
        os:cmd("rm -rf " ++ TestDir)
    end.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup_test() ->
    TestDir = mk_dir(),
    mock_embed(),
    {ok, Pid} = start_store(TestDir),
    {Pid, TestDir}.

cleanup_test({_Pid, TestDir}) ->
    catch barrel_vectordb:stop(?STORE),
    timer:sleep(50),
    unmock_embed(),
    os:cmd("rm -rf " ++ TestDir),
    ok.

mk_dir() ->
    "/tmp/barrel_vectordb_index_only_"
        ++ integer_to_list(erlang:unique_integer([positive])).

start_store(TestDir) ->
    application:ensure_all_started(rocksdb),
    barrel_vectordb:start_link(#{
        name => ?STORE,
        path => TestDir,
        dimension => 3,
        bm25_backend => memory,
        hnsw => #{m => 4, ef_construction => 20}
    }).

%% Index-only writes never embed, but the store initializes barrel_embed
%% at startup; mock it like the other suites do.
mock_embed() ->
    (catch meck:unload(barrel_embed)),
    timer:sleep(10),
    meck:new(barrel_embed, [non_strict, no_link]),
    meck:expect(barrel_embed, init, fun(_Config) ->
        {ok, #{providers => [], dimension => 3, batch_size => 32}}
    end),
    meck:expect(barrel_embed, embed, fun(_Text, _State) ->
        {error, no_embedder_in_index_only_tests}
    end),
    meck:expect(barrel_embed, embed_batch, fun(_Texts, _State) ->
        {error, no_embedder_in_index_only_tests}
    end),
    meck:expect(barrel_embed, info, fun(_State) ->
        #{providers => [], dimension => 3}
    end).

unmock_embed() ->
    (catch meck:unload(barrel_embed)),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_search_hit() ->
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"hello world">>, [1.0, 0.0, 0.0]),
    Results = barrel_vectordb:search_vector(?STORE, [1.0, 0.0, 0.0], #{k => 1}),
    ?assertMatch({ok, [#{key := <<"a">>}]}, Results).

test_get_incomplete() ->
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"text">>, [1.0, 0.0, 0.0]),
    %% No stored text/metadata: the vector exists but doc data does not.
    ?assertEqual({error, incomplete_data}, barrel_vectordb:get(?STORE, <<"a">>)).

test_upsert() ->
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"t1">>, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"t2">>, [0.0, 1.0, 0.0]),
    Count = barrel_vectordb:count(?STORE),
    ?assertEqual(1, Count),
    %% The updated vector wins the search
    {ok, [#{key := <<"a">>}]} = barrel_vectordb:search_vector(
        ?STORE, [0.0, 1.0, 0.0], #{k => 1}),
    ok.

test_delete() ->
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"text">>, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:delete(?STORE, <<"a">>),
    ?assertEqual(not_found, barrel_vectordb:get(?STORE, <<"a">>)),
    Count = barrel_vectordb:count(?STORE),
    ?assertEqual(0, Count).

test_clears_stale_docdata() ->
    %% Full add stores text/metadata in the store's CFs
    ok = barrel_vectordb:add_vector(
        ?STORE, <<"a">>, <<"old text">>, #{v => 1}, [1.0, 0.0, 0.0]),
    {ok, #{text := <<"old text">>}} = barrel_vectordb:get(?STORE, <<"a">>),
    %% Index-only re-add clears the stale rows in the same batch
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"new text">>, [0.0, 1.0, 0.0]),
    ?assertEqual({error, incomplete_data}, barrel_vectordb:get(?STORE, <<"a">>)).

test_dimension_mismatch() ->
    ?assertMatch({error, {dimension_mismatch, 3, 2}},
                 barrel_vectordb:add_index_only(
                     ?STORE, <<"a">>, <<"t">>, [1.0, 0.0])),
    ?assertMatch({error, {dimension_mismatch, 3, 4}},
                 barrel_vectordb:add_index_only_batch(
                     ?STORE, [{<<"b">>, <<"t">>, [1.0, 0.0, 0.0, 0.0]}])).

test_batch() ->
    Entries = [
        {<<"a">>, <<"alpha">>, [1.0, 0.0, 0.0]},
        {<<"b">>, <<"beta">>, [0.0, 1.0, 0.0]},
        {<<"c">>, <<>>, [0.0, 0.0, 1.0]}
    ],
    {ok, #{inserted := 3}} =
        barrel_vectordb:add_index_only_batch(?STORE, Entries),
    Count = barrel_vectordb:count(?STORE),
    ?assertEqual(3, Count),
    {ok, [#{key := <<"b">>}]} = barrel_vectordb:search_vector(
        ?STORE, [0.0, 1.0, 0.0], #{k => 1}),
    ok.

test_bm25() ->
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"the quick brown fox">>, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"b">>, <<"lazy dogs sleep">>, [0.0, 1.0, 0.0]),
    {ok, Hits} = barrel_vectordb:search_bm25(?STORE, <<"quick fox">>, #{k => 2}),
    ?assertMatch([{<<"a">>, _Score} | _], Hits).
