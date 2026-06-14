%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_store module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_store_tests).

-include_lib("eunit/include/eunit.hrl").
-include("barrel_vectordb.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

%% Note: These tests require RocksDB and are integration tests.
%% They will create/delete a test database directory.

store_test_() ->
    {setup,
     fun setup_store/0,
     fun cleanup_store/1,
     {foreach,
      fun setup_test/0,
      fun cleanup_test/1,
      [
        {"add with explicit vector", fun test_add_explicit_vector/0},
        {"get returns stored data", fun test_get/0},
        {"delete removes data", fun test_delete/0},
        {"search finds similar vectors", fun test_search/0},
        {"count returns correct number", fun test_count/0}
      ]
     }
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup_store() ->
    %% Start required applications
    application:ensure_all_started(rocksdb),
    ok.

cleanup_store(_) ->
    ok.

setup_test() ->
    %% Create unique test directory
    TestDir = "/tmp/barrel_vectordb_test_" ++ integer_to_list(erlang:unique_integer([positive])),
    Config = #{
        db_path => TestDir,
        dimension => 3,
        hnsw => #{m => 4, ef_construction => 20}
    },

    %% Ensure meck is clean before starting - be thorough
    (catch meck:unload(barrel_embed)),
    timer:sleep(10),  %% Allow meck to fully unload

    %% Start a mock embeddings server that just returns dummy vectors
    meck:new(barrel_embed, [non_strict, no_link]),
    meck:expect(barrel_embed, init, fun(_Config) ->
        {ok, #{providers => [], dimension => 3, batch_size => 32}}
    end),
    meck:expect(barrel_embed, embed, fun(Text, _EmbedState) ->
        %% Generate deterministic vector from text
        Hash = erlang:phash2(Text, 1000000),
        Vec = [
            Hash / 1000000.0,
            (Hash rem 1000) / 1000.0,
            ((Hash rem 100) / 100.0)
        ],
        {ok, Vec}
    end),

    %% Start store
    {ok, Pid} = barrel_vectordb_store:start_link(Config),
    {Pid, TestDir}.

cleanup_test({_Pid, TestDir}) ->
    %% Stop store
    catch barrel_vectordb_store:stop(),
    timer:sleep(50),  %% Allow gen_server to stop

    %% Cleanup meck
    (catch meck:unload(barrel_embed)),

    %% Remove test directory
    os:cmd("rm -rf " ++ TestDir),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_add_explicit_vector() ->
    Vector = [1.0, 0.0, 0.0],
    Metadata = #{file => <<"test.txt">>, type => test},

    %% Add with explicit vector
    ok = barrel_vectordb_store:add(<<"chunk1">>, <<"test content">>, Metadata, Vector),

    %% Verify it was stored
    {ok, Result} = barrel_vectordb_store:get(<<"chunk1">>),
    ?assertEqual(<<"chunk1">>, maps:get(key, Result)),
    ?assertEqual(<<"test content">>, maps:get(text, Result)),
    ?assertEqual(Metadata, maps:get(metadata, Result)),
    ?assertEqual(Vector, maps:get(vector, Result)).

test_get() ->
    Vector = [0.5, 0.5, 0.0],
    Metadata = #{file => <<"file.erl">>, start_line => 1, end_line => 10},

    ok = barrel_vectordb_store:add(<<"chunk2">>, <<"some code">>, Metadata, Vector),

    %% Get existing
    {ok, Result} = barrel_vectordb_store:get(<<"chunk2">>),
    ?assertEqual(<<"chunk2">>, maps:get(key, Result)),

    %% Get non-existing
    ?assertEqual(not_found, barrel_vectordb_store:get(<<"nonexistent">>)).

test_delete() ->
    Vector = [0.0, 1.0, 0.0],
    Metadata = #{type => temp},

    ok = barrel_vectordb_store:add(<<"to_delete">>, <<"delete me">>, Metadata, Vector),
    ?assertMatch({ok, _}, barrel_vectordb_store:get(<<"to_delete">>)),

    %% Delete
    ok = barrel_vectordb_store:delete(<<"to_delete">>),
    ?assertEqual(not_found, barrel_vectordb_store:get(<<"to_delete">>)).

test_search() ->
    %% Add some vectors
    Vectors = [
        {<<"a">>, [1.0, 0.0, 0.0], #{type => a}},
        {<<"b">>, [0.9, 0.1, 0.0], #{type => b}},
        {<<"c">>, [0.0, 1.0, 0.0], #{type => c}},
        {<<"d">>, [0.0, 0.0, 1.0], #{type => d}}
    ],

    lists:foreach(
        fun({Id, Vec, Meta}) ->
            ok = barrel_vectordb_store:add(Id, <<"text">>, Meta, Vec)
        end,
        Vectors
    ),

    %% Search with a vector query
    QueryVec = [1.0, 0.0, 0.0],
    {ok, Results} = barrel_vectordb_store:search(QueryVec, 2, #{}),

    ?assertEqual(2, length(Results)),

    %% First result should be "a" (exact match)
    [First | _] = Results,
    ?assertEqual(<<"a">>, maps:get(key, First)),
    ?assert(maps:get(score, First) > 0.99).  %% Score close to 1.0

test_count() ->
    ?assertEqual(0, barrel_vectordb_store:count()),

    ok = barrel_vectordb_store:add(<<"c1">>, <<"t1">>, #{}, [1.0, 0.0, 0.0]),
    ?assertEqual(1, barrel_vectordb_store:count()),

    ok = barrel_vectordb_store:add(<<"c2">>, <<"t2">>, #{}, [0.0, 1.0, 0.0]),
    ?assertEqual(2, barrel_vectordb_store:count()),

    ok = barrel_vectordb_store:delete(<<"c1">>),
    ?assertEqual(1, barrel_vectordb_store:count()).
