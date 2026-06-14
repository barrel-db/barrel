%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_vectordb when used as a library/embedded
%%%
%%% These tests verify that barrel_vectordb can be used as a library
%%% by starting stores directly via barrel_vectordb:start_link/1,
%%% without starting the barrel_vectordb application.
%%%
%%% This pattern is used by projects like barrel_memory that embed
%%% barrel_vectordb in their own supervisor tree.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_library_tests).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_STORE, test_embedded_store).
-define(TEST_PATH, "test_data/embedded_store").

%%====================================================================
%% Test Fixtures
%%====================================================================

library_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [
         {"start store directly without application", fun test_start_store_directly/0},
         {"add and get documents", fun test_add_get/0},
         {"search documents", fun test_search/0},
         {"stop store", fun test_stop_store/0}
     ]}.

setup() ->
    %% Ensure rocksdb is started (required dependency)
    {ok, _} = application:ensure_all_started(rocksdb),
    %% Clean up any previous test data
    os:cmd("rm -rf " ++ ?TEST_PATH),
    ok.

cleanup(_) ->
    %% Stop the store if running
    catch barrel_vectordb:stop(?TEST_STORE),
    %% Clean up test data
    os:cmd("rm -rf " ++ ?TEST_PATH),
    ok.

%%====================================================================
%% Tests
%%====================================================================

%% Test that we can start a store directly via start_link without
%% starting the barrel_vectordb application. This mimics how barrel_memory
%% embeds barrel_vectordb in its own supervisor.
test_start_store_directly() ->
    Config = #{
        name => ?TEST_STORE,
        path => ?TEST_PATH,
        dimension => 128
    },

    %% Start the store directly - this is how barrel_memory does it
    {ok, Pid} = barrel_vectordb:start_link(Config),
    ?assert(is_pid(Pid)),

    %% Verify it's registered
    ?assertEqual(Pid, whereis(?TEST_STORE)),

    %% Verify we can get stats
    {ok, Stats} = barrel_vectordb:stats(?TEST_STORE),
    ?assertMatch(#{dimension := 128}, Stats),
    ok.

%% Test basic add/get operations
test_add_get() ->
    %% Add a document with a pre-computed vector
    Vector = [0.1 || _ <- lists:seq(1, 128)],
    ok = barrel_vectordb:add_vector(?TEST_STORE, <<"doc1">>, <<"test text">>,
                                    #{type => <<"test">>}, Vector),

    %% Get the document
    {ok, Doc} = barrel_vectordb:get(?TEST_STORE, <<"doc1">>),
    ?assertEqual(<<"doc1">>, maps:get(key, Doc)),
    ?assertEqual(<<"test text">>, maps:get(text, Doc)),
    ?assertMatch(#{type := <<"test">>}, maps:get(metadata, Doc)),
    ok.

%% Test search operations
test_search() ->
    %% Add more documents
    Vector1 = [0.1 || _ <- lists:seq(1, 128)],
    Vector2 = [0.2 || _ <- lists:seq(1, 128)],
    ok = barrel_vectordb:add_vector(?TEST_STORE, <<"doc2">>, <<"another document">>,
                                    #{type => <<"semantic">>}, Vector1),
    ok = barrel_vectordb:add_vector(?TEST_STORE, <<"doc3">>, <<"different content">>,
                                    #{type => <<"episodic">>}, Vector2),

    %% Search with a query vector
    QueryVector = [0.1 || _ <- lists:seq(1, 128)],
    {ok, Results} = barrel_vectordb:search_vector(?TEST_STORE, QueryVector, #{k => 5}),

    %% Should get results
    ?assert(length(Results) >= 1),

    %% First result should be the most similar (doc1 or doc2 with same vector)
    [First | _] = Results,
    ?assert(maps:get(score, First) > 0.9),
    ok.

%% Test stopping the store
test_stop_store() ->
    %% Store should be running
    ?assert(is_pid(whereis(?TEST_STORE))),

    %% Stop it
    ok = barrel_vectordb:stop(?TEST_STORE),

    %% Should no longer be registered
    ?assertEqual(undefined, whereis(?TEST_STORE)),
    ok.

%%====================================================================
%% Supervisor Integration Test
%%====================================================================

%% Test that mimics exactly how barrel_memory integrates barrel_vectordb
%% in its own supervisor tree
supervisor_integration_test_() ->
    {setup,
     fun setup_supervisor/0,
     fun cleanup_supervisor/1,
     [
         {"store works under custom supervisor", fun test_supervised_store/0}
     ]}.

setup_supervisor() ->
    {ok, _} = application:ensure_all_started(rocksdb),
    os:cmd("rm -rf test_data/supervised_store"),
    ok.

cleanup_supervisor(_) ->
    catch exit(whereis(test_vectordb_sup), shutdown),
    os:cmd("rm -rf test_data/supervised_store"),
    ok.

test_supervised_store() ->
    %% Start a simple supervisor that manages the vectordb store
    %% This is the pattern barrel_memory uses
    Self = self(),

    SupPid = spawn_link(fun() ->
        %% Register as supervisor
        register(test_vectordb_sup, self()),

        %% Start the store
        StoreConfig = #{
            name => test_supervised_store,
            path => "test_data/supervised_store",
            dimension => 64
        },
        {ok, StorePid} = barrel_vectordb:start_link(StoreConfig),
        link(StorePid),

        Self ! {started, StorePid},

        %% Wait for termination signal
        receive
            stop ->
                barrel_vectordb:stop(test_supervised_store),
                ok
        end
    end),

    %% Wait for store to start
    StorePid = receive
        {started, Pid} -> Pid
    after 5000 ->
        error(timeout_waiting_for_store)
    end,

    ?assert(is_pid(StorePid)),

    %% Use the store
    Vector = [0.5 || _ <- lists:seq(1, 64)],
    ok = barrel_vectordb:add_vector(test_supervised_store, <<"mem1">>,
                                    <<"memory content">>, #{user => <<"test">>}, Vector),

    {ok, Doc} = barrel_vectordb:get(test_supervised_store, <<"mem1">>),
    ?assertEqual(<<"memory content">>, maps:get(text, Doc)),

    %% Stop the supervisor
    SupPid ! stop,
    timer:sleep(100),

    ?assertEqual(undefined, whereis(test_supervised_store)),
    ok.
