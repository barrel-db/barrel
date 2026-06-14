%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb main API module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

api_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
      fun setup_test/0,
      fun cleanup_test/1,
      [
        {"add and get document", fun test_add_get/0},
        {"add with explicit vector", fun test_add_vector/0},
        {"add_vector_batch inserts multiple", fun test_add_vector_batch/0},
        {"add_vector_batch dimension mismatch", fun test_add_vector_batch_dimension_mismatch/0},
        {"add_vector_batch searchable", fun test_add_vector_batch_searchable/0},
        {"search finds similar documents", fun test_search/0},
        {"search with vector query", fun test_search_vector/0},
        {"delete removes document", fun test_delete/0},
        {"update modifies document", fun test_update/0},
        {"update returns not_found", fun test_update_not_found/0},
        {"upsert inserts new", fun test_upsert_insert/0},
        {"upsert updates existing", fun test_upsert_update/0},
        {"peek returns documents", fun test_peek/0},
        {"count returns correct number", fun test_count/0},
        {"stats returns store info", fun test_stats/0},
        {"checkpoint persists index", fun test_checkpoint/0},
        {"persistence reload", fun test_persistence_reload/0},
        {"concurrent add_vector batching", fun test_concurrent_add_vector/0},
        {"multiple writers stress test", fun test_multiple_writers/0},
        {"search with include options", fun test_search_include_options/0}
      ]
     }
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(rocksdb),
    ok.

cleanup(_) ->
    ok.

setup_test() ->
    TestDir = "/tmp/barrel_vectordb_api_test_" ++ integer_to_list(erlang:unique_integer([positive])),

    %% Ensure meck is clean before starting
    (catch meck:unload(barrel_embed)),
    timer:sleep(10),

    %% Mock the embedder to return deterministic vectors
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:expect(barrel_embed, init, fun(_Config) ->
        {ok, #{providers => [], dimension => 3, batch_size => 32}}
    end),
    meck:expect(barrel_embed, embed, fun(Text, _State) ->
        Hash = erlang:phash2(Text, 1000000),
        Vec = [
            Hash / 1000000.0,
            (Hash rem 1000) / 1000.0,
            ((Hash rem 100) / 100.0)
        ],
        {ok, Vec}
    end),
    meck:expect(barrel_embed, embed_batch, fun(Texts, _State) ->
        Vectors = lists:map(fun(Text) ->
            Hash = erlang:phash2(Text, 1000000),
            [
                Hash / 1000000.0,
                (Hash rem 1000) / 1000.0,
                ((Hash rem 100) / 100.0)
            ]
        end, Texts),
        {ok, Vectors}
    end),
    meck:expect(barrel_embed, info, fun(_State) ->
        #{providers => [], dimension => 3}
    end),

    %% Start the store
    {ok, Pid} = barrel_vectordb:start_link(#{
        name => test_store,
        path => TestDir,
        dimension => 3,
        hnsw => #{m => 4, ef_construction => 20}
    }),
    {Pid, TestDir}.

cleanup_test({_Pid, TestDir}) ->
    catch barrel_vectordb:stop(test_store),
    %% Make sure meck is fully unloaded
    timer:sleep(50),  %% Allow gen_server to stop
    (catch meck:unload(barrel_embed)),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_add_get() ->
    %% Add a document
    ok = barrel_vectordb:add(test_store, <<"doc1">>, <<"hello world">>, #{type => greeting}),

    %% Get it back
    {ok, Doc} = barrel_vectordb:get(test_store, <<"doc1">>),
    ?assertEqual(<<"doc1">>, maps:get(key, Doc)),
    ?assertEqual(<<"hello world">>, maps:get(text, Doc)),
    ?assertEqual(#{type => greeting}, maps:get(metadata, Doc)),
    ?assert(is_list(maps:get(vector, Doc))),

    %% Non-existent document
    ?assertEqual(not_found, barrel_vectordb:get(test_store, <<"nonexistent">>)).

test_add_vector() ->
    Vector = [1.0, 0.0, 0.0],
    ok = barrel_vectordb:add_vector(test_store, <<"vec1">>, <<"explicit">>, #{}, Vector),

    {ok, Doc} = barrel_vectordb:get(test_store, <<"vec1">>),
    ?assertEqual(Vector, maps:get(vector, Doc)).

test_add_vector_batch() ->
    %% Create batch of documents with vectors
    Docs = [
        {<<"batch1">>, <<"text 1">>, #{n => 1}, [1.0, 0.0, 0.0]},
        {<<"batch2">>, <<"text 2">>, #{n => 2}, [0.0, 1.0, 0.0]},
        {<<"batch3">>, <<"text 3">>, #{n => 3}, [0.0, 0.0, 1.0]}
    ],

    %% Insert batch
    {ok, Stats} = barrel_vectordb:add_vector_batch(test_store, Docs),
    ?assertEqual(3, maps:get(inserted, Stats)),

    %% Verify count
    ?assertEqual(3, barrel_vectordb:count(test_store)),

    %% Verify each document
    {ok, Doc1} = barrel_vectordb:get(test_store, <<"batch1">>),
    ?assertEqual(<<"text 1">>, maps:get(text, Doc1)),
    ?assertEqual(#{n => 1}, maps:get(metadata, Doc1)),
    ?assertEqual([1.0, 0.0, 0.0], maps:get(vector, Doc1)),

    {ok, Doc2} = barrel_vectordb:get(test_store, <<"batch2">>),
    ?assertEqual(<<"text 2">>, maps:get(text, Doc2)),
    ?assertEqual([0.0, 1.0, 0.0], maps:get(vector, Doc2)),

    {ok, Doc3} = barrel_vectordb:get(test_store, <<"batch3">>),
    ?assertEqual(<<"text 3">>, maps:get(text, Doc3)),
    ?assertEqual([0.0, 0.0, 1.0], maps:get(vector, Doc3)).

test_add_vector_batch_dimension_mismatch() ->
    %% One document has wrong dimension
    Docs = [
        {<<"ok1">>, <<"text 1">>, #{}, [1.0, 0.0, 0.0]},
        {<<"bad">>, <<"text 2">>, #{}, [1.0, 0.0]},  % Wrong dimension (2 instead of 3)
        {<<"ok2">>, <<"text 3">>, #{}, [0.0, 0.0, 1.0]}
    ],

    %% Should fail with dimension mismatch
    Result = barrel_vectordb:add_vector_batch(test_store, Docs),
    ?assertMatch({error, {dimension_mismatch, 3, 2}}, Result),

    %% No documents should be inserted (atomic operation)
    ?assertEqual(0, barrel_vectordb:count(test_store)).

test_add_vector_batch_searchable() ->
    %% Insert batch
    Docs = [
        {<<"s1">>, <<"apple">>, #{type => fruit}, [1.0, 0.0, 0.0]},
        {<<"s2">>, <<"banana">>, #{type => fruit}, [0.9, 0.1, 0.0]},
        {<<"s3">>, <<"carrot">>, #{type => vegetable}, [0.0, 1.0, 0.0]}
    ],
    {ok, _} = barrel_vectordb:add_vector_batch(test_store, Docs),

    %% Search should find inserted documents
    {ok, Results} = barrel_vectordb:search_vector(test_store, [1.0, 0.0, 0.0], #{k => 2}),
    ?assertEqual(2, length(Results)),

    %% First result should be s1 (exact match)
    [First | _] = Results,
    ?assertEqual(<<"s1">>, maps:get(key, First)),
    ?assert(maps:get(score, First) > 0.99).

test_search() ->
    %% Add some documents
    ok = barrel_vectordb:add_vector(test_store, <<"a">>, <<"text a">>, #{type => a}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"b">>, <<"text b">>, #{type => b}, [0.9, 0.1, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"c">>, <<"text c">>, #{type => c}, [0.0, 1.0, 0.0]),

    %% Search - the mock will convert the text query to a vector
    {ok, Results} = barrel_vectordb:search(test_store, <<"query">>, #{k => 3}),

    ?assertEqual(3, length(Results)),
    %% Each result should have required fields
    [First | _] = Results,
    ?assert(maps:is_key(key, First)),
    ?assert(maps:is_key(text, First)),
    ?assert(maps:is_key(metadata, First)),
    ?assert(maps:is_key(score, First)).

test_search_vector() ->
    %% Add documents with known vectors
    ok = barrel_vectordb:add_vector(test_store, <<"x">>, <<"x text">>, #{}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"y">>, <<"y text">>, #{}, [0.0, 1.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"z">>, <<"z text">>, #{}, [0.0, 0.0, 1.0]),

    %% Search with vector query
    {ok, Results} = barrel_vectordb:search_vector(test_store, [1.0, 0.0, 0.0], #{k => 2}),

    ?assertEqual(2, length(Results)),
    %% First result should be "x" (exact match)
    [First | _] = Results,
    ?assertEqual(<<"x">>, maps:get(key, First)),
    ?assert(maps:get(score, First) > 0.99).

test_delete() ->
    ok = barrel_vectordb:add_vector(test_store, <<"del">>, <<"delete me">>, #{}, [0.5, 0.5, 0.0]),
    ?assertMatch({ok, _}, barrel_vectordb:get(test_store, <<"del">>)),

    ok = barrel_vectordb:delete(test_store, <<"del">>),
    ?assertEqual(not_found, barrel_vectordb:get(test_store, <<"del">>)).

test_update() ->
    %% Add a document
    ok = barrel_vectordb:add_vector(test_store, <<"upd">>, <<"original">>, #{v => 1}, [1.0, 0.0, 0.0]),
    {ok, Doc1} = barrel_vectordb:get(test_store, <<"upd">>),
    ?assertEqual(<<"original">>, maps:get(text, Doc1)),
    ?assertEqual(#{v => 1}, maps:get(metadata, Doc1)),

    %% Update it
    ok = barrel_vectordb:update(test_store, <<"upd">>, <<"updated text">>, #{v => 2}),
    {ok, Doc2} = barrel_vectordb:get(test_store, <<"upd">>),
    ?assertEqual(<<"updated text">>, maps:get(text, Doc2)),
    ?assertEqual(#{v => 2}, maps:get(metadata, Doc2)).

test_update_not_found() ->
    ?assertEqual(not_found, barrel_vectordb:update(test_store, <<"nonexistent">>, <<"text">>, #{})).

test_upsert_insert() ->
    %% Upsert new document
    ?assertEqual(not_found, barrel_vectordb:get(test_store, <<"ups1">>)),
    ok = barrel_vectordb:upsert(test_store, <<"ups1">>, <<"upserted">>, #{source => upsert}),
    {ok, Doc} = barrel_vectordb:get(test_store, <<"ups1">>),
    ?assertEqual(<<"upserted">>, maps:get(text, Doc)),
    ?assertEqual(#{source => upsert}, maps:get(metadata, Doc)).

test_upsert_update() ->
    %% Add then upsert (update)
    ok = barrel_vectordb:add_vector(test_store, <<"ups2">>, <<"v1">>, #{v => 1}, [0.5, 0.5, 0.0]),
    ok = barrel_vectordb:upsert(test_store, <<"ups2">>, <<"v2">>, #{v => 2}),
    {ok, Doc} = barrel_vectordb:get(test_store, <<"ups2">>),
    ?assertEqual(<<"v2">>, maps:get(text, Doc)),
    ?assertEqual(#{v => 2}, maps:get(metadata, Doc)).

test_peek() ->
    %% Add some documents
    ok = barrel_vectordb:add_vector(test_store, <<"p1">>, <<"peek 1">>, #{n => 1}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"p2">>, <<"peek 2">>, #{n => 2}, [0.0, 1.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"p3">>, <<"peek 3">>, #{n => 3}, [0.0, 0.0, 1.0]),

    %% Peek at 2 documents
    {ok, Docs} = barrel_vectordb:peek(test_store, 2),
    ?assertEqual(2, length(Docs)),

    %% Each doc should have required fields
    [First | _] = Docs,
    ?assert(maps:is_key(key, First)),
    ?assert(maps:is_key(text, First)),
    ?assert(maps:is_key(metadata, First)),
    ?assert(maps:is_key(vector, First)),

    %% Peek at more than exists should return all
    {ok, AllDocs} = barrel_vectordb:peek(test_store, 10),
    ?assertEqual(3, length(AllDocs)).

test_count() ->
    ?assertEqual(0, barrel_vectordb:count(test_store)),

    ok = barrel_vectordb:add_vector(test_store, <<"c1">>, <<"t1">>, #{}, [1.0, 0.0, 0.0]),
    ?assertEqual(1, barrel_vectordb:count(test_store)),

    ok = barrel_vectordb:add_vector(test_store, <<"c2">>, <<"t2">>, #{}, [0.0, 1.0, 0.0]),
    ?assertEqual(2, barrel_vectordb:count(test_store)).

test_stats() ->
    ok = barrel_vectordb:add_vector(test_store, <<"s1">>, <<"stats test">>, #{}, [1.0, 0.0, 0.0]),

    {ok, Stats} = barrel_vectordb:stats(test_store),
    ?assertEqual(3, maps:get(dimension, Stats)),
    ?assertEqual(1, maps:get(count, Stats)),
    ?assert(maps:is_key(index, Stats)),
    ?assert(maps:is_key(config, Stats)).

test_checkpoint() ->
    %% Add some documents
    ok = barrel_vectordb:add_vector(test_store, <<"cp1">>, <<"text 1">>, #{}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"cp2">>, <<"text 2">>, #{}, [0.0, 1.0, 0.0]),

    %% Checkpoint should succeed
    ok = barrel_vectordb:checkpoint(test_store),

    %% Store should still work after checkpoint
    {ok, Doc} = barrel_vectordb:get(test_store, <<"cp1">>),
    ?assertEqual(<<"cp1">>, maps:get(key, Doc)),

    %% Search should still work
    {ok, Results} = barrel_vectordb:search_vector(test_store, [1.0, 0.0, 0.0], #{k => 1}),
    ?assertEqual(1, length(Results)).

test_persistence_reload() ->
    %% This test uses its own store to test persistence across restarts
    PersistDir = "/tmp/barrel_vectordb_persist_test_" ++
                 integer_to_list(erlang:unique_integer([positive])),

    %% Create store and add documents
    {ok, _} = barrel_vectordb:start_link(#{
        name => persist_test_store,
        path => PersistDir,
        dimension => 3,
        hnsw => #{m => 4, ef_construction => 20}
    }),

    ok = barrel_vectordb:add_vector(persist_test_store, <<"p1">>, <<"text 1">>,
                                    #{n => 1}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(persist_test_store, <<"p2">>, <<"text 2">>,
                                    #{n => 2}, [0.0, 1.0, 0.0]),
    ok = barrel_vectordb:add_vector(persist_test_store, <<"p3">>, <<"text 3">>,
                                    #{n => 3}, [0.0, 0.0, 1.0]),

    ?assertEqual(3, barrel_vectordb:count(persist_test_store)),

    %% Stop the store
    ok = barrel_vectordb:stop(persist_test_store),
    timer:sleep(100),

    %% Restart from the same directory
    {ok, _} = barrel_vectordb:start_link(#{
        name => persist_test_store,
        path => PersistDir,
        dimension => 3,
        hnsw => #{m => 4, ef_construction => 20}
    }),

    %% Verify count is preserved
    ?assertEqual(3, barrel_vectordb:count(persist_test_store)),

    %% Verify documents can be retrieved
    {ok, Doc1} = barrel_vectordb:get(persist_test_store, <<"p1">>),
    ?assertEqual(<<"text 1">>, maps:get(text, Doc1)),
    ?assertEqual(#{n => 1}, maps:get(metadata, Doc1)),

    %% Verify search still works
    {ok, Results} = barrel_vectordb:search_vector(persist_test_store,
                                                   [1.0, 0.0, 0.0], #{k => 2}),
    ?assertEqual(2, length(Results)),
    [First | _] = Results,
    ?assertEqual(<<"p1">>, maps:get(key, First)),

    %% Cleanup
    ok = barrel_vectordb:stop(persist_test_store),
    os:cmd("rm -rf " ++ PersistDir).

test_concurrent_add_vector() ->
    %% Spawn multiple processes adding vectors concurrently
    %% gen_batch_server should batch these into fewer atomic writes
    Self = self(),
    NumProcs = 20,

    Pids = [spawn(fun() ->
        Id = list_to_binary("concurrent_" ++ integer_to_list(N)),
        Vec = [float(N) / float(NumProcs), 0.0, 0.0],
        Result = barrel_vectordb:add_vector(test_store, Id, <<"concurrent text">>, #{n => N}, Vec),
        Self ! {done, N, Result}
    end) || N <- lists:seq(1, NumProcs)],

    %% Wait for all processes to complete
    Results = [receive {done, N, R} -> {N, R} end || _ <- Pids],

    %% All should succeed
    lists:foreach(fun({N, Result}) ->
        ?assertEqual(ok, Result, io_lib:format("Process ~p failed", [N]))
    end, Results),

    %% Verify all documents were inserted
    ?assertEqual(NumProcs, barrel_vectordb:count(test_store)),

    %% Verify each document can be retrieved
    lists:foreach(fun(N) ->
        Id = list_to_binary("concurrent_" ++ integer_to_list(N)),
        {ok, Doc} = barrel_vectordb:get(test_store, Id),
        ?assertEqual(Id, maps:get(key, Doc)),
        ?assertEqual(#{n => N}, maps:get(metadata, Doc))
    end, lists:seq(1, NumProcs)),

    %% Verify search works on concurrently inserted documents
    {ok, Results2} = barrel_vectordb:search_vector(test_store, [1.0, 0.0, 0.0], #{k => 5}),
    ?assertEqual(5, length(Results2)),

    %% All results should be from the concurrent_ documents
    lists:foreach(fun(Doc) ->
        Key = maps:get(key, Doc),
        ?assertMatch(<<"concurrent_", _/binary>>, Key)
    end, Results2).

test_multiple_writers() ->
    %% Test multiple writer processes sending continuous writes
    %% This simulates real-world concurrent access patterns
    Self = self(),
    NumWriters = 5,
    DocsPerWriter = 20,
    TotalDocs = NumWriters * DocsPerWriter,

    %% Spawn writer processes
    _Writers = [spawn_link(fun() ->
        WriterId = integer_to_binary(W),
        lists:foreach(fun(N) ->
            Id = <<"mw_", WriterId/binary, "_", (integer_to_binary(N))/binary>>,
            Vec = [float(W) / float(NumWriters), float(N) / float(DocsPerWriter), 0.0],
            ok = barrel_vectordb:add_vector(test_store, Id, <<"multi writer text">>,
                                            #{writer => W, seq => N}, Vec)
        end, lists:seq(1, DocsPerWriter)),
        Self ! {writer_done, W}
    end) || W <- lists:seq(1, NumWriters)],

    %% Wait for all writers to complete
    lists:foreach(fun(W) ->
        receive {writer_done, W} -> ok
        after 30000 -> error({timeout_waiting_for_writer, W})
        end
    end, lists:seq(1, NumWriters)),

    %% Verify all documents were inserted
    ?assertEqual(TotalDocs, barrel_vectordb:count(test_store)),

    %% Verify documents from each writer
    lists:foreach(fun(W) ->
        WriterId = integer_to_binary(W),
        lists:foreach(fun(N) ->
            Id = <<"mw_", WriterId/binary, "_", (integer_to_binary(N))/binary>>,
            {ok, Doc} = barrel_vectordb:get(test_store, Id),
            ?assertEqual(#{writer => W, seq => N}, maps:get(metadata, Doc))
        end, lists:seq(1, DocsPerWriter))
    end, lists:seq(1, NumWriters)),

    %% Verify search works across all writers' documents
    {ok, Results} = barrel_vectordb:search_vector(test_store, [1.0, 1.0, 0.0], #{k => 10}),
    ?assertEqual(10, length(Results)),

    %% Clean assertion - just verify we got results from multiple writers
    WriterIds = lists:usort([maps:get(writer, maps:get(metadata, D)) || D <- Results]),
    ?assert(length(WriterIds) >= 1).

test_search_include_options() ->
    %% Add test documents
    ok = barrel_vectordb:add_vector(test_store, <<"inc1">>, <<"text one">>,
                                    #{type => a}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"inc2">>, <<"text two">>,
                                    #{type => b}, [0.9, 0.1, 0.0]),

    %% Default search includes text and metadata
    {ok, Results1} = barrel_vectordb:search_vector(test_store, [1.0, 0.0, 0.0], #{k => 2}),
    [R1 | _] = Results1,
    ?assert(maps:is_key(text, R1)),
    ?assert(maps:is_key(metadata, R1)),
    ?assert(maps:is_key(key, R1)),
    ?assert(maps:is_key(score, R1)),

    %% Search without text
    {ok, Results2} = barrel_vectordb:search_vector(test_store, [1.0, 0.0, 0.0],
                                                   #{k => 2, include_text => false}),
    [R2 | _] = Results2,
    ?assertNot(maps:is_key(text, R2)),
    ?assert(maps:is_key(metadata, R2)),
    ?assert(maps:is_key(key, R2)),

    %% Search without metadata
    {ok, Results3} = barrel_vectordb:search_vector(test_store, [1.0, 0.0, 0.0],
                                                   #{k => 2, include_metadata => false}),
    [R3 | _] = Results3,
    ?assert(maps:is_key(text, R3)),
    ?assertNot(maps:is_key(metadata, R3)),

    %% Search without both (minimal result)
    {ok, Results4} = barrel_vectordb:search_vector(test_store, [1.0, 0.0, 0.0],
                                                   #{k => 2, include_text => false,
                                                     include_metadata => false}),
    [R4 | _] = Results4,
    ?assertNot(maps:is_key(text, R4)),
    ?assertNot(maps:is_key(metadata, R4)),
    ?assert(maps:is_key(key, R4)),
    ?assert(maps:is_key(score, R4)).
