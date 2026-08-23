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
     fun setup_store/0,
     fun cleanup_store/1,
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

hybrid_test_() ->
    {foreach,
     fun setup_store/0,
     fun cleanup_store/1,
     [
       {"hybrid results carry text and metadata", fun test_hybrid_hydrated/0},
       {"linear fusion is hydrated too", fun test_hybrid_linear_hydrated/0},
       {"include flags are honored", fun test_hybrid_include_flags/0},
       {"index-only entries hydrate as empty metadata", fun test_hybrid_index_only/0},
       {"query_vector skips the embedder", fun test_hybrid_query_vector_skips_embed/0}
     ]
    }.

stray_message_test_() ->
    {foreach,
     fun setup_store/0,
     fun cleanup_store/1,
     [
       {"a bare message does not kill the store", fun test_stray_info/0},
       {"a cast does not kill the store", fun test_stray_cast/0},
       {"an 'EXIT' signal does not kill the store", fun test_stray_exit/0}
     ]
    }.

coalesce_test_() ->
    {foreach,
     fun setup_store/0,
     fun cleanup_store/1,
     [
       {"queued writes coalesce into one batch", fun test_concurrent_writes/0},
       {"reads interleaved with queued writes", fun test_reads_interleaved/0},
       {"a bad write in a batch fails only its caller", fun test_bad_write_in_batch/0}
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

setup_store() ->
    TestDir = mk_dir(),
    mock_embed(),
    {ok, Pid} = start_store(TestDir),
    {Pid, TestDir}.

cleanup_store({_Pid, TestDir}) ->
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

%%====================================================================
%% Test Cases - hybrid hydration + query_vector
%%====================================================================

test_hybrid_hydrated() ->
    ok = barrel_vectordb:add_vector(
        ?STORE, <<"a">>, <<"the quick brown fox">>, #{kind => fox},
        [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(
        ?STORE, <<"b">>, <<"lazy dogs sleep">>, #{kind => dog},
        [0.0, 1.0, 0.0]),
    {ok, Results} = barrel_vectordb:search_hybrid(
        ?STORE, <<"quick fox">>,
        #{k => 2, query_vector => [1.0, 0.0, 0.0]}),
    ?assert(length(Results) >= 1),
    [Top | _] = Results,
    ?assertEqual(<<"a">>, maps:get(key, Top)),
    ?assertEqual(<<"the quick brown fox">>, maps:get(text, Top)),
    ?assertEqual(#{kind => fox}, maps:get(metadata, Top)),
    ?assert(is_float(maps:get(score, Top))).

test_hybrid_linear_hydrated() ->
    ok = barrel_vectordb:add_vector(
        ?STORE, <<"a">>, <<"alpha bravo">>, #{n => 1}, [1.0, 0.0, 0.0]),
    {ok, [Top | _]} = barrel_vectordb:search_hybrid(
        ?STORE, <<"alpha">>,
        #{k => 1, fusion => linear, query_vector => [1.0, 0.0, 0.0]}),
    ?assertEqual(<<"alpha bravo">>, maps:get(text, Top)),
    ?assertEqual(#{n => 1}, maps:get(metadata, Top)).

test_hybrid_include_flags() ->
    ok = barrel_vectordb:add_vector(
        ?STORE, <<"a">>, <<"alpha bravo">>, #{n => 1}, [1.0, 0.0, 0.0]),
    {ok, [Bare | _]} = barrel_vectordb:search_hybrid(
        ?STORE, <<"alpha">>,
        #{k => 1, query_vector => [1.0, 0.0, 0.0],
          include_text => false, include_metadata => false}),
    ?assertEqual(false, maps:is_key(text, Bare)),
    ?assertEqual(false, maps:is_key(metadata, Bare)),
    ?assert(maps:is_key(key, Bare)),
    ?assert(maps:is_key(score, Bare)).

test_hybrid_index_only() ->
    %% Index-only entries have no stored doc data: they must still rank,
    %% with empty metadata and no text.
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"alpha bravo">>, [1.0, 0.0, 0.0]),
    {ok, [Top | _]} = barrel_vectordb:search_hybrid(
        ?STORE, <<"alpha">>,
        #{k => 1, query_vector => [1.0, 0.0, 0.0]}),
    ?assertEqual(<<"a">>, maps:get(key, Top)),
    ?assertEqual(#{}, maps:get(metadata, Top)),
    ?assertEqual(false, maps:is_key(text, Top)).

test_hybrid_query_vector_skips_embed() ->
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"alpha">>, [1.0, 0.0, 0.0]),
    %% The mocked embedder always fails, so hybrid WITHOUT query_vector
    %% propagates the embed error...
    ?assertMatch({error, _},
                 barrel_vectordb:search_hybrid(?STORE, <<"alpha">>, #{k => 1})),
    %% ...and WITH query_vector it succeeds without touching the embedder.
    {ok, [_ | _]} = barrel_vectordb:search_hybrid(
        ?STORE, <<"alpha">>, #{k => 1, query_vector => [1.0, 0.0, 0.0]}),
    ok.

%%====================================================================
%% Stray messages (init traps exits, so 'EXIT' signals arrive as
%% handle_info messages)
%%====================================================================

test_stray_info() ->
    Pid = whereis_store(),
    Pid ! {unexpected, make_ref()},
    assert_store_alive(Pid).

test_stray_cast() ->
    Pid = whereis_store(),
    ok = gen_server:cast(Pid, unexpected_cast),
    assert_store_alive(Pid).

test_stray_exit() ->
    Pid = whereis_store(),
    %% What a port opened (and ended) inside the store delivers under
    %% trap_exit; a non-normal reason must not kill it either.
    Pid ! {'EXIT', self(), normal},
    Pid ! {'EXIT', self(), {shutdown, stray}},
    assert_store_alive(Pid).

%%====================================================================
%% Write coalescing (sys:suspend queues the calls so the first write
%% drains the others in one batch)
%%====================================================================

test_concurrent_writes() ->
    N = 50,
    Results = run_queued([fun() ->
        barrel_vectordb:add_index_only(?STORE, integer_to_binary(I), <<"t">>, angle_vec(I, N))
    end || I <- lists:seq(1, N)]),
    ?assertEqual(lists:duplicate(N, ok), Results),
    ?assertEqual(N, barrel_vectordb:count(?STORE)),
    {ok, Hits} = barrel_vectordb:search_vector(?STORE, angle_vec(7, N), #{k => 3}),
    ?assertEqual(3, length(Hits)).

test_reads_interleaved() ->
    N = 20,
    Calls = lists:append([[fun() ->
        barrel_vectordb:add_index_only(?STORE, integer_to_binary(I), <<"t">>, angle_vec(I, N))
    end, fun() -> barrel_vectordb:count(?STORE) end] || I <- lists:seq(1, N)]),
    Results = run_queued(Calls),
    Writes = [R || R <- Results, R =:= ok],
    Counts = [R || R <- Results, is_integer(R)],
    ?assertEqual(N, length(Writes)),
    ?assertEqual(N, length(Counts)),
    ?assertEqual(N, barrel_vectordb:count(?STORE)).

test_bad_write_in_batch() ->
    Results = run_queued([
        fun() -> barrel_vectordb:add_index_only(?STORE, <<"a">>, <<"t">>, [1.0, 0.0, 0.0]) end,
        fun() -> barrel_vectordb:add_index_only(?STORE, <<"bad">>, <<"t">>, [1.0, 0.0]) end,
        fun() -> barrel_vectordb:add_index_only(?STORE, <<"b">>, <<"t">>, [0.0, 1.0, 0.0]) end]),
    ?assertMatch([ok, {error, {dimension_mismatch, 3, 2}}, ok], Results),
    ?assertEqual(2, barrel_vectordb:count(?STORE)).

%% Suspend the store, issue the calls so they queue in arrival order,
%% resume, and collect the replies in the same order.
run_queued(Funs) ->
    Pid = whereis_store(),
    ok = sys:suspend(Pid),
    Self = self(),
    Refs = [begin
        Ref = make_ref(),
        spawn_link(fun() -> Self ! {Ref, F()} end),
        Ref
    end || F <- Funs],
    wait_queued(Pid, length(Funs)),
    ok = sys:resume(Pid),
    [receive {Ref, R} -> R after 10000 -> error({timeout, Ref}) end || Ref <- Refs].

wait_queued(Pid, N) ->
    case erlang:process_info(Pid, message_queue_len) of
        {message_queue_len, L} when L >= N -> ok;
        _ -> timer:sleep(5), wait_queued(Pid, N)
    end.

angle_vec(I, N) ->
    A = 2 * math:pi() * I / N,
    [math:cos(A), math:sin(A), 0.0].

whereis_store() ->
    Pid = barrel_vectordb_registry:whereis_name(
        {vstore, atom_to_binary(?STORE, utf8)}),
    ?assert(is_pid(Pid)),
    Pid.

assert_store_alive(Pid) ->
    ok = barrel_vectordb:add_index_only(
        ?STORE, <<"a">>, <<"text">>, [1.0, 0.0, 0.0]),
    ?assertEqual(1, barrel_vectordb:count(?STORE)),
    ?assert(is_process_alive(Pid)).
