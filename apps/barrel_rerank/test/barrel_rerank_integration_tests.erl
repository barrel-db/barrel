%%%-------------------------------------------------------------------
%%% @doc Integration tests for barrel_rerank
%%%
%%% These tests require a working Python environment with transformers
%%% and torch installed. Tests will be skipped if dependencies are not
%%% available.
%%%
%%% Setup:
%%%   The managed venv is auto-created when the application starts,
%%%   or you can manually run: barrel_rerank_venv:ensure_venv().
%%%
%%% Run:
%%%   rebar3 eunit --module=barrel_rerank_integration_tests
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rerank_integration_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Descriptions
%%====================================================================

rerank_integration_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(State) ->
         case State of
             skip ->
                 [];
             _ ->
                 %% Use 60 second timeout for each test (model inference can be slow)
                 [
                     {timeout, 60, {"Initialize reranker", fun() -> test_rerank_init(State) end}},
                     {timeout, 60, {"Basic reranking", fun() -> test_rerank_basic(State) end}},
                     {timeout, 60, {"Rerank with top_k", fun() -> test_rerank_top_k(State) end}},
                     {timeout, 60, {"Rerank empty documents", fun() -> test_rerank_empty_docs(State) end}},
                     {timeout, 60, {"Rerank single document", fun() -> test_rerank_single_doc(State) end}},
                     {timeout, 60, {"Concurrent rerank requests", fun() -> test_rerank_concurrent(State) end}}
                 ]
         end
     end}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    Python = get_python(),
    case check_python_deps(Python) of
        ok ->
            case barrel_rerank:start_link(#{python => Python}) of
                {ok, Server} ->
                    Server;
                {error, Reason} ->
                    io:format(standard_error,
                              "~n*** Skipping rerank integration tests: ~p~n", [Reason]),
                    skip
            end;
        {error, Reason} ->
            io:format(standard_error,
                      "~n*** Skipping rerank integration tests: ~s~n", [Reason]),
            skip
    end.

cleanup(skip) ->
    ok;
cleanup(Server) ->
    barrel_rerank:stop(Server).

%%====================================================================
%% Tests
%%====================================================================

test_rerank_init(Server) ->
    %% Just verify the server is available from setup
    ?assertEqual(true, barrel_rerank:available(Server)).

test_rerank_basic(Server) ->
    Query = <<"What is machine learning?">>,
    Documents = [
        <<"Machine learning is a subset of artificial intelligence that enables systems to learn from data.">>,
        <<"Python is a popular programming language used for web development.">>,
        <<"Deep learning uses neural networks with many layers to process complex patterns.">>
    ],

    {ok, Results} = barrel_rerank:rerank(Server, Query, Documents),

    %% Should return all 3 documents
    ?assertEqual(3, length(Results)),

    %% Results should be sorted by score descending
    Scores = [Score || {_Idx, Score} <- Results],
    ?assertEqual(Scores, lists:reverse(lists:sort(Scores))),

    %% The ML-related documents should rank higher than Python doc
    %% (Index 0 = ML, Index 1 = Python, Index 2 = Deep Learning)
    TopIndices = [Idx || {Idx, _Score} <- lists:sublist(Results, 2)],
    ?assertNot(lists:member(1, TopIndices)),

    ok.

test_rerank_top_k(Server) ->
    Query = <<"database query optimization">>,
    Documents = [
        <<"SQL databases use indexes to speed up queries.">>,
        <<"Cooking recipes require fresh ingredients.">>,
        <<"Query optimization improves database performance.">>,
        <<"Weather forecasts predict rain tomorrow.">>,
        <<"NoSQL databases offer flexible schemas.">>
    ],

    %% Request only top 2 results
    {ok, Results} = barrel_rerank:rerank(Server, Query, Documents, #{top_k => 2}),

    %% Should return exactly 2 results
    ?assertEqual(2, length(Results)),

    %% Results should be sorted by score descending
    Scores = [Score || {_Idx, Score} <- Results],
    ?assertEqual(Scores, lists:reverse(lists:sort(Scores))),

    ok.

test_rerank_empty_docs(Server) ->
    Query = <<"test query">>,
    Documents = [],

    {ok, Results} = barrel_rerank:rerank(Server, Query, Documents),

    ?assertEqual([], Results),

    ok.

test_rerank_single_doc(Server) ->
    Query = <<"search query">>,
    Documents = [<<"This is the only document.">>],

    {ok, Results} = barrel_rerank:rerank(Server, Query, Documents),

    ?assertEqual(1, length(Results)),
    [{0, Score}] = Results,
    ?assert(is_float(Score)),

    ok.

test_rerank_concurrent(Server) ->
    %% Test that concurrent requests are handled correctly
    Self = self(),
    Query1 = <<"What is machine learning?">>,
    Docs1 = [<<"ML is AI">>, <<"Python is a language">>],
    Query2 = <<"What is database?">>,
    Docs2 = [<<"SQL stores data">>, <<"Java is a language">>],

    %% Spawn concurrent requests
    spawn(fun() ->
        Result = barrel_rerank:rerank(Server, Query1, Docs1),
        Self ! {1, Result}
    end),
    spawn(fun() ->
        Result = barrel_rerank:rerank(Server, Query2, Docs2),
        Self ! {2, Result}
    end),

    %% Collect results
    Results = receive_results(2, #{}),

    %% Both should succeed
    {ok, R1} = maps:get(1, Results),
    {ok, R2} = maps:get(2, Results),

    ?assertEqual(2, length(R1)),
    ?assertEqual(2, length(R2)),

    ok.

%%====================================================================
%% Helpers
%%====================================================================

receive_results(0, Acc) ->
    Acc;
receive_results(N, Acc) ->
    receive
        {Id, Result} ->
            receive_results(N - 1, Acc#{Id => Result})
    after 30000 ->
        error(timeout_waiting_for_results)
    end.

%% @doc Get the Python executable to use.
%% Uses managed venv if available, then falls back to system python.
get_python() ->
    %% Try managed venv first
    ManagedVenv = barrel_rerank_venv:venv_path(),
    ManagedPython = filename:join([ManagedVenv, "bin", "python"]),
    case filelib:is_file(ManagedPython) of
        true ->
            ManagedPython;
        false ->
            %% Try other common venv paths
            VenvPaths = [
                "priv/.venv/bin/python",
                ".venv/bin/python",
                "../.venv/bin/python"
            ],
            case find_python(VenvPaths) of
                {ok, Path} ->
                    Path;
                not_found ->
                    "python3"
            end
    end.

find_python([]) ->
    not_found;
find_python([Path | Rest]) ->
    case filelib:is_file(Path) of
        true -> {ok, Path};
        false -> find_python(Rest)
    end.

%% @doc Check if Python has required dependencies.
check_python_deps(Python) ->
    %% Try to import transformers and torch
    Cmd = Python ++ " -c \"import transformers; import torch; print('ok')\" 2>/dev/null",
    case os:cmd(Cmd) of
        "ok\n" ->
            ok;
        _ ->
            {error, "Python dependencies not available. Run: barrel_rerank_venv:ensure_venv()."}
    end.
