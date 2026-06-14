#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa _build/default/lib/barrel_vectordb/ebin -pa _build/default/lib/rocksdb/ebin

main(_) ->
    io:format("=== DiskANN Batch Insert Performance Benchmark ===~n~n"),

    %% Setup
    TmpDir = "/tmp/diskann_bench_" ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_dir(filename:join(TmpDir, "dummy")),

    try
        %% Test parameters
        Dim = 128,
        InitialSize = 100,
        BatchSize = 200,

        %% Generate random vectors
        rand:seed(exsss, {42, 42, 42}),
        InitialVectors = [{integer_to_binary(I), random_vector(Dim)}
                          || I <- lists:seq(1, InitialSize)],
        NewVectors = [{integer_to_binary(InitialSize + I), random_vector(Dim)}
                      || I <- lists:seq(1, BatchSize)],

        %% Build initial index
        Config = #{
            dimension => Dim,
            r => 32,
            l_build => 100,
            l_search => 100,
            alpha => 1.2,
            storage_mode => disk,
            base_path => TmpDir,
            use_pq => false
        },

        io:format("Building initial index with ~p vectors...~n", [InitialSize]),
        {ok, Index0} = barrel_vectordb_diskann:build(Config, InitialVectors),
        io:format("Initial index size: ~p~n~n", [barrel_vectordb_diskann:size(Index0)]),

        %% Benchmark batch insert
        io:format("Batch inserting ~p vectors into ~p-vector index...~n",
                  [BatchSize, InitialSize]),

        {Time, {ok, Index1}} = timer:tc(fun() ->
            barrel_vectordb_diskann:insert_batch(Index0, NewVectors, #{})
        end),

        TimeMs = Time / 1000,
        MsPerVec = TimeMs / BatchSize,

        io:format("~n=== Results ===~n"),
        io:format("Total time: ~.2f ms~n", [TimeMs]),
        io:format("Time per vector: ~.2f ms/vec~n", [MsPerVec]),
        io:format("Final index size: ~p~n", [barrel_vectordb_diskann:size(Index1)]),

        %% Verify recall
        io:format("~nVerifying recall...~n"),
        AllVectors = InitialVectors ++ NewVectors,
        Recalls = [measure_recall(Index1, random_vector(Dim), AllVectors, 10)
                   || _ <- lists:seq(1, 5)],
        AvgRecall = lists:sum(Recalls) / length(Recalls),
        io:format("Average recall@10: ~.2f%~n", [AvgRecall * 100]),

        %% Cleanup
        barrel_vectordb_diskann:close(Index1)
    after
        os:cmd("rm -rf " ++ TmpDir)
    end,

    io:format("~n=== Benchmark Complete ===~n").

random_vector(Dim) ->
    Vec = [rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)],
    normalize(Vec).

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([V*V || V <- Vec])),
    case Norm < 0.0001 of
        true -> Vec;
        false -> [V / Norm || V <- Vec]
    end.

measure_recall(Index, Query, Vectors, K) ->
    Results = barrel_vectordb_diskann:search(Index, Query, K),
    ResultIds = [Id || {Id, _} <- Results],
    TrueTopK = brute_force_search(Vectors, Query, K),
    TrueIds = [Id || {Id, _} <- TrueTopK],
    Intersection = length([Id || Id <- ResultIds, lists:member(Id, TrueIds)]),
    Intersection / K.

brute_force_search(Vectors, Query, K) ->
    Distances = [{Id, cosine_distance(Query, Vec)} || {Id, Vec} <- Vectors],
    Sorted = lists:sort(fun({_, D1}, {_, D2}) -> D1 =< D2 end, Distances),
    lists:sublist(Sorted, K).

cosine_distance(Vec1, Vec2) ->
    Dot = lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]),
    Norm1 = math:sqrt(lists:sum([V*V || V <- Vec1])),
    Norm2 = math:sqrt(lists:sum([V*V || V <- Vec2])),
    Denom = Norm1 * Norm2,
    case Denom < 1.0e-10 of
        true -> 1.0;
        false -> 1.0 - (Dot / Denom)
    end.
