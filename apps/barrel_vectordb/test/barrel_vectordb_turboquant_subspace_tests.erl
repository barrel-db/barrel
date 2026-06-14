%%%-------------------------------------------------------------------
%%% @doc Tests for Subspace-TurboQuant module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_turboquant_subspace_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Configuration
%%====================================================================

new_test_() ->
    [
        {"create config with auto M for 128-dim", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{dimension => 128}),
            Info = barrel_vectordb_turboquant_subspace:info(Config),
            ?assertEqual(128, maps:get(dimension, Info)),
            ?assertEqual(1, maps:get(m, Info)),
            ?assertEqual(128, maps:get(subdim, Info))
        end},
        {"create config with auto M for 768-dim", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{dimension => 768}),
            Info = barrel_vectordb_turboquant_subspace:info(Config),
            ?assertEqual(768, maps:get(dimension, Info)),
            ?assertEqual(8, maps:get(m, Info)),
            ?assertEqual(96, maps:get(subdim, Info))
        end},
        {"create config with explicit M", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{
                dimension => 768,
                m => 4
            }),
            Info = barrel_vectordb_turboquant_subspace:info(Config),
            ?assertEqual(4, maps:get(m, Info)),
            ?assertEqual(192, maps:get(subdim, Info))
        end},
        {"fail on odd dimension", fun() ->
            ?assertMatch({error, {dimension_must_be_even, _}},
                        barrel_vectordb_turboquant_subspace:new(#{dimension => 127}))
        end},
        {"fail on non-divisible dimension", fun() ->
            ?assertMatch({error, {dimension_not_divisible_by_m, _, _}},
                        barrel_vectordb_turboquant_subspace:new(#{dimension => 128, m => 3}))
        end},
        {"fail on odd subdim", fun() ->
            %% 96 / 3 = 32, which is even, but let's test a case where subdim would be odd
            %% 100 / 10 = 10, which is even. Try 36 / 6 = 6 (even).
            %% Actually 14 / 7 = 2 (even). We need dimension/m to be odd.
            %% 98 / 7 = 14 (even). 98 / 14 = 7 (odd!)
            ?assertMatch({error, {subdim_must_be_even, _}},
                        barrel_vectordb_turboquant_subspace:new(#{dimension => 98, m => 14}))
        end}
    ].

encode_decode_test_() ->
    [
        {"encode/decode roundtrip 128-dim", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{dimension => 128}),
            Vector = [rand:uniform() - 0.5 || _ <- lists:seq(1, 128)],
            Code = barrel_vectordb_turboquant_subspace:encode(Config, Vector),

            %% Check code format
            ?assertEqual(2, binary:at(Code, 0)),  %% Version
            ?assertEqual(3, binary:at(Code, 1)),  %% Default bits

            %% Decode and check approximate reconstruction
            Decoded = barrel_vectordb_turboquant_subspace:decode(Config, Code),
            ?assertEqual(128, length(Decoded)),

            %% Check reconstruction error is reasonable (< 0.5 per component on average)
            Errors = [abs(V - D) || {V, D} <- lists:zip(Vector, Decoded)],
            MeanError = lists:sum(Errors) / length(Errors),
            ?assert(MeanError < 0.5)
        end},
        {"encode/decode roundtrip 768-dim M=8", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{dimension => 768, m => 8}),
            Vector = [rand:uniform() - 0.5 || _ <- lists:seq(1, 768)],
            Code = barrel_vectordb_turboquant_subspace:encode(Config, Vector),

            %% Check header
            <<Version:8, Bits:8, M:8, _Flags:8, _/binary>> = Code,
            ?assertEqual(2, Version),
            ?assertEqual(3, Bits),
            ?assertEqual(8, M),

            Decoded = barrel_vectordb_turboquant_subspace:decode(Config, Code),
            ?assertEqual(768, length(Decoded))
        end},
        {"encode fails on wrong dimension", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{dimension => 128}),
            Vector = [rand:uniform() || _ <- lists:seq(1, 64)],
            ?assertError({dimension_mismatch, 128, 64},
                        barrel_vectordb_turboquant_subspace:encode(Config, Vector))
        end}
    ].

distance_test_() ->
    [
        {"distance computation 128-dim", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{dimension => 128}),

            Query = [rand:uniform() - 0.5 || _ <- lists:seq(1, 128)],
            Vec1 = [rand:uniform() - 0.5 || _ <- lists:seq(1, 128)],
            Vec2 = Query,  %% Same as query

            Code1 = barrel_vectordb_turboquant_subspace:encode(Config, Vec1),
            Code2 = barrel_vectordb_turboquant_subspace:encode(Config, Vec2),

            Tables = barrel_vectordb_turboquant_subspace:precompute_tables(Config, Query),

            Dist1 = barrel_vectordb_turboquant_subspace:distance(Tables, Code1),
            Dist2 = barrel_vectordb_turboquant_subspace:distance(Tables, Code2),

            %% Distance to self should be much smaller than to a random vector
            %% Note: Quantization introduces error, so dist to self isn't 0
            ?assert(Dist2 < Dist1 * 0.5 orelse Dist2 < 2.0),
            %% Distance to different vector should be positive
            ?assert(Dist1 >= 0.0)
        end},
        {"distance computation 768-dim M=8", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{dimension => 768, m => 8}),

            Query = [rand:uniform() - 0.5 || _ <- lists:seq(1, 768)],
            Vec = [rand:uniform() - 0.5 || _ <- lists:seq(1, 768)],

            Code = barrel_vectordb_turboquant_subspace:encode(Config, Vec),
            Tables = barrel_vectordb_turboquant_subspace:precompute_tables(Config, Query),

            Dist = barrel_vectordb_turboquant_subspace:distance(Tables, Code),
            ?assert(Dist >= 0.0)
        end},
        {"batch encode", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{dimension => 128}),
            Vectors = [[rand:uniform() - 0.5 || _ <- lists:seq(1, 128)]
                       || _ <- lists:seq(1, 10)],

            Codes = barrel_vectordb_turboquant_subspace:batch_encode(Config, Vectors),
            ?assertEqual(10, length(Codes))
        end}
    ].

info_test_() ->
    [
        {"info returns expected fields", fun() ->
            {ok, Config} = barrel_vectordb_turboquant_subspace:new(#{
                dimension => 768,
                m => 8,
                bits => 3
            }),
            Info = barrel_vectordb_turboquant_subspace:info(Config),

            ?assertEqual(768, maps:get(dimension, Info)),
            ?assertEqual(8, maps:get(m, Info)),
            ?assertEqual(96, maps:get(subdim, Info)),
            ?assertEqual(3, maps:get(bits, Info)),
            ?assertEqual(false, maps:get(training_required, Info)),

            %% Check compression ratio
            CompressionRatio = maps:get(compression_ratio, Info),
            ?assert(CompressionRatio > 1.0),

            %% Check rotation matrix memory is much smaller than full TQ
            RotationBytes = maps:get(rotation_matrix_bytes, Info),
            FullRotationBytes = 768 * 768 * 8,
            ?assert(RotationBytes < FullRotationBytes / 4)
        end}
    ].

select_m_test_() ->
    [
        {"select_m returns correct values", fun() ->
            ?assertEqual(1, barrel_vectordb_turboquant_subspace:select_m(64)),
            ?assertEqual(1, barrel_vectordb_turboquant_subspace:select_m(128)),
            ?assertEqual(2, barrel_vectordb_turboquant_subspace:select_m(256)),
            ?assertEqual(4, barrel_vectordb_turboquant_subspace:select_m(512)),
            ?assertEqual(8, barrel_vectordb_turboquant_subspace:select_m(768)),
            ?assertEqual(8, barrel_vectordb_turboquant_subspace:select_m(1024)),
            ?assertEqual(16, barrel_vectordb_turboquant_subspace:select_m(2048)),
            ?assertEqual(32, barrel_vectordb_turboquant_subspace:select_m(4096))
        end}
    ].

split_subvectors_test_() ->
    [
        {"split_subvectors correctly divides vector", fun() ->
            Vec = lists:seq(1, 12),
            Result = barrel_vectordb_turboquant_subspace:split_subvectors(Vec, 3, 4),
            ?assertEqual([[1,2,3,4], [5,6,7,8], [9,10,11,12]], Result)
        end}
    ].

%% Performance comparison test (disabled by default, run manually)
performance_comparison_test_() ->
    {timeout, 60, fun() ->
        %% Compare encode latency between full TQ and subspace TQ
        Dim = 768,
        M = 8,

        %% Create configs
        {ok, FullConfig} = barrel_vectordb_turboquant:new(#{dimension => Dim}),
        {ok, SubspaceConfig} = barrel_vectordb_turboquant_subspace:new(#{
            dimension => Dim,
            m => M
        }),

        %% Generate test vector
        Vec = [rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)],

        %% Warm up
        _ = barrel_vectordb_turboquant:encode(FullConfig, Vec),
        _ = barrel_vectordb_turboquant_subspace:encode(SubspaceConfig, Vec),

        %% Benchmark full TQ (reduced iterations for CI)
        N = 10,
        {FullTime, _} = timer:tc(fun() ->
            [barrel_vectordb_turboquant:encode(FullConfig, Vec) || _ <- lists:seq(1, N)]
        end),

        %% Benchmark subspace TQ
        {SubspaceTime, _} = timer:tc(fun() ->
            [barrel_vectordb_turboquant_subspace:encode(SubspaceConfig, Vec) || _ <- lists:seq(1, N)]
        end),

        FullAvgMs = FullTime / N / 1000,
        SubspaceAvgMs = SubspaceTime / N / 1000,
        Speedup = FullTime / max(1, SubspaceTime),

        io:format("~nPerformance comparison (D=~p, M=~p):~n", [Dim, M]),
        io:format("  Full TQ:      ~.3f ms/encode~n", [FullAvgMs]),
        io:format("  Subspace TQ:  ~.3f ms/encode~n", [SubspaceAvgMs]),
        io:format("  Speedup:      ~.2fx~n", [Speedup]),

        %% Memory comparison
        SubspaceInfo = barrel_vectordb_turboquant_subspace:info(SubspaceConfig),

        %% Subspace TQ should use less rotation matrix memory
        SubspaceRotBytes = maps:get(rotation_matrix_bytes, SubspaceInfo),
        FullRotBytes = Dim * Dim * 8,  %% Full TQ uses D*D matrix

        io:format("~n  Rotation matrix memory:~n"),
        io:format("    Full TQ:     ~.2f MB~n", [FullRotBytes / 1024 / 1024]),
        io:format("    Subspace TQ: ~.2f MB~n", [SubspaceRotBytes / 1024 / 1024]),
        io:format("    Reduction:   ~.1fx~n", [FullRotBytes / SubspaceRotBytes]),

        %% Verify subspace is faster (or at least not much slower)
        ?assert(SubspaceAvgMs < FullAvgMs * 1.5)
    end}.
