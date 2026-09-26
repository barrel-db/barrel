%%%-------------------------------------------------------------------
%%% @doc Encode latency of full TurboQuant against Subspace-TurboQuant.
%%%
%%% Usage:
%%%   rebar3 as bench shell
%%%   barrel_vectordb_turboquant_bench:compare().
%%%   barrel_vectordb_turboquant_bench:compare(#{dimension => 768, m => 8, n => 10}).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_turboquant_bench).

-export([compare/0, compare/1]).

compare() ->
    compare(#{}).

%% Building the full D x D rotation takes tens of seconds at D = 768.
compare(Opts) ->
    Dim = maps:get(dimension, Opts, 768),
    M = maps:get(m, Opts, 8),
    N = maps:get(n, Opts, 10),
    {FullNewUs, {ok, Full}} =
        timer:tc(fun() -> barrel_vectordb_turboquant:new(#{dimension => Dim}) end),
    {SubNewUs, {ok, Sub}} =
        timer:tc(fun() ->
            barrel_vectordb_turboquant_subspace:new(#{dimension => Dim, m => M})
        end),
    Vec = [rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)],
    _ = barrel_vectordb_turboquant:encode(Full, Vec),
    _ = barrel_vectordb_turboquant_subspace:encode(Sub, Vec),
    {FullUs, _} = timer:tc(fun() ->
        [barrel_vectordb_turboquant:encode(Full, Vec) || _ <- lists:seq(1, N)]
    end),
    {SubUs, _} = timer:tc(fun() ->
        [barrel_vectordb_turboquant_subspace:encode(Sub, Vec) || _ <- lists:seq(1, N)]
    end),
    #{rotation_matrix_bytes := SubRotBytes} =
        barrel_vectordb_turboquant_subspace:info(Sub),
    FullRotBytes = Dim * Dim * 8,
    io:format("D=~p M=~p~n", [Dim, M]),
    io:format("  new:    full ~.1f ms, subspace ~.1f ms~n",
              [FullNewUs / 1000, SubNewUs / 1000]),
    io:format("  encode: full ~.3f ms, subspace ~.3f ms (x~.2f)~n",
              [FullUs / N / 1000, SubUs / N / 1000, FullUs / max(1, SubUs)]),
    io:format("  rotation: full ~.2f MB, subspace ~.2f MB~n",
              [FullRotBytes / 1048576, SubRotBytes / 1048576]),
    #{full_encode_ms => FullUs / N / 1000,
      subspace_encode_ms => SubUs / N / 1000,
      full_rotation_bytes => FullRotBytes,
      subspace_rotation_bytes => SubRotBytes}.
