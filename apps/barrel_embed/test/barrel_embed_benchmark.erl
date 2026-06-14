%%%-------------------------------------------------------------------
%%% @doc Benchmark module for port-based backend performance
%%%
%%% == Usage ==
%%% ```
%%% %% Run with defaults (100 texts, fastembed)
%%% barrel_embed_benchmark:run(#{venv => "/path/to/.venv"}).
%%%
%%% %% Run with custom options
%%% barrel_embed_benchmark:run(#{
%%%     count => 500,
%%%     model => <<"BAAI/bge-base-en-v1.5">>,
%%%     venv => "/path/to/.venv",
%%%     provider => fastembed
%%% }).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_benchmark).

-export([
    run/0,
    run/1,
    benchmark/4
]).

-define(DEFAULT_COUNT, 100).
-define(DEFAULT_MODEL, "BAAI/bge-small-en-v1.5").
-define(DEFAULT_PROVIDER, fastembed).
-define(SINGLE_TEXT_SAMPLES, 50).

%%====================================================================
%% API
%%====================================================================

%% @doc Run benchmark with default options.
-spec run() -> ok.
run() ->
    io:format("~nError: venv option is required~n"),
    io:format("Usage: barrel_embed_benchmark:run(#{venv => \"/path/to/.venv\"}).~n"),
    ok.

%% @doc Run benchmark with options.
-spec run(map()) -> ok.
run(Opts) ->
    case maps:get(venv, Opts, undefined) of
        undefined ->
            io:format("~nError: venv option is required~n"),
            io:format("Usage: barrel_embed_benchmark:run(#{venv => \"/path/to/.venv\"}).~n"),
            ok;
        Venv ->
            Count = maps:get(count, Opts, ?DEFAULT_COUNT),
            Model = maps:get(model, Opts, ?DEFAULT_MODEL),
            Provider = maps:get(provider, Opts, ?DEFAULT_PROVIDER),

            io:format("~n========================================~n"),
            io:format("  Benchmark: Port-based Backend~n"),
            io:format("========================================~n"),
            io:format("~nProvider: ~p~n", [Provider]),
            io:format("Model: ~s~n", [Model]),
            io:format("Text count: ~p~n", [Count]),
            io:format("Venv: ~s~n", [Venv]),

            Texts = generate_texts(Count),

            io:format("~n--- Results ---~n"),
            Results = benchmark(Provider, Texts, Model, Venv),
            print_results(Results, Count),

            ok
    end.

%% @doc Benchmark port-based backend.
-spec benchmark(atom(), [binary()], string(), string() | binary()) -> map().
benchmark(Provider, Texts, Model, Venv) ->
    ProviderMod = provider_module(Provider),
    Config = #{
        venv => Venv,
        model => Model
    },

    %% Cold start (start port + load model)
    io:format("Measuring cold start...~n"),
    {ColdTime, InitResult} = timer:tc(fun() ->
        ProviderMod:init(Config)
    end),

    case InitResult of
        {ok, InitConfig} ->
            %% Warm single-text latency
            io:format("Measuring single-text latency (~p samples)...~n", [?SINGLE_TEXT_SAMPLES]),
            SingleTimes = measure_single_latency(ProviderMod, InitConfig, Texts, ?SINGLE_TEXT_SAMPLES),

            %% Batch throughput
            io:format("Measuring batch throughput...~n"),
            {BatchTime, BatchResult} = timer:tc(fun() ->
                ProviderMod:embed_batch(Texts, InitConfig)
            end),

            BatchOk = case BatchResult of
                {ok, _} -> true;
                _ -> false
            end,

            %% Stop the server
            case maps:get(server, InitConfig, undefined) of
                undefined -> ok;
                Server -> barrel_embed_port_server:stop(Server)
            end,

            #{
                cold_start_ms => ColdTime / 1000,
                warm_p50_ms => percentile(SingleTimes, 50) / 1000,
                warm_p95_ms => percentile(SingleTimes, 95) / 1000,
                warm_p99_ms => percentile(SingleTimes, 99) / 1000,
                warm_mean_ms => lists:sum(SingleTimes) / length(SingleTimes) / 1000,
                batch_time_ms => BatchTime / 1000,
                batch_throughput => length(Texts) * 1000000 / BatchTime,
                batch_success => BatchOk
            };
        {error, Reason} ->
            io:format("Error initializing: ~p~n", [Reason]),
            #{error => Reason}
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

provider_module(local) -> barrel_embed_local;
provider_module(fastembed) -> barrel_embed_fastembed;
provider_module(splade) -> barrel_embed_splade;
provider_module(colbert) -> barrel_embed_colbert;
provider_module(clip) -> barrel_embed_clip.

generate_texts(Count) ->
    Sentences = [
        <<"The quick brown fox jumps over the lazy dog.">>,
        <<"Machine learning is transforming the way we process information.">>,
        <<"Erlang is a functional programming language designed for concurrent systems.">>,
        <<"Vector databases enable efficient similarity search at scale.">>,
        <<"Natural language processing helps computers understand human language.">>,
        <<"The weather today is sunny with a chance of afternoon showers.">>,
        <<"Python is widely used for data science and machine learning applications.">>,
        <<"Embeddings convert text into dense vector representations.">>,
        <<"The cat sat on the mat and watched the birds outside.">>,
        <<"Distributed systems require careful handling of network partitions.">>
    ],
    NumSentences = length(Sentences),
    [lists:nth((I rem NumSentences) + 1, Sentences) || I <- lists:seq(1, Count)].

measure_single_latency(ProviderMod, Config, Texts, Samples) ->
    SampleTexts = lists:sublist(Texts, min(Samples, length(Texts))),
    [begin
        {T, _} = timer:tc(fun() ->
            ProviderMod:embed(Text, Config)
        end),
        T
    end || Text <- SampleTexts].

percentile(List, P) when length(List) > 0 ->
    Sorted = lists:sort(List),
    N = length(Sorted),
    Rank = (P / 100) * (N - 1) + 1,
    K = trunc(Rank),
    D = Rank - K,
    case K >= N of
        true -> lists:nth(N, Sorted);
        false ->
            V1 = lists:nth(K, Sorted),
            V2 = lists:nth(K + 1, Sorted),
            V1 + D * (V2 - V1)
    end;
percentile(_, _) ->
    0.

print_results(#{error := Reason}, _Count) ->
    io:format("Error: ~p~n", [Reason]);
print_results(Results, Count) ->
    io:format("~n  Cold start:       ~.1f ms~n", [maps:get(cold_start_ms, Results)]),
    io:format("  Warm latency:~n"),
    io:format("    p50:            ~.2f ms~n", [maps:get(warm_p50_ms, Results)]),
    io:format("    p95:            ~.2f ms~n", [maps:get(warm_p95_ms, Results)]),
    io:format("    p99:            ~.2f ms~n", [maps:get(warm_p99_ms, Results)]),
    io:format("    mean:           ~.2f ms~n", [maps:get(warm_mean_ms, Results)]),
    io:format("  Batch (~p texts):~n", [Count]),
    io:format("    Total time:     ~.1f ms~n", [maps:get(batch_time_ms, Results)]),
    io:format("    Throughput:     ~.1f texts/sec~n", [maps:get(batch_throughput, Results)]).
