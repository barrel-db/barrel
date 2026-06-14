%%%-------------------------------------------------------------------
%%% @doc Barrel DocDB Performance Benchmark
%%%
%%% Simple benchmarking tool to measure barrel_docdb performance
%%% across different document sizes and structures.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench).

-export([run/0, run/1]).
-export([run_crud/0, run_crud/1]).
-export([run_query/0, run_query/1]).
-export([run_changes/0, run_changes/1]).
-export([run_doc_types/0, run_doc_types/1]).

-define(DEFAULT_NUM_DOCS, 10000).
-define(DEFAULT_ITERATIONS, 10000).
-define(BENCH_DB, <<"barrel_bench_db">>).

%% @doc Run all benchmarks with default configuration
-spec run() -> map().
run() ->
    run(#{}).

%% @doc Run all benchmarks with custom configuration
%%
%% Options:
%% - `num_docs': Number of documents to load (default: 10000)
%% - `iterations': Operations per test (default: 10000)
%% - `doc_type': Document type to use (default: user_doc for legacy)
-spec run(map()) -> map().
run(Config) ->
    NumDocs = maps:get(num_docs, Config, ?DEFAULT_NUM_DOCS),
    Iterations = maps:get(iterations, Config, ?DEFAULT_ITERATIONS),

    io:format("~n=== barrel_docdb Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n~n", [NumDocs, Iterations]),

    {ok, Db} = setup_db(),

    Results = #{
        crud => run_workload(barrel_bench_crud, Db, NumDocs, Iterations),
        query => run_workload(barrel_bench_query, Db, NumDocs, Iterations),
        changes => run_workload(barrel_bench_changes, Db, NumDocs, Iterations)
    },

    cleanup_db(),

    print_results(Results),

    %% Save JSON output
    Output = #{
        timestamp => timestamp(),
        config => #{num_docs => NumDocs, iterations => Iterations},
        results => Results
    },
    save_results(Output),

    Results.

%% @doc Run benchmarks for all document types
-spec run_doc_types() -> map().
run_doc_types() ->
    run_doc_types(#{}).

%% @doc Run benchmarks for all document types with configuration
%%
%% Options:
%% - `num_docs': Number of documents per type (default: 1000)
%% - `iterations': Operations per test (default: 500)
%% - `doc_types': List of doc types to test (default: all)
-spec run_doc_types(map()) -> map().
run_doc_types(Config) ->
    NumDocs = maps:get(num_docs, Config, 1000),
    Iterations = maps:get(iterations, Config, 500),
    DocTypes = maps:get(doc_types, Config, barrel_bench_generator:doc_types()),

    io:format("~n=== barrel_docdb Document Type Benchmark ===~n"),
    io:format("Documents per type: ~p, Iterations: ~p~n", [NumDocs, Iterations]),
    io:format("Testing ~p document types~n~n", [length(DocTypes)]),

    Results = lists:foldl(fun(DocType, Acc) ->
        Info = barrel_bench_generator:doc_type_info(DocType),
        Name = maps:get(name, Info),
        Generator = maps:get(generator, Info),

        io:format("~n--- Testing: ~s ---~n", [Name]),
        io:format("  Fields: ~p, Depth: ~p, Size: ~s~n",
                  [maps:get(fields, Info), maps:get(depth, Info), maps:get(approx_size, Info)]),

        {ok, Db} = setup_db(),

        %% Run CRUD benchmark with this doc type
        CrudResults = run_crud_for_type(Db, Generator, NumDocs, Iterations),

        %% Run query benchmark with this doc type
        QueryResults = run_query_for_type(Db, DocType, Generator, NumDocs, Iterations),

        cleanup_db(),

        %% Print summary for this type
        io:format("  CRUD: insert=~.1f ops/s, read=~.1f ops/s~n",
                  [get_throughput(CrudResults, insert), get_throughput(CrudResults, read)]),
        io:format("  Query: eq=~.1f ops/s, multi=~.1f ops/s~n",
                  [get_throughput(QueryResults, simple_eq), get_throughput(QueryResults, multi_index)]),

        Acc#{DocType => #{crud => CrudResults, query => QueryResults}}
    end, #{}, DocTypes),

    %% Save results
    Output = #{
        timestamp => timestamp(),
        config => #{num_docs => NumDocs, iterations => Iterations, doc_types => DocTypes},
        results => Results
    },
    save_results(Output, "doc_types"),

    %% Print comparison table
    print_doc_type_comparison(Results, DocTypes),

    Results.

%% @doc Run only CRUD benchmarks
-spec run_crud() -> map().
run_crud() ->
    run_crud(#{}).

-spec run_crud(map()) -> map().
run_crud(Config) ->
    NumDocs = maps:get(num_docs, Config, ?DEFAULT_NUM_DOCS),
    Iterations = maps:get(iterations, Config, ?DEFAULT_ITERATIONS),

    io:format("~n=== CRUD Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n~n", [NumDocs, Iterations]),

    {ok, Db} = setup_db(),
    Result = run_workload(barrel_bench_crud, Db, NumDocs, Iterations),
    cleanup_db(),

    print_workload_result(crud, Result),
    Result.

%% @doc Run only query benchmarks
-spec run_query() -> map().
run_query() ->
    run_query(#{}).

-spec run_query(map()) -> map().
run_query(Config) ->
    NumDocs = maps:get(num_docs, Config, ?DEFAULT_NUM_DOCS),
    Iterations = maps:get(iterations, Config, ?DEFAULT_ITERATIONS),

    io:format("~n=== Query Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n~n", [NumDocs, Iterations]),

    {ok, Db} = setup_db(),
    Result = run_workload(barrel_bench_query, Db, NumDocs, Iterations),
    cleanup_db(),

    print_workload_result(query, Result),
    Result.

%% @doc Run only changes benchmarks
-spec run_changes() -> map().
run_changes() ->
    run_changes(#{}).

-spec run_changes(map()) -> map().
run_changes(Config) ->
    NumDocs = maps:get(num_docs, Config, ?DEFAULT_NUM_DOCS),
    Iterations = maps:get(iterations, Config, ?DEFAULT_ITERATIONS),

    io:format("~n=== Changes Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n~n", [NumDocs, Iterations]),

    {ok, Db} = setup_db(),
    Result = run_workload(barrel_bench_changes, Db, NumDocs, Iterations),
    cleanup_db(),

    print_workload_result(changes, Result),
    Result.

%%====================================================================
%% Internal functions
%%====================================================================

setup_db() ->
    %% Ensure application is started
    application:ensure_all_started(barrel_docdb),

    %% Clean up any existing bench db
    _ = barrel_docdb:delete_db(?BENCH_DB),

    %% Create fresh database
    barrel_docdb:create_db(?BENCH_DB).

cleanup_db() ->
    _ = barrel_docdb:delete_db(?BENCH_DB),
    ok.

run_workload(Module, Db, NumDocs, Iterations) ->
    case code:ensure_loaded(Module) of
        {module, Module} ->
            Module:run(Db, NumDocs, Iterations);
        {error, _} ->
            %% Workload not yet implemented
            #{}
    end.

run_crud_for_type(Db, Generator, NumDocs, Iterations) ->
    io:format("  Running CRUD benchmarks...~n"),

    %% Insert
    io:format("    Inserting ~p documents...~n", [NumDocs]),
    InsertMetrics = barrel_bench_metrics:new(),
    InsertMetrics1 = lists:foldl(fun(I, Acc) ->
        Doc = Generator(I),
        {Time, _} = timer:tc(fun() -> barrel_docdb:put_doc(Db, Doc) end),
        barrel_bench_metrics:record(Acc, Time)
    end, InsertMetrics, lists:seq(0, NumDocs - 1)),

    %% Read
    io:format("    Reading ~p documents...~n", [Iterations]),
    ReadMetrics = barrel_bench_metrics:new(),
    ReadMetrics1 = lists:foldl(fun(_, Acc) ->
        DocId = Generator(rand:uniform(NumDocs) - 1),
        Id = maps:get(<<"_id">>, DocId),
        {Time, _} = timer:tc(fun() -> barrel_docdb:get_doc(Db, Id) end),
        barrel_bench_metrics:record(Acc, Time)
    end, ReadMetrics, lists:seq(1, Iterations)),

    %% Update
    io:format("    Updating ~p documents...~n", [Iterations]),
    UpdateMetrics = barrel_bench_metrics:new(),
    UpdateMetrics1 = lists:foldl(fun(I, Acc) ->
        Doc = Generator(I rem NumDocs),
        Id = maps:get(<<"_id">>, Doc),
        case barrel_docdb:get_doc(Db, Id) of
            {ok, Existing, _} ->
                Updated = Existing#{<<"updated">> => true},
                {Time, _} = timer:tc(fun() -> barrel_docdb:put_doc(Db, Updated) end),
                barrel_bench_metrics:record(Acc, Time);
            _ ->
                Acc
        end
    end, UpdateMetrics, lists:seq(0, Iterations - 1)),

    #{
        insert => barrel_bench_metrics:summarize(InsertMetrics1),
        read => barrel_bench_metrics:summarize(ReadMetrics1),
        update => barrel_bench_metrics:summarize(UpdateMetrics1)
    }.

run_query_for_type(Db, DocType, Generator, NumDocs, Iterations) ->
    io:format("  Running query benchmarks...~n"),

    %% First ensure docs are loaded (they should be from CRUD)
    case barrel_docdb:get_doc(Db, maps:get(<<"_id">>, Generator(0))) of
        {error, not_found} ->
            io:format("    Loading ~p documents...~n", [NumDocs]),
            lists:foreach(fun(I) ->
                barrel_docdb:put_doc(Db, Generator(I))
            end, lists:seq(0, NumDocs - 1));
        _ ->
            ok
    end,

    %% Get queries appropriate for this doc type
    Queries = get_queries_for_type(DocType),

    %% Run each query type
    maps:map(fun(QueryName, Query) ->
        io:format("    Running ~s...~n", [QueryName]),
        Metrics = barrel_bench_metrics:new(),
        Metrics1 = lists:foldl(fun(_, Acc) ->
            {Time, _} = timer:tc(fun() ->
                barrel_docdb:find(Db, Query)
            end),
            barrel_bench_metrics:record(Acc, Time)
        end, Metrics, lists:seq(1, Iterations)),
        barrel_bench_metrics:summarize(Metrics1)
    end, Queries).

%% Get appropriate queries for each document type
get_queries_for_type(small_flat) ->
    #{
        simple_eq => #{where => [{path, [<<"type">>], <<"item">>}], include_docs => false},
        simple_eq_limit => #{where => [{path, [<<"type">>], <<"item">>}], limit => 10, include_docs => false},
        multi_index => #{where => [
            {path, [<<"type">>], <<"item">>},
            {path, [<<"status">>], <<"active">>}
        ], include_docs => false}
    };
get_queries_for_type(medium_flat) ->
    #{
        simple_eq => #{where => [{path, [<<"type">>], <<"product">>}], include_docs => false},
        simple_eq_limit => #{where => [{path, [<<"type">>], <<"product">>}], limit => 10, include_docs => false},
        selective_eq => #{where => [{path, [<<"category">>], <<"electronics">>}], include_docs => false},
        multi_index => #{where => [
            {path, [<<"type">>], <<"product">>},
            {path, [<<"status">>], <<"active">>}
        ], include_docs => false},
        three_cond => #{where => [
            {path, [<<"type">>], <<"product">>},
            {path, [<<"status">>], <<"active">>},
            {path, [<<"in_stock">>], true}
        ], include_docs => false}
    };
get_queries_for_type(large_flat) ->
    #{
        simple_eq => #{where => [{path, [<<"type">>], <<"product">>}], include_docs => false},
        simple_eq_limit => #{where => [{path, [<<"type">>], <<"product">>}], limit => 10, include_docs => false},
        multi_index => #{where => [
            {path, [<<"type">>], <<"product">>},
            {path, [<<"status">>], <<"active">>}
        ], include_docs => false},
        with_docs => #{where => [{path, [<<"status">>], <<"active">>}], limit => 10, include_docs => true}
    };
get_queries_for_type(small_nested) ->
    #{
        simple_eq => #{where => [{path, [<<"type">>], <<"order">>}], include_docs => false},
        simple_eq_limit => #{where => [{path, [<<"type">>], <<"order">>}], limit => 10, include_docs => false},
        nested_path => #{where => [{path, [<<"shipping">>, <<"city">>], <<"Paris">>}], include_docs => false},
        multi_index => #{where => [
            {path, [<<"type">>], <<"order">>},
            {path, [<<"status">>], <<"active">>}
        ], include_docs => false}
    };
get_queries_for_type(medium_nested) ->
    #{
        simple_eq => #{where => [{path, [<<"type">>], <<"account">>}], include_docs => false},
        simple_eq_limit => #{where => [{path, [<<"type">>], <<"account">>}], limit => 10, include_docs => false},
        nested_2_levels => #{where => [{path, [<<"profile">>, <<"age">>], 25}], include_docs => false},
        nested_3_levels => #{where => [{path, [<<"profile">>, <<"location">>, <<"city">>], <<"Paris">>}], include_docs => false},
        nested_4_levels => #{where => [{path, [<<"profile">>, <<"location">>, <<"coordinates">>, <<"lat">>], 40.0}], include_docs => false},
        multi_index => #{where => [
            {path, [<<"type">>], <<"account">>},
            {path, [<<"status">>], <<"active">>}
        ], include_docs => false}
    };
get_queries_for_type(large_nested) ->
    #{
        simple_eq => #{where => [{path, [<<"type">>], <<"organization">>}], include_docs => false},
        simple_eq_limit => #{where => [{path, [<<"type">>], <<"organization">>}], limit => 10, include_docs => false},
        nested_3_levels => #{where => [{path, [<<"company">>, <<"headquarters">>, <<"address">>], <<"123 Main St">>}], include_docs => false},
        nested_deep => #{where => [{path, [<<"company">>, <<"headquarters">>, <<"address">>, <<"city">>], <<"Paris">>}], include_docs => false},
        multi_index => #{where => [
            {path, [<<"type">>], <<"organization">>},
            {path, [<<"status">>], <<"active">>}
        ], include_docs => false},
        with_docs => #{where => [{path, [<<"status">>], <<"active">>}], limit => 10, include_docs => true}
    };
get_queries_for_type(_) ->
    %% Default for user_doc
    #{
        simple_eq => #{where => [{path, [<<"type">>], <<"user">>}], include_docs => false},
        multi_index => #{where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ], include_docs => false}
    }.

get_throughput(Results, Key) ->
    case maps:get(Key, Results, undefined) of
        undefined -> 0.0;
        Summary -> maps:get(throughput, Summary, 0.0)
    end.

print_results(Results) ->
    io:format("~n=== Results ===~n~n"),
    maps:foreach(fun(Name, Result) ->
        print_workload_result(Name, Result)
    end, Results).

print_workload_result(_Name, Result) when map_size(Result) =:= 0 ->
    ok;
print_workload_result(Name, Result) ->
    io:format("~s:~n", [string:uppercase(atom_to_list(Name))]),
    maps:foreach(fun(Op, Summary) ->
        print_summary(Op, Summary)
    end, Result),
    io:format("~n").

print_summary(Op, Summary) ->
    Count = maps:get(count, Summary, 0),
    Throughput = maps:get(throughput, Summary, 0.0),
    P50 = maps:get(latency_p50, Summary, 0),
    P99 = maps:get(latency_p99, Summary, 0),
    io:format("  ~-15s ~8.1f ops/sec, p50: ~6wus, p99: ~6wus (~p ops)~n",
              [atom_to_list(Op) ++ ":", Throughput, P50, P99, Count]).

print_doc_type_comparison(Results, DocTypes) ->
    io:format("~n=== Document Type Comparison ===~n~n"),

    %% Header
    io:format("| ~-15s | ~-10s | ~-10s | ~-10s | ~-10s |~n",
              ["Type", "Insert", "Read", "Simple EQ", "Multi-Idx"]),
    io:format("|~s|~s|~s|~s|~s|~n",
              [lists:duplicate(17, $-), lists:duplicate(12, $-), lists:duplicate(12, $-),
               lists:duplicate(12, $-), lists:duplicate(12, $-)]),

    lists:foreach(fun(DocType) ->
        case maps:get(DocType, Results, undefined) of
            undefined -> ok;
            TypeResult ->
                Crud = maps:get(crud, TypeResult, #{}),
                Query = maps:get(query, TypeResult, #{}),
                io:format("| ~-15s | ~10.1f | ~10.1f | ~10.1f | ~10.1f |~n",
                          [atom_to_list(DocType),
                           get_throughput(Crud, insert),
                           get_throughput(Crud, read),
                           get_throughput(Query, simple_eq),
                           get_throughput(Query, multi_index)])
        end
    end, DocTypes),
    io:format("~n").

timestamp() ->
    {{Y, M, D}, {H, Mi, S}} = calendar:local_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B_~2..0B-~2..0B-~2..0B",
                                  [Y, M, D, H, Mi, S])).

save_results(Output) ->
    save_results(Output, "").

save_results(Output, Suffix) ->
    %% Ensure results directory exists
    ResultsDir = "results",
    ok = filelib:ensure_dir(ResultsDir ++ "/"),

    %% Save with timestamp
    Timestamp = maps:get(timestamp, Output),
    SuffixStr = case Suffix of "" -> ""; _ -> "_" ++ Suffix end,
    Filename = io_lib:format("~s/~s~s.json", [ResultsDir, Timestamp, SuffixStr]),

    %% Also save as latest.json
    LatestFile = case Suffix of
        "" -> ResultsDir ++ "/latest.json";
        _ -> ResultsDir ++ "/latest_" ++ Suffix ++ ".json"
    end,

    Json = json:encode(Output),

    ok = file:write_file(Filename, Json),
    ok = file:write_file(LatestFile, Json),

    io:format("Results saved to: ~s~n", [Filename]).
