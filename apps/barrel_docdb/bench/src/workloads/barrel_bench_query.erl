%%%-------------------------------------------------------------------
%%% @doc Query workload for barrel_bench
%%%
%%% Benchmarks different query patterns using barrel_docdb:find/2.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_query).

-export([run/3]).

%% @doc Run query benchmarks
-spec run(pid(), non_neg_integer(), non_neg_integer()) -> map().
run(Db, NumDocs, Iterations) ->
    io:format("Running query benchmarks...~n"),

    %% First load documents if not already loaded
    case barrel_docdb:get_doc(Db, <<"user_0">>) of
        {error, not_found} ->
            io:format("  Loading ~p documents...~n", [NumDocs]),
            load_docs(Db, NumDocs);
        {ok, _} ->
            ok
    end,

    %% Benchmark different query types - summarize immediately after each to get correct elapsed time
    io:format("  Running simple equality queries...~n"),
    SimpleEq = barrel_bench_metrics:summarize(bench_query(Db, simple_eq_query(), Iterations)),

    io:format("  Running simple equality with LIMIT 10...~n"),
    SimpleEqLimit = barrel_bench_metrics:summarize(bench_query(Db, simple_eq_limit_query(), Iterations)),

    io:format("  Running selective equality (status=active, ~~1/3 docs)...~n"),
    SelectiveEq = barrel_bench_metrics:summarize(bench_query(Db, selective_eq_query(), Iterations)),

    io:format("  Running very selective equality (city=Paris, ~~1/8 docs)...~n"),
    VerySelectiveEq = barrel_bench_metrics:summarize(bench_query(Db, very_selective_eq_query(), Iterations)),

    io:format("  Running range queries...~n"),
    Range = barrel_bench_metrics:summarize(bench_query(Db, range_query(), Iterations)),

    io:format("  Running range with LIMIT 10 (streaming body fetch)...~n"),
    RangeLimit = barrel_bench_metrics:summarize(bench_query(Db, range_limit_query(), Iterations)),

    io:format("  Running pure compare queries (age > 50)...~n"),
    PureCompare = barrel_bench_metrics:summarize(bench_query(Db, pure_compare_query(), Iterations)),

    io:format("  Running pure compare with LIMIT 10...~n"),
    PureCompareLimit = barrel_bench_metrics:summarize(bench_query(Db, pure_compare_limit_query(), Iterations)),

    io:format("  Running multi-condition queries...~n"),
    MultiCond = barrel_bench_metrics:summarize(bench_query(Db, multi_condition_query(), Iterations)),

    io:format("  Running multi-index intersection (type=user AND status=active)...~n"),
    MultiIndex = barrel_bench_metrics:summarize(bench_query(Db, multi_index_query(), Iterations)),

    io:format("  Running multi-index range (type=user AND age>50)...~n"),
    MultiIndexRange = barrel_bench_metrics:summarize(bench_query(Db, multi_index_range_query(), Iterations)),

    io:format("  Running 3-condition query (type=user AND status=active AND age>50)...~n"),
    ThreeCond = barrel_bench_metrics:summarize(bench_query(Db, three_condition_query(), Iterations)),

    io:format("  Running 3-condition with LIMIT 10...~n"),
    ThreeCondLimit = barrel_bench_metrics:summarize(bench_query(Db, three_condition_limit_query(), Iterations)),

    io:format("  Running multi-index with LIMIT 10...~n"),
    MultiIndexLimit = barrel_bench_metrics:summarize(bench_query(Db, multi_index_limit_query(), Iterations)),

    io:format("  Running multi-index range with LIMIT 10...~n"),
    MultiIndexRangeLimit = barrel_bench_metrics:summarize(bench_query(Db, multi_index_range_limit_query(), Iterations)),

    io:format("  Running nested path queries...~n"),
    NestedPath = barrel_bench_metrics:summarize(bench_query(Db, nested_path_query(), Iterations)),

    io:format("  Running ORDER BY + LIMIT queries (Top-K)...~n"),
    TopK = barrel_bench_metrics:summarize(bench_query(Db, order_by_limit_query(), Iterations)),

    io:format("  Running pure ORDER BY + LIMIT (no filter)...~n"),
    PureTopK = barrel_bench_metrics:summarize(bench_query(Db, pure_order_limit_query(), Iterations)),

    io:format("  Running FILTER + ORDER BY + LIMIT (lazy ORDER BY with equality)...~n"),
    FilterOrderEq = barrel_bench_metrics:summarize(bench_query(Db, filter_order_eq_limit_query(), Iterations)),

    io:format("  Running FILTER + ORDER BY + LIMIT (lazy ORDER BY with range)...~n"),
    FilterOrderRange = barrel_bench_metrics:summarize(bench_query(Db, filter_order_range_limit_query(), Iterations)),

    io:format("  Running FILTER + ORDER BY + LIMIT (lazy ORDER BY with 2 conditions)...~n"),
    FilterOrder2Cond = barrel_bench_metrics:summarize(bench_query(Db, filter_order_2cond_limit_query(), Iterations)),

    io:format("  Running prefix queries...~n"),
    PrefixQ = barrel_bench_metrics:summarize(bench_query(Db, prefix_query(), Iterations)),

    io:format("  Running prefix with LIMIT 10...~n"),
    PrefixLimit = barrel_bench_metrics:summarize(bench_query(Db, prefix_limit_query(), Iterations)),

    io:format("  Running exists queries...~n"),
    ExistsQ = barrel_bench_metrics:summarize(bench_query(Db, exists_query(), Iterations)),

    io:format("  Running exists with LIMIT 10...~n"),
    ExistsLimit = barrel_bench_metrics:summarize(bench_query(Db, exists_limit_query(), Iterations)),

    io:format("  Running OR condition queries...~n"),
    OrCond = barrel_bench_metrics:summarize(bench_query(Db, or_condition_query(), Iterations)),

    io:format("  Running OR condition with LIMIT 10...~n"),
    OrCondLimit = barrel_bench_metrics:summarize(bench_query(Db, or_condition_limit_query(), Iterations)),

    io:format("  Running range with continuation (100 per page, ~~48% of docs)...~n"),
    RangeContinuation = barrel_bench_metrics:summarize(bench_range_continuation(Db, 100)),

    io:format("  Running range with continuation (500 per page)...~n"),
    RangeContinuation500 = barrel_bench_metrics:summarize(bench_range_continuation(Db, 500)),

    %% Pipelining tests - unbounded queries WITH include_docs=true
    io:format("  Running range WITH include_docs (tests pipelining)...~n"),
    RangeWithDocs = barrel_bench_metrics:summarize(bench_query(Db, range_with_docs_query(), Iterations)),

    io:format("  Running multi_condition WITH include_docs (tests pipelining)...~n"),
    MultiCondWithDocs = barrel_bench_metrics:summarize(bench_query(Db, multi_condition_with_docs_query(), Iterations)),

    #{
        simple_eq => SimpleEq,
        simple_eq_limit => SimpleEqLimit,
        selective_eq => SelectiveEq,
        very_selective_eq => VerySelectiveEq,
        range => Range,
        range_limit => RangeLimit,
        pure_compare => PureCompare,
        pure_compare_limit => PureCompareLimit,
        multi_condition => MultiCond,
        multi_index => MultiIndex,
        multi_index_range => MultiIndexRange,
        three_cond => ThreeCond,
        three_cond_limit => ThreeCondLimit,
        multi_index_limit => MultiIndexLimit,
        multi_index_range_limit => MultiIndexRangeLimit,
        nested_path => NestedPath,
        order_by_limit => TopK,
        pure_topk => PureTopK,
        filter_order_eq => FilterOrderEq,
        filter_order_range => FilterOrderRange,
        filter_order_2cond => FilterOrder2Cond,
        prefix => PrefixQ,
        prefix_limit => PrefixLimit,
        exists => ExistsQ,
        exists_limit => ExistsLimit,
        or_condition => OrCond,
        or_condition_limit => OrCondLimit,
        range_cont_100 => RangeContinuation,
        range_cont_500 => RangeContinuation500,
        range_with_docs => RangeWithDocs,
        multi_cond_docs => MultiCondWithDocs
    }.

%%====================================================================
%% Query definitions
%%====================================================================
%%
%% Result counts for 5000 documents:
%% - simple_eq (type=user): ALL 5000 docs
%% - selective_eq (status=active): ~1666 docs (1/3)
%% - very_selective_eq (city=Paris): ~625 docs (1/8)
%% - pure_compare (age>50): ~2380 docs (48%, ages 51-80 of 18-80 range)
%% - range (type=user AND age>50): ~2380 docs (same as pure_compare since all are users)
%% - multi_index (type=user AND status=active): ~1666 docs
%% - prefix (name starts "User 1"): ~1111 docs (1 + 10-19 + 100-199 + 1000-1999)
%% - exists (profile field): ALL 5000 docs
%%====================================================================

simple_eq_query() ->
    %% Simple equality: find all users with type=user
    %% Returns ALL 5000 docs - all test docs have type=user
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [{path, [<<"type">>], <<"user">>}], include_docs => false}.

simple_eq_limit_query() ->
    %% Simple equality with LIMIT - tests early termination
    #{where => [{path, [<<"type">>], <<"user">>}], limit => 10, include_docs => false}.

selective_eq_query() ->
    %% Selective equality: find docs with status=active (~1/3 of docs)
    %% Shows that we only scan matching keys, not all docs
    #{where => [{path, [<<"status">>], <<"active">>}], include_docs => false}.

very_selective_eq_query() ->
    %% Very selective: find docs with profile.city=Paris (~1/8 of docs)
    #{where => [{path, [<<"profile">>, <<"city">>], <<"Paris">>}], include_docs => false}.

range_query() ->
    %% Multi-condition range query: type=user AND age > 50
    %% Returns ~2380 docs (48% of 5000) with include_docs=true (default)
    %% Ages cycle 18-80, so age>50 matches ages 51-80 = 30 values out of 63
    %% This is SLOW because it fetches ~2380 document bodies
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>', 50}
    ]}.

range_limit_query() ->
    %% Range query WITH LIMIT and include_docs=true
    %% This tests the streaming batch fetch optimization:
    %% - Has ~2380 matching DocIds but only needs 10 results
    %% - Streaming should only fetch ~30-50 bodies instead of 2380
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>', 50}
    ], limit => 10}.

pure_compare_query() ->
    %% Pure compare query: find all docs where age > 50
    %% Uses optimized range scan instead of full scan + filter
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [{compare, [<<"age">>], '>', 50}], include_docs => false}.

pure_compare_limit_query() ->
    %% Pure compare with LIMIT - tests early termination on range scans
    #{where => [{compare, [<<"age">>], '>', 50}], limit => 10, include_docs => false}.

multi_condition_query() ->
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ]}.

multi_index_query() ->
    %% Multi-condition query using posting list intersection
    %% Uses optimized index intersection instead of full scan + filter
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ], include_docs => false}.

multi_index_range_query() ->
    %% Multi-condition with equality + compare using index intersection
    %% Tests intersection of equality posting list with range scan
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>', 50}
    ], include_docs => false}.

multi_index_limit_query() ->
    %% Multi-condition with LIMIT - tests early termination in intersection
    %% Should be much faster than unbounded multi_index_query
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ], limit => 10, include_docs => false}.

multi_index_range_limit_query() ->
    %% Multi-condition range with LIMIT - tests early termination
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>', 50}
    ], limit => 10, include_docs => false}.

three_condition_query() ->
    %% 3 conditions - triggers parallel collection
    %% type=user AND status=active AND age>50
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>},
        {compare, [<<"age">>], '>', 50}
    ], include_docs => false}.

three_condition_limit_query() ->
    %% 3 conditions with LIMIT
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>},
        {compare, [<<"age">>], '>', 50}
    ], limit => 10, include_docs => false}.

nested_path_query() ->
    #{where => [{path, [<<"profile">>, <<"city">>], <<"Paris">>}]}.

order_by_limit_query() ->
    %% Get the 10 most recently created users (ORDER BY created_at DESC LIMIT 10)
    #{where => [{path, [<<"type">>], <<"user">>}],
      order_by => {[<<"created_at">>], desc},
      limit => 10}.

pure_order_limit_query() ->
    %% Pure ORDER BY + LIMIT with no filter conditions
    %% This is where Top-K optimization provides the most benefit
    #{where => [],
      order_by => {[<<"created_at">>], desc},
      limit => 10}.

filter_order_eq_limit_query() ->
    %% FILTER + ORDER BY + LIMIT with equality filter (lazy ORDER BY)
    %% Tests lazy index-based filtering: type=user, ~5000 matches
    %% Iterates ORDER BY index, checks type via docid_has_value (O(1))
    #{where => [{path, [<<"type">>], <<"user">>}],
      order_by => {[<<"created_at">>], desc},
      limit => 10}.

filter_order_range_limit_query() ->
    %% FILTER + ORDER BY + LIMIT with range filter (lazy ORDER BY)
    %% Tests lazy index-based filtering: age > 50, ~2380 matches
    %% Iterates ORDER BY index, checks age via docid_satisfies_compare
    #{where => [{compare, [<<"age">>], '>', 50}],
      order_by => {[<<"created_at">>], desc},
      limit => 10}.

filter_order_2cond_limit_query() ->
    %% FILTER + ORDER BY + LIMIT with 2 conditions (lazy ORDER BY)
    %% Tests lazy index-based filtering: type=user AND age > 50
    %% This is the key optimization: O(limit) instead of O(matching_docs)
    %% Before: collect 2380 DocIds, sort, apply limit
    %% After: iterate ORDER BY index, lazy check both conditions, stop at limit
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>', 50}
    ],
      order_by => {[<<"created_at">>], desc},
      limit => 10}.

prefix_query() ->
    %% Prefix query: find all users whose name starts with "User 1"
    %% Uses optimized interval scan instead of full scan + regex
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [{prefix, [<<"name">>], <<"User 1">>}], include_docs => false}.

prefix_limit_query() ->
    %% Prefix query with LIMIT - tests early termination
    %% Should be very fast regardless of how many docs match
    #{where => [{prefix, [<<"name">>], <<"User 1">>}], limit => 10, include_docs => false}.

exists_query() ->
    %% Exists query: find all docs that have a "profile" field
    %% Uses path index scan without fetching full documents
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [{exists, [<<"profile">>]}], include_docs => false}.

exists_limit_query() ->
    %% Exists query with LIMIT - tests early termination
    %% Should be very fast regardless of how many docs have the field
    #{where => [{exists, [<<"profile">>]}], limit => 10, include_docs => false}.

or_condition_query() ->
    %% OR condition query: find docs with type=user OR type=admin
    %% Tests full scan with OR filter
    #{where => [{'or', [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ]}], include_docs => false}.

or_condition_limit_query() ->
    %% OR condition with LIMIT - tests relaxed early limit with remaining conditions
    %% Uses over-collection to handle filter losses
    #{where => [{'or', [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ]}], limit => 10, include_docs => false}.

range_with_docs_query() ->
    %% Range query WITH include_docs=true - tests pipelined body fetching
    %% Returns ~2380 docs (48% of 5000) and fetches their bodies
    %% Should trigger pipelined execution (parallel body fetch)
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>', 50}
    ], include_docs => true}.

multi_condition_with_docs_query() ->
    %% Multi-condition WITH include_docs=true - tests pipelined body fetching
    %% Returns ~1666 docs (1/3 of 5000) and fetches their bodies
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ], include_docs => true}.

%%====================================================================
%% Internal functions
%%====================================================================

load_docs(Db, NumDocs) ->
    lists:foreach(fun(I) ->
        Doc = barrel_bench_generator:user_doc(I),
        barrel_docdb:put_doc(Db, Doc)
    end, lists:seq(0, NumDocs - 1)).

bench_query(Db, Query, Iterations) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(_I, Acc) ->
        {Time, _Result} = timer:tc(fun() ->
            {ok, _Results, _Meta} = barrel_docdb:find(Db, Query),
            ok
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(1, Iterations)).

%% @doc Benchmark range query with continuation pattern.
%% Pages through all results using continuation tokens.
%% Each call to execute returns at most PageSize results.
%% Measures time per page, not total time for all pages.
%%
%% NOTE: Uses include_docs=false because chunked body-fetch queries
%% are not yet implemented. This tests pure index pagination performance.
bench_range_continuation(Db, PageSize) ->
    %% Query: age > 50, returns ~2380 docs (48% of 5000)
    %% Uses include_docs=false for pure index pagination benchmark
    {ok, StoreRef} = barrel_db_server:get_store_ref(Db),
    {ok, Info} = barrel_db_server:info(Db),
    DbName = maps:get(name, Info),
    QuerySpec = #{
        where => [{compare, [<<"age">>], '>', 50}],
        include_docs => false
    },
    {ok, Plan} = barrel_query:compile(QuerySpec),
    Metrics = barrel_bench_metrics:new(),
    bench_range_continuation_loop(StoreRef, DbName, Plan, PageSize, undefined, Metrics).

bench_range_continuation_loop(StoreRef, DbName, Plan, PageSize, Continuation, Metrics) ->
    ChunkOpts = case Continuation of
        undefined -> #{chunk_size => PageSize};
        Token -> #{chunk_size => PageSize, continuation => Token}
    end,
    {Time, Result} = timer:tc(fun() ->
        barrel_query:execute(StoreRef, DbName, Plan, ChunkOpts)
    end),
    Metrics1 = barrel_bench_metrics:record(Metrics, Time),
    case Result of
        {ok, _Results, #{has_more := true, continuation := NextToken}} ->
            bench_range_continuation_loop(StoreRef, DbName, Plan, PageSize, NextToken, Metrics1);
        {ok, _Results, #{has_more := false}} ->
            %% No more pages
            Metrics1;
        {error, Reason} ->
            io:format("Range continuation error: ~p~n", [Reason]),
            Metrics1
    end.
