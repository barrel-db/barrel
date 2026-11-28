%% A fixed-rows FROM source for the registered-source tests:
%% `rows(N[, push => true])' yields #{id, n, key => [g, n]} for 1..N in
%% n order, paged by chunk_size. With push it narrows on `n =' itself
%% (a pushdown); without it returns every row and relies on the
%% executor to apply WHERE. Every call is reported to the test process
%% registered as `bql_test_source_probe', when there is one.
-module(barrel_bql_test_source).
-behaviour(barrel_bql_source).

-export([init_args/2, run/4, ordered/2, explain/2]).

init_args([N], Named) when is_integer(N), N >= 0 ->
    case maps:keys(Named) -- [<<"push">>] of
        [] -> {ok, #{n => N, push => maps:get(<<"push">>, Named, false) =:= true}};
        Unknown -> {error, {unknown_options, Unknown}}
    end;
init_args(_Positional, _Named) ->
    {error, count_required}.

run(_Db, #{n := N, push := Push}, Query, _Opts) ->
    report(Query),
    #{where := Where, chunk_size := Chunk} = Query,
    All = [row(I) || I <- lists:seq(1, N)],
    Rows = case Push of
        true -> [R || R <- All, pushed(Where, R)];
        false -> All
    end,
    Start = maps:get(continuation, Query, 0),
    Page = lists:sublist(Rows, Start + 1, Chunk),
    Next = Start + length(Page),
    Meta = case Next < length(Rows) of
        true -> #{continuation => Next, pages => served};
        false -> #{pages => served}
    end,
    {ok, Page, Meta}.

ordered(_Args, {[<<"n">>], asc}) -> true;
ordered(_Args, _Order) -> false.

explain(#{n := N}, _Query) -> #{rows => N}.

row(I) ->
    #{<<"id">> => integer_to_binary(I), <<"n">> => I, <<"key">> => [<<"g">>, I]}.

pushed(Where, Row) ->
    lists:all(fun({path, [<<"n">>], V}) -> maps:get(<<"n">>, Row) =:= V;
                 (_) -> true
              end, Where).

report(Query) ->
    case whereis(bql_test_source_probe) of
        undefined -> ok;
        Pid -> Pid ! {source_run, Query}
    end.
