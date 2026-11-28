%% @doc The `view' BQL source: `FROM view('design', 'view'[, group_level
%% => N | reduce => true]) AS v'.
%%
%% Map rows are `{id, key, value}', grouped rows `{key, value}'. Rows
%% come in key order, so `ORDER BY v.key' streams. Conditions on
%% `v.key' narrow the scan (barrel_design_pushdown). Query options
%% `freshness', `timeout_ms', `authorize' and `max_scan' pass through to
%% barrel_design:query/3.
-module(barrel_design_bql).
-behaviour(barrel_bql_source).

-export([init_args/2, run/4, ordered/2, explain/2]).

init_args([Design, View], Named) when is_binary(Design), is_binary(View) ->
    try
        Mode = maps:fold(fun
            (<<"group_level">>, N, Acc) when is_integer(N), N >= 0 -> Acc#{group_level => N};
            (<<"group_level">>, <<"exact">>, Acc) -> Acc#{group_level => exact};
            (<<"reduce">>, B, Acc) when is_boolean(B) -> Acc#{reduce => B};
            (K, V, _Acc) -> throw({invalid_option, K, V})
        end, #{}, Named),
        Reduce = case Mode of
            #{reduce := R} -> R;
            #{group_level := _} -> true;
            _ -> false
        end,
        Reduce orelse not maps:is_key(group_level, Mode) orelse throw(group_level_without_reduce),
        {ok, #{design => Design, view => View, reduce => Reduce,
               group_level => maps:get(group_level, Mode, 0)}}
    catch
        throw:Why -> {error, Why}
    end;
init_args(_Positional, _Named) ->
    {error, design_and_view_required}.

run(Db, #{design := D, view := V, reduce := Reduce, group_level := Level},
    #{where := Where, chunk_size := Chunk} = Query, Opts) ->
    QOpts0 = maps:with([freshness, timeout_ms, authorize, max_scan], Opts),
    QOpts1 = QOpts0#{where => Where, limit => Chunk, reduce => Reduce, group_level => Level},
    QOpts = case maps:find(continuation, Query) of
        {ok, C} -> QOpts1#{continuation => C};
        error -> QOpts1
    end,
    case barrel_design:query(Db, {D, V}, QOpts) of
        {ok, Rows, Meta} ->
            Out = [row(R) || R <- Rows],
            {ok, Out, maps:with([continuation, generation, checkpoint], Meta)};
        {error, _} = Error ->
            Error
    end.

row(#{id := Id, key := K, value := Val}) ->
    #{<<"id">> => Id, <<"key">> => K, <<"value">> => Val};
row(#{key := K, value := Val}) ->
    #{<<"key">> => K, <<"value">> => Val}.

ordered(_Args, {[<<"key">>], asc}) -> true;
ordered(_Args, _Order) -> false.

explain(#{design := D, view := V, reduce := R, group_level := L}, #{where := Where}) ->
    #{design => D, view => V, reduce => R, group_level => L,
      key_conditions => length([C || C <- Where, key_cond(C)])}.

key_cond({path, [<<"key">> | _], _}) -> true;
key_cond({compare, [<<"key">> | _], _, _}) -> true;
key_cond({prefix, [<<"key">> | _], _}) -> true;
key_cond(_) -> false.
