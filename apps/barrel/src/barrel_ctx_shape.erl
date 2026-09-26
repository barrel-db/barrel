%%%-------------------------------------------------------------------
%%% @doc Federated query shapes: classify a compiled BQL plan into one
%%% of the three first-release shapes (ordered rows, unordered rows,
%%% retrieval) or reject it with a reason, and read a row's order key.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_shape).

-export([classify/3, order_value/2, retrieval/1]).

-define(MAX_ROWS, 1000).

-type merge() :: grouped | ordered | interleave | score.
%% `score_mode': `auto' falls back to grouped when members are not
%% score comparable; `required' rejects the request instead.
-type shape() :: #{kind := ordered | unordered | retrieval,
                   merge := merge(),
                   bound := pos_integer(),
                   retrieval := exact | approximate,
                   order := undefined | {term(), asc | desc},
                   fn := atom() | undefined,
                   score_mode => auto | required}.
-type reason() :: subscribe | offset | unnest | continuation
                | limit_required | limit_too_large | order_key_not_projected
                | order_by_required | {merge_not_supported, atom()}
                | {merge_not_allowed, atom()}.
-export_type([merge/0, shape/0, reason/0]).

%% @doc Accept a plan for the requested merge (`default' picks one).
-spec classify(barrel_bql_lower:plan(), merge() | default | atom(), map()) ->
    {ok, shape()} | {error, {unsupported_federated_query, reason()}}.
classify(#{subscribe := true}, _Merge, _Req) ->
    reject(subscribe);
classify(_Plan, _Merge, #{continuation := _}) ->
    reject(continuation);
classify(#{post := #{offset := Offset}}, _Merge, _Req) when Offset > 0 ->
    reject(offset);
classify(#{unnest := #{}}, _Merge, _Req) ->
    reject(unnest);
%% rerank needs a validated cross-encoder (opt-in, not built); RRF across
%% contexts is not a relevance order.
classify(_Plan, Merge, _Req) when Merge =:= rerank; Merge =:= rrf ->
    reject({merge_not_supported, Merge});
classify(#{source := {collection, _}, post := #{limit := undefined}},
         _Merge, _Req) ->
    reject(limit_required);
classify(#{source := {collection, _}, post := #{limit := Limit}},
         _Merge, _Req) when Limit > ?MAX_ROWS ->
    reject(limit_too_large);
classify(#{source := {collection, _}}, score, _Req) ->
    reject({merge_not_allowed, score});
classify(#{source := {collection, _},
           post := #{order := undefined, limit := Limit}}, Merge, _Req) ->
    case Merge of
        ordered -> reject(order_by_required);
        interleave -> {ok, shape(unordered, interleave, Limit, exact,
                                 undefined, undefined)};
        _ -> {ok, shape(unordered, grouped, Limit, exact, undefined,
                        undefined)}
    end;
classify(#{source := {collection, _},
           post := #{order := Order, limit := Limit, project := Project}},
         Merge, _Req) ->
    case row_order(Order, Project) of
        {ok, RowOrder} ->
            {ok, shape(ordered, ordered_merge(Merge), Limit, exact,
                       RowOrder, undefined)};
        error ->
            reject(order_key_not_projected)
    end;
classify(#{source := {table_fn, Fn, _Args}} = Plan, Merge, _Req) ->
    Bound = barrel_bql_query:row_bound(Plan),
    case Bound > ?MAX_ROWS of
        true -> reject(limit_too_large);
        false -> retrieval_merge(Fn, Merge, Bound)
    end.

%% @doc The value a row sorts on (`null' when absent, as members sort).
-spec order_value(map(), shape() | {term(), asc | desc}) -> term().
order_value(Row, #{order := Order}) ->
    order_value(Row, Order);
order_value(Row, {{score, Name}, _Dir}) ->
    maps:get(Name, Row, null);
order_value(Row, {{b, Comps}, _Dir}) ->
    get_path(Row, Comps);
order_value(Row, {{out, Name}, _Dir}) ->
    maps:get(Name, Row, null).

%% @doc `exact' for bm25 and collections, `approximate' for ANN.
-spec retrieval(atom()) -> exact | approximate.
retrieval(bm25_top_k) -> exact;
retrieval(vector_top_k) -> approximate;
retrieval(hybrid_top_k) -> approximate.

%%====================================================================
%% Internal
%%====================================================================

shape(Kind, Merge, Bound, Retrieval, Order, Fn) ->
    #{kind => Kind, merge => Merge, bound => Bound, retrieval => Retrieval,
      order => Order, fn => Fn}.

%% Vector scores merge only under one embedding space: automatic for
%% vector_top_k, on request otherwise refused. BM25 and hybrid (RRF)
%% scores never merge across contexts.
retrieval_merge(_Fn, ordered, _Bound) ->
    reject({merge_not_allowed, ordered});
retrieval_merge(vector_top_k, score, Bound) ->
    {ok, (retrieval_shape(vector_top_k, score, Bound))#{score_mode => required}};
retrieval_merge(_Fn, score, _Bound) ->
    reject({merge_not_allowed, score});
retrieval_merge(vector_top_k, default, Bound) ->
    {ok, (retrieval_shape(vector_top_k, score, Bound))#{score_mode => auto}};
retrieval_merge(Fn, interleave, Bound) ->
    {ok, retrieval_shape(Fn, interleave, Bound)};
retrieval_merge(Fn, _GroupedOrDefault, Bound) ->
    {ok, retrieval_shape(Fn, grouped, Bound)}.

retrieval_shape(Fn, Merge, Bound) ->
    shape(retrieval, Merge, Bound, retrieval(Fn), undefined, Fn).

reject(Reason) ->
    {error, {unsupported_federated_query, Reason}}.

ordered_merge(grouped) -> grouped;
ordered_merge(interleave) -> interleave;
ordered_merge(_Default) -> ordered.

%% Where the order key sits in a result row: under SELECT * at its own
%% path; with a projection list, under the output name that selects it.
row_order({{b, _}, _Dir} = Order, star) ->
    {ok, Order};
row_order({{score, _}, _Dir} = Order, star) ->
    {ok, Order};
row_order({Key, Dir}, Project) when is_list(Project) ->
    case [Name || {Tag, K, Name} <- Project, {Tag, K} =:= Key] of
        [Name | _] -> {ok, {{out, Name}, Dir}};
        [] -> error
    end;
row_order(_Order, _Project) ->
    error.

get_path(Value, []) ->
    Value;
get_path(Map, [Key | Rest]) when is_map(Map), is_binary(Key) ->
    case maps:find(Key, Map) of
        {ok, V} -> get_path(V, Rest);
        error -> null
    end;
get_path(List, [Index | Rest]) when is_list(List), is_integer(Index),
                                    Index >= 0, Index < length(List) ->
    get_path(lists:nth(Index + 1, List), Rest);
get_path(_Other, _Path) ->
    null.
