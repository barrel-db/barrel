%% @doc Native projections: a view's key and value read straight from
%% document paths, without a guest. A missing path or a value of another
%% type yields no row, as a guest map that finds no field emits nothing.
-module(barrel_design_native).

-export([map/2]).

-define(I64_MIN, -(1 bsl 63)).
-define(I64_MAX, (1 bsl 63) - 1).

-spec map(map(), map()) -> barrel_design_guest:result().
map(#{key := KeySpec, value := ValueSpec}, Doc) ->
    case {key(KeySpec, Doc), value(ValueSpec, Doc)} of
        {{ok, Key}, {ok, Value}} -> {ok, [{Key, Value}]};
        {_, {error, _} = Error} -> Error;
        _ -> {ok, []}
    end.

key({array, Comps}, Doc) ->
    Parts = [part(C, Doc) || C <- Comps],
    case lists:all(fun({ok, _}) -> true; (_) -> false end, Parts) of
        true -> {ok, [V || {ok, V} <- Parts]};
        false -> none
    end;
key({scalar, Comp}, Doc) ->
    part(Comp, Doc).

part(#{path := Path, type := Type}, Doc) ->
    case lookup(Path, Doc) of
        {ok, V} ->
            case typed(Type, V) of
                true -> {ok, V};
                false -> none
            end;
        not_found -> none
    end.

value(#{sum := Path}, Doc) ->
    case lookup(Path, Doc) of
        {ok, L} when is_list(L) ->
            case lists:all(fun is_integer/1, L) of
                true ->
                    case lists:sum(L) of
                        S when S >= ?I64_MIN, S =< ?I64_MAX -> {ok, S};
                        _ -> {error, sum_overflow}
                    end;
                false -> none
            end;
        _ -> none
    end;
value(#{path := Path, type := Type}, Doc) ->
    case lookup(Path, Doc) of
        {ok, V} ->
            case typed(Type, V) of
                true -> {ok, V};
                false -> none
            end;
        not_found -> none
    end;
value(#{type := _}, _Doc) ->
    {ok, null}.

typed(text, V) -> is_binary(V);
typed(i64, V) -> is_integer(V) andalso V >= ?I64_MIN andalso V =< ?I64_MAX;
typed(bool, V) -> is_boolean(V);
typed(any, V) -> barrel_design_key:valid(V).

lookup([], V) -> {ok, V};
lookup([K | Rest], M) when is_map(M), is_binary(K) ->
    case maps:find(K, M) of
        {ok, V} -> lookup(Rest, V);
        error -> not_found
    end;
lookup([I | Rest], L) when is_list(L), is_integer(I), I >= 0, I < length(L) ->
    lookup(Rest, lists:nth(I + 1, L));
lookup(_, _) -> not_found.
