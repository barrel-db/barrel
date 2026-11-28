%% @doc Key bounds from BQL WHERE conditions on `v.key'.
%%
%% Only conditions that can narrow the scan are used, and BQL applies
%% every condition again to the rows, so the bounds may be wider than
%% the result but never narrower:
%% <ul>
%% <li>equalities on `v.key[0]', `v.key[1]', ... from component 0 form
%%     a key prefix;</li>
%% <li>one range (`<', `<=', `>', `>=', or `LIKE 'stem%'') on the next
%%     component bounds the scan inside that prefix;</li>
%% <li>on a scalar key, `v.key = X', ranges and LIKE on `v.key'.</li>
%% </ul>
%% For grouped rows only components below the group level count: WHERE
%% filters the groups the view returns. `v.key = X' on an array key means
%% membership in BQL, so it is never used.
-module(barrel_design_pushdown).

-export([bounds/3]).

-spec bounds({array, list()} | {scalar, map()}, [term()], map | {reduce, exact | non_neg_integer()}) ->
    none | {binary(), binary()}.
bounds({array, Comps}, Where, Mode) ->
    Max = case Mode of
        map -> length(Comps);
        {reduce, exact} -> length(Comps);
        {reduce, N} -> min(N, length(Comps))
    end,
    Eq = maps:from_list([{I, V} || {path, [<<"key">>, I], V} <- Where,
                                   is_integer(I), I < Max, valid(V)]),
    Prefix = leading(Eq, 0, []),
    K = length(Prefix),
    Base = barrel_design_key:prefix(Prefix),
    Range = case K < Max of
        true -> range([<<"key">>, K], Where, Base);
        false -> none
    end,
    case {K, Range} of
        {0, none} -> none;
        {_, none} -> {Base, barrel_design_key:after_prefix(Base)};
        {_, R} -> R
    end;
bounds({scalar, _}, Where, Mode) when Mode =:= map; Mode =:= {reduce, exact} ->
    scalar(Where);
bounds({scalar, _}, Where, {reduce, N}) when N >= 1 ->
    scalar(Where);
bounds(_KeySpec, _Where, _Mode) ->
    none.

scalar(Where) ->
    case [V || {path, [<<"key">>], V} <- Where, valid(V), not is_list(V)] of
        [V | _] ->
            E = barrel_design_key:encode(V),
            {E, barrel_design_key:after_prefix(E)};
        [] ->
            range([<<"key">>], Where, <<>>)
    end.

leading(Eq, I, Acc) ->
    case maps:find(I, Eq) of
        {ok, V} -> leading(Eq, I + 1, Acc ++ [V]);
        error -> Acc
    end.

%% Bounds inside Base from the range conditions on Path.
range(Path, Where, Base) ->
    Conds = [C || C <- Where, on(Path, C)],
    Lower = lists:foldl(fun
        ({compare, _, '>=', V}, L) -> pick_max(L, <<Base/binary, (enc(V))/binary>>);
        ({compare, _, '>', V}, L) -> pick_max(L, barrel_design_key:after_prefix(<<Base/binary, (enc(V))/binary>>));
        ({prefix, _, Stem}, L) -> pick_max(L, <<Base/binary, (barrel_design_key:text_prefix(Stem))/binary>>);
        (_, L) -> L
    end, undefined, Conds),
    Upper = lists:foldl(fun
        ({compare, _, '<', V}, U) -> pick_min(U, <<Base/binary, (enc(V))/binary>>);
        ({compare, _, '=<', V}, U) -> pick_min(U, barrel_design_key:after_prefix(<<Base/binary, (enc(V))/binary>>));
        ({prefix, _, Stem}, U) ->
            pick_min(U, barrel_design_key:after_prefix(<<Base/binary, (barrel_design_key:text_prefix(Stem))/binary>>));
        (_, U) -> U
    end, undefined, Conds),
    case {Lower, Upper} of
        {undefined, undefined} -> none;
        _ ->
            Top = case Base of
                <<>> -> <<16#FF>>;
                _ -> barrel_design_key:after_prefix(Base)
            end,
            {default(Lower, Base), default(Upper, Top)}
    end.

on(Path, {compare, Path, Op, V}) ->
    lists:member(Op, ['<', '=<', '>', '>=']) andalso valid(V) andalso not is_list(V);
on(Path, {prefix, Path, Stem}) -> is_binary(Stem);
on(_Path, _C) -> false.

enc(V) -> barrel_design_key:encode(V).

valid(V) -> barrel_design_key:valid(V).

pick_max(undefined, B) -> B;
pick_max(A, B) -> max(A, B).

pick_min(undefined, B) -> B;
pick_min(A, B) -> min(A, B).

default(undefined, D) -> D;
default(V, _D) -> V.
