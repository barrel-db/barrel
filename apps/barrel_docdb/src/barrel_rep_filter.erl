%% @doc Wire codec for replication filters (paths, query, channel).
%%
%% Query conditions are Erlang tuples; on the wire they become tagged
%% JSON arrays whitelisted 1:1 against the engine forms, e.g.
%% `["path", ["type"], "user"]' or `["compare", ["rank"], ">", 3]'.
%% Decoding uses explicit clauses only: no atoms are created from wire
%% input, unknown tags reject with {bad_filter, _}. The one lossy spot:
%% the wildcard path component `'*'' rides as the string "*" (a doc
%% field literally named "*" cannot be addressed through a wire
%% filter).
%%
%% The same codec serializes filters for replication-task persistence.
-module(barrel_rep_filter).

%% Bounds and/or/not nesting from the wire so a hostile peer cannot force
%% deep recursion (stack/CPU) with a pathological filter.
-define(MAX_FILTER_DEPTH, 32).

-export([to_wire/1, from_wire/1]).

%%====================================================================
%% Encode
%%====================================================================

-spec to_wire(map()) -> map().
to_wire(Filter) ->
    Acc0 = #{},
    Acc1 = case maps:get(paths, Filter, undefined) of
        undefined -> Acc0;
        Paths -> Acc0#{<<"paths">> => Paths}
    end,
    Acc2 = case maps:get(channel, Filter, undefined) of
        undefined -> Acc1;
        Channel -> Acc1#{<<"channel">> => Channel}
    end,
    case maps:get(query, Filter, undefined) of
        undefined ->
            Acc2;
        Query ->
            Where = maps:get(where, Query, []),
            Acc2#{<<"query">> =>
                      #{<<"where">> => [cond_to_wire(C) || C <- Where]}}
    end.

cond_to_wire({path, Path, Value}) ->
    [<<"path">>, path_to_wire(Path), Value];
cond_to_wire({compare, Path, Op, Value}) ->
    [<<"compare">>, path_to_wire(Path), op_to_wire(Op), Value];
cond_to_wire({'and', Conds}) ->
    [<<"and">>, [cond_to_wire(C) || C <- Conds]];
cond_to_wire({'or', Conds}) ->
    [<<"or">>, [cond_to_wire(C) || C <- Conds]];
cond_to_wire({'not', Cond}) ->
    [<<"not">>, cond_to_wire(Cond)];
cond_to_wire({in, Path, Values}) ->
    [<<"in">>, path_to_wire(Path), Values];
cond_to_wire({contains, Path, Value}) ->
    [<<"contains">>, path_to_wire(Path), Value];
cond_to_wire({exists, Path}) ->
    [<<"exists">>, path_to_wire(Path)];
cond_to_wire({missing, Path}) ->
    [<<"missing">>, path_to_wire(Path)];
cond_to_wire({regex, Path, Pattern}) ->
    [<<"regex">>, path_to_wire(Path), Pattern];
cond_to_wire({prefix, Path, Prefix}) ->
    [<<"prefix">>, path_to_wire(Path), Prefix].

path_to_wire(Path) ->
    [case Component of
         '*' -> <<"*">>;
         _ -> Component
     end || Component <- Path].

op_to_wire('>') -> <<">">>;
op_to_wire('<') -> <<"<">>;
op_to_wire('>=') -> <<">=">>;
op_to_wire('=<') -> <<"=<">>;
op_to_wire('==') -> <<"==">>;
op_to_wire('=/=') -> <<"=/=">>.

%%====================================================================
%% Decode
%%====================================================================

-spec from_wire(map()) -> {ok, map()} | {error, {bad_filter, term()}}.
from_wire(Wire) when is_map(Wire) ->
    try
        Acc0 = #{},
        Acc1 = case maps:get(<<"paths">>, Wire, undefined) of
            undefined -> Acc0;
            Paths when is_list(Paths) ->
                lists:foreach(
                    fun(P) when is_binary(P) -> ok;
                       (P) -> throw({bad_filter, {paths, P}})
                    end, Paths),
                Acc0#{paths => Paths};
            Bad -> throw({bad_filter, {paths, Bad}})
        end,
        Acc2 = case maps:get(<<"channel">>, Wire, undefined) of
            undefined -> Acc1;
            Channel when is_binary(Channel) -> Acc1#{channel => Channel};
            BadC -> throw({bad_filter, {channel, BadC}})
        end,
        Acc3 = case maps:get(<<"query">>, Wire, undefined) of
            undefined ->
                Acc2;
            #{<<"where">> := Where} when is_list(Where) ->
                Acc2#{query =>
                          #{where => [cond_from_wire(C, ?MAX_FILTER_DEPTH)
                                      || C <- Where]}};
            BadQ ->
                throw({bad_filter, {query, BadQ}})
        end,
        {ok, Acc3}
    catch
        throw:{bad_filter, _} = Reason -> {error, Reason}
    end;
from_wire(Other) ->
    {error, {bad_filter, Other}}.

cond_from_wire(_Cond, 0) ->
    throw({bad_filter, too_deeply_nested});
cond_from_wire([<<"path">>, Path, Value], _D) ->
    {path, path_from_wire(Path), value_from_wire(Value)};
cond_from_wire([<<"compare">>, Path, Op, Value], _D) ->
    {compare, path_from_wire(Path), op_from_wire(Op),
     value_from_wire(Value)};
cond_from_wire([<<"and">>, Conds], D) when is_list(Conds) ->
    {'and', [cond_from_wire(C, D - 1) || C <- Conds]};
cond_from_wire([<<"or">>, Conds], D) when is_list(Conds) ->
    {'or', [cond_from_wire(C, D - 1) || C <- Conds]};
cond_from_wire([<<"not">>, Cond], D) ->
    {'not', cond_from_wire(Cond, D - 1)};
cond_from_wire([<<"in">>, Path, Values], _D) when is_list(Values) ->
    {in, path_from_wire(Path), [value_from_wire(V) || V <- Values]};
cond_from_wire([<<"contains">>, Path, Value], _D) ->
    {contains, path_from_wire(Path), value_from_wire(Value)};
cond_from_wire([<<"exists">>, Path], _D) ->
    {exists, path_from_wire(Path)};
cond_from_wire([<<"missing">>, Path], _D) ->
    {missing, path_from_wire(Path)};
cond_from_wire([<<"regex">>, Path, Pattern], _D) when is_binary(Pattern) ->
    {regex, path_from_wire(Path), Pattern};
cond_from_wire([<<"prefix">>, Path, Prefix], _D) when is_binary(Prefix) ->
    {prefix, path_from_wire(Path), Prefix};
cond_from_wire(Other, _D) ->
    throw({bad_filter, Other}).

path_from_wire(Path) when is_list(Path), Path =/= [] ->
    [path_component(C) || C <- Path];
path_from_wire(Other) ->
    throw({bad_filter, {path, Other}}).

path_component(<<"*">>) -> '*';
path_component(C) when is_binary(C) -> C;
path_component(C) when is_integer(C), C >= 0 -> C;
path_component(Other) -> throw({bad_filter, {path_component, Other}}).

op_from_wire(<<">">>) -> '>';
op_from_wire(<<"<">>) -> '<';
op_from_wire(<<">=">>) -> '>=';
op_from_wire(<<"=<">>) -> '=<';
op_from_wire(<<"==">>) -> '==';
op_from_wire(<<"=/=">>) -> '=/=';
op_from_wire(Other) -> throw({bad_filter, {op, Other}}).

value_from_wire(V) when is_binary(V); is_number(V); is_boolean(V);
                        V =:= null ->
    V;
value_from_wire(Other) ->
    throw({bad_filter, {value, Other}}).
