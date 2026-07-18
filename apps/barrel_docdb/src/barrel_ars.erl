%%%-------------------------------------------------------------------
%%% @doc Document path extraction for automatic indexing
%%%
%%% Extracts all paths from a document for automatic path indexing.
%%% Based on barrel_ars_view.erl from barrel apps branch.
%%%
%%% Path format: [field1, field2, ..., value]
%%% Arrays are indexed by MEMBERSHIP: each element contributes
%%% [field, ..., element] with no positional index, so a path query on the
%%% field matches any member (#5).
%%%
%%% Example:
%%% ```
%%% Doc = #{<<"type">> => <<"user">>,
%%%         <<"profile">> => #{<<"name">> => <<"Alice">>}},
%%% Paths = barrel_ars:analyze(Doc),
%%% %% [{[<<"type">>, <<"user">>], <<>>},
%%% %%  {[<<"profile">>, <<"name">>, <<"Alice">>], <<>>}]
%%% '''
%%%
%%% Performance optimizations:
%%% - Paths built in reverse then flipped (O(n) vs O(n^2) for ++)
%%% - Uses maps for O(1) diff lookups instead of sets
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ars).

-export([
    analyze/1,
    diff/2,
    short/1,
    paths_to_topics/1,
    path_to_topic/1
]).

-define(MAX_VALUE_LENGTH, 100).

%%====================================================================
%% API
%%====================================================================

%% @doc Extract all paths from a document.
%% Accepts both Erlang maps and indexed CBOR binaries.
%% Returns a list of `{Path, Value}' tuples where Path is a list of
%% field names/indices ending with the value.
-spec analyze(map() | binary()) -> [{Path :: [term()], binary()}].
analyze(Doc) when is_map(Doc) ->
    %% Build paths in reverse for O(n) complexity, then reverse each path.
    %% Reserved top-level fields (`_' prefix, e.g. the persisted
    %% `_embedding' vector) are never path-indexed.
    analyze_doc(maps:filter(fun(K, _V) -> not reserved_field(K) end, Doc),
                [], []);
analyze(Doc) when is_binary(Doc) ->
    %% Indexed CBOR - use barrel_doc API to access values via index
    case barrel_doc:is_indexed(Doc) of
        true ->
            Keys = barrel_doc:keys(Doc),
            lists:foldl(
                fun(K, Acc) ->
                    case reserved_field(K) of
                        true ->
                            Acc;
                        false ->
                            V = barrel_doc:get(Doc, [K]),
                            analyze_value(V, [K], Acc)
                    end
                end,
                [],
                Keys
            );
        false ->
            %% Plain CBOR - decode first (will be indexed during storage)
            analyze(barrel_doc:to_map(Doc))
    end;
analyze(_) ->
    [].

%% @private Reserved top-level fields are metadata, never indexed.
reserved_field(<<"_", _/binary>>) -> true;
reserved_field(_) -> false.

%% @doc Compute the difference between old and new paths.
%% Returns {Added, Removed} where:
%%   Added = paths in New but not in Old
%%   Removed = paths in Old but not in New
%%
%% Uses maps for O(1) membership tests instead of sets.
-spec diff(Old :: [{term(), term()}], New :: [{term(), term()}]) ->
    {Added :: [{term(), term()}], Removed :: [{term(), term()}]}.
diff(Old, New) ->
    %% Convert to maps for O(1) lookup
    OldMap = maps:from_list([{Path, true} || {Path, _} <- Old]),
    NewMap = maps:from_list([{Path, true} || {Path, _} <- New]),

    %% Find added: in New but not in Old
    Added = [{Path, <<>>} || {Path, _} <- New, not maps:is_key(Path, OldMap)],

    %% Find removed: in Old but not in New
    Removed = [{Path, <<>>} || {Path, _} <- Old, not maps:is_key(Path, NewMap)],

    {Added, Removed}.

%% @doc Truncate a value for indexing.
%% Binary values longer than 100 bytes are truncated.
-spec short(term()) -> term().
short(<<S:?MAX_VALUE_LENGTH/binary, _/binary>>) -> S;
short(S) when is_binary(S) -> S;
short(S) -> S.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Analyze document, building paths in reverse
-spec analyze_doc(map(), [term()], [{[term()], <<>>}]) -> [{[term()], <<>>}].
analyze_doc(Doc, RevPath, Acc) ->
    maps:fold(
        fun(K, V, Acc1) ->
            analyze_value(V, [K | RevPath], Acc1)
        end,
        Acc,
        Doc
    ).

%% @private Analyze a value (dispatch based on type)
-spec analyze_value(term(), [term()], [{[term()], <<>>}]) -> [{[term()], <<>>}].
analyze_value(V, RevPath, Acc) when is_map(V) ->
    analyze_doc(V, RevPath, Acc);
analyze_value(V, RevPath, Acc) when is_list(V) ->
    analyze_list(V, RevPath, Acc);
analyze_value(V, RevPath, Acc) ->
    %% Leaf value - reverse path and add truncated value at end
    Path = lists:reverse([short(V) | RevPath]),
    [{Path, <<>>} | Acc].

%% @private Analyze a list/array as MEMBERSHIP: index each element at the
%% field path WITHOUT its positional index (#5), so a path query on the
%% field matches any member. Duplicate elements collapse to one path.
-spec analyze_list(list(), [term()], [{[term()], <<>>}]) ->
    [{[term()], <<>>}].
analyze_list([Item | Rest], RevPath, Acc) ->
    Acc1 = analyze_value(Item, RevPath, Acc),
    analyze_list(Rest, RevPath, Acc1);
analyze_list([], _RevPath, Acc) ->
    Acc.

%%====================================================================
%% Path to Topic conversion (for subscriptions)
%%====================================================================

%% @doc Convert analyzed paths to MQTT-style topic strings.
%% Each path `[field1, field2, value]' becomes `"field1/field2/value"'.
%%
%% This is used for subscription matching with barrel_sub.
-spec paths_to_topics([{[term()], binary()}]) -> [binary()].
paths_to_topics(Paths) ->
    [path_to_topic(Path) || {Path, _} <- Paths].

%% @doc Convert a single path to an MQTT-style topic string.
%% Handles various value types: binaries, integers, atoms, etc.
-spec path_to_topic([term()]) -> binary().
path_to_topic(Path) when is_list(Path) ->
    Parts = [to_binary(Part) || Part <- Path],
    join_with_slash(Parts).

%% @private Join binary parts with slash separator
join_with_slash([]) ->
    <<>>;
join_with_slash([H | T]) ->
    lists:foldl(
        fun(Part, Acc) -> <<Acc/binary, $/, Part/binary>> end,
        H,
        T
    ).

%% @private Convert any term to binary for topic path
to_binary(B) when is_binary(B) -> B;
to_binary(I) when is_integer(I) -> integer_to_binary(I);
to_binary(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_binary(F) when is_float(F) -> float_to_binary(F, [{decimals, 10}, compact]);
to_binary(T) -> iolist_to_binary(io_lib:format("~p", [T])).
