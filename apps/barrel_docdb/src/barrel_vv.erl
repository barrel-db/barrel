%%%-------------------------------------------------------------------
%%% @doc Version vectors: per-writer-node high-water marks.
%%%
%%% A document's version vector maps each node id to the highest HLC
%%% this replica has seen from that node for the document. Comparing
%%% two vectors decides how replicated writes relate:
%%%
%%% <ul>
%%% <li>`dominates': the remote saw everything we have and more, so its
%%%     version fast-forwards ours.</li>
%%% <li>`dominated' / `eq': we already cover the remote version; it is
%%%     stored as superseded history, not applied.</li>
%%% <li>`concurrent': a real conflict; last-write-wins by
%%%     {@link barrel_version:compare/2} picks the winner and the loser
%%%     is retained as a conflict sibling.</li>
%%% </ul>
%%%
%%% `contains/2' answers the replication diff question: does this
%%% replica already cover a given version?
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vv).

-export([new/0]).
-export([bump/2, merge/2]).
-export([compare/2, contains/2]).
-export([encode/1, decode/1]).

-type vv() :: #{binary() => barrel_hlc:timestamp()}.
%% NodeId -> highest HLC seen from that node.

-export_type([vv/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc The empty vector.
-spec new() -> vv().
new() ->
    #{}.

%% @doc Record a version in the vector (monotonic: never goes backward).
-spec bump(vv(), barrel_version:version()) -> vv().
bump(VV, {Hlc, Node}) ->
    case maps:get(Node, VV, undefined) of
        undefined ->
            VV#{Node => Hlc};
        Existing ->
            case barrel_hlc:compare(Hlc, Existing) of
                gt -> VV#{Node => Hlc};
                _ -> VV
            end
    end.

%% @doc Pointwise maximum of two vectors.
-spec merge(vv(), vv()) -> vv().
merge(A, B) ->
    maps:fold(
        fun(Node, Hlc, Acc) -> bump(Acc, {Hlc, Node}) end,
        A,
        B).

%% @doc Relate two vectors.
-spec compare(vv(), vv()) -> eq | dominates | dominated | concurrent.
compare(A, B) ->
    Nodes = maps:keys(maps:merge(A, B)),
    relate(Nodes, A, B, eq).

%% @doc Does the vector cover a version (same or newer HLC from that
%% node)? This is the replication "have" test.
-spec contains(vv(), barrel_version:version()) -> boolean().
contains(VV, {Hlc, Node}) ->
    case maps:get(Node, VV, undefined) of
        undefined -> false;
        Seen -> barrel_hlc:compare(Seen, Hlc) =/= lt
    end.

%%====================================================================
%% Codec
%%====================================================================

%% @doc Deterministic binary encoding (entries sorted by node id):
%% `<<Count:16, [NodeLen:8, Node/binary, Hlc:12/binary] ...>>'.
-spec encode(vv()) -> binary().
encode(VV) ->
    Entries = lists:keysort(1, maps:to_list(VV)),
    Encoded = [<<(byte_size(Node)):8, Node/binary,
                 (barrel_hlc:encode(Hlc))/binary>>
               || {Node, Hlc} <- Entries],
    iolist_to_binary([<<(length(Entries)):16>> | Encoded]).

%% @doc Decode an encoded vector.
-spec decode(binary()) -> vv().
decode(<<Count:16, Rest/binary>>) ->
    decode_entries(Count, Rest, #{}).

%%====================================================================
%% Internal
%%====================================================================

relate([], _A, _B, Acc) ->
    Acc;
relate([Node | Nodes], A, B, Acc) ->
    Cmp = entry_compare(maps:get(Node, A, undefined),
                        maps:get(Node, B, undefined)),
    case {Acc, Cmp} of
        {_, eq} -> relate(Nodes, A, B, Acc);
        {eq, gt} -> relate(Nodes, A, B, dominates);
        {eq, lt} -> relate(Nodes, A, B, dominated);
        {dominates, gt} -> relate(Nodes, A, B, dominates);
        {dominated, lt} -> relate(Nodes, A, B, dominated);
        _Mixed -> concurrent
    end.

%% A missing entry is lower than any present entry.
entry_compare(undefined, undefined) -> eq;
entry_compare(undefined, _) -> lt;
entry_compare(_, undefined) -> gt;
entry_compare(H1, H2) -> barrel_hlc:compare(H1, H2).

decode_entries(0, <<>>, Acc) ->
    Acc;
decode_entries(N, <<NodeLen:8, Node:NodeLen/binary, HlcBin:12/binary,
                    Rest/binary>>, Acc) when N > 0 ->
    decode_entries(N - 1, Rest, Acc#{Node => barrel_hlc:decode(HlcBin)}).
