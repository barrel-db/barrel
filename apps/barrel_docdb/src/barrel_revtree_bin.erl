%%%-------------------------------------------------------------------
%%% @doc Compact one-parent revision tree with binary encoding
%%%
%%% Features:
%%%  - O(1) add
%%%  - leaves/conflicts + "longest branch" winner
%%%  - compact binary encode/decode
%%%  - "partial decode" mode: scan binary to compute {winner, leaves}
%%%    without rebuilding full tree structures.
%%%
%%% Revision model: each revision has exactly one parent (or none for root).
%%%
%%% Binary format:
%%%   Header: [N:varint]
%%%   For each node i=0..N-1:
%%%     [Flags:u8]                bit0=root, bit1=deleted
%%%     [ParentDelta:varint]?     only if not root: ParentDelta = i - parent_id (>=1)
%%%     [Gen:varint]
%%%     [RevLen:varint][RevId:bytes]
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_revtree_bin).

%% Public API (in-memory tree operations)
-export([new/0, add_root/4, add_child/5]).
-export([leaves/1, conflicts/1, winner/1]).
-export([contains/2, history/2, history/3]).
-export([prune/2]).

%% Binary encoding/decoding
-export([encode/1, decode/1]).
-export([decode_winner_leaves/1]).

%% Conversion from/to map format (for barrel_db_server compatibility)
-export([from_map/1, to_map/1]).
-export([conflicts_from_map/1]).

-record(rt, {
    %% NodeId -> ParentId (-1 for root)
    parent = array:new([{default,-1}])    :: array:array(integer()),
    %% NodeId -> generation (Pos)
    gen    = array:new([{default,0}])     :: array:array(non_neg_integer()),
    %% NodeId -> RevId (binary)
    rev    = array:new()                  :: array:array(binary()),
    %% RevId -> NodeId
    rev2id = #{}                          :: map(),
    %% NodeId -> deleted? (optional but often handy)
    deleted = array:new([{default,false}]):: array:array(boolean()),
    %% Next NodeId
    next   = 0                            :: non_neg_integer()
}).

%%====================================================================
%% Public API (in-memory)
%%====================================================================

-spec new() -> #rt{}.
new() ->
    #rt{}.

-spec add_root(#rt{}, RevId :: binary(), Gen :: non_neg_integer(), Deleted :: boolean()) ->
    {ok, #rt{}, non_neg_integer()} | {error, term()}.
add_root(RT, RevId, Gen, Deleted) ->
    add(RT, RevId, none, Gen, Deleted).

-spec add_child(#rt{}, RevId :: binary(), ParentRevId :: binary(),
                Gen :: non_neg_integer(), Deleted :: boolean()) ->
    {ok, #rt{}, non_neg_integer()} | {error, term()}.
add_child(RT, RevId, ParentRevId, Gen, Deleted) ->
    add(RT, RevId, ParentRevId, Gen, Deleted).

add(RT=#rt{rev2id=R2I, next=Id}, RevId, ParentRevIdOrNone, Gen, Deleted) ->
    case maps:is_key(RevId, R2I) of
        true  -> {error, already_exists};
        false ->
            ParentId =
                case ParentRevIdOrNone of
                    none -> -1;
                    ParentRevId ->
                        case maps:get(ParentRevId, R2I, undefined) of
                            undefined -> {error, parent_missing};
                            Pid -> Pid
                        end
                end,
            case ParentId of
                {error, _} = Err -> Err;
                _ ->
                    RT1 = RT#rt{
                        parent  = array:set(Id, ParentId, RT#rt.parent),
                        gen     = array:set(Id, Gen, RT#rt.gen),
                        rev     = array:set(Id, RevId, RT#rt.rev),
                        deleted = array:set(Id, Deleted, RT#rt.deleted),
                        rev2id  = R2I#{ RevId => Id },
                        next    = Id + 1
                    },
                    {ok, RT1, Id}
            end
    end.

%% @doc Compute leaves (RevIds) from in-memory tree (O(N))
-spec leaves(#rt{}) -> [binary()].
leaves(RT=#rt{next=N}) ->
    HasChild = compute_has_child(RT),
    leaves_from_has_child(RT, HasChild, N).

%% @doc Conflicts = leaves excluding winner (winner is longest branch)
-spec conflicts(#rt{}) -> [binary()].
conflicts(RT) ->
    L = leaves(RT),
    W = winner(RT),
    [R || R <- L, R =/= W].

%% @doc Winner = leaf with max generation; tiebreaker: lexicographic RevId
-spec winner(#rt{}) -> binary() | undefined.
winner(RT=#rt{next=N}) ->
    HasChild = compute_has_child(RT),
    pick_winner_from_has_child(RT, HasChild, N).

compute_has_child(#rt{parent=Parent, next=N}) ->
    %% bitset: 1 means "this node has at least one child"
    Bits0 = bitset_new(N),
    lists:foldl(
      fun(I, Bits) ->
          P = array:get(I, Parent),
          if
              P >= 0 -> bitset_set(Bits, P);
              true   -> Bits
          end
      end,
      Bits0,
      lists:seq(0, N-1)
    ).

leaves_from_has_child(RT, HasChild, N) ->
    lists:foldl(
      fun(I, Acc) ->
          case bitset_get(HasChild, I) of
              true  -> Acc;
              false -> [array:get(I, RT#rt.rev) | Acc]
          end
      end,
      [],
      lists:seq(0, N-1)
    ).

pick_winner_from_has_child(RT, HasChild, N) ->
    %% Among leaves only
    {BestId, _BestGen, _BestRev} =
        lists:foldl(
          fun(I, {BestId0, BestGen0, BestRev0}) ->
              case bitset_get(HasChild, I) of
                  true -> {BestId0, BestGen0, BestRev0}; % not a leaf
                  false ->
                      G = array:get(I, RT#rt.gen),
                      R = array:get(I, RT#rt.rev),
                      case best_cmp(G, R, BestGen0, BestRev0) of
                          true  -> {I, G, R};
                          false -> {BestId0, BestGen0, BestRev0}
                      end
              end
          end,
          {undefined, -1, <<>>},
          lists:seq(0, N-1)
        ),
    case BestId of
        undefined -> undefined;
        _         -> array:get(BestId, RT#rt.rev)
    end.

best_cmp(G, R, BestG, BestR) ->
    %% true if (G,R) is better than (BestG,BestR)
    (G > BestG) orelse (G =:= BestG andalso R > BestR).

%% @doc Check if a revision exists in the tree
-spec contains(#rt{}, binary()) -> boolean().
contains(#rt{rev2id = R2I}, RevId) ->
    maps:is_key(RevId, R2I).

%% @doc Get the history (ancestor chain) of a revision, from newest to oldest
%% Returns a list of revision IDs starting with the given RevId.
-spec history(#rt{}, binary()) -> [binary()].
history(RT, RevId) ->
    history(RT, RevId, infinity).

%% @doc Get the history of a revision up to a maximum depth
%% Depth includes the starting revision (depth=1 returns just the RevId itself)
-spec history(#rt{}, binary(), non_neg_integer() | infinity) -> [binary()].
history(#rt{rev2id = R2I} = RT, RevId, MaxDepth) ->
    case maps:get(RevId, R2I, undefined) of
        undefined -> [];
        NodeId -> history_from_node(RT, NodeId, MaxDepth, [])
    end.

history_from_node(_RT, _NodeId, 0, Acc) ->
    lists:reverse(Acc);
history_from_node(RT, NodeId, Depth, Acc) ->
    RevId = array:get(NodeId, RT#rt.rev),
    ParentId = array:get(NodeId, RT#rt.parent),
    NewAcc = [RevId | Acc],
    case ParentId of
        -1 -> lists:reverse(NewAcc);  % root reached
        _ ->
            NewDepth = case Depth of
                infinity -> infinity;
                N -> N - 1
            end,
            history_from_node(RT, ParentId, NewDepth, NewAcc)
    end.

%% @doc Prune the revision tree, keeping only ancestors of leaves up to Depth
%% Returns {PrunedTree, PrunedRevisions} where PrunedRevisions is the list of
%% revisions that were removed.
-spec prune(#rt{}, non_neg_integer()) -> {#rt{}, [binary()]}.
prune(RT, Depth) when Depth >= 0 ->
    prune_tree(RT, Depth).

prune_tree(#rt{next = 0} = RT, _Depth) ->
    {RT, []};
prune_tree(RT, Depth) ->
    %% Find all leaves
    LeafRevIds = leaves(RT),

    %% Collect all revisions to keep: ancestors of each leaf up to Depth
    ToKeep = lists:foldl(
        fun(LeafRevId, Acc) ->
            History = history(RT, LeafRevId, Depth),
            lists:foldl(fun(R, S) -> sets:add_element(R, S) end, Acc, History)
        end,
        sets:new(),
        LeafRevIds
    ),

    %% Find pruned revisions
    AllRevs = [array:get(I, RT#rt.rev) || I <- lists:seq(0, RT#rt.next - 1)],
    Pruned = [R || R <- AllRevs, not sets:is_element(R, ToKeep)],

    case Pruned of
        [] ->
            %% Nothing to prune
            {RT, []};
        _ ->
            %% Rebuild tree with only kept revisions
            %% Convert to map, filter, convert back
            Map = to_map(RT),
            KeptMap = maps:filter(
                fun(RevId, _) -> sets:is_element(RevId, ToKeep) end,
                Map
            ),
            %% Fix parent references for nodes whose parents were pruned
            FixedMap = maps:map(
                fun(_RevId, #{parent := Parent} = Info) ->
                    case Parent of
                        undefined -> Info;
                        _ ->
                            case sets:is_element(Parent, ToKeep) of
                                true -> Info;
                                false ->
                                    %% Parent was pruned, make this a new root
                                    Info#{parent => undefined}
                            end
                    end
                end,
                KeptMap
            ),
            NewRT = from_map(FixedMap),
            {NewRT, Pruned}
    end.

%%====================================================================
%% Binary encoding/decoding
%%====================================================================

%% @doc Encode revision tree to compact binary format
-spec encode(#rt{}) -> binary().
encode(#rt{parent=Parent, gen=GenA, rev=RevA, deleted=DelA, next=N}) ->
    Nodes =
        [ encode_node(I, Parent, GenA, RevA, DelA)
          || I <- lists:seq(0, N-1) ],
    iolist_to_binary([venc(N), Nodes]).

encode_node(I, Parent, GenA, RevA, DelA) ->
    P = array:get(I, Parent),
    G = array:get(I, GenA),
    R = array:get(I, RevA),
    D = array:get(I, DelA),

    RootFlag = if P =:= -1 -> 1; true -> 0 end,
    DelFlag  = if D =:= true -> 2; true -> 0 end,
    Flags = RootFlag bor DelFlag,

    ParentBin =
        case P of
            -1 -> <<>>;
            _  -> venc(I - P)
        end,

    <<
      Flags:8,
      ParentBin/binary,
      (venc(G))/binary,
      (venc(byte_size(R)))/binary,
      R/binary
    >>.

%% @doc Decode binary to revision tree
-spec decode(binary()) -> #rt{}.
decode(Bin) ->
    {N, Rest} = vdec(Bin),
    decode_nodes(N, Rest, 0, #rt{}).

decode_nodes(0, _Bin, _I, RT) ->
    RT;
decode_nodes(N, Bin, I, RT) ->
    <<Flags:8, Rest0/binary>> = Bin,
    Root = (Flags band 1) =/= 0,
    Deleted = (Flags band 2) =/= 0,

    {ParentId, Rest1} =
        case Root of
            true  -> {-1, Rest0};
            false ->
                {Delta, R1} = vdec(Rest0),
                {I - Delta, R1}
        end,

    {Gen, Rest2} = vdec(Rest1),
    {Len, Rest3} = vdec(Rest2),
    <<RevId:Len/binary, Rest4/binary>> = Rest3,

    Id = RT#rt.next,
    RT1 = RT#rt{
        parent  = array:set(Id, ParentId, RT#rt.parent),
        gen     = array:set(Id, Gen, RT#rt.gen),
        rev     = array:set(Id, RevId, RT#rt.rev),
        deleted = array:set(Id, Deleted, RT#rt.deleted),
        rev2id  = (RT#rt.rev2id)#{ RevId => Id },
        next    = Id + 1
    },
    decode_nodes(N-1, Rest4, I+1, RT1).

%%====================================================================
%% Partial decode (fast path)
%%
%% Goal: compute {Winner, Leaves, Conflicts} without building rev2id,
%% without building parent array, etc.
%%====================================================================

-spec decode_winner_leaves(binary()) ->
    #{winner := binary() | undefined,
      leaves := [binary()],
      conflicts := [binary()]}.
decode_winner_leaves(Bin) ->
    {N, Rest} = vdec(Bin),
    HasChild0 = bitset_new(N),
    GenA0 = array:new([{default,0}]),
    RevA0 = array:new(),
    DelA0 = array:new([{default,false}]),

    {HasChild, GenA, RevA, DelA, _RestEnd} =
        scan_nodes_for_winner_leaves(N, Rest, 0, HasChild0, GenA0, RevA0, DelA0),

    Leaves = lists:foldl(
      fun(I, Acc) ->
          case bitset_get(HasChild, I) of
              true  -> Acc;
              false -> [array:get(I, RevA) | Acc]
          end
      end, [], lists:seq(0, N-1)),

    Winner = pick_winner_from_arrays(HasChild, GenA, RevA, N),

    %% Conflicts are non-deleted leaves excluding the winner
    Conflicts = lists:foldl(
      fun(I, Acc) ->
          case bitset_get(HasChild, I) of
              true -> Acc;
              false ->
                  R = array:get(I, RevA),
                  Del = array:get(I, DelA),
                  case {R =:= Winner, Del} of
                      {true, _} -> Acc;      % winner, skip
                      {_, true} -> Acc;      % deleted, skip
                      _ -> [R | Acc]
                  end
          end
      end, [], lists:seq(0, N-1)),

    #{winner => Winner, leaves => Leaves, conflicts => Conflicts}.

scan_nodes_for_winner_leaves(0, Bin, _I, HasChild, GenA, RevA, DelA) ->
    {HasChild, GenA, RevA, DelA, Bin};
scan_nodes_for_winner_leaves(N, Bin, I, HasChild0, GenA0, RevA0, DelA0) ->
    <<Flags:8, Rest0/binary>> = Bin,
    Root = (Flags band 1) =/= 0,
    Deleted = (Flags band 2) =/= 0,

    {HasChild1, Rest1} =
        case Root of
            true -> {HasChild0, Rest0};
            false ->
                {Delta, R1} = vdec(Rest0),
                ParentId = I - Delta,
                {bitset_set(HasChild0, ParentId), R1}
        end,

    {Gen, Rest2} = vdec(Rest1),
    {Len, Rest3} = vdec(Rest2),
    <<RevId:Len/binary, Rest4/binary>> = Rest3,

    GenA1 = array:set(I, Gen, GenA0),
    RevA1 = array:set(I, RevId, RevA0),
    DelA1 = array:set(I, Deleted, DelA0),

    scan_nodes_for_winner_leaves(N-1, Rest4, I+1, HasChild1, GenA1, RevA1, DelA1).

pick_winner_from_arrays(HasChild, GenA, RevA, N) ->
    {BestId, _BestGen, _BestRev} =
        lists:foldl(
          fun(I, {BestId0, BestGen0, BestRev0}) ->
              case bitset_get(HasChild, I) of
                  true -> {BestId0, BestGen0, BestRev0}; % not leaf
                  false ->
                      G = array:get(I, GenA),
                      R = array:get(I, RevA),
                      case best_cmp(G, R, BestGen0, BestRev0) of
                          true  -> {I, G, R};
                          false -> {BestId0, BestGen0, BestRev0}
                      end
              end
          end,
          {undefined, -1, <<>>},
          lists:seq(0, N-1)
        ),
    case BestId of
        undefined -> undefined;
        _         -> array:get(BestId, RevA)
    end.

%%====================================================================
%% Conversion from/to map format (barrel_db_server compatibility)
%%
%% barrel_db_server uses: #{RevId => #{id => RevId, parent => Parent, deleted => Del}}
%% where Parent is undefined for root.
%%====================================================================

%% @doc Convert from map format to #rt{} record
-spec from_map(map()) -> #rt{}.
from_map(Map) when is_map(Map), map_size(Map) =:= 0 ->
    new();
from_map(Map) when is_map(Map) ->
    %% Build dependency order: roots first, then children
    Entries = maps:to_list(Map),
    %% Find roots (parent = undefined)
    {Roots, NonRoots} = lists:partition(
        fun({_RevId, #{parent := P}}) -> P =:= undefined end,
        Entries
    ),
    %% Add roots first
    RT0 = new(),
    RT1 = lists:foldl(
        fun({RevId, #{id := Id, deleted := Del}}, Acc) ->
            Gen = parse_gen(Id),
            {ok, NewAcc, _} = add_root(Acc, RevId, Gen, Del),
            NewAcc
        end,
        RT0,
        Roots
    ),
    %% Add children in order (may need multiple passes for deep trees)
    add_children_iteratively(RT1, NonRoots, Map).

add_children_iteratively(RT, [], _Map) ->
    RT;
add_children_iteratively(RT, Remaining, Map) ->
    {Added, StillRemaining} = lists:partition(
        fun({_RevId, #{parent := Parent}}) ->
            maps:is_key(Parent, RT#rt.rev2id)
        end,
        Remaining
    ),
    case Added of
        [] when StillRemaining =/= [] ->
            %% Orphan nodes - shouldn't happen with valid tree
            RT;
        _ ->
            RT1 = lists:foldl(
                fun({RevId, #{id := Id, parent := Parent, deleted := Del}}, Acc) ->
                    Gen = parse_gen(Id),
                    {ok, NewAcc, _} = add_child(Acc, RevId, Parent, Gen, Del),
                    NewAcc
                end,
                RT,
                Added
            ),
            add_children_iteratively(RT1, StillRemaining, Map)
    end.

%% @doc Convert from #rt{} record to map format
-spec to_map(#rt{}) -> map().
to_map(#rt{next=0}) ->
    #{};
to_map(#rt{parent=Parent, rev=Rev, deleted=Del, next=N}) ->
    lists:foldl(
        fun(I, Acc) ->
            RevId = array:get(I, Rev),
            ParentId = array:get(I, Parent),
            Deleted = array:get(I, Del),
            ParentRev = case ParentId of
                -1 -> undefined;
                Pid -> array:get(Pid, Rev)
            end,
            Acc#{RevId => #{id => RevId, parent => ParentRev, deleted => Deleted}}
        end,
        #{},
        lists:seq(0, N-1)
    ).

%% @doc Get conflicts from a map-format revision tree
%% Convenience function for barrel_changes.erl compatibility
-spec conflicts_from_map(map()) -> [binary()].
conflicts_from_map(Map) when is_map(Map), map_size(Map) =:= 0 ->
    [];
conflicts_from_map(Map) when is_map(Map) ->
    RT = from_map(Map),
    conflicts(RT).

%% Parse generation from revision id (e.g., <<"3-abc">> -> 3)
parse_gen(RevId) when is_binary(RevId) ->
    case binary:split(RevId, <<"-">>) of
        [GenBin, _Hash] ->
            binary_to_integer(GenBin);
        _ ->
            1
    end.

%%====================================================================
%% Bitset helpers (small + fast)
%%====================================================================

bitset_new(N) ->
    Bytes = (N + 7) div 8,
    binary:copy(<<0>>, Bytes).

bitset_set(Bits, I) when I >= 0 ->
    ByteIx = I bsr 3,
    Bit = 1 bsl (I band 7),
    <<Prefix:ByteIx/binary, Byte:8, Suffix/binary>> = Bits,
    NewByte = Byte bor Bit,
    <<Prefix/binary, NewByte:8, Suffix/binary>>.

bitset_get(Bits, I) when I >= 0 ->
    ByteIx = I bsr 3,
    Bit = 1 bsl (I band 7),
    <<_:ByteIx/binary, Byte:8, _/binary>> = Bits,
    (Byte band Bit) =/= 0.

%%====================================================================
%% Varint (unsigned LEB128)
%%====================================================================

venc(N) when is_integer(N), N >= 0 ->
    iolist_to_binary(venc_iolist(N)).

venc_iolist(N) when N < 128 ->
    [N];
venc_iolist(N) ->
    [((N band 127) bor 128) | venc_iolist(N bsr 7)].

vdec(Bin) ->
    vdec(Bin, 0, 0).

vdec(<<Byte:8, Rest/binary>>, Shift, Acc) ->
    Value = Acc bor ((Byte band 127) bsl Shift),
    case (Byte band 128) of
        0 -> {Value, Rest};
        _ -> vdec(Rest, Shift + 7, Value)
    end.
