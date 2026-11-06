%%%-------------------------------------------------------------------
%%% @doc The replicated state of one document: a multi-value register in
%%% the causal CRDT framework (Almeida, Shoker, Baquero, "Delta state
%%% replicated data types", 2018), with a per-document causal context.
%%%
%%% `live' holds the current versions; `obs' (a per-writer high-water
%%% mark, raised only by that writer's own writes) and `cloud' (exact
%%% tokens) record every version this state has seen. A version seen and
%%% no longer live was replaced and never comes back. Pure functions:
%%% no storage, no clock.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_live).

-export([new/0, merge/2, seen/2]).
-export([author/4, replaces/3, legacy_arrival/4, convert/2]).
-export([view/1, evict/3]).
-export([auto_inputs/1, auto_step/3, auto_token/2, base/2]).
-export([live/1, tokens/1, digest/1]).

-export_type([state/0, token/0, info/0]).

-type token() :: barrel_version:version().
-type info() :: #{deleted := boolean(),
                  vv := #{binary() => barrel_hlc:timestamp()},
                  base => [token()],          %% automatic resolutions only
                  result => binary()}.        %% hash of their result
-type state() :: #{live := #{token() => info()},
                   obs := #{binary() => barrel_hlc:timestamp()},
                   cloud := #{token() => true}}.

-define(AUTO, "auto:").
-define(HASH_VSN, 1).

%%====================================================================
%% Join
%%====================================================================

-spec new() -> state().
new() ->
    #{live => #{}, obs => #{}, cloud => #{}}.

%% @doc Whether this state has seen `Token'.
-spec seen(state(), token()) -> boolean().
seen(#{obs := Obs, cloud := Cloud}, {Hlc, Writer} = Token) ->
    maps:is_key(Token, Cloud) orelse
        case Obs of
            #{Writer := High} -> not barrel_hlc:less(High, Hlc);
            _ -> false
        end.

%% @doc The join: keep a version both sides hold, or one the other side
%% never saw. Commutative, associative and idempotent.
-spec merge(state(), state()) -> state().
merge(#{live := LA} = A, #{live := LB} = B) ->
    Both = maps:intersect_with(fun(_T, IA, IB) -> merge_info(IA, IB) end,
                               LA, LB),
    OnlyA = maps:filter(fun(T, _) -> not maps:is_key(T, LB)
                                         andalso not seen(B, T) end, LA),
    OnlyB = maps:filter(fun(T, _) -> not maps:is_key(T, LA)
                                         andalso not seen(A, T) end, LB),
    #{live => maps:merge(maps:merge(OnlyA, OnlyB), Both),
      obs => maps:merge_with(fun(_W, H1, H2) -> hlc_max(H1, H2) end,
                             maps:get(obs, A), maps:get(obs, B)),
      cloud => maps:merge(maps:get(cloud, A), maps:get(cloud, B))}.

%% The same token on both sides: vectors join pointwise; a base that an
%% old peer dropped is the bottom.
merge_info(IA, IB) ->
    I = IA#{vv => vv_join(maps:get(vv, IA), maps:get(vv, IB))},
    case {maps:find(base, IA), maps:find(base, IB)} of
        {{ok, BA}, {ok, BB}} -> I#{base => lists:usort(BA ++ BB)};
        {error, {ok, BB}} -> I#{base => BB, result => maps:get(result, IB)};
        _ -> I
    end.

%%====================================================================
%% Writes
%%====================================================================

%% @doc A write by `Writer' at `Hlc' replacing `Replaces'. The token must
%% sort above every live version and above the writer's high-water mark.
-spec author(state(), token(), boolean(), [token()]) -> state().
author(#{live := Live, obs := Obs, cloud := Cloud} = State, {Hlc, Writer} = Token,
       Deleted, Replaces) ->
    true = lists:all(fun(T) -> barrel_version:compare(T, Token) =:= lt end,
                     maps:keys(Live)),
    true = case Obs of
               #{Writer := High} -> barrel_hlc:less(High, Hlc);
               _ -> true
           end,
    Replaced = maps:with(Replaces, Live),
    VV = vv_bump(lists:foldl(fun vv_join/2, #{},
                             [V || #{vv := V} <- maps:values(Replaced)]),
                 Token),
    State#{live => (maps:without(Replaces, Live))#{Token => #{deleted => Deleted,
                                                             vv => VV}},
           obs => Obs#{Writer => Hlc},
           cloud => Cloud}.

%% @doc What a write replaces: an explicit observed set takes
%% precedence; over a delete, every delete (checking `Rev' if given);
%% otherwise `Rev' must be the winner.
-spec replaces(state(), token() | undefined, [token()] | undefined) ->
    {ok, [token()]} | {error, conflict}.
replaces(#{live := Live}, _Rev, Observed) when is_list(Observed) ->
    {ok, [T || T <- Observed, maps:is_key(T, Live)]};
replaces(#{live := Live}, _Rev, undefined) when map_size(Live) =:= 0 ->
    {ok, []};
replaces(#{live := Live} = State, Rev, undefined) ->
    #{winner := Winner, deleted := WinnerDeleted} = view(State),
    case {WinnerDeleted, Rev} of
        {true, undefined} -> {ok, deletes(Live)};
        {true, Winner} -> {ok, deletes(Live)};
        {false, Winner} -> {ok, [Winner]};
        _ -> {error, conflict}
    end.

deletes(Live) ->
    lists:sort([T || {T, #{deleted := true}} <- maps:to_list(Live)]).

%% @doc A version from an old peer (`put_version/5'): ignored if already
%% seen as that exact token, else kept as a conflict. Never removes
%% anything and never raises a high-water mark.
-spec legacy_arrival(state(), token(), boolean(), map()) -> state().
legacy_arrival(#{live := Live, cloud := Cloud} = State, Token, Deleted, VV) ->
    case seen(State, Token) of
        true ->
            State;
        false ->
            State#{live => Live#{Token => #{deleted => Deleted,
                                            vv => vv_bump(VV, Token)}},
                   cloud => Cloud#{Token => true}}
    end.

%% @doc First conversion of a document stored by an older version: the
%% current versions become `live'; only the exact versions this replica
%% stores (and their stored ancestors) count as seen. An old vector is
%% never imported: it can claim versions this replica never saw.
-spec convert([{token(), info()}], [token()]) -> state().
convert(Current, StoredSeen) ->
    Live = maps:from_list(Current),
    #{live => Live, obs => #{},
      cloud => maps:from_keys(maps:keys(Live) ++ StoredSeen, true)}.

%%====================================================================
%% Reads
%%====================================================================

%% @doc Winner = highest token; the other live versions are conflicts.
-spec view(state()) -> #{winner := token() | undefined,
                         conflicts := [token()],
                         observed := [token()],
                         deleted := boolean()}.
view(#{live := Live}) when map_size(Live) =:= 0 ->
    #{winner => undefined, conflicts => [], observed => [], deleted => true};
view(#{live := Live}) ->
    Sorted = lists:sort(fun(A, B) -> barrel_version:compare(A, B) =/= gt end,
                        maps:keys(Live)),
    Winner = lists:last(Sorted),
    #{deleted := Deleted} = maps:get(Winner, Live),
    #{winner => Winner, conflicts => lists:droplast(Sorted),
      observed => Sorted, deleted => Deleted}.

-spec live(state()) -> #{token() => info()}.
live(#{live := Live}) -> Live.

-spec tokens(state()) -> [token()].
tokens(State) -> maps:get(observed, view(State)).

%% @doc Bound the live versions to `Max': old-peer versions go first,
%% lowest token first, then the lowest of the rest; never the winner.
%% Evicted versions stay seen.
-spec evict(state(), pos_integer(), fun((token()) -> boolean())) -> state().
evict(#{live := Live, cloud := Cloud} = State, Max, IsLegacy) ->
    case map_size(Live) > Max of
        false ->
            State;
        true ->
            #{conflicts := Conflicts} = view(State),
            {Legacy, Other} = lists:partition(IsLegacy, Conflicts),
            Drop = lists:sublist(Legacy ++ Other, map_size(Live) - Max),
            State#{live => maps:without(Drop, Live),
                   cloud => maps:merge(Cloud, maps:from_keys(Drop, true))}
    end.

%%====================================================================
%% Automatic resolution
%%====================================================================

%% @doc The original writes a live version covers: itself, or for an
%% automatic resolution the union of its inputs' bases.
-spec base(state(), token()) -> [token()].
base(#{live := Live}, Token) ->
    case maps:get(Token, Live) of
        #{base := Base} -> Base;
        _ -> [Token]
    end.

%% @doc The live versions a resolver sees, sorted by token, or none
%% when there is no conflict.
-spec auto_inputs(state()) -> [token()].
auto_inputs(#{live := Live} = State) when map_size(Live) > 1 ->
    tokens(State);
auto_inputs(_State) ->
    [].

%% @doc Apply a resolver result (its deleted flag and the hash of its
%% body) over every live version. Two replicas resolving the same
%% original writes to the same result write the same version. Stops,
%% writing nothing, when a live automatic version covers the same
%% writes with another result: replicas run different functions.
-spec auto_step(state(), boolean(), binary()) ->
    {ok, state(), token()} | {noop, state()} | {stop, disagreement}.
auto_step(#{live := Live, cloud := Cloud} = State, Deleted, ResultHash) ->
    Replaces = maps:keys(Live),
    Base = lists:usort(lists:append([base(State, T) || T <- Replaces])),
    Clash = [T || {T, #{base := B, result := R}} <- maps:to_list(Live),
                  B =:= Base, R =/= ResultHash],
    Token = auto_token(Base, ResultHash),
    case {Clash, maps:is_key(Token, Live)} of
        {[_ | _], _} ->
            {stop, disagreement};
        {[], true} when map_size(Live) =:= 1 ->
            {noop, State};
        {[], _} ->
            VV = vv_bump(lists:foldl(fun vv_join/2, #{},
                                     [V || #{vv := V} <- maps:values(Live)]),
                         Token),
            {ok, State#{live => #{Token => #{deleted => Deleted, vv => VV,
                                             base => Base,
                                             result => ResultHash}},
                        cloud => maps:merge(Cloud, maps:from_keys(
                                                     [Token | Replaces], true))},
             Token}
    end.

%% @doc Token of an automatic resolution: after the newest write it
%% covers, one logical step per covered write, so it sorts above
%% everything it replaces; named by a canonical, versioned hash.
-spec auto_token([token()], binary()) -> token().
auto_token(Base, ResultHash) ->
    Hlc = lists:foldl(fun({H, _}, Acc) -> hlc_max(H, Acc) end,
                      barrel_hlc:min(), Base),
    Enc = << <<(byte_size(E)):16, E/binary>>
             || E <- [barrel_version:encode(T) || T <- lists:usort(Base)] >>,
    Hash = crypto:hash(sha256, <<?HASH_VSN, Enc/binary, ResultHash/binary>>),
    {barrel_hlc:add_logical(Hlc, length(Base)),
     <<?AUTO, (binary:encode_hex(Hash, lowercase))/binary>>}.

%%====================================================================
%% Digest
%%====================================================================

%% @doc sha256 of the replicated state: equal digests, equal states.
-spec digest(state()) -> binary().
digest(#{live := Live, obs := Obs, cloud := Cloud}) ->
    crypto:hash(sha256, term_to_binary(
                          {lists:sort([{T, maps:get(deleted, I),
                                        lists:sort(maps:to_list(maps:get(vv, I))),
                                        maps:get(base, I, none)}
                                       || {T, I} <- maps:to_list(Live)]),
                           lists:sort(maps:to_list(Obs)),
                           lists:sort(maps:keys(Cloud))},
                          [{minor_version, 2}, deterministic])).

%%====================================================================
%% Internal
%%====================================================================

vv_join(A, B) ->
    maps:merge_with(fun(_W, H1, H2) -> hlc_max(H1, H2) end, A, B).

vv_bump(VV, {Hlc, Writer}) ->
    vv_join(VV, #{Writer => Hlc}).

hlc_max(A, B) ->
    case barrel_hlc:less(A, B) of
        true -> B;
        false -> A
    end.
