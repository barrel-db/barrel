%%% @doc Model gate for barrel_live: the counterexamples found in review
%%% (each with the broken rule as a negative control that must fail), the
%%% join laws over reachable states, and a bounded exhaustive search of
%%% replicas editing, deleting, crashing, resolving and syncing.
-module(barrel_live_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, explore/1]).
-export([merge_keeps_unseen/1,
         edit_rules/1,
         relayed_version_keeps_sibling/1,
         conversion_keeps_unseen_sibling/1,
         pre_upgrade_delete_stays_deleted/1,
         overlapping_auto_one_version/1,
         different_functions_never_empty/1,
         auto_sorts_above_replaced/1,
         eviction_order/1,
         join_laws_on_reachable_states/1,
         search_line/1,
         search_triangle/1,
         search_resolver/1,
         search_negative_control/1]).

all() -> [{group, directed}, {group, search}].

groups() ->
    [{directed, [], [merge_keeps_unseen, edit_rules,
                     relayed_version_keeps_sibling,
                     conversion_keeps_unseen_sibling,
                     pre_upgrade_delete_stays_deleted,
                     overlapping_auto_one_version,
                     different_functions_never_empty,
                     auto_sorts_above_replaced, eviction_order]},
     {search, [], [join_laws_on_reachable_states, search_line,
                   search_triangle, search_resolver,
                   search_negative_control]}].

-define(L, barrel_live).

%%====================================================================
%% Directed scenarios
%%====================================================================

%% Concurrent writes both survive a merge; a write that saw both
%% replaces both everywhere.
merge_keeps_unseen(_Config) ->
    A = write(?L:new(), t(1, a), []),
    B = write(?L:new(), t(2, b), []),
    AB = ?L:merge(A, B),
    ?assertEqual([t(1, a), t(2, b)], ?L:tokens(AB)),
    ?assertEqual(AB, ?L:merge(B, A)),
    R = write(AB, t(3, a), [t(1, a), t(2, b)]),
    ?assertEqual([t(3, a)], ?L:tokens(?L:merge(R, B))),
    ?assertEqual([t(3, a)], ?L:tokens(?L:merge(B, R))).

edit_rules(_Config) ->
    S = ?L:merge(write(?L:new(), t(1, a), []), write(?L:new(), t(2, b), [])),
    %% editing the winner replaces only the winner
    ?assertEqual({ok, [t(2, b)]}, ?L:replaces(S, t(2, b), undefined)),
    %% a rev that is not the winner, or no rev on a live doc: conflict
    ?assertEqual({error, conflict}, ?L:replaces(S, t(1, a), undefined)),
    ?assertEqual({error, conflict}, ?L:replaces(S, undefined, undefined)),
    %% an observed set takes precedence
    ?assertEqual({ok, [t(1, a), t(2, b)]},
                 ?L:replaces(S, t(1, a), [t(1, a), t(2, b), t(9, z)])),
    %% over deletes, a re-create replaces every delete; a given rev is
    %% still checked
    D = ?L:merge(?L:author(?L:new(), t(1, a), true, []),
                 ?L:author(?L:new(), t(2, b), true, [])),
    ?assertEqual({ok, [t(1, a), t(2, b)]}, ?L:replaces(D, undefined, undefined)),
    ?assertEqual({ok, [t(1, a), t(2, b)]}, ?L:replaces(D, t(2, b), undefined)),
    ?assertEqual({error, conflict}, ?L:replaces(D, t(1, a), undefined)),
    ?assertEqual({ok, []}, ?L:replaces(?L:new(), undefined, undefined)).

%% A holds a1 and b1, edits the winner b1 into a2 and keeps a1. An old
%% peer relays a2 to B. Recording a2 as a high-water mark would claim
%% B saw a1 and drop it on the next merge.
relayed_version_keeps_sibling(_Config) ->
    A0 = ?L:merge(write(?L:new(), t(1, a), []), write(?L:new(), t(2, b), [])),
    A = write(A0, t(3, a), [t(2, b)]),
    #{vv := VV} = maps:get(t(3, a), ?L:live(A)),
    B = ?L:legacy_arrival(?L:new(), t(3, a), false, VV),
    ?assert(lists:member(t(1, a), ?L:tokens(?L:merge(A, B)))),
    %% negative control: the arrival raises the high-water mark
    Bad = raise_mark(?L:legacy_arrival(?L:new(), t(3, a), false, VV), t(3, a)),
    ?assertNot(lists:member(t(1, a), ?L:tokens(?L:merge(A, Bad)))).

%% A writes a0; an old peer's L1 wins; A edits L1 into a1, keeping a0.
%% Old B pulls a1, whose vector covers a0, then upgrades. Only the
%% exact stored versions count as seen, so a0 survives the merge.
conversion_keeps_unseen_sibling(_Config) ->
    A0 = ?L:legacy_arrival(write(?L:new(), t(1, a), []), t(2, old), false, #{}),
    A = write(A0, t(3, a), [t(2, old)]),
    #{vv := VV} = maps:get(t(3, a), ?L:live(A)),
    B = ?L:convert([{t(3, a), #{deleted => false, vv => VV}}], []),
    ?assert(lists:member(t(1, a), ?L:tokens(?L:merge(A, B)))),
    %% negative control: the old vector is imported at conversion
    Bad = import_vector(B, VV),
    ?assertNot(lists:member(t(1, a), ?L:tokens(?L:merge(A, Bad)))).

%% A replica stored x, then its delete d, before upgrading. A stale x
%% pushed later by an old peer is recognised as seen.
pre_upgrade_delete_stays_deleted(_Config) ->
    D = #{deleted => true, vv => #{<<"old">> => h(2)}},
    B = ?L:convert([{t(2, old), D}], [t(1, old)]),
    S = ?L:legacy_arrival(B, t(1, old), false, #{}),
    ?assertEqual([t(2, old)], ?L:tokens(S)),
    %% negative control: no conversion seed
    Bad = ?L:convert([{t(2, old), D}], []),
    ?assertEqual([t(1, old), t(2, old)],
                 ?L:tokens(?L:legacy_arrival(Bad, t(1, old), false, #{}))).

%% Three concurrent writes resolved in different groupings by one
%% order-independent function end in one version everywhere.
overlapping_auto_one_version(_Config) ->
    [Wa, Wb, Wc] = [write(?L:new(), t(I, W), []) || {I, W} <- [{1, a}, {2, b}, {3, c}]],
    Fun = fun fmax/1,
    A1 = resolve(?L:merge(Wa, Wb), Fun),
    B1 = resolve(?L:merge(Wb, Wc), Fun),
    A2 = resolve(?L:merge(A1, Wc), Fun),
    B2 = resolve(?L:merge(B1, Wa), Fun),
    ?assertEqual(?L:tokens(A2), ?L:tokens(B2)),
    ?assertMatch([_], ?L:tokens(settle([A2, B2, A1, B1], Fun))),
    %% negative control: a resolution named by the resolving writer's
    %% own clock never converges to one version
    BadA = own_clock_resolve(?L:merge(Wa, Wb), t(10, a)),
    BadB = own_clock_resolve(?L:merge(Wa, Wb), t(11, b)),
    ?assertMatch([_, _], ?L:tokens(?L:merge(BadA, BadB))).

%% max on A, min on B and C: whatever the order, resolution ends and the
%% document never loses all its versions.
different_functions_never_empty(_Config) ->
    {A1, B1, C1} = mixed_trace(fun resolve/2),
    Final = settle_mixed([A1, B1, C1]),
    ?assertNotEqual([], ?L:tokens(Final)),
    %% negative control: without the no-replace stop the same trace
    %% empties the document
    {A2, B2, C2} = mixed_trace(fun resolve_no_stop/2),
    ?assertEqual([], ?L:tokens(?L:merge(?L:merge(A2, B2), C2))).

auto_sorts_above_replaced(_Config) ->
    Wc = write(?L:new(), t(1, c), []),
    Wa = write(?L:new(), t(5, a), []),
    Wb = write(?L:new(), t(3, b), []),
    Rab = resolve(?L:merge(Wa, Wb), fun fmax/1),
    S = resolve(?L:merge(Rab, Wc), fun fmax/1),
    [Rt] = ?L:tokens(Rab),
    [St] = ?L:tokens(S),
    ?assertEqual(gt, barrel_version:compare(St, Rt)),
    %% negative control: one step instead of one per covered write
    {H, _} = St,
    BadHlc = barrel_hlc:add_logical(hlc_of_max([t(1, c), t(5, a), t(3, b)]), 1),
    ?assertNotEqual(gt, barrel_version:compare({BadHlc, <<"x">>}, Rt)),
    ?assert(barrel_hlc:less(BadHlc, H)).

eviction_order(_Config) ->
    S0 = lists:foldl(fun(S, Acc) -> ?L:merge(Acc, S) end, ?L:new(),
                     [write(?L:new(), t(1, a), []),
                      ?L:legacy_arrival(?L:new(), t(2, old), false, #{}),
                      ?L:legacy_arrival(?L:new(), t(3, old), false, #{}),
                      write(?L:new(), t(4, b), [])]),
    S = ?L:evict(S0, 2, fun({_, W}) -> W =:= <<"old">> end),
    ?assertEqual([t(1, a), t(4, b)], ?L:tokens(S)),
    ?assert(?L:seen(S, t(2, old))),
    S2 = ?L:evict(S0, 1, fun({_, W}) -> W =:= <<"old">> end),
    ?assertEqual([t(4, b)], ?L:tokens(S2)).

%%====================================================================
%% Exhaustive searches
%%====================================================================

join_laws_on_reachable_states(_Config) ->
    %% a token names one write within an execution, so the laws are
    %% checked on the states of one world: its replicas and past states
    {Worlds, _} = explore(#{topology => triangle, ops => 3, crashes => 0,
                            resolver => none, limit => 3000}),
    N = lists:sum(
          [begin
               ?assertEqual(digest(?L:merge(X, Y)), digest(?L:merge(Y, X))),
               ?assertEqual(digest(?L:merge(X, X)), digest(X)),
               ?assertEqual(digest(?L:merge(?L:merge(X, Y), Z)),
                            digest(?L:merge(X, ?L:merge(Y, Z)))),
               1
           end || W <- Worlds, X <- W, Y <- W, Z <- W]),
    ct:log("~p triples", [N]).

search_line(_Config) ->
    check_search(#{topology => line, ops => 3, crashes => 1,
                   resolver => none, limit => 60000}).

search_triangle(_Config) ->
    check_search(#{topology => triangle, ops => 3, crashes => 1,
                   resolver => none, limit => 60000}).

search_resolver(_Config) ->
    check_search(#{topology => line, ops => 3, crashes => 0,
                   resolver => max, limit => 60000}).

%% The search has teeth: a merge that ignores the causal context
%% brings replaced versions back.
search_negative_control(_Config) ->
    {_, Violations} = explore(#{topology => line, ops => 3, crashes => 0,
                                resolver => none, limit => 20000,
                                merge => fun naive_merge/2}),
    ?assertNotEqual([], Violations).

check_search(Opts) ->
    {Worlds, Violations} = explore(Opts),
    ct:log("~p: ~p configurations explored", [Opts, length(Worlds)]),
    ?assertEqual([], lists:sublist(Violations, 3)).

%% Breadth-first over replica configurations. Each terminal check
%% settles every pair to a fixpoint and asserts: same state everywhere,
%% a live version if any write happened, no user write lost unless a
%% write or a resolution that saw it replaced it, nothing replaced live
%% again.
explore(Opts) ->
    Nodes = case maps:get(topology, Opts) of
        line -> #{1 => [2], 2 => [1, 3], 3 => [2]};
        triangle -> #{1 => [2, 3], 2 => [1, 3], 3 => [1, 2]}
    end,
    Init = #{reps => #{1 => ?L:new(), 2 => ?L:new(), 3 => ?L:new()},
             writers => #{1 => 0, 2 => 0, 3 => 0},
             clock => 0, ops => 0, crashes => 0, past => [],
             written => [], replaced => []},
    bfs([Init], #{}, Nodes, Opts, [], [], maps:get(limit, Opts)).

bfs([], _Seen, _Nodes, _Opts, States, Violations, _Limit) ->
    {States, Violations};
bfs(_Queue, _Seen, _Nodes, _Opts, States, Violations, 0) ->
    {States, Violations};
bfs([W | Queue], Seen, Nodes, Opts, States, Violations, Limit) ->
    Key = erlang:phash2({maps:get(reps, W), maps:get(replaced, W)}),
    case maps:is_key(Key, Seen) of
        true ->
            bfs(Queue, Seen, Nodes, Opts, States, Violations, Limit);
        false ->
            V = check_world(W, Nodes, Opts),
            Next = successors(W, Nodes, Opts),
            bfs(Queue ++ Next, Seen#{Key => true}, Nodes, Opts,
                [maps:values(maps:get(reps, W)) ++ maps:get(past, W)
                 | States],
                V ++ Violations, Limit - 1)
    end.

successors(W, Nodes, Opts) ->
    #{reps := Reps, ops := Ops, crashes := Crashes} = W,
    MaxOps = maps:get(ops, Opts),
    Writes = case Ops < MaxOps of
        true -> [user_write(W, N, Kind, Opts)
                 || N <- maps:keys(Reps), Kind <- [edit, observed, delete]];
        false -> []
    end,
    Syncs = [sync(W, From, To, Opts)
             || {From, Tos} <- maps:to_list(Nodes), To <- Tos],
    Stale = [stale(W, N, Past, Opts)
             || N <- maps:keys(Reps), Past <- maps:get(past, W)],
    Crash = case Crashes < maps:get(crashes, Opts) of
        true -> [crash(W, N) || N <- maps:keys(Reps)];
        false -> []
    end,
    [S || S <- Writes ++ Syncs ++ Stale ++ Crash, S =/= skip].

user_write(#{reps := Reps, writers := Ws, clock := C} = W, N, Kind, Opts) ->
    S = maps:get(N, Reps),
    Token = {barrel_hlc:from_wall_time(C + 1),
             <<"w", (integer_to_binary(N))/binary, "_",
               (integer_to_binary(maps:get(N, Ws)))/binary>>},
    #{winner := Winner, observed := Observed} = ?L:view(S),
    Req = case Kind of
        observed -> ?L:replaces(S, Winner, Observed);
        _ -> ?L:replaces(S, Winner, undefined)
    end,
    case Req of
        {error, conflict} ->
            skip;
        {ok, Replaces} ->
            S1 = ?L:author(S, Token, Kind =:= delete, Replaces),
            {S2, Auto} = maybe_resolve(S1, Opts),
            W#{reps := Reps#{N := S2}, clock := C + 1,
               ops := maps:get(ops, W) + 1,
               past := lists:sublist([S | maps:get(past, W)], 2),
               written := [Token | maps:get(written, W)],
               replaced := Auto ++ Replaces ++ maps:get(replaced, W)}
    end.

sync(#{reps := Reps} = W, From, To, Opts) ->
    Merge = maps:get(merge, Opts, fun ?L:merge/2),
    {S, Auto} = maybe_resolve(Merge(maps:get(To, Reps), maps:get(From, Reps)),
                              Opts),
    W#{reps := Reps#{To := S},
       replaced := Auto ++ maps:get(replaced, W)}.

stale(#{reps := Reps} = W, N, Past, Opts) ->
    Merge = maps:get(merge, Opts, fun ?L:merge/2),
    {S, Auto} = maybe_resolve(Merge(maps:get(N, Reps), Past), Opts),
    W#{reps := Reps#{N := S}, replaced := Auto ++ maps:get(replaced, W)}.

%% A crash loses nothing synced in this model; it only rotates the
%% writer id, which is what lets the replica write again safely.
crash(#{writers := Ws} = W, N) ->
    W#{writers := Ws#{N := maps:get(N, Ws) + 1},
       crashes := maps:get(crashes, W) + 1}.

maybe_resolve(S, #{resolver := none}) ->
    {S, []};
maybe_resolve(S, #{resolver := max}) ->
    case ?L:auto_inputs(S) of
        [] -> {S, []};
        Inputs ->
            {Deleted, Hash} = fmax([value(S, T) || T <- Inputs]),
            case ?L:auto_step(S, Deleted, Hash) of
                {ok, S1, _} -> {S1, Inputs};
                _ -> {S, []}
            end
    end.

check_world(#{written := []}, _Nodes, _Opts) ->
    [];
check_world(#{reps := Reps, written := Written, replaced := Replaced},
            Nodes, Opts) ->
    case settle_world(Reps, Nodes, Opts, 12, Replaced) of
        stuck ->
            [stuck];
        {Final, Replaced1} ->
            Digests = lists:usort([digest(S) || S <- maps:values(Final)]),
            Any = hd(maps:values(Final)),
            Live = ?L:tokens(Any),
            Lost = [T || T <- Written, not lists:member(T, Replaced1),
                         not lists:member(T, Live)],
            Back = [T || T <- Live, lists:member(T, Replaced)],
            [diverge || length(Digests) > 1] ++
            [empty || Live =:= []] ++
            [{lost, Lost} || Lost =/= []] ++
            [{resurrected, Back} || Back =/= []]
    end.

settle_world(_Reps, _Nodes, _Opts, 0, _Replaced) ->
    stuck;
settle_world(Reps, Nodes, Opts, N, Replaced) ->
    Merge = maps:get(merge, Opts, fun ?L:merge/2),
    {Next, Replaced1} = lists:foldl(
        fun({From, To}, {Acc, RAcc}) ->
            {S, Auto} = maybe_resolve(Merge(maps:get(To, Acc),
                                            maps:get(From, Acc)), Opts),
            {Acc#{To := S}, Auto ++ RAcc}
        end,
        {Reps, Replaced},
        [{F, T} || {F, Ts} <- lists:sort(maps:to_list(Nodes)), T <- Ts]),
    case Next =:= Reps of
        true -> {Reps, Replaced1};
        false -> settle_world(Next, Nodes, Opts, N - 1, Replaced1)
    end.

%%====================================================================
%% Helpers and broken variants
%%====================================================================

h(N) -> barrel_hlc:from_wall_time(N).
t(N, W) -> {h(N), atom_to_binary(W)}.

write(S, Token, Replaces) -> ?L:author(S, Token, false, Replaces).

digest(S) -> ?L:digest(S).

%% Bodies are the token's HLC wall time; a resolution's value is the
%% max over its inputs, encoded in its result hash.
value(S, {H, W} = T) ->
    case maps:get(T, ?L:live(S)) of
        #{result := R} -> binary_to_term(R);
        _ when W =:= <<"auto">> -> 0;
        _ -> barrel_hlc:wall_time(H)
    end.

resolve(S, Fun) ->
    case ?L:auto_inputs(S) of
        [] -> S;
        Inputs ->
            Vals = [value(S, T) || T <- Inputs],
            {Deleted, Hash} = Fun(Vals),
            case ?L:auto_step(S, Deleted, Hash) of
                {ok, S1, _} -> S1;
                {noop, S1} -> S1;
                {stop, _} -> S
            end
    end.

settle(States, Fun) ->
    lists:foldl(fun(S, Acc) -> resolve(?L:merge(Acc, S), Fun) end,
                ?L:new(), States ++ States).

fmax(Vals) -> {false, term_to_binary(lists:max(Vals))}.
fmin(Vals) -> {false, term_to_binary(lists:min(Vals))}.

%% The empty-Live trace from the final gate: a=2, b=4, c=8; A runs max,
%% B and C run min.
mixed_trace(Resolve) ->
    [Wa, Wb, Wc] = [write(?L:new(), t(I, W), []) || {I, W} <- [{2, a}, {4, b}, {8, c}]],
    A1 = Resolve(?L:merge(Wa, Wb), fun fmax/1),
    B1 = Resolve(?L:merge(Wa, Wb), fun fmin/1),
    C1 = Resolve(?L:merge(?L:merge(Wc, Wa), Wb), fun fmin/1),
    A2 = Resolve(?L:merge(A1, C1), fun fmax/1),
    B2 = Resolve(?L:merge(B1, A2), fun fmin/1),
    {A2, B2, C1}.

settle_mixed([A, B, C]) ->
    lists:foldl(fun({S, F}, Acc) -> resolve(?L:merge(Acc, S), F) end,
                ?L:new(),
                [{A, fun fmax/1}, {B, fun fmin/1}, {C, fun fmin/1},
                 {A, fun fmax/1}, {B, fun fmin/1}]).

%% auto_step without the no-replace stop.
resolve_no_stop(S, Fun) ->
    case ?L:auto_inputs(S) of
        [] -> S;
        Inputs ->
            {Deleted, Hash} = Fun([value(S, T) || T <- Inputs]),
            Base = lists:usort(lists:append([?L:base(S, T) || T <- Inputs])),
            Token = ?L:auto_token(Base, Hash),
            #{cloud := Cloud} = S,
            S#{live => #{Token => #{deleted => Deleted, vv => #{},
                                    base => Base, result => Hash}},
               cloud => maps:merge(Cloud, maps:from_keys([Token | Inputs], true))}
    end.

own_clock_resolve(S, Token) ->
    #{cloud := Cloud, live := Live} = S,
    S#{live => #{Token => #{deleted => false, vv => #{}}},
       cloud => maps:merge(Cloud, maps:from_keys([Token | maps:keys(Live)], true))}.

raise_mark(#{obs := Obs} = S, {H, W}) ->
    S#{obs => Obs#{W => H}}.

import_vector(#{obs := Obs} = S, VV) ->
    S#{obs => maps:merge(Obs, VV)}.

hlc_of_max(Tokens) ->
    lists:max([H || {H, _} <- Tokens]).

%% A merge that ignores the causal context: union of live versions.
naive_merge(#{live := LA} = A, #{live := LB} = B) ->
    M = ?L:merge(A, B),
    M#{live => maps:merge(LA, LB)}.
