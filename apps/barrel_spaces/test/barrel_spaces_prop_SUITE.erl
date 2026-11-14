%%%-------------------------------------------------------------------
%%% @doc Random concurrent writes on three replicas with random
%%% one-shot replications between them, then a full sync, from fixed
%%% seeds. An oracle tracks the versions each replica holds: a write
%%% adds a version, a removal adds one only when the replica sees the
%%% item, replication joins the sets, and a replica shows the newest
%%% version it holds. After every step the replica that acted, or
%%% received, must show what the oracle says; after the full sync every
%%% replica must show the same thing, counters must be exact, a
%%% revocation must hold everywhere and a handoff status must never
%%% have gone back on any replica.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_prop_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([t_sessions/1, t_handoffs/1, t_grants/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(barrel_spaces_mesh, [on/2, sync/2, sync_all/1, space/1]).

-define(SEEDS, 25).
-define(STEPS, 40).

all() ->
    [t_sessions, t_handoffs, t_grants].

init_per_suite(Config) ->
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    {ok, _} = application:ensure_all_started(barrel_spaces),
    Config.

end_per_suite(_Config) ->
    ok.

t_sessions(_Config) ->
    each_seed(<<"ps">>, fun sessions/2).

t_handoffs(_Config) ->
    each_seed(<<"ph">>, fun handoffs/2).

t_grants(_Config) ->
    each_seed(<<"pg">>, fun grants/2).

each_seed(Tag, Run) ->
    lists:foreach(
      fun(Seed) ->
          put(prop_rand, rand:seed_s(exsss, {Seed, 7, 11})),
          Prefix = <<Tag/binary, (integer_to_binary(Seed))/binary>>,
          Rs = barrel_spaces_mesh:start(Prefix, 3),
          try
              Run(Rs, Seed)
          catch
              C:E:St ->
                  ct:pal("seed ~p failed", [Seed]),
                  erlang:raise(C, E, St)
          after
              barrel_spaces_mesh:stop(Rs)
          end
      end, seeds()).

%% PROP_SEED=N runs one seed.
seeds() ->
    case os:getenv("PROP_SEED") of
        false -> lists:seq(1, ?SEEDS);
        S -> [list_to_integer(S)]
    end.

%%====================================================================
%% Sessions: data keys, pins, counters
%%====================================================================

sessions(Rs, _Seed) ->
    {ok, Sid} = barrel_session:create(space(hd(Rs)), #{}),
    ok = sync_all(Rs),
    O0 = #{n => 0, held => maps:from_list([{name(R), #{}} || R <- Rs]),
           counts => #{}},
    O = lists:foldl(fun(_, O1) -> session_step(Rs, Sid, O1) end, O0,
                    lists:seq(1, ?STEPS)),
    ok = sync_all(Rs),
    Final = visible(all_versions(O)),
    Views = [session_view(R, Sid) || R <- Rs],
    [?assertEqual(Final, V) || V <- Views],
    Counts = maps:get(counts, O),
    [?assertEqual({ok, Counts}, barrel_session:get_counters(space(R), Sid))
     || R <- Rs],
    %% a count document has one writer: never a conflict to lose
    [[?assertNot(maps:is_key(<<"_conflicts">>, D)) || D <- count_docs(R, Sid)]
     || R <- Rs],
    ok.

count_docs(R, Sid) ->
    #{db := #{docdb := Db}} = space(R),
    Ids = barrel_spaces_docs:fold_prefix(
            Db, <<"session:", Sid/binary, ":count:">>,
            fun(#{<<"id">> := Id}, Acc) -> [Id | Acc] end, []),
    [element(2, barrel_docdb:get_doc(Db, Id, #{conflicts => true})) || Id <- Ids].

session_step(Rs, Sid, O) ->
    R = pick(Rs),
    case uniform(6) of
        1 ->
            Key = pick([<<"k1">>, <<"k2">>, <<"k3">>]),
            Val = maps:get(n, O),
            {ok, _} = barrel_session:set_data(space(R), Sid, Key, Val),
            check(R, Sid, write(R, {data, Key}, {val, Val}, O));
        2 ->
            Key = pick([<<"k1">>, <<"k2">>, <<"k3">>]),
            {ok, _} = barrel_session:delete_data(space(R), Sid, Key),
            check(R, Sid, remove(R, {data, Key}, O));
        3 ->
            Pin = pick([<<"p1">>, <<"p2">>]),
            Content = maps:get(n, O),
            case barrel_session:pin_context(space(R), Sid,
                                            #{id => Pin, content => Content}) of
                {ok, Pin} ->
                    check(R, Sid, write(R, {pin, Pin}, {val, Content}, O));
                {error, conflict} ->
                    ?assertMatch({val, _}, shown(R, {pin, Pin}, O)),
                    check(R, Sid, O)
            end;
        4 ->
            Pin = pick([<<"p1">>, <<"p2">>]),
            {ok, _} = barrel_session:unpin_context(space(R), Sid, Pin),
            check(R, Sid, remove(R, {pin, Pin}, O));
        5 ->
            Name = pick([<<"c1">>, <<"c2">>]),
            By = uniform(3),
            {ok, _} = barrel_session:incr_counter(space(R), Sid, Name, By),
            Counts = maps:get(counts, O),
            O#{counts => Counts#{Name => maps:get(Name, Counts, 0) + By}};
        6 ->
            To = pick(Rs -- [R]),
            ok = sync(R, To),
            check(To, Sid, join(R, To, O))
    end.

%% What a replica shows must be what the oracle says it holds.
check(R, Sid, O) ->
    ?assertEqual(visible(held(R, O)), session_view(R, Sid)),
    O.

session_view(R, Sid) ->
    {ok, S} = barrel_session:get(space(R), Sid),
    Data = [{{data, K}, V} || {K, V} <- maps:to_list(maps:get(<<"data">>, S))],
    Pins = [{{pin, I}, C} || #{<<"id">> := I, <<"content">> := C}
                                 <- maps:get(<<"pinned">>, S)],
    lists:sort(Data ++ Pins).

%%====================================================================
%% Handoffs: accepts and completions
%%====================================================================

handoffs(Rs, _Seed) ->
    A = hd(Rs),
    {ok, #{handoff_id := Hid, token := T}} =
        on(A, fun() -> barrel_handoff:create(<<"sp_mesh">>,
                                             #{task_name => <<"t">>})
              end),
    ok = sync_all(Rs),
    Rank0 = maps:from_list([{name(R), 0} || R <- Rs]),
    {Accepted, Completed, Rank} =
        lists:foldl(fun(_, Acc) -> handoff_step(Rs, Hid, T, Acc) end,
                    {[], false, Rank0}, lists:seq(1, ?STEPS)),
    ok = sync_all(Rs),
    Views = [element(2, on(R, fun() -> barrel_handoff:get(Hid) end))
             || R <- Rs],
    [V0 | _] = Views,
    [?assertEqual(maps:with([<<"status">>, <<"accepted_by">>,
                             <<"accepted_at">>, <<"result">>], V0),
                  maps:with([<<"status">>, <<"accepted_by">>,
                             <<"accepted_at">>, <<"result">>], V))
     || V <- Views],
    Status = case {Completed, Accepted} of
        {true, _} -> <<"completed">>;
        {false, [_ | _]} -> <<"accepted">>;
        {false, []} -> <<"pending">>
    end,
    ?assertEqual(Status, maps:get(<<"status">>, V0)),
    case Accepted of
        [] -> ok;
        _ -> ?assert(lists:member(maps:get(<<"accepted_by">>, V0), Accepted))
    end,
    %% the final status is the furthest any replica ever showed
    ?assertEqual(lists:max(maps:values(Rank)), rank(Status)),
    ok.

handoff_step(Rs, Hid, T, {Accepted, Completed, Rank}) ->
    R = pick(Rs),
    {Acc1, Done1} = case uniform(3) of
        1 ->
            Agent = pick([<<"x">>, <<"y">>, <<"z">>]),
            case on(R, fun() ->
                           barrel_handoff:accept(T, #{agent => Agent,
                                                      session => false})
                       end) of
                {ok, _} -> {[Agent | Accepted], Completed};
                {error, already_accepted} -> {Accepted, Completed};
                {error, already_completed} -> {Accepted, Completed}
            end;
        2 ->
            case on(R, fun() ->
                           barrel_handoff:complete(T, #{revoke => false,
                                                        result => name(R)})
                       end) of
                {ok, _} -> {Accepted, true};
                {error, already_completed} -> {Accepted, Completed}
            end;
        3 ->
            ok = sync(R, pick(Rs -- [R])),
            {Accepted, Completed}
    end,
    %% no replica's status ever goes back
    Rank1 = lists:foldl(
              fun(Rx, M) ->
                  {ok, #{<<"status">> := S}} =
                      on(Rx, fun() -> barrel_handoff:get(Hid) end),
                  Old = maps:get(name(Rx), M),
                  ?assert(rank(S) >= Old),
                  M#{name(Rx) => rank(S)}
              end, Rank, Rs),
    {Acc1, Done1, Rank1}.

rank(<<"pending">>) -> 0;
rank(<<"accepted">>) -> 1;
rank(<<"completed">>) -> 2.

%%====================================================================
%% Grants: revocations against grant rewrites by old nodes
%%====================================================================

grants(Rs, _Seed) ->
    A = hd(Rs),
    Tokens = [begin
                  {ok, Tok, #{<<"token_id">> := Tid}} =
                      on(A, fun() -> barrel_caps:grant(<<"sp_mesh">>, #{}) end),
                  {Tok, Tid}
              end || _ <- lists:seq(1, 3)],
    ok = sync_all(Rs),
    Refused0 = #{},
    {Revoked, Refused} =
        lists:foldl(fun(_, Acc) -> grant_step(Rs, Tokens, Acc) end,
                    {[], Refused0}, lists:seq(1, ?STEPS)),
    ok = sync_all(Rs),
    [begin
         Want = case lists:member(Tok, Revoked) of
             true -> {error, revoked};
             false -> ok
         end,
         [?assertEqual(Want, verified(R, Tok)) || R <- Rs]
     end || {Tok, _} <- Tokens],
    _ = Refused,
    ok.

grant_step(Rs, Tokens, {Revoked, Refused}) ->
    R = pick(Rs),
    {Tok, Tid} = pick(Tokens),
    Revoked1 = case uniform(3) of
        1 ->
            ok = on(R, fun() -> barrel_caps:revoke(Tok) end),
            [Tok | Revoked];
        2 ->
            %% an old node rewrites the grant, clearing revoked_at
            on(R, fun() ->
                      Reg = barrel_spaces:registry_db(),
                      {ok, G} = barrel_docdb:get_doc(Reg,
                                                     <<"grant:", Tid/binary>>),
                      {ok, _} = barrel_docdb:put_doc(
                                  Reg, G#{<<"label">> => integer_to_binary(
                                                           uniform(99)),
                                          <<"revoked_at">> => 0})
                  end),
            Revoked;
        3 ->
            ok = sync(R, pick(Rs -- [R])),
            Revoked
    end,
    %% once a replica refuses a token it never accepts it again
    Refused1 = lists:foldl(
                 fun({T, _}, M) ->
                     lists:foldl(
                       fun(Rx, M1) ->
                           Key = {name(Rx), T},
                           case verified(Rx, T) of
                               {error, revoked} -> M1#{Key => true};
                               ok -> ?assertNot(maps:is_key(Key, M1)), M1
                           end
                       end, M, Rs)
                 end, Refused, Tokens),
    {Revoked1, Refused1}.

verified(R, Tok) ->
    case on(R, fun() -> barrel_caps:verify(Tok, <<"sp_mesh">>, read) end) of
        {ok, _} -> ok;
        Err -> Err
    end.

%%====================================================================
%% Oracle
%%====================================================================

%% Versions are {N, Item, Value}: N grows with every write, so the
%% newest version of an item is the one with the highest N.
write(R, Item, Value, #{n := N, held := Held} = O) ->
    Mine = maps:get(name(R), Held),
    O#{n => N + 1,
       held => Held#{name(R) => Mine#{N => {Item, Value}}}}.

%% A removal writes a tombstone only when the replica shows the item.
remove(R, Item, O) ->
    case shown(R, Item, O) of
        {val, _} -> write(R, Item, deleted, O);
        _ -> O
    end.

join(From, To, #{held := Held} = O) ->
    O#{held => Held#{name(To) => maps:merge(maps:get(name(To), Held),
                                            maps:get(name(From), Held))}}.

shown(R, Item, O) ->
    newest(Item, held(R, O)).

held(R, #{held := Held}) ->
    maps:get(name(R), Held).

all_versions(#{held := Held}) ->
    maps:fold(fun(_, V, Acc) -> maps:merge(Acc, V) end, #{}, Held).

newest(Item, Versions) ->
    case lists:reverse(lists:sort([{N, V} || {N, {I, V}} <- maps:to_list(Versions),
                                             I =:= Item])) of
        [{_, V} | _] -> V;
        [] -> absent
    end.

visible(Versions) ->
    Items = lists:usort([I || {I, _} <- maps:values(Versions)]),
    lists:sort([{I, V} || I <- Items, {val, V} <- [newest(I, Versions)]]).

name(#{name := Name}) ->
    Name.

pick(List) ->
    lists:nth(uniform(length(List)), List).

%% The suite's own random state: code under test drawing from the
%% process's state must not change the steps a seed runs.
uniform(N) ->
    {X, S} = rand:uniform_s(N, get(prop_rand)),
    put(prop_rand, S),
    X.
