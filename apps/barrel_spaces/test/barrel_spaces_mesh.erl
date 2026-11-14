%%%-------------------------------------------------------------------
%%% @doc Test helper: replicas of the agent layer in one VM. A replica
%%% is a registry database plus a space database; `on/2' runs a call
%%% against one replica's registry, `sync/2' replicates one replica
%%% into another with barrel_rep.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_mesh).

-export([start/2, start/3, stop/1, on/2, sync/2, sync_all/1, space/1,
         conflicted/1]).

-define(SPACE_ID, <<"sp_mesh">>).

%% Replicas named Prefix_a, Prefix_b, ... (N of them), opened with the
%% resolvers barrel_spaces opens its registry and spaces with.
start(Prefix, N) ->
    start(Prefix, N, {barrel_spaces_resolver, space}).

%% start/2 with the resolver of the space databases.
start(Prefix, N, SpaceResolver) ->
    [replica(<<Prefix/binary, "_", C>>, SpaceResolver)
     || C <- lists:sublist([$a, $b, $c, $d, $e], N)].

replica(Name, SpaceResolver) ->
    Reg = <<Name/binary, "_reg">>,
    {ok, _} = barrel_docdb:create_db(
                Reg, #{conflict_resolver => {barrel_spaces_resolver, registry}}),
    {ok, Db} = barrel_dbs:ensure(<<Name/binary, "_sp">>,
                                 #{vectordb => none,
                                   docdb => #{ttl_sweep_interval => 60000,
                                              conflict_resolver => SpaceResolver}}),
    #{name => Name, reg => Reg, space => #{id => ?SPACE_ID, db => Db}}.

stop(Replicas) ->
    lists:foreach(fun(#{reg := Reg, space := #{db := #{docdb := Sp}}}) ->
                      _ = barrel_dbs:destroy(Sp),
                      _ = barrel_docdb:delete_db(Reg)
                  end, Replicas),
    application:unset_env(barrel_spaces, registry_db).

%% Run Fun against the replica's registry.
on(#{reg := Reg}, Fun) ->
    ok = application:set_env(barrel_spaces, registry_db, Reg),
    Fun().

space(#{space := Space}) ->
    Space.

%% One-shot replication of everything From holds into To.
sync(#{reg := RegA, space := #{db := #{docdb := SpA}}},
     #{reg := RegB, space := #{db := #{docdb := SpB}}}) ->
    {ok, _} = barrel_rep:replicate(RegA, RegB),
    {ok, _} = barrel_rep:replicate(SpA, SpB),
    ok.

%% Every pair, twice: every replica holds every version.
sync_all(Replicas) ->
    Pairs = [{A, B} || A <- Replicas, B <- Replicas, A =/= B],
    lists:foreach(fun({A, B}) -> ok = sync(A, B) end, Pairs ++ Pairs).

%% The documents of a replica (registry and space) that hold more than
%% one live version, tombstones included.
conflicted(#{reg := Reg, space := #{db := #{docdb := Sp}}}) ->
    [{Db, Id} || Db <- [Reg, Sp],
                 Id <- ids(Db),
                 case barrel_spaces_docs:state(Db, Id) of
                     {ok, Live, _} -> length(Live) > 1;
                     not_found -> false
                 end].

ids(Db) ->
    {ok, Ids} = barrel_docdb:fold_docs(
                  Db, fun(#{<<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end, [],
                  #{include_deleted => true}),
    Ids.
