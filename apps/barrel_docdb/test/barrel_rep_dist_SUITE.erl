%%% @doc Continuous replication between two nodes of one cluster over
%%% distributed Erlang (barrel_rep_transport_dist), run twice with the
%%% same cases: over TCP distribution and over QUIC distribution
%%% (erlang_quic's quic_dist). Each group first proves which carrier its
%%% peers really use.
-module(barrel_rep_dist_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).
-export([carrier_in_use/1,
         continuous_both_converges/1,
         concurrent_edits_conflict_everywhere/1,
         attachments_cross_nodes/1,
         peer_restart_resumes/1,
         task_down_with_map_endpoint/1]).
%% a transport whose calls crash, for task_down_with_map_endpoint
-export([get_changes/3, get_local_doc/2, put_local_doc/3, db_info/1]).

cases() ->
    [carrier_in_use, continuous_both_converges,
     concurrent_edits_conflict_everywhere, attachments_cross_nodes,
     peer_restart_resumes, task_down_with_map_endpoint].

all() ->
    [{group, tcp}, {group, quic}].

groups() ->
    [{tcp, [sequence], cases()}, {quic, [sequence], cases()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(Carrier, Config) ->
    Dir = filename:join(?config(priv_dir, Config), atom_to_list(Carrier)),
    Peers = barrel_dist_peer:start(Carrier, "rep_" ++ atom_to_list(Carrier), 2, Dir),
    [{carrier, Carrier}, {peers, Peers}, {dir, Dir} | Config].

end_per_group(_Carrier, Config) ->
    barrel_dist_peer:stop(peers(Config)),
    ok.

init_per_testcase(TC, Config) ->
    [{db, atom_to_binary(TC)} | Config].

end_per_testcase(_TC, Config) ->
    lists:foreach(
      fun(P) ->
              try on(P, barrel_rep_tasks, list_tasks, []) of
                  {ok, Tasks} ->
                      [on(P, barrel_rep_tasks, delete_task, [Id]) || #{id := Id} <- Tasks];
                  _ -> ok
              catch _:_ -> ok
              end
      end, peers(Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% The peers run the carrier of the group, and reach each other over it.
carrier_in_use(Config) ->
    [A, B] = peers(Config),
    Carrier = ?config(carrier, Config),
    [?assertEqual(Carrier, barrel_dist_peer:carrier(P)) || P <- [A, B]],
    ?assertEqual(pong, on(A, net_adm, ping, [barrel_dist_peer:node(B)])),
    ok.

%% A continuous two-way task over a node endpoint carries writes made
%% on either node to the other.
continuous_both_converges(Config) ->
    [A, B] = peers(Config),
    Db = ?config(db, Config),
    create(A, Db), create(B, Db),
    _ = task(A, B, Db),
    [put(A, Db, <<"a", (integer_to_binary(I))/binary>>, I) || I <- lists:seq(1, 20)],
    [put(B, Db, <<"b", (integer_to_binary(I))/binary>>, I) || I <- lists:seq(1, 20)],
    ok = until(fun() -> ids(A, Db) =:= ids(B, Db) andalso length(ids(A, Db)) =:= 40 end),
    ok.

%% The same document edited on both nodes at once ends as the same
%% winner with the same conflict on both.
concurrent_edits_conflict_everywhere(Config) ->
    [A, B] = peers(Config),
    Db = ?config(db, Config),
    create(A, Db), create(B, Db),
    put(A, Db, <<"d">>, 0),
    {ok, TaskId} = task(A, B, Db),
    ok = until(fun() -> ids(B, Db) =:= [<<"d">>] end),
    ok = on(A, barrel_rep_tasks, pause_task, [TaskId]),
    edit(A, Db, <<"d">>, <<"from a">>),
    edit(B, Db, <<"d">>, <<"from b">>),
    ok = on(A, barrel_rep_tasks, resume_task, [TaskId]),
    ok = until(fun() ->
                   {Ra, Ca} = rev_conflicts(A, Db, <<"d">>),
                   {Rb, Cb} = rev_conflicts(B, Db, <<"d">>),
                   Ra =:= Rb andalso Ca =:= Cb andalso length(Ca) =:= 1
               end),
    ok.

%% Attachments go both ways, a large one in many chunks.
attachments_cross_nodes(Config) ->
    [A, B] = peers(Config),
    Db = ?config(db, Config),
    Opts = #{att_opts => #{chunk_threshold => 1024, chunk_size => 4096}},
    create(A, Db, Opts), create(B, Db, Opts),
    put(A, Db, <<"x">>, 0),
    put(B, Db, <<"y">>, 0),
    Big = crypto:strong_rand_bytes(200000),
    Small = <<"small">>,
    {ok, _} = on(A, barrel_docdb, put_attachment, [Db, <<"x">>, <<"big">>, Big]),
    {ok, _} = on(B, barrel_docdb, put_attachment, [Db, <<"y">>, <<"s">>, Small]),
    _ = task(A, B, Db),
    ok = until(fun() -> attachment(B, Db, <<"x">>, <<"big">>) =:= {ok, Big} end),
    ok = until(fun() -> attachment(A, Db, <<"y">>, <<"s">>) =:= {ok, Small} end),
    ok.

%% The other node going away leaves the task backing off; once it is
%% back, the writes made meanwhile reach it.
peer_restart_resumes(Config) ->
    [A, B] = peers(Config),
    Db = ?config(db, Config),
    create(A, Db), create(B, Db),
    {ok, TaskId} = task(A, B, Db),
    put(A, Db, <<"before">>, 0),
    ok = until(fun() -> ids(B, Db) =:= [<<"before">>] end),
    barrel_dist_peer:stop(B),
    put(A, Db, <<"while_down">>, 0),
    timer:sleep(500),
    {ok, #{status := running}} = on(A, barrel_rep_tasks, get_task, [TaskId]),
    B2 = barrel_dist_peer:restart(B, [A, B], ?config(dir, Config)),
    {ok, _} = on(B2, barrel_docdb, create_db, [Db, #{}]),
    ok = until(fun() -> ids(B2, Db) =:= [<<"before">>, <<"while_down">>] end, 600),
    ok.

%% A task whose endpoint is a map and whose process dies leaves the task
%% manager running (it used to crash on looking the map up as a name).
task_down_with_map_endpoint(Config) ->
    [A, _] = peers(Config),
    Db = ?config(db, Config),
    create(A, Db),
    put(A, Db, <<"d">>, 0),
    Manager = on(A, erlang, whereis, [barrel_rep_tasks]),
    {ok, TaskId} = on(A, barrel_rep_tasks, start_task,
                      [#{source => Db, target => #{node => nowhere, db => Db},
                         target_transport => ?MODULE, mode => one_shot}]),
    ok = until(fun() ->
                   case on(A, barrel_rep_tasks, get_task, [TaskId]) of
                       {ok, #{status := failed}} -> true;
                       _ -> false
                   end
               end),
    ?assertEqual(Manager, on(A, erlang, whereis, [barrel_rep_tasks])),
    {ok, _} = on(A, barrel_rep_tasks, list_tasks, []),
    ok.

%% The crashing transport.
get_changes(_, _, _) -> exit(transport_crash).
get_local_doc(_, _) -> exit(transport_crash).
put_local_doc(_, _, _) -> exit(transport_crash).
db_info(_) -> exit(transport_crash).

%%====================================================================
%% Helpers
%%====================================================================

peers(Config) -> ?config(peers, Config).

on(P, M, F, A) -> barrel_dist_peer:call(P, M, F, A).

create(P, Db) -> create(P, Db, #{}).

create(P, Db, Opts) ->
    _ = on(P, barrel_docdb, delete_db, [Db]),
    {ok, _} = on(P, barrel_docdb, create_db, [Db, Opts]).

%% A continuous two-way task on A with B's database as a node endpoint.
task(A, B, Db) ->
    on(A, barrel_rep_tasks, start_task,
       [#{source => Db, target => #{node => barrel_dist_peer:node(B), db => Db},
          direction => both, mode => continuous}]).

put(P, Db, Id, V) ->
    {ok, _} = on(P, barrel_docdb, put_doc, [Db, #{<<"id">> => Id, <<"v">> => V}]).

edit(P, Db, Id, V) ->
    {ok, #{<<"_rev">> := Rev}} = on(P, barrel_docdb, get_doc, [Db, Id, #{}]),
    {ok, _} = on(P, barrel_docdb, put_doc,
                 [Db, #{<<"id">> => Id, <<"v">> => V, <<"_rev">> => Rev}]).

ids(P, Db) ->
    {ok, Ids} = on(P, barrel_docdb, fold_docs,
                   [Db, fun(#{<<"id">> := Id}, Acc) -> {ok, [Id | Acc]} end, []]),
    lists:sort(Ids).

rev_conflicts(P, Db, Id) ->
    case on(P, barrel_docdb, get_doc, [Db, Id, #{conflicts => true}]) of
        {ok, D} -> {maps:get(<<"_rev">>, D), lists:sort(maps:get(<<"_conflicts">>, D, []))};
        Other -> {Other, []}
    end.

attachment(P, Db, Id, Name) ->
    on(P, barrel_docdb, get_attachment, [Db, Id, Name]).

until(Fun) -> until(Fun, 300).

until(_Fun, 0) -> {error, timeout};
until(Fun, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(100), until(Fun, N - 1)
    end.
