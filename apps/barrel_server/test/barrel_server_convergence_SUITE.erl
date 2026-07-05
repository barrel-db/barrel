%%%-------------------------------------------------------------------
%%% @doc Convergence over the wire: the version-vector convergence
%%% cases from barrel_convergence_SUITE, run between a local database
%%% and a served one through barrel_rep_transport_http. Both replicas
%%% must settle on the same winner, body, and deletion state for
%%% every document, whatever the interleaving.
%%%
%%% Helpers duplicated from barrel_convergence_SUITE on purpose: the
%%% suites live in different apps and the duplication is small.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_convergence_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_concurrent_edits_converge/1,
    t_deletes_converge/1,
    t_resolution_replicates/1,
    t_random_workload_converges/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_concurrent_edits_converge, t_deletes_converge,
     t_resolution_replicates, t_random_workload_converges].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    #{h1 := Port} = livery:which_listeners(Pid),
    Base = "http://127.0.0.1:" ++ integer_to_list(Port),
    [{base, Base} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    ok.

init_per_testcase(TC, Config) ->
    Local = <<(atom_to_binary(TC, utf8))/binary, "_local">>,
    Served = atom_to_list(TC) ++ "_served",
    {ok, _} = barrel_docdb:create_db(Local, #{
        data_dir => filename:join(?config(priv_dir, Config), "local")
    }),
    Endpoint = barrel_rep_transport_http:endpoint(
        list_to_binary(?config(base, Config) ++ "/db/" ++ Served)),
    %% lazy-create the served db so both sides exist up front
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    [{local, Local}, {served, list_to_binary(Served)},
     {endpoint, Endpoint} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(local, Config)) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Helpers (ported from barrel_convergence_SUITE, wire edition)
%%====================================================================

%% Replicate both ways over HTTP until neither direction ships a
%% document, so vector-merge writes settle too.
sync_until_quiescent(Local, Endpoint) ->
    sync_until_quiescent(Local, Endpoint, 10).

sync_until_quiescent(_Local, _Endpoint, 0) ->
    ct:fail(no_quiescence);
sync_until_quiescent(Local, Endpoint, N) ->
    {ok, R1} = barrel_rep:replicate(
        Local, Endpoint, #{target_transport => barrel_rep_transport_http}),
    {ok, R2} = barrel_rep:replicate(
        Endpoint, Local, #{source_transport => barrel_rep_transport_http}),
    case maps:get(docs_written, R1) + maps:get(docs_written, R2) of
        0 -> ok;
        _ -> sync_until_quiescent(Local, Endpoint, N - 1)
    end.

%% The replicable state of a doc: winner token, body, deletion flag.
%% The served db lives in this VM, so both sides read directly.
doc_state(Db, DocId) ->
    case barrel_docdb:get_doc_for_replication(Db, DocId) of
        {ok, #{doc := Doc, version := V, deleted := D}} ->
            {V, D, maps:remove(<<"id">>, Doc)};
        {error, not_found} ->
            not_found
    end.

assert_converged(A, B, DocIds) ->
    lists:foreach(
        fun(DocId) ->
            SA = doc_state(A, DocId),
            SB = doc_state(B, DocId),
            case SA =:= SB of
                true -> ok;
                false -> ct:fail({diverged, DocId, SA, SB})
            end
        end,
        DocIds).

update_doc(Db, DocId, Value) ->
    Doc = case barrel_docdb:get_doc(Db, DocId) of
        {ok, Existing} -> Existing#{<<"v">> => Value};
        {error, not_found} -> #{<<"id">> => DocId, <<"v">> => Value}
    end,
    case barrel_docdb:put_doc(Db, Doc) of
        {ok, _} -> ok;
        %% A tombstone winner: recreate over it
        {error, conflict} ->
            {ok, _} = barrel_docdb:put_doc(
                Db, #{<<"id">> => DocId, <<"v">> => Value}),
            ok
    end.

%%====================================================================
%% Cases
%%====================================================================

t_concurrent_edits_converge(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    DocIds = [<<"doc", (integer_to_binary(N))/binary>>
              || N <- lists:seq(1, 10)],

    [ok = update_doc(Local, Id, <<"seed">>) || Id <- DocIds],
    ok = sync_until_quiescent(Local, Endpoint),

    %% Divergent edits: both sides update every doc concurrently
    [ok = update_doc(Local, Id, <<"from-local">>) || Id <- DocIds],
    [ok = update_doc(Served, Id, <<"from-served">>) || Id <- DocIds],

    ok = sync_until_quiescent(Local, Endpoint),
    assert_converged(Local, Served, DocIds),

    {ok, Doc1} = barrel_docdb:get_doc(Local, hd(DocIds)),
    ?assert(lists:member(maps:get(<<"v">>, Doc1),
                         [<<"from-local">>, <<"from-served">>])),
    ok.

t_deletes_converge(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    DocIds = [<<"del", (integer_to_binary(N))/binary>>
              || N <- lists:seq(1, 6)],

    [ok = update_doc(Local, Id, <<"seed">>) || Id <- DocIds],
    ok = sync_until_quiescent(Local, Endpoint),

    %% Local deletes half while the served side updates all
    [begin
         {ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(Local, Id),
         {ok, _} = barrel_docdb:delete_doc(Local, Id, #{rev => Rev})
     end || Id <- lists:sublist(DocIds, 3)],
    [ok = update_doc(Served, Id, <<"survivor">>) || Id <- DocIds],

    ok = sync_until_quiescent(Local, Endpoint),
    assert_converged(Local, Served, DocIds),
    ok.

t_resolution_replicates(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    DocId = <<"resolved">>,

    ok = update_doc(Local, DocId, <<"seed">>),
    ok = sync_until_quiescent(Local, Endpoint),
    ok = update_doc(Local, DocId, <<"from-local">>),
    ok = update_doc(Served, DocId, <<"from-served">>),
    ok = sync_until_quiescent(Local, Endpoint),
    assert_converged(Local, Served, [DocId]),

    %% Resolve wherever a conflict was recorded, then sync
    lists:foreach(
        fun(Db) ->
            case barrel_docdb:get_conflicts(Db, DocId) of
                {ok, []} ->
                    ok;
                {ok, _Some} ->
                    {ok, #{<<"_rev">> := Rev}} =
                        barrel_docdb:get_doc(Db, DocId),
                    {ok, _} = barrel_docdb:resolve_conflict(
                        Db, DocId, Rev,
                        {merge, #{<<"v">> => <<"settled">>}})
            end
        end,
        [Local, Served]),
    ok = sync_until_quiescent(Local, Endpoint),
    assert_converged(Local, Served, [DocId]),

    {ok, Final} = barrel_docdb:get_doc(Local, DocId),
    ?assertEqual(<<"settled">>, maps:get(<<"v">>, Final)),
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(Local, DocId)),
    ?assertEqual({ok, []}, barrel_docdb:get_conflicts(Served, DocId)),
    ok.

t_random_workload_converges(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    rand:seed(exsss, {7, 7, 7}),
    DocIds = [<<"rnd", (integer_to_binary(N))/binary>>
              || N <- lists:seq(1, 8)],

    %% Interleaved rounds of random writes and partial syncs
    lists:foreach(
        fun(Round) ->
            lists:foreach(
                fun(Id) ->
                    Db = case rand:uniform(2) of
                        1 -> Local;
                        2 -> Served
                    end,
                    case rand:uniform(5) of
                        1 ->
                            case barrel_docdb:get_doc(Db, Id) of
                                {ok, #{<<"_rev">> := Rev}} ->
                                    _ = barrel_docdb:delete_doc(
                                            Db, Id, #{rev => Rev}),
                                    ok;
                                _ ->
                                    ok
                            end;
                        _ ->
                            V = iolist_to_binary(
                                [<<"r">>, integer_to_binary(Round),
                                 <<"-">>,
                                 integer_to_binary(rand:uniform(1000))]),
                            ok = update_doc(Db, Id, V)
                    end
                end,
                DocIds),
            %% Occasionally sync one direction mid-workload
            case rand:uniform(3) of
                1 ->
                    {ok, _} = barrel_rep:replicate(
                        Local, Endpoint,
                        #{target_transport => barrel_rep_transport_http}),
                    ok;
                2 ->
                    {ok, _} = barrel_rep:replicate(
                        Endpoint, Local,
                        #{source_transport => barrel_rep_transport_http}),
                    ok;
                _ ->
                    ok
            end
        end,
        lists:seq(1, 6)),

    ok = sync_until_quiescent(Local, Endpoint),
    assert_converged(Local, Served, DocIds),
    ok.
