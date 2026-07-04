%%%-------------------------------------------------------------------
%%% @doc Replication over the wire: local databases replicating with
%%% served ones through barrel_rep_transport_http, in both directions,
%%% with filters, checkpoints, and graceful attachment degradation
%%% (the HTTP attachment leg lands in a later step).
%%%
%%% Harness note: barrel_server is a registered singleton, so the
%%% "remote" databases live in the same VM behind a real HTTP
%%% listener; the wire path (livery + hackney) is fully exercised.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_rep_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_push_over_http/1,
    t_pull_over_http/1,
    t_bidirectional_convergence/1,
    t_filtered_pull/1,
    t_checkpoint_reuse/1,
    t_att_sync_degrades/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_push_over_http, t_pull_over_http, t_bidirectional_convergence,
     t_filtered_pull, t_checkpoint_reuse, t_att_sync_degrades].

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
    %% one local db + one served db per case
    Local = <<(atom_to_binary(TC, utf8))/binary, "_local">>,
    Served = atom_to_list(TC) ++ "_served",
    {ok, _} = barrel_docdb:create_db(Local, #{
        data_dir => filename:join(?config(priv_dir, Config), "local")
    }),
    Endpoint = barrel_rep_transport_http:endpoint(
        list_to_binary(?config(base, Config) ++ "/db/" ++ Served)),
    [{local, Local}, {served, list_to_binary(Served)},
     {endpoint, Endpoint} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(local, Config)) catch _:_ -> ok end,
    ok.

push_opts() ->
    #{target_transport => barrel_rep_transport_http}.

pull_opts() ->
    #{source_transport => barrel_rep_transport_http}.

%%====================================================================
%% Cases
%%====================================================================

t_push_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"a">>,
                                            <<"v">> => 1}),
    {ok, #{<<"rev">> := RevB}} = barrel_docdb:put_doc(
        Local, #{<<"id">> => <<"b">>, <<"v">> => 1}),
    {ok, _} = barrel_docdb:delete_doc(Local, <<"b">>, #{rev => RevB}),
    {ok, R} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertMatch(#{docs_written := 2}, R),
    %% the served db lives in this VM: read it directly
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Served, <<"a">>),
    {error, not_found} = barrel_docdb:get_doc(Served, <<"b">>),
    %% the tombstone arrived, not just an absence
    {ok, #{deleted := true}} =
        barrel_docdb:get_doc_for_replication(Served, <<"b">>),
    ok.

t_pull_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    %% ensure the served db exists (lazy creation on first touch)
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"p">>,
                                             <<"kind">> => <<"pulled">>}),
    {ok, R} = barrel_rep:replicate(Endpoint, Local, pull_opts()),
    ?assertMatch(#{docs_written := 1}, R),
    {ok, #{<<"kind">> := <<"pulled">>}} =
        barrel_docdb:get_doc(Local, <<"p">>),
    ok.

t_bidirectional_convergence(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    %% concurrent edits on both sides
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"x">>,
                                            <<"from">> => <<"local">>}),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"x">>,
                                             <<"from">> => <<"served">>}),
    sync_until_quiescent(Local, Endpoint, 10),
    {ok, LocalState} = barrel_docdb:get_doc_for_replication(Local, <<"x">>),
    {ok, ServedState} = barrel_docdb:get_doc_for_replication(Served,
                                                             <<"x">>),
    ?assertEqual(maps:get(version, LocalState),
                 maps:get(version, ServedState)),
    ?assertEqual(maps:get(doc, LocalState), maps:get(doc, ServedState)),
    ok.

sync_until_quiescent(_Local, _Endpoint, 0) ->
    ct:fail(no_quiescence);
sync_until_quiescent(Local, Endpoint, N) ->
    {ok, R1} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    {ok, R2} = barrel_rep:replicate(Endpoint, Local, pull_opts()),
    case maps:get(docs_written, R1) + maps:get(docs_written, R2) of
        0 -> ok;
        _ -> sync_until_quiescent(Local, Endpoint, N - 1)
    end.

t_filtered_pull(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"f1">>,
                                             <<"type">> => <<"post">>}),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"f2">>,
                                             <<"type">> => <<"user">>}),
    %% the paths filter crosses the wire inside the changes request
    Opts = (pull_opts())#{filter => #{paths => [<<"type/post">>]}},
    {ok, _} = barrel_rep:replicate(Endpoint, Local, Opts),
    {ok, _} = barrel_docdb:get_doc(Local, <<"f1">>),
    {error, not_found} = barrel_docdb:get_doc(Local, <<"f2">>),
    %% a query filter crosses too
    Opts2 = (pull_opts())#{filter =>
        #{query => #{where => [{path, [<<"type">>], <<"user">>}]}}},
    {ok, _} = barrel_rep:replicate(Endpoint, Local, Opts2),
    {ok, _} = barrel_docdb:get_doc(Local, <<"f2">>),
    ok.

t_checkpoint_reuse(Config) ->
    Local = ?config(local, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"c1">>}),
    {ok, #{docs_read := 1}} =
        barrel_rep:replicate(Local, Endpoint, push_opts()),
    %% the second run resumes from the wire-persisted checkpoint
    {ok, #{docs_read := 0, docs_written := 0}} =
        barrel_rep:replicate(Local, Endpoint, push_opts()),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"c2">>}),
    {ok, #{docs_read := 1, docs_written := 1}} =
        barrel_rep:replicate(Local, Endpoint, push_opts()),
    ok.

t_att_sync_degrades(Config) ->
    Local = ?config(local, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:put_attachment(Local, <<"d">>, <<"f">>,
                                          <<"blob">>),
    %% the HTTP transport has no attachment callbacks yet: the phase
    %% must degrade to skipped, not fail
    {ok, R} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertEqual(skipped, maps:get(att_sync, R)),
    ok.
