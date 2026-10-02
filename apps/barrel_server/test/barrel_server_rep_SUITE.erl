%%%-------------------------------------------------------------------
%%% @doc Replication over the wire: local databases replicating with
%%% served ones through barrel_rep_transport_http, in both directions,
%%% with filters, checkpoints, and the attachment phase riding along
%%% (attachment depth is covered by barrel_server_att_SUITE).
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
    t_att_sync_rides_along/1,
    t_continuous_push_over_http/1,
    t_continuous_pull_over_http/1,
    t_registry_grants_over_http/1,
    t_binary_field_round_trip/1,
    t_json_only_client/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_push_over_http, t_pull_over_http, t_bidirectional_convergence,
     t_filtered_pull, t_checkpoint_reuse, t_att_sync_rides_along,
     t_continuous_push_over_http, t_continuous_pull_over_http,
     t_registry_grants_over_http, t_binary_field_round_trip,
     t_json_only_client].

init_per_suite(Config) ->
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    Port = barrel_server_test:h1_port(Pid),
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
    _ = meck:unload(),
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

t_att_sync_rides_along(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:put_attachment(Local, <<"d">>, <<"f">>,
                                          <<"blob">>),
    %% the attachment phase runs over the wire as part of the rep
    {ok, R} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertMatch(#{atts_written := 1}, maps:get(att_sync, R)),
    {ok, <<"blob">>} = barrel_docdb:get_attachment(Served, <<"d">>,
                                                   <<"f">>),
    ok.

%% A continuous task with a remote URL target: the task manager
%% resolves the HTTP transport itself, and the local changes stream
%% wakes the push loop.
t_continuous_push_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, TaskId} = barrel_rep_tasks:start_task(#{
        source => Local,
        target => maps:get(url, Endpoint),
        mode => continuous,
        direction => push
    }),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"cp1">>}),
    ok = wait_until(doc_in(Served, <<"cp1">>), 100, 300),
    %% still alive: a second write flows too
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"cp2">>}),
    ok = wait_until(doc_in(Served, <<"cp2">>), 100, 300),
    ok = barrel_rep_tasks:stop_task(TaskId),
    ok = barrel_rep_tasks:delete_task(TaskId),
    ok.

%% A continuous pull from a remote URL source: adaptive polling picks
%% up remote writes (500ms floor, doubling while idle up to 15 s, so the
%% wait outlasts the longest poll).
t_continuous_pull_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_rep_transport_http:db_info(Endpoint),
    {ok, TaskId} = barrel_rep_tasks:start_task(#{
        source => Local,
        target => maps:get(url, Endpoint),
        mode => continuous,
        direction => pull
    }),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"cl1">>}),
    ok = wait_until(doc_in(Local, <<"cl1">>), 100, 300),
    ok = barrel_rep_tasks:stop_task(TaskId),
    ok = barrel_rep_tasks:delete_task(TaskId),
    ok.

wait_until(_Fun, _IntervalMs, 0) ->
    ct:fail(condition_never_met);
wait_until(Fun, IntervalMs, Tries) ->
    case Fun() of
        true -> ok;
        false ->
            timer:sleep(IntervalMs),
            wait_until(Fun, IntervalMs, Tries - 1)
    end.

doc_in(Db, DocId) ->
    fun() ->
        case barrel_docdb:get_doc(Db, DocId) of
            {ok, _} -> true;
            _ -> false
        end
    end.

%% The spaces registry replicates both ways once a token exists, its
%% revocation included, and so does a grant minted before 1.3.0 (raw hash).
t_registry_grants_over_http(Config) ->
    Endpoint = ?config(endpoint, Config),
    Served = ?config(served, Config),
    Local = ?config(local, Config),
    Registry = barrel_spaces:registry_db(),
    {ok, Token, #{<<"token_id">> := Id}} =
        barrel_caps:grant(<<"sp_registry_case">>, #{rights => [read]}),
    {ok, Legacy, #{<<"token_id">> := LegacyId}} =
        barrel_caps:grant(<<"sp_registry_case">>, #{rights => [read]}),
    LegacyKey = <<"grant:", LegacyId/binary>>,
    {ok, LegacyDoc} = barrel_docdb:get_doc(Registry, LegacyKey),
    {ok, _} = barrel_docdb:put_doc(Registry, LegacyDoc#{
        <<"token_hash">> => crypto:hash(sha256, Legacy)}),
    {ok, _} = barrel_rep:replicate(Registry, Endpoint, push_opts()),
    Key = <<"grant:", Id/binary>>,
    {ok, Grant} = barrel_docdb:get_doc(Registry, Key),
    {ok, Pushed} = barrel_docdb:get_doc(Served, Key),
    ?assertEqual(maps:get(<<"token_hash">>, Grant),
                 maps:get(<<"token_hash">>, Pushed)),
    {ok, #{<<"token_hash">> := LegacyHash}} =
        barrel_docdb:get_doc(Served, LegacyKey),
    ?assertEqual(crypto:hash(sha256, Legacy), LegacyHash),
    ok = barrel_caps:revoke(Token),
    {ok, _} = barrel_rep:replicate(Registry, Endpoint, push_opts()),
    {ok, #{<<"revoked_at">> := Revoked}} = barrel_docdb:get_doc(Served, Key),
    ?assert(Revoked > 0),
    {ok, _} = barrel_rep:replicate(Endpoint, Local, pull_opts()),
    {ok, #{<<"revoked_at">> := Revoked}} = barrel_docdb:get_doc(Local, Key),
    {ok, #{<<"token_hash">> := LegacyHash}} =
        barrel_docdb:get_doc(Local, LegacyKey),
    ok.

%% A field JSON cannot carry crosses as CBOR, both ways; a JSON-safe
%% document still travels as JSON.
t_binary_field_round_trip(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    Raw = <<255, 0, 254, 1>>,
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"bin">>,
                                            <<"raw">> => Raw,
                                            <<"nested">> => [#{<<"k">> => Raw}]}),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"text">>,
                                            <<"v">> => <<"plain">>}),
    ok = meck:new(hackney, [passthrough, no_link]),
    {ok, _} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertEqual(<<"application/cbor">>, put_content_type(<<"bin">>)),
    ?assertEqual(<<"application/json">>, put_content_type(<<"text">>)),
    ok = meck:unload(hackney),
    {ok, #{<<"raw">> := Raw, <<"nested">> := [#{<<"k">> := Raw}]}} =
        barrel_docdb:get_doc(Served, <<"bin">>),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"served_bin">>,
                                             <<"raw">> => Raw}),
    Pulled = <<Local/binary, "_pulled">>,
    {ok, _} = barrel_docdb:create_db(Pulled, #{
        data_dir => filename:join(?config(priv_dir, Config), "local")}),
    try
        {ok, _} = barrel_rep:replicate(Endpoint, Pulled, pull_opts()),
        {ok, #{<<"raw">> := Raw}} = barrel_docdb:get_doc(Pulled, <<"bin">>),
        {ok, #{<<"raw">> := Raw}} =
            barrel_docdb:get_doc(Pulled, <<"served_bin">>),
        {ok, #{<<"v">> := <<"plain">>}} =
            barrel_docdb:get_doc(Pulled, <<"text">>)
    after
        barrel_docdb:delete_db(Pulled)
    end,
    ok.

%% A client that only takes JSON gets a 406 for a document JSON cannot
%% carry; a CBOR client gets it, the version vector as a byte string.
t_json_only_client(Config) ->
    Served = ?config(served, Config),
    #{url := Base} = ?config(endpoint, Config),
    Raw = <<255, 0, 254, 1>>,
    {ok, _} = barrel_server_dbs:ensure(Served),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"raw">>,
                                             <<"raw">> => Raw}),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"text">>,
                                             <<"v">> => <<"plain">>}),
    Get = fun(Id, Accept) ->
        {ok, Status, Headers, Body} =
            hackney:request(get, <<Base/binary, "/_sync/doc/", Id/binary>>,
                            [{<<"accept">>, Accept}], <<>>, [with_body]),
        {Status, proplists:get_value(<<"content-type">>, Headers), Body}
    end,
    {406, <<"application/json">>, _} = Get(<<"raw">>, <<"application/json">>),
    {200, <<"application/cbor">>, Cbor} =
        Get(<<"raw">>, <<"application/cbor">>),
    #{<<"doc">> := #{<<"raw">> := Raw}, <<"vv">> := VV} =
        barrel_docdb_codec_cbor:decode_cbor(Cbor),
    {200, <<"application/json">>, Json} =
        Get(<<"text">>, <<"application/json">>),
    #{<<"vv">> := VVB64} = json:decode(Json),
    {ok, #{vv := VVText}} =
        barrel_docdb:get_doc_for_replication(Served, <<"text">>),
    ?assertEqual(VVText, base64:decode(VVB64)),
    {ok, #{vv := VVRaw}} =
        barrel_docdb:get_doc_for_replication(Served, <<"raw">>),
    ?assertEqual(VVRaw, VV),
    ok.

%% Content type of the PUT that carried a document, from hackney's history.
put_content_type(DocId) ->
    Suffix = <<"/_sync/doc/", DocId/binary>>,
    [CT] = [proplists:get_value(<<"content-type">>, Headers)
            || {_, {hackney, request, [put, Url, Headers | _]}, _}
                   <- meck:history(hackney),
               binary:longest_common_suffix([Url, Suffix])
                   =:= byte_size(Suffix)],
    CT.
