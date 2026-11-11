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
    t_client_vector_over_http/1,
    t_sibling_vectors_over_http/1,
    t_pull_over_http/1,
    t_bidirectional_convergence/1,
    t_filtered_pull/1,
    t_checkpoint_reuse/1,
    t_att_sync_rides_along/1,
    t_continuous_push_over_http/1,
    t_continuous_pull_over_http/1,
    t_registry_grants_over_http/1,
    t_binary_field_round_trip/1,
    t_json_only_client/1,
    t_unknown_db/1,
    t_att_filters_over_http/1,
    t_att_filter_errors_over_http/1,
    t_att_doc_enters_scope_over_http/1,
    t_att_empty_page_over_http/1,
    t_att_touch_over_http/1,
    t_reconcile_push_over_http/1,
    t_reconcile_pull_over_http/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_push_over_http, t_pull_over_http, t_bidirectional_convergence,
     t_filtered_pull, t_checkpoint_reuse, t_att_sync_rides_along,
     t_continuous_push_over_http, t_continuous_pull_over_http,
     t_registry_grants_over_http, t_binary_field_round_trip,
     t_json_only_client, t_unknown_db, t_att_filters_over_http,
     t_att_filter_errors_over_http, t_att_doc_enters_scope_over_http,
     t_att_empty_page_over_http, t_att_touch_over_http,
     t_reconcile_push_over_http, t_reconcile_pull_over_http,
     t_client_vector_over_http, t_sibling_vectors_over_http].

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
    %% replication never creates the served database
    {ok, _} = barrel_server_dbs:ensure(list_to_binary(Served)),
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

%% A client-supplied vector crosses _sync/state both ways; a computed
%% one stays home.
t_client_vector_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"up">>,
                                            <<"_embedding">> => [0.5, 0.25]}),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"down">>,
                                             <<"_embedding">> => [0.75, 1.0]}),
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"c">>}),
    ok = barrel_docdb:set_doc_embedding(Local, <<"c">>, Rev, [1.0, 2.0]),
    {ok, _} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    {ok, _} = barrel_rep:replicate(Endpoint, Local, pull_opts()),
    ?assertEqual(#{<<"vector">> => [0.5, 0.25], <<"source">> => <<"client">>},
                 embedding(Served, <<"up">>)),
    ?assertEqual(#{<<"vector">> => [0.75, 1.0], <<"source">> => <<"client">>},
                 embedding(Local, <<"down">>)),
    ?assertEqual(undefined, embedding(Served, <<"c">>)),
    ok.

%% A version that loses on the server keeps its vector there: resolving
%% to it and pulling back gives the local side the same vector, and the
%% past version reads with its vector over REST.
t_sibling_vectors_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    #{url := Base} = Endpoint,
    {ok, #{<<"rev">> := RL}} = barrel_docdb:put_doc(
        Local, #{<<"id">> => <<"s">>, <<"v">> => <<"local">>,
                 <<"_embedding">> => [0.5, 0.25]}),
    timer:sleep(2),
    {ok, #{<<"rev">> := RS}} = barrel_docdb:put_doc(
        Served, #{<<"id">> => <<"s">>, <<"v">> => <<"served">>,
                  <<"_embedding">> => [0.75, 1.0]}),
    {ok, _} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    {ok, #{vectors := V}} = barrel_docdb:get_doc_state(Served, <<"s">>),
    ?assertEqual(#{RL => [0.5, 0.25], RS => [0.75, 1.0]}, V),
    {ok, _} = barrel_docdb:resolve_conflict(Served, <<"s">>, RS, {choose, RL}),
    {ok, _} = barrel_rep:replicate(Endpoint, Local, pull_opts()),
    ?assertEqual(#{<<"vector">> => [0.5, 0.25], <<"source">> => <<"client">>},
                 embedding(Local, <<"s">>)),
    Url = <<Base/binary, "/doc/s/_versions/", RS/binary, "?include_embedding=true">>,
    {ok, 200, _, Json} = hackney:request(get, Url, [], <<>>, [with_body]),
    #{<<"v">> := <<"served">>, <<"_embedding">> := #{<<"vector">> := B64}} =
        json:decode(Json),
    ?assertEqual([0.75, 1.0], barrel_doc:decode_embedding(base64:decode(B64))),
    ok.

embedding(Db, Id) ->
    {ok, Doc} = barrel_docdb:get_doc(Db, Id, #{include_embedding => true}),
    maps:get(<<"_embedding">>, Doc, undefined).

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
    %% current servers take whole document states, always as CBOR
    ?assertEqual(<<"application/cbor">>, put_content_type(<<"bin">>)),
    ?assertEqual(<<"application/cbor">>, put_content_type(<<"text">>)),
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

%% The sync routes answer 404 for a database that does not exist and
%% create nothing; a replication to it fails instead.
t_unknown_db(Config) ->
    Local = ?config(local, Config),
    Name = <<"t_unknown_db_nowhere">>,
    Url = list_to_binary(?config(base, Config) ++ "/db/" ++ binary_to_list(Name)),
    {ok, 404, _, Body} =
        hackney:request(post, <<Url/binary, "/_sync/changes">>,
                        [{<<"content-type">>, <<"application/json">>}],
                        <<"{}">>, [with_body]),
    ?assertEqual(#{<<"error">> => <<"not_found">>}, json:decode(Body)),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"a">>}),
    Endpoint = barrel_rep_transport_http:endpoint(Url),
    {error, _} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertNot(barrel_docdb:db_exists(Name, #{})),
    ?assertEqual({error, not_found}, barrel_docdb:db_pid(Name)),
    ok.

%% Content type of the PUT that carried a document, from hackney's history.
put_content_type(DocId) ->
    Suffix = <<"/_sync/state/", DocId/binary>>,
    [CT] = [proplists:get_value(<<"content-type">>, Headers)
            || {_, {hackney, request, [put, Url, Headers | _]}, _}
                   <- meck:history(hackney),
               binary:longest_common_suffix([Url, Suffix])
                   =:= byte_size(Suffix)],
    CT.

%%====================================================================
%% Attachment filters and origins over the wire
%%====================================================================

-define(PUBLIC_QUERY, #{where => [{path, [<<"type">>], <<"public">>}]}).

with_channels(Served) ->
    ok = barrel_server_dbs:close(Served),
    {ok, _} = barrel_server_dbs:ensure(
                Served, #{docdb => #{channels =>
                                         #{<<"public">> => [<<"type/public">>]}}}),
    ok.

seed_scoped(Served) ->
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"pub">>,
                                             <<"type">> => <<"public">>}),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"priv">>,
                                             <<"type">> => <<"private">>}),
    {ok, _} = barrel_docdb:put_attachment(Served, <<"pub">>, <<"a">>,
                                          <<"public">>),
    {ok, _} = barrel_docdb:put_attachment(Served, <<"priv">>, <<"a">>,
                                          <<"private">>),
    ok.

%% The served source filters its attachment feed: path, query, channel.
t_att_filters_over_http(Config) ->
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    ok = with_channels(Served),
    ok = seed_scoped(Served),
    Dir = filename:join(?config(priv_dir, Config), "local"),
    lists:foreach(
      fun({N, Filter}) ->
          Local = <<"att_filters_http_", (integer_to_binary(N))/binary>>,
          _ = barrel_docdb:delete_db(Local),
          {ok, _} = barrel_docdb:create_db(Local, #{data_dir => Dir}),
          try
              {ok, R} = barrel_rep:replicate(Endpoint, Local,
                                             (pull_opts())#{filter => Filter}),
              ?assertMatch(#{docs_written := 1}, R),
              ?assertEqual({ok, <<"public">>},
                           barrel_docdb:get_attachment(Local, <<"pub">>, <<"a">>)),
              ?assertEqual({error, not_found},
                           barrel_docdb:get_attachment(Local, <<"priv">>, <<"a">>))
          after
              _ = barrel_docdb:delete_db(Local)
          end
      end,
      [{1, #{paths => [<<"type/public">>]}},
       {2, #{query => ?PUBLIC_QUERY}},
       {3, #{channel => <<"public">>}}]),
    ok.

t_att_filter_errors_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    ok = seed_scoped(Served),
    ?assertMatch({error, _},
                 barrel_rep:replicate(Endpoint, Local,
                                      (pull_opts())#{filter =>
                                                         #{channel => <<"nope">>}})),
    %% the attachment phase alone stops on the server's refusal
    ?assertMatch({error, {http, 400, _}},
                 barrel_rep_att:sync_from(
                   Endpoint, Local, barrel_rep_transport_http,
                   barrel_rep_transport_local, first, fun(_) -> ok end,
                   #{filter => #{channel => <<"nope">>}})),
    %% a filter evaluation error stops it too
    ok = meck:new(barrel_query, [passthrough]),
    ok = meck:expect(barrel_query, match, fun(_, _) -> error(boom) end),
    ?assertMatch({error, {http, 400, _}},
                 barrel_rep_att:sync_from(
                   Endpoint, Local, barrel_rep_transport_http,
                   barrel_rep_transport_local, first, fun(_) -> ok end,
                   #{filter => #{query => ?PUBLIC_QUERY}})),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Local, <<"priv">>, <<"a">>)),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Local, <<"pub">>, <<"a">>)),
    ok.

t_att_doc_enters_scope_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    Opts = (pull_opts())#{filter => #{query => ?PUBLIC_QUERY}},
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"d">>,
                                             <<"type">> => <<"private">>}),
    {ok, _} = barrel_docdb:put_attachment(Served, <<"d">>, <<"a">>, <<"kept">>),
    {ok, _} = barrel_rep:replicate(Endpoint, Local, Opts),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Local, <<"d">>, <<"a">>)),
    {ok, Doc} = barrel_docdb:get_doc(Served, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Served, Doc#{<<"type">> => <<"public">>}),
    {ok, _} = barrel_rep:replicate(Endpoint, Local, Opts),
    ?assertEqual({ok, <<"kept">>},
                 barrel_docdb:get_attachment(Local, <<"d">>, <<"a">>)),
    ok.

t_att_empty_page_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    Opts = (pull_opts())#{filter => #{query => ?PUBLIC_QUERY},
                          att_batch_size => 2},
    [{ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => Id,
                                              <<"type">> => T})
     || {Id, T} <- [{<<"p1">>, <<"private">>}, {<<"p2">>, <<"private">>},
                    {<<"pub">>, <<"public">>}]],
    {ok, _} = barrel_rep:replicate(Endpoint, Local, Opts),
    [{ok, _} = barrel_docdb:put_attachment(Served, Id, <<"a">>, Id)
     || Id <- [<<"p1">>, <<"p2">>, <<"pub">>]],
    {ok, R} = barrel_rep:replicate(Endpoint, Local, Opts),
    ?assertMatch(#{atts_written := 1}, maps:get(att_sync, R)),
    ?assertEqual({ok, <<"pub">>},
                 barrel_docdb:get_attachment(Local, <<"pub">>, <<"a">>)),
    ?assertEqual({error, not_found},
                 barrel_docdb:get_attachment(Local, <<"p1">>, <<"a">>)),
    ok.

%% Pushed over the wire: same bytes, newer origin, touched not resent;
%% an older delete then loses on the served side.
t_att_touch_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    H1 = barrel_hlc:new_hlc(),
    H2 = barrel_hlc:new_hlc(),
    H3 = barrel_hlc:new_hlc(),
    {ok, _} = barrel_docdb:put_attachment(Served, <<"d">>, <<"a">>, <<"same">>,
                                          #{origin_hlc => H1}),
    {ok, _} = barrel_docdb:put_attachment(Local, <<"d">>, <<"a">>, <<"same">>,
                                          #{origin_hlc => H3}),
    {ok, R} = barrel_rep:replicate(Local, Endpoint, push_opts()),
    ?assertMatch(#{atts_touched := 1, atts_written := 0}, maps:get(att_sync, R)),
    ok = barrel_docdb:delete_attachment(Served, <<"d">>, <<"a">>,
                                        #{origin_hlc => H2}),
    ?assertEqual({ok, <<"same">>},
                 barrel_docdb:get_attachment(Served, <<"d">>, <<"a">>)),
    ok.

%% A served copy damaged by 2.0 (a live body stored as #{}, an
%% attachment never sent) is repaired from the healthy local one.
t_reconcile_push_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Local, #{<<"id">> => <<"b">>,
                                            <<"v">> => 1}),
    {ok, _} = barrel_rep:replicate(Local, Endpoint,
                                   (push_opts())#{attachments => false}),
    {ok, _} = barrel_docdb:put_attachment(Local, <<"b">>, <<"f">>, <<"blob">>),
    empty_body(Served, <<"b">>),
    {ok, R} = barrel_rep:reconcile(Local, Endpoint, push_opts()),
    ?assertMatch(#{docs_checked := 1, bodies_repaired := 1, atts_resent := 1,
                   doc_failures := [], att_failures := []}, R),
    ?assertMatch({ok, #{<<"v">> := 1}}, barrel_docdb:get_doc(Served, <<"b">>)),
    ?assertEqual({ok, <<"blob">>},
                 barrel_docdb:get_attachment(Served, <<"b">>, <<"f">>)),
    ok.

t_reconcile_pull_over_http(Config) ->
    Local = ?config(local, Config),
    Served = ?config(served, Config),
    Endpoint = ?config(endpoint, Config),
    {ok, _} = barrel_docdb:put_doc(Served, #{<<"id">> => <<"p">>,
                                             <<"v">> => 2}),
    {ok, _} = barrel_rep:replicate(Endpoint, Local, pull_opts()),
    empty_body(Local, <<"p">>),
    {ok, #{bodies_repaired := 1}} =
        barrel_rep:reconcile(Endpoint, Local, pull_opts()),
    ?assertMatch({ok, #{<<"v">> := 2}}, barrel_docdb:get_doc(Local, <<"p">>)),
    ok.

%% What the 2.0 missing-body bug left: a live winner stored as #{}.
empty_body(Db, DocId) ->
    Ks = barrel_keyspace:resolve(Db),
    StoreRef = persistent_term:get({barrel_store, Db}),
    ok = barrel_store_rocksdb:write_batch(
           StoreRef, [{body_put, barrel_store_keys:doc_body(Ks, DocId),
                       barrel_docdb_codec_cbor:encode_cbor(#{})}], #{}),
    {ok, #{<<"id">> := DocId} = Doc} = barrel_docdb:get_doc(Db, DocId),
    ?assertEqual([], [K || K <- maps:keys(Doc), binary:first(K) =/= $_,
                           K =/= <<"id">>]).
