%%% @doc Recovery defects from the 2.0.0 review: writer restarts and
%%% failed databases, stale store handles, compaction filter death,
%%% task credentials and config validation, HTTP request lifetime and
%%% wait_for versions.
-module(barrel_review_recovery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([log/2]).
-export([writer_crash_restarts/1,
         closed_db_stays_closed/1,
         restart_budget_fails_db/1,
         failed_db_deletes/1,
         crash_loop_spares_siblings/1,
         killed_with_attachment_stream_reopens/1,
         compaction_filter_exit_logged/1,
         inline_credentials_rejected/1,
         userinfo_rejected_one_shot/1,
         env_token_survives_resume/1,
         tuning_keys_survive_resume/1,
         invalid_configs_rejected/1,
         corrupted_filter_fails_on_restore/1,
         http_worker_dies_with_caller/1,
         http_stall_before_headers_times_out/1,
         http_stall_mid_body_times_out/1,
         http_upload_stall_times_out/1,
         http_download_stall_times_out/1,
         http_target_reject_closes_source/1,
         wait_for_stale_version_not_acknowledged/1,
         wait_for_delete_acknowledged/1]).

all() ->
    [writer_crash_restarts,
     closed_db_stays_closed,
     restart_budget_fails_db,
     failed_db_deletes,
     crash_loop_spares_siblings,
     killed_with_attachment_stream_reopens,
     compaction_filter_exit_logged,
     inline_credentials_rejected,
     userinfo_rejected_one_shot,
     env_token_survives_resume,
     tuning_keys_survive_resume,
     invalid_configs_rejected,
     corrupted_filter_fails_on_restore,
     http_worker_dies_with_caller,
     http_stall_before_headers_times_out,
     http_stall_mid_body_times_out,
     http_upload_stall_times_out,
     http_download_stall_times_out,
     http_target_reject_closes_source,
     wait_for_stale_version_not_acknowledged,
     wait_for_delete_acknowledged].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_binary(TC),
    Dbs = [Db | [<<Db/binary, S/binary>> || S <- [<<"_t">>, <<"_d">>]]],
    [barrel_docdb:delete_db(D) || D <- Dbs],
    [{db, Db}, {dbs, Dbs} | Config].

end_per_testcase(_TC, Config) ->
    _ = meck:unload(),
    _ = logger:remove_handler(review_capture),
    [application:unset_env(barrel_docdb, K)
     || K <- [sync_auth, sync_signing, sync_ssl]],
    {ok, Tasks} = barrel_rep_tasks:list_tasks(),
    [barrel_rep_tasks:delete_task(Id) || #{id := Id} <- Tasks],
    [barrel_docdb:delete_db(D) || D <- ?config(dbs, Config)],
    ok.

%%====================================================================
%% Writer restarts
%%====================================================================

writer_crash_restarts(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = create(Db),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>, <<"v">> => 1},
                                   #{sync => true}),
    kill(Pid),
    Pid2 = await_restart(Db, Pid),
    ?assertMatch({ok, #{<<"v">> := 1}}, barrel_docdb:get_doc(Db, <<"d">>)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"e">>}),
    ?assertEqual([Db], [N || N <- barrel_docdb:list_dbs(), N =:= Db]),
    ?assertEqual({ok, Pid2}, barrel_docdb:open_db(Db)).

closed_db_stays_closed(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = create(Db),
    ok = barrel_docdb:close_db(Db),
    ?assertNot(is_process_alive(Pid)),
    timer:sleep(200),
    ?assertEqual({error, not_found}, barrel_docdb:open_db(Db)),
    %% a direct stop does not restart either
    {ok, Pid2} = create(Db),
    ok = barrel_db_server:stop(Pid2),
    timer:sleep(200),
    ?assertEqual({error, not_found}, barrel_docdb:open_db(Db)),
    ?assertEqual({error, not_found}, barrel_docdb:reopen_db(Db)).

restart_budget_fails_db(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>}, #{sync => true}),
    fail_db(Db),
    ?assertEqual({error, {db_failed, killed}}, barrel_docdb:open_db(Db)),
    ?assertEqual({error, {db_failed, killed}}, barrel_docdb:get_doc(Db, <<"d">>)),
    ?assertEqual({error, {db_failed, killed}}, barrel_docdb:create_db(Db, #{})),
    %% stays down
    timer:sleep(200),
    ?assertEqual({error, {db_failed, killed}}, barrel_docdb:open_db(Db)),
    {ok, Pid} = barrel_docdb:reopen_db(Db),
    ?assertEqual({ok, Pid}, barrel_docdb:open_db(Db)),
    ?assertMatch({ok, #{<<"id">> := <<"d">>}}, barrel_docdb:get_doc(Db, <<"d">>)),
    %% the reopened database has a fresh budget
    kill(Pid),
    _ = await_restart(Db, Pid).

failed_db_deletes(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = create(Db),
    {ok, #{db_path := Path}} = barrel_db_server:info(Pid),
    fail_db(Db),
    ok = barrel_docdb:delete_db(Db),
    ?assertNot(filelib:is_dir(Path)),
    ?assertEqual({error, not_found}, barrel_docdb:open_db(Db)),
    {ok, _} = create(Db),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Db, <<"d">>)).

crash_loop_spares_siblings(Config) ->
    Db = ?config(db, Config),
    Siblings = [<<Db/binary, "_", (integer_to_binary(I))/binary>>
                || I <- lists:seq(1, 3)],
    try
        Pids = [begin
                    _ = barrel_docdb:delete_db(S),
                    {ok, P} = create(S),
                    P
                end || S <- Siblings],
        {ok, _} = create(Db),
        fail_db(Db),
        %% a second failure cycle after reopening
        {ok, _} = barrel_docdb:reopen_db(Db),
        fail_db(Db),
        ?assertEqual([{ok, P} || P <- Pids],
                     [barrel_docdb:open_db(S) || S <- Siblings]),
        [{ok, _} = barrel_docdb:put_doc(S, #{<<"id">> => <<"x">>})
         || S <- Siblings]
    after
        [barrel_docdb:delete_db(S) || S <- Siblings]
    end.

killed_with_attachment_stream_reopens(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = create(Db),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a">>,
                                          <<"0123456789abcdef">>, #{sync => true}),
    Self = self(),
    %% a caller keeps the old stream (and its store handle) open
    Holder = spawn(fun() ->
        {ok, S} = barrel_docdb:open_attachment_stream(Db, <<"d">>, <<"a">>),
        {ok, _, S1} = barrel_docdb:read_attachment_chunk(S),
        Self ! {holding, self()},
        receive read ->
            Self ! {read, try barrel_docdb:read_attachment_chunk(S1)
                          catch error:badarg -> closed
                          end}
        end
    end),
    receive {holding, Holder} -> ok after 5000 -> ct:fail(no_stream) end,
    kill(Pid),
    _ = await_restart(Db, Pid),
    ?assertEqual({ok, <<"0123456789abcdef">>},
                 barrel_docdb:get_attachment(Db, <<"d">>, <<"a">>)),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"b">>, <<"more">>),
    %% the old handle is closed, not shared with the new instance
    Holder ! read,
    receive {read, R} -> ?assertEqual({error, db_restarted}, R)
    after 5000 -> ct:fail(no_read)
    end.

%%====================================================================
%% Compaction filter
%%====================================================================

compaction_filter_exit_logged(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = create(Db),
    ok = logger:add_handler(review_capture, ?MODULE,
                            #{level => warning, config => #{pid => self()}}),
    {links, Links} = process_info(Pid, links),
    [Filter] = [L || L <- Links, initial_module(L) =:= barrel_compaction_filter],
    kill(Filter),
    receive
        {log, warning, Msg} -> ?assertNotEqual(nomatch, string:find(Msg, "killed"))
    after 5000 ->
        ct:fail(no_warning)
    end,
    ?assert(is_process_alive(Pid)),
    ?assertEqual({ok, Pid}, barrel_docdb:open_db(Db)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>}, #{sync => true}),
    ?assertMatch({ok, _}, barrel_docdb:get_doc(Db, <<"d">>)),
    ok = barrel_docdb:close_db(Db).

%%====================================================================
%% Task credentials
%%====================================================================

inline_credentials_rejected(Config) ->
    Db = ?config(db, Config),
    Url = <<"http://127.0.0.1:1/db/x">>,
    Secrets = [{auth, #{token => <<"t">>}},
               {signing, #{key_id => <<"k">>, priv_key => <<0:256>>}},
               {ssl_options, [{verify, verify_peer}]},
               {headers, [{<<"authorization">>, <<"Bearer t">>}]}],
    Cases = [{K, #{url => Url, K => V}} || {K, V} <- Secrets] ++
            [{userinfo, <<"http://u:p@127.0.0.1:1/db/x">>},
             {userinfo, #{url => <<"http://u@127.0.0.1:1/db/x">>}}],
    lists:foreach(
        fun({Key, Ep}) ->
            Err = {error, {inline_credentials, [Key]}},
            ?assertEqual(Err, barrel_rep_tasks:start_task(
                                #{source => Ep, target => Db})),
            ?assertEqual(Err, barrel_rep_tasks:start_task(
                                #{source => Db, target => Ep})),
            ?assertEqual(Err, barrel_rep_tasks:start_task(
                                #{source => Db, target => Url,
                                  wait_for => [Ep]}))
        end, Cases),
    ?assertEqual({ok, []}, barrel_rep_tasks:list_tasks()).

userinfo_rejected_one_shot(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    ?assertError({invalid_sync_url, userinfo},
                 barrel_rep_transport_http:endpoint(
                   <<"http://u:p@127.0.0.1:1/db/x">>)),
    Http = barrel_rep_transport_http,
    ?assertEqual({error, {invalid_sync_url, userinfo}},
                 barrel_rep:replicate(
                   Db, #{url => <<"http://u:p@127.0.0.1:1/db/x">>},
                   #{target_transport => Http})),
    ?assertEqual({error, {invalid_sync_url, userinfo}},
                 barrel_rep:replicate(
                   <<"http://u:p@127.0.0.1:1/db/x">>, Db,
                   #{source_transport => Http})).

%% A token from sync_auth, keyed by URL then by origin, still reaches
%% the server after pause/resume.
env_token_survives_resume(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    {Stub, Port} = stub(fun empty_changes/4),
    Origin = iolist_to_binary(["http://127.0.0.1:", integer_to_list(Port)]),
    Url = <<Origin/binary, "/db/remote">>,
    lists:foreach(
        fun({EnvKey, Token}) ->
            ok = application:set_env(barrel_docdb, sync_auth,
                                     #{EnvKey => Token,
                                       <<"http://other:1">> => <<"no">>}),
            Expected = <<"Bearer ", Token/binary>>,
            {ok, Id} = barrel_rep_tasks:start_task(
                         #{source => Url, target => Db, mode => continuous,
                           attachments => false}),
            ?assertEqual(Expected, next_auth()),
            ok = barrel_rep_tasks:pause_task(Id),
            flush_requests(),
            ok = barrel_rep_tasks:resume_task(Id),
            ?assertEqual(Expected, next_auth()),
            ok = barrel_rep_tasks:delete_task(Id),
            flush_requests()
        end,
        [{Url, <<"url-token">>}, {Origin, <<"origin-token">>}]),
    %% the URL key wins over the origin key
    ok = application:set_env(barrel_docdb, sync_auth,
                             #{Url => <<"url">>, Origin => <<"origin">>}),
    {ok, _} = barrel_rep_transport_http:get_local_doc(
                barrel_rep_transport_http:endpoint(Url), <<"x">>),
    ?assertEqual(<<"Bearer url">>, next_auth()),
    exit(Stub, kill).

tuning_keys_survive_resume(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    {Stub, Port} = stub(fun empty_changes/4),
    Url = iolist_to_binary(["http://127.0.0.1:", integer_to_list(Port),
                            "/db/remote"]),
    Tuning = #{pool => default, connect_timeout => 1234,
               recv_timeout => 4321, request_timeout => 9876},
    {ok, Id} = barrel_rep_tasks:start_task(
                 #{source => Tuning#{url => Url}, target => Db,
                   mode => continuous, attachments => false}),
    _ = next_auth(),
    ok = barrel_rep_tasks:pause_task(Id),
    ok = meck:new(hackney, [passthrough, no_link]),
    ok = barrel_rep_tasks:resume_task(Id),
    _ = next_auth(),
    {ok, #{config := #{source := Source}}} = barrel_rep_tasks:get_task(Id),
    ?assertEqual(Tuning#{url => Url}, Source),
    Opts = [O || {_, {hackney, request, [_, _, _, _, O]}, _}
                     <- meck:history(hackney)],
    ?assertMatch([_ | _], Opts),
    [begin
         ?assertEqual(4321, proplists:get_value(recv_timeout, O)),
         ?assertEqual(1234, proplists:get_value(connect_timeout, O))
     end || O <- Opts],
    exit(Stub, kill).

%%====================================================================
%% Task config validation
%%====================================================================

invalid_configs_rejected(Config) ->
    Db = ?config(db, Config),
    Manager = whereis(barrel_rep_tasks),
    ?assertEqual({error, invalid_config}, barrel_rep_tasks:start_task([])),
    ?assertEqual({error, invalid_config},
                 gen_server:call(barrel_rep_tasks, {start_task, []})),
    Base = #{source => Db, target => <<Db/binary, "_t">>},
    Bad = [{#{}, missing_source},
           {#{source => Db}, missing_target},
           {Base#{source => 42}, {invalid_endpoint, 42}},
           {Base#{source => <<"Not A Name">>}, {invalid_endpoint, <<"Not A Name">>}},
           {Base#{target => <<"http://h/nope">>}, {invalid_endpoint, <<"http://h/nope">>}},
           {Base#{target => #{url => 42}}, {invalid_endpoint, #{url => 42}}},
           {Base#{mode => forever}, {invalid_option, {mode, forever}}},
           {Base#{direction => sideways}, {invalid_option, {direction, sideways}}},
           {Base#{batch_size => 0}, {invalid_option, {batch_size, 0}}},
           {Base#{att_batch_size => -1}, {invalid_option, {att_batch_size, -1}}},
           {Base#{attachments => yes}, {invalid_option, {attachments, yes}}},
           {Base#{filter => nope}, {invalid_filter, nope}},
           {Base#{wait_for => nope}, {invalid_option, {wait_for, nope}}},
           {Base#{wait_for => [42]}, {invalid_endpoint, 42}}],
    [?assertEqual({error, Reason}, barrel_rep_tasks:start_task(C))
     || {C, Reason} <- Bad],
    ?assertMatch({error, {invalid_query, _}},
                 barrel_rep_tasks:start_task(
                   Base#{filter => #{query => #{where => bogus}}})),
    ?assertEqual(Manager, whereis(barrel_rep_tasks)),
    ?assertEqual({ok, []}, barrel_rep_tasks:list_tasks()).

%% A persisted filter that no longer decodes fails the task: it never
%% runs unfiltered.
corrupted_filter_fails_on_restore(Config) ->
    Db = ?config(db, Config),
    Tgt = <<Db/binary, "_t">>,
    {ok, _} = create(Db),
    {ok, _} = create(Tgt),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"private">>}),
    Manager = whereis(barrel_rep_tasks),
    Id = <<"corruptedfilter">>,
    Now = erlang:system_time(millisecond),
    Doc = #{<<"_type">> => <<"rep_task">>, <<"id">> => Id,
            <<"config">> => #{<<"source">> => Db, <<"target">> => Tgt,
                              <<"filter">> => <<"garbage">>},
            <<"status">> => <<"running">>, <<"last_seq">> => <<"first">>,
            <<"att_seq">> => <<"first">>,
            <<"created_at">> => Now, <<"updated_at">> => Now},
    ok = barrel_docdb:put_local_doc(<<"_replication_tasks">>,
                                    <<"rep_task:", Id/binary>>, Doc),
    %% the periodic check restores it
    ok = wait_until(fun() ->
                        case barrel_rep_tasks:get_task(Id) of
                            {ok, #{status := failed}} -> true;
                            _ -> false
                        end
                    end, 100, 120),
    {ok, #{error := Error}} = barrel_rep_tasks:get_task(Id),
    ?assertNotEqual(nomatch, binary:match(Error, <<"invalid_filter">>)),
    ?assertEqual({error, not_running}, barrel_rep_tasks:get_task_pid(Id)),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Tgt, <<"private">>)),
    ?assertEqual(Manager, whereis(barrel_rep_tasks)).

%%====================================================================
%% HTTP request lifetime
%%====================================================================

http_worker_dies_with_caller(_Config) ->
    {Stub, Port} = stub(fun stall/4),
    E = endpoint(Port, #{}),
    Caller = spawn(fun() -> barrel_rep_transport_http:get_local_doc(E, <<"a">>) end),
    receive {stub_request, _, _, _, _} -> ok after 5000 -> ct:fail(no_request) end,
    ?assertMatch([_], http_workers()),
    kill(Caller),
    receive {stub_closed, _} -> ok after 2000 -> ct:fail(connection_kept) end,
    ?assertEqual([], http_workers()),
    exit(Stub, kill).

http_stall_before_headers_times_out(_Config) ->
    {Stub, Port} = stub(fun stall/4),
    E = endpoint(Port, #{request_timeout => 500}),
    {Time, R} = timer:tc(barrel_rep_transport_http, get_local_doc, [E, <<"a">>]),
    ?assertEqual({error, request_timeout}, R),
    ?assert(Time < 1000000),
    receive {stub_closed, _} -> ok after 2000 -> ct:fail(connection_kept) end,
    ?assertEqual([], http_workers()),
    exit(Stub, kill).

http_stall_mid_body_times_out(_Config) ->
    {Stub, Port} = stub(fun(_M, _P, _H, S) ->
        ok = gen_tcp:send(S, [response_head(200, 100), <<"{\"a\":">>]),
        stall(S)
    end),
    E = endpoint(Port, #{request_timeout => 500}),
    {Time, R} = timer:tc(barrel_rep_transport_http, get_local_doc, [E, <<"a">>]),
    ?assertEqual({error, request_timeout}, R),
    ?assert(Time < 1000000),
    receive {stub_closed, _} -> ok after 2000 -> ct:fail(connection_kept) end,
    ?assertEqual([], http_workers()),
    exit(Stub, kill).

%% The upload reads every chunk, then the peer never answers.
http_upload_stall_times_out(_Config) ->
    {Stub, Port} = stub(fun stall/4),
    E = endpoint(Port, #{request_timeout => 500}),
    Meta = #{content_type => <<"text/plain">>, digest => <<"d">>,
             origin_hlc => barrel_hlc:get_hlc()},
    Read = chunks([<<"aaaa">>, <<"bbbb">>]),
    {Time, R} = timer:tc(barrel_rep_transport_http, put_attachment,
                         [E, <<"d">>, <<"a">>, Meta, Read]),
    ?assertEqual({error, request_timeout}, R),
    ?assert(Time < 1000000),
    receive {stub_closed, _} -> ok after 2000 -> ct:fail(connection_kept) end,
    ?assertEqual([], http_workers()),
    exit(Stub, kill).

%% Headers arrive, the body stalls: the stream deadline covers chunks.
http_download_stall_times_out(_Config) ->
    {Stub, Port} = stub(fun(_M, _P, _H, S) ->
        ok = gen_tcp:send(S, [response_head(200, 100), <<"abcd">>]),
        stall(S)
    end),
    E = endpoint(Port, #{request_timeout => 500}),
    T0 = erlang:monotonic_time(millisecond),
    {ok, _Info, Read} = barrel_rep_transport_http:get_attachment_stream(
                          E, <<"d">>, <<"a">>),
    ?assertEqual({error, request_timeout}, read_all(Read)),
    ?assert(erlang:monotonic_time(millisecond) - T0 < 1000),
    receive {stub_closed, _} -> ok after 2000 -> ct:fail(connection_kept) end,
    ?assertEqual([], http_workers()),
    exit(Stub, kill).

%% A read-only target refuses before reading a chunk: the source
%% download is closed, not left open.
http_target_reject_closes_source(Config) ->
    Db = ?config(db, Config),
    {ok, _} = create(Db),
    ok = barrel_docdb:close_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, #{read_only => true}),
    Seq = base64:encode(barrel_hlc:encode(barrel_hlc:get_hlc())),
    Entry = #{<<"id">> => <<"d">>, <<"name">> => <<"a">>, <<"seq">> => Seq,
              <<"origin">> => Seq, <<"op">> => <<"put">>,
              <<"digest">> => <<"sha256-x">>, <<"length">> => 100,
              <<"content_type">> => <<"text/plain">>},
    Self = self(),
    {Stub, Port} = stub(fun(get, <<"/db/remote/_sync/att_changes?since=first", _/binary>>, _, S) ->
                                json(S, #{<<"changes">> => [Entry],
                                          <<"last_seq">> => Seq});
                           (get, <<"/db/remote/_sync/att_changes", _/binary>>, _, S) ->
                                json(S, #{<<"changes">> => [],
                                          <<"last_seq">> => Seq});
                           (get, <<"/db/remote/_sync/att/", _/binary>>, _, S) ->
                                Self ! download_started,
                                ok = gen_tcp:send(S, [response_head(200, 100),
                                                      <<"abcd">>]),
                                stall(S)
                        end),
    E = endpoint(Port, #{}),
    {ok, Stats} = barrel_rep_att:sync_from(E, Db, barrel_rep_transport_http,
                                           barrel_rep_transport_local, first,
                                           fun(_) -> ok end, #{}),
    ?assertEqual(1, maps:get(att_write_failures, Stats)),
    receive download_started -> ok after 0 -> ct:fail(no_download) end,
    receive {stub_closed, _} -> ok after 2000 -> ct:fail(source_kept_open) end,
    ?assertEqual([], http_workers()),
    exit(Stub, kill).

%%====================================================================
%% wait_for versions
%%====================================================================

wait_for_stale_version_not_acknowledged(Config) ->
    [S, T, D] = ?config(dbs, Config),
    [{ok, _} = create(X) || X <- [S, T, D]],
    {ok, _} = barrel_docdb:put_doc(S, #{<<"id">> => <<"d">>, <<"v">> => 1}),
    {ok, _} = barrel_rep:replicate(S, D),
    {ok, Doc} = barrel_docdb:get_doc(S, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(S, Doc#{<<"v">> => 2}),
    {ok, Id} = barrel_rep_tasks:start_task(#{source => S, target => T,
                                             wait_for => [D],
                                             attachments => false}),
    %% the downstream still has v1
    timer:sleep(1500),
    ?assertMatch({ok, #{status := running}}, barrel_rep_tasks:get_task(Id)),
    {ok, _} = barrel_rep:replicate(S, D),
    ok = wait_until(task_status(Id, completed), 100, 60).

wait_for_delete_acknowledged(Config) ->
    [S, T, D] = ?config(dbs, Config),
    [{ok, _} = create(X) || X <- [S, T, D]],
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(S, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_rep:replicate(S, D),
    {ok, _} = barrel_docdb:delete_doc(S, <<"d">>, #{rev => Rev}),
    {ok, Id} = barrel_rep_tasks:start_task(#{source => S, target => T,
                                             wait_for => [D],
                                             attachments => false}),
    timer:sleep(1500),
    ?assertMatch({ok, #{status := running}}, barrel_rep_tasks:get_task(Id)),
    %% the stub reaches the downstream: acknowledged
    {ok, _} = barrel_rep:replicate(S, D),
    ok = wait_until(task_status(Id, completed), 100, 60).

%% logger handler
log(#{level := Level, msg := Msg}, #{config := #{pid := Pid}}) ->
    Text = case Msg of
        {report, R} -> io_lib:format("~p", [R]);
        {string, S} -> S;
        {Fmt, Args} -> io_lib:format(Fmt, Args)
    end,
    Pid ! {log, Level, lists:flatten(Text)},
    ok.

%%====================================================================
%% Helpers
%%====================================================================

create(Db) ->
    barrel_docdb:create_db(Db, #{att_opts => #{chunk_threshold => 4,
                                               chunk_size => 4}}).

kill(Pid) ->
    MRef = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', MRef, process, Pid, _} -> ok end.

await_restart(Db, Old) ->
    await_restart(Db, Old, 100).

await_restart(_Db, _Old, 0) ->
    ct:fail(no_restart);
await_restart(Db, Old, N) ->
    case barrel_docdb:open_db(Db) of
        {ok, Pid} when Pid =/= Old -> Pid;
        _ -> timer:sleep(20), await_restart(Db, Old, N - 1)
    end.

%% Kill the writer past its restart budget (3 in 60 s).
fail_db(Db) ->
    fail_db(Db, 4).

fail_db(Db, 0) ->
    await_failed(Db, 100);
fail_db(Db, N) ->
    {ok, Pid} = await_open(Db, 100),
    kill(Pid),
    fail_db(Db, N - 1).

await_open(Db, 0) ->
    ct:fail({not_open, Db});
await_open(Db, N) ->
    case barrel_docdb:open_db(Db) of
        {ok, _} = Ok -> Ok;
        _ -> timer:sleep(20), await_open(Db, N - 1)
    end.

await_failed(Db, 0) ->
    ct:fail({not_failed, Db, barrel_docdb:open_db(Db)});
await_failed(Db, N) ->
    case barrel_docdb:open_db(Db) of
        {error, {db_failed, _}} -> ok;
        _ -> timer:sleep(20), await_failed(Db, N - 1)
    end.

initial_module(P) ->
    case process_info(P, dictionary) of
        {dictionary, D} ->
            case proplists:get_value('$initial_call', D) of
                {M, _, _} -> M;
                _ -> none
            end;
        _ ->
            none
    end.

wait_until(_Fun, _Interval, 0) ->
    ct:fail(condition_never_met);
wait_until(Fun, Interval, N) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(Interval), wait_until(Fun, Interval, N - 1)
    end.

task_status(Id, Status) ->
    fun() ->
        case barrel_rep_tasks:get_task(Id) of
            {ok, #{status := Status}} -> true;
            _ -> false
        end
    end.

endpoint(Port, Opts) ->
    barrel_rep_transport_http:endpoint(
      Opts#{url => iolist_to_binary(["http://127.0.0.1:",
                                     integer_to_list(Port), "/db/remote"])}).

%% Transport request workers and download owners (not their callers).
http_workers() ->
    [P || P <- processes(),
          case process_info(P, current_stacktrace) of
              {current_stacktrace, Frames} ->
                  lists:any(fun({barrel_rep_transport_http, F, _, _}) ->
                                    lists:member(F, [guarded, download,
                                                     download_loop])
                                        orelse lists:prefix(
                                                 "-bounded", atom_to_list(F));
                               (_) ->
                                    false
                            end, Frames);
              undefined ->
                  false
          end].

chunks([]) -> fun() -> eof end;
chunks([C | Rest]) -> fun() -> {ok, C, chunks(Rest)} end.

read_all(Read) ->
    case Read() of
        {ok, _Chunk, Next} -> read_all(Next);
        Other -> Other
    end.

next_auth() ->
    receive
        {stub_request, _M, _P, Headers, _Conn} ->
            proplists:get_value(<<"authorization">>, Headers)
    after 5000 ->
        ct:fail(no_request)
    end.

flush_requests() ->
    receive {stub_request, _, _, _, _} -> flush_requests()
    after 200 -> ok
    end.

%% A minimal HTTP/1.1 server: `Handler(Method, Path, Headers, Socket)'
%% answers each request; the test gets {stub_request, ...} for every
%% request and {stub_closed, Conn} when a client closes a connection.
stub(Handler) ->
    Parent = self(),
    Pid = spawn(fun() ->
        {ok, L} = gen_tcp:listen(0, [binary, {active, false},
                                     {packet, http_bin}, {reuseaddr, true}]),
        {ok, Port} = inet:port(L),
        Parent ! {stub_port, self(), Port},
        accept_loop(L, Handler, Parent)
    end),
    receive {stub_port, Pid, Port} -> {Pid, Port} end.

accept_loop(L, Handler, Parent) ->
    {ok, S} = gen_tcp:accept(L),
    C = spawn(fun() ->
        receive go -> ok end,
        conn_loop(S, Handler, Parent)
    end),
    ok = gen_tcp:controlling_process(S, C),
    C ! go,
    accept_loop(L, Handler, Parent).

conn_loop(S, Handler, Parent) ->
    case read_request(S) of
        {ok, Method, Path, Headers} ->
            Parent ! {stub_request, Method, Path, Headers, self()},
            case Handler(Method, Path, Headers, S) of
                closed -> Parent ! {stub_closed, self()};
                _ -> conn_loop(S, Handler, Parent)
            end;
        {error, _} ->
            Parent ! {stub_closed, self()}
    end.

read_request(S) ->
    case gen_tcp:recv(S, 0) of
        {ok, {http_request, M, {abs_path, P}, _}} ->
            read_headers(S, method(M), P, []);
        {ok, Other} ->
            {error, Other};
        {error, _} = E ->
            E
    end.

read_headers(S, M, P, Acc) ->
    case gen_tcp:recv(S, 0) of
        {ok, {http_header, _, Name, _, V}} ->
            read_headers(S, M, P, [{header_name(Name), V} | Acc]);
        {ok, http_eoh} ->
            case proplists:get_value(<<"content-length">>, Acc) of
                undefined ->
                    {ok, M, P, Acc};
                Len ->
                    ok = inet:setopts(S, [{packet, raw}]),
                    {ok, _} = recv_body(S, binary_to_integer(Len)),
                    ok = inet:setopts(S, [{packet, http_bin}]),
                    {ok, M, P, Acc}
            end;
        {error, _} = E ->
            E
    end.

recv_body(_S, 0) -> {ok, <<>>};
recv_body(S, Len) -> gen_tcp:recv(S, Len).

method(M) when is_atom(M) -> list_to_atom(string:lowercase(atom_to_list(M)));
method(M) -> binary_to_atom(string:lowercase(M)).

header_name(N) when is_atom(N) -> string:lowercase(atom_to_binary(N));
header_name(N) -> string:lowercase(N).

response_head(Status, Length) ->
    [<<"HTTP/1.1 ">>, integer_to_binary(Status), <<" OK\r\n">>,
     <<"content-type: application/json\r\n">>,
     <<"content-length: ">>, integer_to_binary(Length), <<"\r\n\r\n">>].

json(S, Term) ->
    Body = iolist_to_binary(json:encode(Term)),
    ok = gen_tcp:send(S, [response_head(200, byte_size(Body)), Body]).

empty_changes(_M, _P, _H, S) ->
    json(S, #{<<"changes">> => [], <<"last_seq">> => <<"first">>}).

stall(_M, _P, _H, S) ->
    stall(S).

%% Swallow whatever comes until the client closes.
stall(S) ->
    ok = inet:setopts(S, [{packet, raw}]),
    case gen_tcp:recv(S, 0) of
        {ok, _} -> stall(S);
        {error, _} -> closed
    end.
