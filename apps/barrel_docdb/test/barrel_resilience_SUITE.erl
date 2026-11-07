-module(barrel_resilience_SUITE).
-include_lib("stdlib/include/assert.hrl").
-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([writer_crash_recovers/1, filter_crash_recovers/1,
         close_stays_closed/1, both_task_attachment_positions/1,
         missing_replication_body_rejected/1, invalid_task_keeps_manager_alive/1,
         http_request_lifetime/1]).

all() -> [writer_crash_recovers, filter_crash_recovers, close_stays_closed,
          both_task_attachment_positions, missing_replication_body_rejected,
          invalid_task_keeps_manager_alive, http_request_lifetime].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.
end_per_suite(_) -> ok.
init_per_testcase(TC, Config) ->
    Name = atom_to_binary(TC),
    Dir = proplists:get_value(priv_dir, Config),
    {ok, Pid} = barrel_docdb:create_db(Name, #{data_dir => Dir}),
    [{db, Name}, {pid, Pid} | Config].
end_per_testcase(_, Config) ->
    barrel_docdb:delete_db(proplists:get_value(db, Config)),
    ok.

writer_crash_recovers(Config) ->
    Name = proplists:get_value(db, Config), Pid = proplists:get_value(pid, Config),
    {ok, _} = barrel_docdb:put_doc(Name, #{<<"id">> => <<"d">>, <<"v">> => 1},
                                   #{sync => true}),
    {ok, _} = barrel_docdb:put_attachment(Name, <<"d">>, <<"f">>, <<"bytes">>,
                                          #{sync => true}),
    exit(Pid, kill),
    New = await_reopen(Name, Pid, 100),
    ?assert(is_pid(New)),
    ?assertMatch({ok, #{<<"v">> := 1}}, barrel_docdb:get_doc(Name, <<"d">>)),
    ?assertEqual({ok, <<"bytes">>}, barrel_docdb:get_attachment(Name, <<"d">>, <<"f">>)),
    ?assertMatch({ok, _}, barrel_docdb:put_doc(Name, #{<<"id">> => <<"after">>})).

filter_crash_recovers(Config) ->
    Pid = proplists:get_value(pid, Config), Name = proplists:get_value(db, Config),
    {links, Links} = process_info(Pid, links),
    [Filter] = [P || P <- Links, initial_module(P) =:= barrel_compaction_filter],
    exit(Filter, kill),
    ?assert(is_pid(await_reopen(Name, Pid, 100))),
    ?assertMatch({ok, _}, barrel_docdb:put_doc(Name, #{<<"id">> => <<"d">>})).

initial_module(Pid) ->
    case process_info(Pid, dictionary) of
        {dictionary, Dict} ->
            case proplists:get_value('$initial_call', Dict) of
                {M, _, _} -> M;
                _ -> undefined
            end;
        _ -> undefined
    end.

await_reopen(_Name, _Old, 0) -> ct:fail(database_not_restarted);
await_reopen(Name, Old, N) ->
    case barrel_docdb:open_db(Name) of
        {ok, Pid} when Pid =/= Old -> Pid;
        _ -> timer:sleep(20), await_reopen(Name, Old, N - 1)
    end.

close_stays_closed(Config) ->
    Name = proplists:get_value(db, Config),
    ok = barrel_docdb:close_db(Name),
    timer:sleep(100),
    ?assertEqual({error, not_found}, barrel_docdb:open_db(Name)).

both_task_attachment_positions(Config) ->
    Source = proplists:get_value(db, Config), Target = <<"both_att_target">>,
    {ok, _} = barrel_docdb:create_db(Target, #{data_dir => proplists:get_value(priv_dir, Config)}),
    %% Target's row predates source's: sharing source's seq would skip it.
    {ok, _} = barrel_docdb:put_attachment(Target, <<"target">>, <<"f">>, <<"target">>),
    {ok, _} = barrel_docdb:put_attachment(Source, <<"source">>, <<"f">>, <<"source">>),
    {ok, Task} = barrel_rep_tasks:start_task(#{source => Source, target => Target,
                                              direction => both}),
    try
        await_task(Task, 200),
        ?assertEqual({ok, <<"target">>}, barrel_docdb:get_attachment(Source, <<"target">>, <<"f">>)),
        ?assertEqual({ok, <<"source">>}, barrel_docdb:get_attachment(Target, <<"source">>, <<"f">>)),
        {ok, Doc} = barrel_docdb:get_local_doc(<<"_replication_tasks">>, <<"rep_task:", Task/binary>>),
        ?assert(maps:is_key(<<"pull_att_seq">>, Doc))
    after
        barrel_rep_tasks:delete_task(Task),
        barrel_docdb:delete_db(Target)
    end.

await_task(_, 0) -> ct:fail(task_not_completed);
await_task(Task, N) ->
    case barrel_rep_tasks:get_task(Task) of
        {ok, #{status := completed}} -> ok;
        {ok, #{status := failed} = Failed} -> ct:fail(Failed);
        _ -> timer:sleep(20), await_task(Task, N - 1)
    end.

missing_replication_body_rejected(Config) ->
    Source = proplists:get_value(db, Config), Target = <<"missing_body_target">>,
    {ok, _} = barrel_docdb:create_db(Target, #{data_dir => proplists:get_value(priv_dir, Config)}),
    try
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => <<"d">>, <<"v">> => <<"valuable">>}),
        {ok, #{state := State, bodies := Bodies}} = barrel_docdb:get_doc_state(Source, <<"d">>),
        ?assertMatch({error, {invalid_state, _}},
                     barrel_docdb:put_doc_state(Target, <<"d">>, State, #{})),
        ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, <<"d">>)),
        ?assertMatch({ok, _, _}, barrel_docdb:put_doc_state(Target, <<"d">>, State, Bodies)),
        ?assertMatch({ok, #{<<"v">> := <<"valuable">>}}, barrel_docdb:get_doc(Target, <<"d">>))
    after barrel_docdb:delete_db(Target)
    end.

invalid_task_keeps_manager_alive(Config) ->
    Name = proplists:get_value(db, Config), Manager = whereis(barrel_rep_tasks),
    Base = #{source => Name, target => Name},
    lists:foreach(fun(Bad) ->
        ?assertEqual({error, invalid_config}, barrel_rep_tasks:start_task(Bad)),
        ?assertEqual(Manager, whereis(barrel_rep_tasks))
    end, [[], Base#{mode => nonsense}, Base#{direction => sideways},
          Base#{batch_size => 0}, Base#{att_batch_size => -1},
          Base#{filter => bad}, Base#{attachments => bad}, Base#{wait_for => bad}]),
    lists:foreach(fun(Option) ->
        Endpoint = #{url => <<"https://host/db/x">>, Option => #{}},
        ?assertEqual({error, task_security_requires_env},
                     barrel_rep_tasks:start_task(Base#{target => Endpoint}))
    end, [auth, signing, ssl_options]).

http_request_lifetime(_) ->
    Tester = self(),
    ok = meck:new(hackney, [no_link]),
    ok = meck:expect(hackney, request, fun(_, _, _, _, _) ->
        Tester ! {http_worker, self()},
        receive never -> ok end
    end),
    Endpoint = barrel_rep_transport_http:endpoint(<<"https://host/db/x">>),
    try
        Caller = spawn(fun() -> barrel_rep_transport_http:get_local_doc(Endpoint, <<"d">>) end),
        Worker = receive {http_worker, P} -> P after 1000 -> ct:fail(no_worker) end,
        Ref = monitor(process, Worker),
        exit(Caller, kill),
        receive {'DOWN', Ref, process, Worker, _} -> ok
        after 1000 -> ct:fail(orphaned_http_request)
        end,
        ?assertEqual({error, {transport, request_timeout}},
                     barrel_rep_transport_http:get_local_doc(Endpoint#{request_timeout => 20}, <<"d">>)),
        Timed = receive {http_worker, P2} -> P2 after 1000 -> ct:fail(no_timed_worker) end,
        TimedRef = monitor(process, Timed),
        receive {'DOWN', TimedRef, process, Timed, _} -> ok
        after 1000 -> ct:fail(timed_request_alive)
        end
    after meck:unload(hackney)
    end.
