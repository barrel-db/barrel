%%%-------------------------------------------------------------------
%%% @doc The database writer: what it does per document, and what it
%%% leaves to the caller and to the subscription managers.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_writer_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([no_subscriber_no_notify/1,
         subscribers_notified_per_group/1]).

all() ->
    [no_subscriber_no_notify,
     subscribers_notified_per_group].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_writer_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_binary(TC, utf8),
    {ok, Pid} = barrel_docdb:create_db(Db, #{data_dir => ?config(dir, Config)}),
    [{db, Db}, {pid, Pid} | Config].

end_per_testcase(_TC, Config) ->
    _ = erlang:trace(all, false, [call, send]),
    _ = erlang:trace_pattern({'_', '_', '_'}, false, [local]),
    try barrel_docdb:delete_db(?config(db, Config)) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Test cases
%%====================================================================

%% 1000 writes on a database nobody subscribes to: the writer calls
%% nothing in barrel_sub or barrel_query_sub and sends them nothing.
no_subscriber_no_notify(Config) ->
    Db = ?config(db, Config),
    Pid = ?config(pid, Config),
    Events = traced(Pid, fun() -> ok = thousand_writes(Db) end),
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    ?assert(length(Changes) > 0),
    ?assertEqual([], sub_events(Events)).

%% With subscribers, each commit is one cast per manager and every
%% subscriber still gets every matching change.
subscribers_notified_per_group(Config) ->
    Db = ?config(db, Config),
    Pid = ?config(pid, Config),
    {ok, PathRef} = barrel_docdb:subscribe(Db, <<"type/#">>),
    {ok, QueryRef} = barrel_docdb:subscribe_query(
                       Db, #{where => [{path, [<<"type">>], <<"step">>}]}),
    Put = fun(I) ->
        fun() ->
            barrel_docdb:put_doc(Db, #{<<"id">> => integer_to_binary(I),
                                       <<"type">> => <<"step">>})
        end
    end,
    Events = traced(Pid, fun() ->
        Results = grouped(Pid, [Put(I) || I <- lists:seq(1, 8)]),
        8 = length([ok || {ok, _} <- Results])
    end),
    Calls = [MFA || {call, MFA} <- sub_events(Events)],
    ?assertEqual([{barrel_query_sub, notify_changes, 2}, {barrel_sub, notify, 2}],
                 lists:usort([{M, F, length(A)} || {M, F, A} <- Calls])),
    ?assertEqual(2, length(Calls)),
    Ids = [integer_to_binary(I) || I <- lists:seq(1, 8)],
    ?assertEqual(Ids, [Id || #{id := Id} <- recv(barrel_change, 8)]),
    ?assertEqual(Ids, [Id || #{id := Id} <- recv(barrel_query_change, 8)]),
    ok = barrel_docdb:unsubscribe(PathRef),
    ok = barrel_docdb:unsubscribe_query(QueryRef),
    %% the flags drop with the last subscription
    Events2 = traced(Pid, fun() -> {ok, _} = (Put(9))() end),
    ?assertEqual([], sub_events(Events2)).

%%====================================================================
%% Helpers
%%====================================================================

%% New docs, updates, deletes and put_docs from 8 writers: 1000 writes.
thousand_writes(Db) ->
    Results = run_parallel(8, fun(W) ->
        Prefix = <<"w", (integer_to_binary(W))/binary, "-">>,
        lists:foreach(fun(I) ->
            Id = <<Prefix/binary, (integer_to_binary(I))/binary>>,
            {ok, #{<<"rev">> := Rev}} =
                barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"type">> => <<"step">>}),
            {ok, #{<<"rev">> := Rev2}} =
                barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"_rev">> => Rev,
                                           <<"v">> => 2}, #{sync => true}),
            {ok, _} = barrel_docdb:delete_doc(Db, Id, #{rev => Rev2})
        end, lists:seq(1, 40)),
        [{ok, _}, {ok, _}, {ok, _}, {ok, _}, {ok, _}] =
            barrel_docdb:put_docs(Db, [#{<<"id">> => <<Prefix/binary, "b",
                                                     (integer_to_binary(I))/binary>>}
                                       || I <- lists:seq(1, 5)]),
        ok
    end),
    %% 8 x (40 x 3 + 5) = 1000
    ?assertEqual(lists:duplicate(8, ok), Results),
    ok.

%% Trace the writer's calls and sends while Fun runs.
traced(Pid, Fun) ->
    Tracer = spawn_link(fun() -> collect([]) end),
    1 = erlang:trace(Pid, true, [call, send, {tracer, Tracer}]),
    _ = erlang:trace_pattern({barrel_sub, '_', '_'}, true, [local]),
    _ = erlang:trace_pattern({barrel_query_sub, '_', '_'}, true, [local]),
    Fun(),
    1 = erlang:trace(Pid, false, [call, send]),
    _ = erlang:trace_pattern({'_', '_', '_'}, false, [local]),
    Tracer ! {done, self()},
    receive {events, Events} -> Events after 10000 -> error(tracer_timeout) end.

collect(Acc) ->
    receive
        {trace, _, call, MFA} -> collect([{call, MFA} | Acc]);
        {trace, _, send, Msg, To} -> collect([{send, To, Msg} | Acc]);
        {done, From} -> From ! {events, lists:reverse(Acc)}
    end.

%% Calls into the managers and messages sent to them.
sub_events(Events) ->
    Managers = [barrel_sub, barrel_query_sub,
                whereis(barrel_sub), whereis(barrel_query_sub)],
    [E || {call, {M, _, _}} = E <- Events,
          M =:= barrel_sub orelse M =:= barrel_query_sub]
    ++ [E || {send, To, _} = E <- Events, lists:member(To, Managers)].

%% Run N funs at once (released together); results in order.
run_parallel(N, Fun) ->
    Parent = self(),
    Pids = [spawn_link(fun() ->
                receive go -> ok end,
                Parent ! {self(), Fun(I)}
            end) || I <- lists:seq(1, N)],
    _ = [P ! go || P <- Pids],
    [receive {P, R} -> R after 60000 -> error(timeout) end || P <- Pids].

%% Queue the calls at a suspended server, in order, then resume it so
%% they are taken as one group. Results in call order.
grouped(Pid, Funs) ->
    ok = sys:suspend(Pid),
    Parent = self(),
    Callers = lists:map(
        fun({Idx, Fun}) ->
            C = spawn_link(fun() -> Parent ! {self(), Fun()} end),
            wait_queue(Pid, Idx),
            C
        end, lists:zip(lists:seq(1, length(Funs)), Funs)),
    ok = sys:resume(Pid),
    [receive {C, R} -> R after 10000 -> error(timeout) end || C <- Callers].

wait_queue(Pid, N) ->
    case erlang:process_info(Pid, message_queue_len) of
        {message_queue_len, L} when L >= N -> ok;
        _ -> timer:sleep(1), wait_queue(Pid, N)
    end.

recv(Tag, N) ->
    [receive {Tag, _Db, Change} -> Change after 5000 -> error({missing, Tag}) end
     || _ <- lists:seq(1, N)].
