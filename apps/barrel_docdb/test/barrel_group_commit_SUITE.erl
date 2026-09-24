%%%-------------------------------------------------------------------
%%% @doc Group commit: the writes waiting at a database commit in one
%%% batch and one sync, each caller keeping its own answer.
%%%
%%% Deterministic groups are formed by suspending the database server,
%%% queueing calls, then resuming it.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_group_commit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([concurrent_synced_throughput/1,
         create_if_absent_race/1,
         mixed_group/1,
         put_docs_repeated_id/1,
         changes_feed_order/1,
         write_failure/1,
         synced_write_survives_reopen/1,
         max_group_bounds_batch/1,
         reads_keep_their_place/1]).

-define(TAG, <<"hb.task">>).

all() ->
    [concurrent_synced_throughput,
     create_if_absent_race,
     mixed_group,
     put_docs_repeated_id,
     changes_feed_order,
     write_failure,
     synced_write_survives_reopen,
     max_group_bounds_batch,
     reads_keep_their_place].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_group_commit_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(max_group_bounds_batch = TC, Config) ->
    open(TC, #{max_group => 2}, Config);
init_per_testcase(TC, Config) ->
    open(TC, #{}, Config).

end_per_testcase(_TC, Config) ->
    try meck:unload(barrel_store_rocksdb) catch _:_ -> ok end,
    try barrel_docdb:delete_db(?config(db, Config)) catch _:_ -> ok end,
    ok.

open(TC, Opts, Config) ->
    Db = atom_to_binary(TC, utf8),
    {ok, Pid} = barrel_docdb:create_db(Db, Opts#{data_dir => ?config(dir, Config)}),
    [{db, Db}, {pid, Pid} | Config].

%%====================================================================
%% Test cases
%%====================================================================

%% 64 synced writers commit at least 10x faster than one, when a sync
%% costs enough to measure (it is near free on some CI disks).
concurrent_synced_throughput(Config) ->
    Db = ?config(db, Config),
    SingleN = 100,
    {T1, ok} = timer:tc(fun() -> writer(Db, <<"single">>, SingleN) end),
    Single = SingleN * 1000000 / T1,

    Writers = 64,
    PerWriter = 20,
    {T64, Results} = timer:tc(fun() ->
        run_parallel(Writers, fun(I) ->
            writer(Db, <<"w", (integer_to_binary(I))/binary>>, PerWriter)
        end)
    end),
    ?assertEqual(lists:duplicate(Writers, ok), Results),
    Concurrent = Writers * PerWriter * 1000000 / T64,

    {ok, #{write_groups := Groups}} = barrel_docdb:db_info(Db),
    ct:pal("single writer: ~.1f/s, 64 writers: ~.1f/s (x~.1f), groups: ~p",
           [Single, Concurrent, Concurrent / Single, Groups]),
    ?assert(maps:get(max_size, Groups) > 1),
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    ?assertEqual(SingleN + Writers * PerWriter, length(Changes)),
    case Single < 1000 of
        true -> ?assert(Concurrent >= 10 * Single);
        false -> ct:pal("sync is cheap on this disk, ratio not asserted")
    end.

%% 32 creators of the same id: one ok, 31 conflicts, one outbox entry.
create_if_absent_race(Config) ->
    Db = ?config(db, Config),
    Results = run_parallel(32, fun(I) ->
        barrel_docdb:put_doc(Db, #{<<"id">> => <<"step">>, <<"by">> => I},
                             #{outbox => [?TAG], return_hlc => true,
                               sync => true})
    end),
    Oks = [R || {ok, _} = R <- Results],
    ?assertEqual(1, length(Oks)),
    ?assertEqual(31, length([R || {error, conflict} = R <- Results])),
    ?assertEqual(1, length(pending(Db))),
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    ?assertEqual(1, length(Changes)).

%% One group mixing synced and unsynced puts, a conflict, an invalid
%% option, an ack, and a repeated id. Each caller gets its own answer.
mixed_group(Config) ->
    Db = ?config(db, Config),
    Pid = ?config(pid, Config),
    Tagged = #{outbox => [?TAG]},
    {ok, #{<<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => <<"d1">>, <<"v">> => 0}, Tagged),
    {ok, #{hlc := HlcD2}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => <<"d2">>, <<"v">> => 0},
                             Tagged#{return_hlc => true}),
    Put = fun(Doc, Opts) -> fun() -> barrel_docdb:put_doc(Db, Doc, Opts) end end,
    Results = grouped(Pid, [
        Put(#{<<"id">> => <<"n1">>}, Tagged#{sync => true}),
        Put(#{<<"id">> => <<"n2">>}, Tagged),
        Put(#{<<"id">> => <<"d1">>, <<"v">> => 1}, Tagged),
        fun() -> barrel_docdb:outbox_ack(Db, ?TAG, [HlcD2]) end,
        Put(#{<<"id">> => <<"n3">>}, Tagged#{expires_at => -1}),
        Put(#{<<"id">> => <<"d1">>, <<"v">> => 2, <<"_rev">> => Rev1}, Tagged),
        Put(#{<<"id">> => <<"n1">>}, Tagged)
    ]),
    [{ok, #{<<"id">> := <<"n1">>}},
     {ok, #{<<"id">> := <<"n2">>}},
     {error, conflict},
     ok,
     {error, {invalid_expires_at, -1}},
     {ok, #{<<"id">> := <<"d1">>}},
     {error, conflict}] = Results,
    ?assertEqual([<<"d1">>, <<"n1">>, <<"n2">>],
                 lists:sort([Id || #{id := Id} <- pending(Db)])),
    {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Db, <<"d1">>),
    %% The repeated n1 closed the group: 5 requests joined it
    {ok, #{write_groups := #{max_size := 5}}} = barrel_docdb:db_info(Db),
    ok.

%% A repeated id inside put_docs is written after the first one: without
%% a rev it answers conflict, and the first doc's rows stay consistent.
put_docs_repeated_id(Config) ->
    Db = ?config(db, Config),
    [{ok, #{<<"id">> := <<"x">>}}, {error, conflict}, {ok, #{<<"id">> := <<"y">>}}] =
        barrel_docdb:put_docs(Db, [#{<<"id">> => <<"x">>, <<"v">> => 1},
                                   #{<<"id">> => <<"x">>, <<"v">> => 2},
                                   #{<<"id">> => <<"y">>, <<"v">> => 1}],
                              #{outbox => [?TAG]}),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"x">>),
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    ?assertEqual([<<"x">>, <<"y">>], [maps:get(id, C) || C <- Changes]),
    ?assertEqual([<<"x">>, <<"y">>], [Id || #{id := Id} <- pending(Db)]).

%% After concurrent creates and updates, the feed lists each doc once,
%% in HLC order.
changes_feed_order(Config) ->
    Db = ?config(db, Config),
    Results = run_parallel(16, fun(I) ->
        Id = <<"c", (integer_to_binary(I))/binary>>,
        {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(Db, #{<<"id">> => Id}),
        {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"_rev">> => Rev,
                                             <<"v">> => 2}),
        writer(Db, Id, 10)
    end),
    ?assertEqual(lists:duplicate(16, ok), Results),
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    Ids = [maps:get(id, C) || C <- Changes],
    ?assertEqual(16 * 11, length(Ids)),
    ?assertEqual(length(Ids), length(lists:usort(Ids))),
    Hlcs = [maps:get(hlc, C) || C <- Changes],
    ?assertEqual(Hlcs, lists:usort(Hlcs)).

%% A failed batch answers every caller of the group with an error, and
%% the server keeps serving.
write_failure(Config) ->
    Db = ?config(db, Config),
    Pid = ?config(pid, Config),
    ok = meck:new(barrel_store_rocksdb, [passthrough, no_link]),
    ok = meck:expect(barrel_store_rocksdb, write_batch,
                     fun(_Ref, _Ops, _Opts) -> {error, injected} end),
    Put = fun(Id, Opts) ->
        fun() -> barrel_docdb:put_doc(Db, #{<<"id">> => Id}, Opts) end
    end,
    Results = grouped(Pid, [
        Put(<<"f1">>, #{sync => true}),
        Put(<<"f2">>, #{}),
        fun() -> barrel_docdb:put_docs(Db, [#{<<"id">> => <<"f3">>}], #{}) end
    ]),
    ?assertEqual([{error, injected}, {error, injected}, [{error, injected}]],
                 Results),
    ok = meck:unload(barrel_store_rocksdb),
    ?assert(is_process_alive(Pid)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"f1">>}),
    {error, not_found} = barrel_docdb:get_doc(Db, <<"f2">>),
    ok.

%% A synced write whose caller was answered survives a reopen.
synced_write_survives_reopen(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"k">>, <<"v">> => 1},
                                   #{sync => true}),
    ok = barrel_docdb:close_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => ?config(dir, Config)}),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"k">>),
    ok.

%% max_group caps the requests committed in one batch.
max_group_bounds_batch(Config) ->
    Db = ?config(db, Config),
    Pid = ?config(pid, Config),
    Funs = [fun() -> barrel_docdb:put_doc(Db, #{<<"id">> => integer_to_binary(I)}) end
            || I <- lists:seq(1, 5)],
    Results = grouped(Pid, Funs),
    ?assertEqual(5, length([ok || {ok, _} <- Results])),
    {ok, #{write_groups := #{groups := 3, requests := 5, max_size := 2}}} =
        barrel_docdb:db_info(Db),
    ok.

%% A read queued between writes is answered, and the writes still group.
reads_keep_their_place(Config) ->
    Db = ?config(db, Config),
    Pid = ?config(pid, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"r">>}),
    Put = fun(Id) -> fun() -> barrel_docdb:put_doc(Db, #{<<"id">> => Id}) end end,
    [{ok, _}, {ok, _}, {ok, _}, {ok, _}] = grouped(Pid, [
        Put(<<"a">>),
        Put(<<"b">>),
        fun() -> barrel_db_server:diff_versions(Pid, #{}) end,
        Put(<<"c">>)
    ]),
    {ok, #{write_groups := #{max_size := 3}}} = barrel_docdb:db_info(Db),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Synced puts of distinct ids.
writer(Db, Prefix, N) ->
    lists:foreach(
      fun(I) ->
          Id = <<Prefix/binary, "-", (integer_to_binary(I))/binary>>,
          {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id},
                                         #{outbox => [?TAG], sync => true})
      end, lists:seq(1, N)),
    ok.

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

pending(Db) ->
    lists:reverse(
        barrel_docdb:outbox_fold(Db, ?TAG, fun(E, Acc) -> {ok, [E | Acc]} end, [])).
