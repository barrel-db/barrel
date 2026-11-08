%%% @doc Disk use and compaction backlog, read from RocksDB properties:
%%% in db_info and in the storage gauges.
-module(barrel_storage_stats_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([info_reports_storage/1,
         gauges_follow_the_store/1,
         stats_interval_zero_disables_refresh/1]).

all() ->
    [info_reports_storage,
     gauges_follow_the_store,
     stats_interval_zero_disables_refresh].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_binary(TC),
    _ = barrel_docdb:delete_db(Db),
    [{db, Db} | Config].

end_per_testcase(_TC, Config) ->
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

info_reports_storage(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db),
    ok = write_and_flush(Db, 200),
    {ok, #{storage := S}} = barrel_docdb:db_info(Db),
    #{size_bytes := Size, pending_compaction_bytes := Pending,
      compactions_running := Running, compactions_queued := Queued} = S,
    ?assert(Size > 0),
    [?assert(is_integer(V) andalso V >= 0) || V <- [Pending, Running, Queued]].

gauges_follow_the_store(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, #{stats_interval => 50}),
    ok = write_and_flush(Db, 200),
    Size = wait_gauge(<<"barrel_db_size_bytes">>, Db, 100),
    ?assert(Size > 0),
    ?assert(gauge(<<"barrel_db_pending_compaction_bytes">>, Db) =/= undefined),
    ?assert(gauge(<<"barrel_db_compactions_running">>, Db) =/= undefined).

stats_interval_zero_disables_refresh(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = barrel_docdb:create_db(Db, #{stats_interval => 0}),
    ok = write_and_flush(Db, 10),
    timer:sleep(1200),
    {messages, Msgs} = process_info(Pid, messages),
    ?assertNot(lists:member(stats_tick, Msgs)),
    ?assertEqual(undefined, gauge(<<"barrel_db_size_bytes">>, Db)).

%% helpers

write_and_flush(Db, N) ->
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => integer_to_binary(I),
                                         <<"body">> => binary:copy(<<"x">>, 200)},
                                    #{sync => true})
     || I <- lists:seq(1, N)],
    #{ref := Ref} = persistent_term:get({barrel_store, Db}),
    rocksdb:flush(Ref, [{wait, true}]).

wait_gauge(Name, Db, 0) ->
    ct:fail({no_gauge, Name, Db});
wait_gauge(Name, Db, N) ->
    case gauge(Name, Db) of
        V when is_number(V), V > 0 -> V;
        _ -> timer:sleep(20), wait_gauge(Name, Db, N - 1)
    end.

%% Value of a gauge for one database in the Prometheus text export.
gauge(Name, Db) ->
    Text = barrel_metrics:export_text(),
    Re = <<"^", Name/binary, "[a-z_]*\\{db=\"", Db/binary, "\"\\} ([0-9.e+]+)$">>,
    case re:run(Text, Re, [multiline, {capture, all_but_first, binary}]) of
        {match, [V]} -> binary_to_number(V);
        nomatch -> undefined
    end.

binary_to_number(B) ->
    try binary_to_float(B) catch error:badarg -> binary_to_integer(B) end.
