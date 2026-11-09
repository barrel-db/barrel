%%% @doc Disk use and compaction backlog, read from RocksDB properties:
%%% in db_info and in the storage gauges.
-module(barrel_storage_stats_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([info_reports_storage/1,
         gauges_follow_the_store/1,
         stats_interval_zero_disables_refresh/1,
         direct_io_compaction_reads/1,
         doc_count_follows_writes/1,
         doc_count_counts_existing_db/1,
         doc_count_after_replication/1,
         doc_count_on_branches/1,
         att_count_follows_writes/1]).

all() ->
    [info_reports_storage,
     gauges_follow_the_store,
     stats_interval_zero_disables_refresh,
     direct_io_compaction_reads,
     doc_count_follows_writes,
     doc_count_counts_existing_db,
     doc_count_after_replication,
     doc_count_on_branches,
     att_count_follows_writes].

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
    _ = barrel_docdb:delete_db(<<(?config(db, Config))/binary, "_tgt">>),
    _ = barrel_docdb:delete_db(<<(?config(db, Config))/binary, "_now">>),
    _ = barrel_docdb:delete_db(<<(?config(db, Config))/binary, "_then">>),
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

%% Opt-in direct I/O for compaction reads: both stores open with it, the
%% data reads back after a forced compaction, and the option reaches
%% RocksDB (its OPTIONS file records it).
direct_io_compaction_reads(Config) ->
    Db = ?config(db, Config),
    Opts = #{store_opts => #{use_direct_io_for_compaction_reads => true},
             att_opts => #{use_direct_io_for_compaction_reads => true}},
    {ok, _} = barrel_docdb:create_db(Db, Opts),
    ok = write_and_flush(Db, 50),
    #{ref := Ref} = persistent_term:get({barrel_store, Db}),
    ok = rocksdb:compact_range(Ref, undefined, undefined, []),
    ?assertMatch({ok, #{<<"id">> := <<"7">>}}, barrel_docdb:get_doc(Db, <<"7">>)),
    {ok, #{db_path := Path}} = barrel_docdb:db_info(Db),
    [OptionsFile | _] = lists:reverse(lists:sort(
                          filelib:wildcard(filename:join([Path, "docs", "OPTIONS-*"])))),
    {ok, Text} = file:read_file(OptionsFile),
    ?assertNotEqual(nomatch,
                    binary:match(Text, <<"use_direct_io_for_compaction_reads=true">>)).

%% helpers

%% The live-document count moves with every write: new, updated (no
%% change), deleted, put back, batched, written by many processes at
%% once; local documents are not counted.
doc_count_follows_writes(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, #{stats_interval => 50}),
    ok = wait_docs(Db, 0),
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id}) || Id <- [<<"a">>, <<"b">>, <<"c">>]],
    ?assertEqual(3, docs(Db)),
    {ok, A} = barrel_docdb:get_doc(Db, <<"a">>),
    {ok, _} = barrel_docdb:put_doc(Db, A#{<<"v">> => 2}),
    ?assertEqual(3, docs(Db)),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"b">>),
    ?assertEqual(2, docs(Db)),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"b">>}),
    ?assertEqual(3, docs(Db)),
    _ = barrel_docdb:put_docs(Db, [#{<<"id">> => <<"p", (integer_to_binary(I))/binary>>}
                                   || I <- lists:seq(1, 5)]),
    ?assertEqual(8, docs(Db)),
    Self = self(),
    Pids = [spawn_link(fun() ->
                [{ok, _} = barrel_docdb:put_doc(
                              Db, #{<<"id">> => <<"w", (integer_to_binary(W))/binary, "-",
                                                  (integer_to_binary(I))/binary>>})
                 || I <- lists:seq(1, 25)],
                Self ! {done, self()}
            end) || W <- lists:seq(1, 4)],
    [receive {done, P} -> ok end || P <- Pids],
    ?assertEqual(108, docs(Db)),
    ok = barrel_docdb:put_local_doc(Db, <<"l">>, #{<<"x">> => 1}),
    ?assertEqual(108, docs(Db)),
    ?assertEqual(live_docs(Db), docs(Db)),
    108 = wait_gauge_value(<<"barrel_db_documents_total">>, Db, 108, 200).

%% A database written before the counter existed is counted once.
doc_count_counts_existing_db(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, #{stats_interval => 50}),
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => integer_to_binary(I)})
     || I <- lists:seq(1, 20)],
    [{ok, _} = barrel_docdb:delete_doc(Db, integer_to_binary(I)) || I <- lists:seq(1, 5)],
    Ks = barrel_keyspace:resolve(Db),
    Store = persistent_term:get({barrel_store, Db}),
    ok = barrel_store_rocksdb:write_batch(
           Store, [{delete, barrel_store_keys:db_docs_count(Ks)},
                   {delete, barrel_store_keys:db_docs_counted(Ks)}], #{}),
    ?assertEqual(undefined, docs(Db)),
    ok = wait_docs(Db, 15).

%% A replica holds as many live documents as its source, and replaying
%% replication does not move the count.
doc_count_after_replication(Config) ->
    Src = ?config(db, Config),
    Tgt = <<Src/binary, "_tgt">>,
    {ok, _} = barrel_docdb:create_db(Src, #{stats_interval => 50}),
    {ok, _} = barrel_docdb:create_db(Tgt, #{stats_interval => 50}),
    ok = wait_docs(Tgt, 0),
    [{ok, _} = barrel_docdb:put_doc(Src, #{<<"id">> => integer_to_binary(I)})
     || I <- lists:seq(1, 10)],
    [{ok, _} = barrel_docdb:delete_doc(Src, integer_to_binary(I)) || I <- lists:seq(1, 3)],
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    ?assertEqual(7, docs(Tgt)),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    ?assertEqual(7, docs(Tgt)),
    {ok, _} = barrel_docdb:delete_doc(Src, <<"4">>),
    {ok, _} = barrel_rep:replicate(Src, Tgt),
    ?assertEqual(6, docs(Tgt)),
    ?assertEqual(live_docs(Tgt), docs(Tgt)).

%% A branch starts with its parent's count; a branch of a past instant
%% counts the documents of that instant.
doc_count_on_branches(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:create_db(Db, #{stats_interval => 50}),
    ok = wait_docs(Db, 0),
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => integer_to_binary(I)})
     || I <- lists:seq(1, 5)],
    {ok, _, T} = barrel_docdb:get_changes(Db, first, #{limit => 1000}),
    timer:sleep(5),
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => integer_to_binary(I)})
     || I <- lists:seq(6, 8)],
    Now = <<Db/binary, "_now">>,
    Then = <<Db/binary, "_then">>,
    {ok, _} = barrel_docdb:branch_db(Db, Now, #{}),
    ?assertEqual(8, docs(Now)),
    {ok, _} = barrel_docdb:branch_db(Db, Then, #{at => T}),
    ok = wait_docs(Then, 5).

%% The attachment count follows puts, overwrites (no change) and deletes,
%% follows replication, and a database written before the counter is
%% counted once.
att_count_follows_writes(Config) ->
    Db = ?config(db, Config),
    Tgt = <<Db/binary, "_tgt">>,
    {ok, _} = barrel_docdb:create_db(Db, #{stats_interval => 50}),
    {ok, _} = barrel_docdb:create_db(Tgt, #{stats_interval => 50}),
    ok = wait_atts(Db, 0),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a.txt">>, <<"a">>),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"b.txt">>, <<"b">>),
    ?assertEqual(2, atts(Db)),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"a.txt">>, <<"a2">>),
    ?assertEqual(2, atts(Db)),
    ok = barrel_docdb:delete_attachment(Db, <<"d">>, <<"b.txt">>),
    ?assertEqual(1, atts(Db)),
    ok = wait_atts(Tgt, 0),
    {ok, _} = barrel_rep:replicate(Db, Tgt),
    ?assertEqual(1, atts(Tgt)),
    {ok, _} = barrel_rep:replicate(Db, Tgt),
    ?assertEqual(1, atts(Tgt)),
    1 = wait_gauge_value(<<"barrel_db_attachments_total">>, Db, 1, 200),
    %% forget the count: the database is counted again
    #{ref := Ref} = persistent_term:get({barrel_att_ref, Db}),
    Ks = barrel_keyspace:resolve(Db),
    Meta = fun(Name) -> <<16#FF, $M, (byte_size(Ks)):16, Ks/binary, Name/binary>> end,
    ok = rocksdb:delete(Ref, Meta(<<"att_count">>), []),
    ok = rocksdb:delete(Ref, Meta(<<"att_counted">>), []),
    ?assertEqual(undefined, atts(Db)),
    ok = wait_atts(Db, 1).

atts(Db) ->
    {ok, #{storage := #{attachments := N}}} = barrel_docdb:db_info(Db),
    N.

wait_atts(Db, Want) -> wait_atts(Db, Want, 200).
wait_atts(Db, Want, 0) -> ct:fail({att_count, Db, atts(Db), Want});
wait_atts(Db, Want, N) ->
    case atts(Db) of
        Want -> ok;
        _ -> timer:sleep(20), wait_atts(Db, Want, N - 1)
    end.

docs(Db) ->
    {ok, #{storage := #{documents := N}}} = barrel_docdb:db_info(Db),
    N.

wait_docs(Db, Want) -> wait_docs(Db, Want, 200).
wait_docs(Db, Want, 0) -> ct:fail({doc_count, Db, docs(Db), Want});
wait_docs(Db, Want, N) ->
    case docs(Db) of
        Want -> ok;
        _ -> timer:sleep(20), wait_docs(Db, Want, N - 1)
    end.

live_docs(Db) ->
    {ok, N} = barrel_docdb:fold_docs(Db, fun(_Doc, Acc) -> {ok, Acc + 1} end, 0),
    N.

wait_gauge_value(Name, Db, Want, 0) -> ct:fail({gauge, Name, gauge(Name, Db), Want});
wait_gauge_value(Name, Db, Want, N) ->
    case gauge(Name, Db) of
        V when V == Want -> Want;
        _ -> timer:sleep(20), wait_gauge_value(Name, Db, Want, N - 1)
    end.

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
