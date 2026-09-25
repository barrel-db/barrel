%%%-------------------------------------------------------------------
%%% @doc BM25 persistence across close/reopen, compaction, removal,
%%% unclean stop and encryption (disk backend), plus memory rebuild.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_persistence_tests).

-include_lib("eunit/include/eunit.hrl").

-define(STORE, bm25_persist_test).
-define(KEY, <<1:256>>).
-define(QUERIES, [<<"erlang">>, <<"hello world">>, <<"term">>,
                  <<"beam otp">>, <<"7">>, <<"zzz">>]).

persistence_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [fun(Dir) -> {Title, {timeout, 60, fun() -> F(Dir) end}} end
      || {Title, F} <- [
          {"disk: close/reopen keeps hits and scores", fun disk_reopen/1},
          {"disk: every compaction survives reopen", fun disk_compactions/1},
          {"disk: removals survive reopen", fun disk_removals/1},
          {"disk: stats identical after reopen", fun disk_stats/1},
          {"disk: kill then reopen loses no acked write", fun disk_kill/1},
          {"disk: encrypted reopen", fun disk_encrypted/1},
          {"memory: rebuilt from stored text at open", fun memory_reopen/1},
          {"disk: interrupted compaction redone at open", fun disk_torn_segment/1},
          {"disk: store predating the durable format is rebuilt", fun disk_legacy/1},
          {"disk: read-only open serves hits, files untouched", fun disk_read_only/1},
          {"disk: read-only open refuses a legacy store", fun disk_legacy_read_only/1}
      ]]}.

direct_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [fun(Dir) -> {"direct: compacted docs", fun() -> direct_compacted(Dir) end} end]}.

setup() ->
    Dir = "/tmp/barrel_bm25_persist_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    application:ensure_all_started(rocksdb),
    Dir.

cleanup(Dir) ->
    catch barrel_vectordb:stop(?STORE),
    os:cmd("rm -rf " ++ Dir),
    ok.

%%====================================================================
%% Store-level cases
%%====================================================================

disk_reopen(Dir) ->
    Cfg = cfg(Dir, disk),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 20),
    Before = snapshot(),
    ?assertMatch([_, _, _, _, _ | _], hits(<<"erlang">>)),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(Before, snapshot()).

disk_compactions(Dir) ->
    Cfg = cfg(Dir, disk),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 10),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(11, 20),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(21, 30),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    %% an uncompacted tail on top of three segments
    add_docs(31, 35),
    Before = snapshot(),
    ?assertEqual(35, length(hits(<<"erlang">>))),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(Before, snapshot()),
    ?assertEqual(lists:sort(ids(1, 35)), lists:sort(hit_ids(<<"erlang">>))),
    %% compacting after reopen keeps every document
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    ?assertEqual(lists:sort(ids(1, 35)), lists:sort(hit_ids(<<"erlang">>))),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(lists:sort(ids(1, 35)), lists:sort(hit_ids(<<"erlang">>))).

disk_removals(Dir) ->
    Cfg = cfg(Dir, disk),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 10),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(11, 15),
    %% one compacted doc, one hot doc
    ok = barrel_vectordb:delete(?STORE, <<"3">>),
    ok = barrel_vectordb:delete(?STORE, <<"12">>),
    %% a compacted doc rewritten without the "erlang" term
    ok = barrel_vectordb:add_vector(?STORE, <<"5">>, <<"beam otp only">>,
                                    #{}, [0.0, 1.0, 0.0]),
    Expected = lists:sort(ids(1, 15) -- [<<"3">>, <<"12">>, <<"5">>]),
    ?assertEqual(Expected, lists:sort(hit_ids(<<"erlang">>))),
    Before = snapshot(),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(Before, snapshot()),
    ?assertEqual(Expected, lists:sort(hit_ids(<<"erlang">>))),
    ?assertEqual([<<"5">>], hit_ids(<<"only">>)),
    %% still gone after a compaction and another reopen
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(Expected, lists:sort(hit_ids(<<"erlang">>))),
    ?assertEqual(13, maps:get(total_docs, info())).

disk_stats(Dir) ->
    Cfg = cfg(Dir, disk),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 12),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(13, 18),
    ok = barrel_vectordb:delete(?STORE, <<"2">>),
    Info = info(),
    ?assertEqual(17, maps:get(total_docs, Info)),
    Before = snapshot(),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    Info2 = info(),
    [?assertEqual(maps:get(K, Info), maps:get(K, Info2))
     || K <- [total_docs, total_tokens, avg_doc_length]],
    ?assertEqual(Before, snapshot()).

disk_kill(Dir) ->
    Cfg = cfg(Dir, disk),
    {ok, Pid} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 10),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(11, 16),
    ok = barrel_vectordb:delete(?STORE, <<"4">>),
    Before = snapshot(),
    kill(Pid),
    {ok, _} = start_retry(Cfg, 50),
    ?assertEqual(Before, snapshot()),
    ?assertEqual(lists:sort(ids(1, 16) -- [<<"4">>]),
                 lists:sort(hit_ids(<<"erlang">>))).

disk_encrypted(Dir) ->
    Cfg = (cfg(Dir, disk))#{crypto => #{key => ?KEY}},
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 10),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(11, 20),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(21, 25),
    ok = barrel_vectordb:delete(?STORE, <<"7">>),
    Before = snapshot(),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(Before, snapshot()),
    ?assertEqual(lists:sort(ids(1, 25) -- [<<"7">>]),
                 lists:sort(hit_ids(<<"erlang">>))).

memory_reopen(Dir) ->
    Cfg = cfg(Dir, memory),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 12),
    ok = barrel_vectordb:delete(?STORE, <<"6">>),
    Before = snapshot(),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(Before, snapshot()).

%% A compaction killed between the file writes and the marker update.
disk_torn_segment(Dir) ->
    Cfg = cfg(Dir, disk),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 10),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(11, 14),
    Before = snapshot(),
    ok = barrel_vectordb:stop(?STORE),
    BmDir = Dir ++ "/vs/bm25",
    with_ids_db(BmDir, fun(Db, [CfD | _]) ->
        ok = rocksdb:delete(Db, CfD, <<"segment">>, [])
    end),
    ok = file:write_file(BmDir ++ "/bm25.blockmax", <<"torn">>),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(Before, snapshot()),
    ?assertEqual(0, maps:get(hot_docs, info())).

%% Pre-2.4.1 stores have no forward index: rebuilt from the stored text.
disk_legacy(Dir) ->
    Cfg = cfg(Dir, disk),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 12),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(13, 15),
    ok = barrel_vectordb:delete(?STORE, <<"2">>),
    Before = snapshot(),
    ok = barrel_vectordb:stop(?STORE),
    make_legacy(Dir ++ "/vs/bm25"),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(false, maps:get(rebuild_required, info())),
    ?assertEqual(lists:sort(ids(1, 15) -- [<<"2">>]),
                 lists:sort(hit_ids(<<"erlang">>))),
    ?assertEqual(Before, snapshot()),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    ?assertEqual(Before, snapshot()).

disk_read_only(Dir) ->
    Cfg = cfg(Dir, disk),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 12),
    ok = barrel_vectordb_server:bm25_compact(?STORE),
    add_docs(13, 15),
    Before = snapshot(),
    ok = barrel_vectordb:stop(?STORE),
    Files = tree(Dir),
    {ok, _} = barrel_vectordb:start_link(Cfg#{read_only => true}),
    ?assertEqual(Before, snapshot()),
    ok = barrel_vectordb:stop(?STORE),
    ?assertEqual(Files, tree(Dir)).

disk_legacy_read_only(Dir) ->
    Cfg = cfg(Dir, disk),
    {ok, _} = barrel_vectordb:start_link(Cfg),
    add_docs(1, 5),
    ok = barrel_vectordb:stop(?STORE),
    make_legacy(Dir ++ "/vs/bm25"),
    Files = tree(Dir),
    process_flag(trap_exit, true),
    ?assertMatch({error, {bm25_init_failed,
                          {read_only_upgrade_needed,
                           #{missing_cfs := ["doc_terms", "term_df",
                                             "pending"]}}}},
                 barrel_vectordb:start_link(Cfg#{read_only => true})),
    ?assertEqual(Files, tree(Dir)).

%%====================================================================
%% Direct index API
%%====================================================================

direct_compacted(Dir) ->
    Path = filename:join(Dir, "bm25"),
    {ok, I0} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
    {ok, I1} = barrel_vectordb_bm25_disk:add(I0, <<"a">>, <<"hello world">>),
    {ok, I2} = barrel_vectordb_bm25_disk:add(I1, <<"b">>, <<"hello there">>),
    {ok, I3} = barrel_vectordb_bm25_disk:compact(I2),
    %% compacted docs are readable and removable
    {ok, Vec} = barrel_vectordb_bm25_disk:get_vector(I3, <<"a">>),
    ?assert(maps:is_key(<<"world">>, Vec)),
    {ok, I4} = barrel_vectordb_bm25_disk:remove(I3, <<"a">>),
    ?assertEqual([<<"b">>],
                 [Id || {Id, _} <- barrel_vectordb_bm25_disk:search(
                                       I4, <<"hello">>, 10)]),
    ok = barrel_vectordb_bm25_disk:close(I4),
    {ok, I5} = barrel_vectordb_bm25_disk:open(Path),
    ?assertEqual([<<"b">>],
                 [Id || {Id, _} <- barrel_vectordb_bm25_disk:search(
                                       I5, <<"hello">>, 10)]),
    ?assertEqual(1, maps:get(total_docs, barrel_vectordb_bm25_disk:stats(I5))),
    ok = barrel_vectordb_bm25_disk:close(I5).

%%====================================================================
%% Helpers
%%====================================================================

tree(Dir) ->
    lists:sort(filelib:fold_files(
                 Dir, ".*", true,
                 fun(F, Acc) ->
                         {ok, B} = file:read_file(F),
                         [{F, erlang:md5(B)} | Acc]
                 end, [])).

-define(CFS, ["default", "terms_fwd", "terms_rev", "docs_fwd", "docs_rev",
               "doc_terms", "term_df", "pending"]).

with_ids_db(BmDir, Fun) ->
    {ok, Db, Cfs} = rocksdb:open(BmDir ++ "/bm25.ids", [],
                                 [{Name, []} || Name <- ?CFS]),
    try Fun(Db, Cfs)
    after
        rocksdb:close(Db)
    end.

%% Strip what 2.4.1 added to the ids RocksDB.
make_legacy(BmDir) ->
    with_ids_db(BmDir, fun(Db, [CfD, _, _, _, _ | New]) ->
        [ok = rocksdb:drop_column_family(Db, Cf) || Cf <- New],
        [ok = rocksdb:delete(Db, CfD, K, [])
         || K <- [<<"format">>, <<"stats">>, <<"segment">>]],
        ok
    end).

cfg(Dir, Backend) ->
    #{name => ?STORE, db_path => Dir ++ "/vs", dimension => 3,
      bm25_backend => Backend}.

ids(From, To) ->
    [integer_to_binary(I) || I <- lists:seq(From, To)].

%% Varied lengths and term mixes so doc length and IDF matter.
add_docs(From, To) ->
    lists:foreach(
      fun(I) ->
              N = integer_to_binary(I),
              Extra = binary:copy(<<" beam">>, I rem 4),
              Otp = case I rem 3 of 0 -> <<" otp">>; _ -> <<>> end,
              Text = <<"hello world erlang term ", N/binary, Extra/binary,
                       Otp/binary>>,
              ok = barrel_vectordb:add_vector(?STORE, N, Text, #{},
                                              [1.0, 0.0, 0.0])
      end, lists:seq(From, To)).

hits(Query) ->
    {ok, Hits} = barrel_vectordb:search_bm25(?STORE, Query, #{k => 100}),
    Hits.

hit_ids(Query) ->
    [Id || {Id, _} <- hits(Query)].

snapshot() ->
    [{Q, lists:sort(hits(Q))} || Q <- ?QUERIES].

info() ->
    {ok, Info} = barrel_vectordb_server:bm25_info(?STORE),
    Info.

kill(Pid) ->
    unlink(Pid),
    Ref = monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> ok end.

%% The killed store's RocksDB handles are released by GC, not at once.
start_retry(Cfg, N) ->
    Old = process_flag(trap_exit, true),
    try do_start_retry(Cfg, N)
    after
        process_flag(trap_exit, Old)
    end.

do_start_retry(Cfg, 0) ->
    barrel_vectordb:start_link(Cfg);
do_start_retry(Cfg, N) ->
    case catch barrel_vectordb:start_link(Cfg) of
        {ok, _} = Ok -> Ok;
        _ ->
            receive {'EXIT', _, _} -> ok after 0 -> ok end,
            timer:sleep(100),
            do_start_retry(Cfg, N - 1)
    end.
