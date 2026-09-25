%%%-------------------------------------------------------------------
%%% @doc Read-only stores: reads and searches work, every write is
%%% refused, and nothing in the directory is created or rewritten.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_read_only_tests).

-include_lib("eunit/include/eunit.hrl").

-define(STORE, ro_store_test).
-define(DIM, 8).

read_only_memory_bm25_test_() ->
    read_only(memory).

read_only_disk_bm25_test_() ->
    read_only(disk).

read_only_diskann_test_() ->
    {setup, fun() -> setup(disk, diskann_cfg()) end, fun cleanup/1,
     fun(Dir) ->
         [?_test(reads_and_refusals(Dir, disk, diskann_cfg()))]
     end}.

read_only_missing_store_test_() ->
    {setup, fun tmp_dir/0, fun cleanup/1,
     fun(Dir) -> [?_test(missing_store(Dir))] end}.

read_only_missing_cf_test_() ->
    {setup, fun() -> setup(memory, #{}) end, fun cleanup/1,
     fun(Dir) -> [?_test(missing_cf(Dir))] end}.

read_only(Backend) ->
    {setup, fun() -> setup(Backend, #{}) end, fun cleanup/1,
     fun(Dir) ->
         [?_test(reads_and_refusals(Dir, Backend, #{}))]
     end}.

diskann_cfg() ->
    #{backend => diskann, diskann => #{r => 8, l_build => 20, l_search => 20}}.

tmp_dir() ->
    "/tmp/barrel_vectordb_ro_"
        ++ integer_to_list(erlang:unique_integer([positive])).

setup(Backend, Extra) ->
    Dir = tmp_dir(),
    Cfg = Extra#{name => ?STORE, path => Dir, dimension => ?DIM,
                 bm25_backend => Backend},
    {ok, _} = barrel_vectordb:start_link(Cfg),
    Batch = [{<<"d", (integer_to_binary(I))/binary>>,
              <<"token", (integer_to_binary(I))/binary, " shared">>,
              #{}, vec(I)} || I <- lists:seq(1, 20)],
    {ok, #{inserted := 20}} = barrel_vectordb:add_vector_batch(?STORE, Batch),
    ok = barrel_vectordb:stop(?STORE),
    Dir.

cleanup(Dir) ->
    try barrel_vectordb:stop(?STORE) catch _:_ -> ok end,
    os:cmd("rm -rf " ++ Dir),
    ok.

reads_and_refusals(Dir, Backend, Extra) ->
    Before = dir_digest(Dir),
    {ok, _} = barrel_vectordb:start_link(Extra#{name => ?STORE, path => Dir,
                                                dimension => ?DIM,
                                                bm25_backend => Backend,
                                                read_only => true}),
    {ok, Stats} = barrel_vectordb:stats(?STORE),
    ?assertEqual(loaded, maps:get(index_origin, Stats)),
    ?assertEqual(20, maps:get(count, Stats)),
    {ok, [#{key := <<"d3">>} | _]} =
        barrel_vectordb:search_vector(?STORE, vec(3), #{k => 3}),
    {ok, Bm25} = barrel_vectordb:search_bm25(?STORE, <<"token7">>, #{k => 3}),
    ?assert(lists:keymember(<<"d7">>, 1, Bm25)),
    ?assertEqual({error, read_only},
                 barrel_vectordb:add_vector(?STORE, <<"x">>, <<"t">>, #{},
                                            vec(99))),
    ?assertEqual({error, read_only}, barrel_vectordb:delete(?STORE, <<"d1">>)),
    ?assertEqual({error, read_only},
                 barrel_vectordb:add_vector_batch(?STORE,
                     [{<<"y">>, <<"t">>, #{}, vec(98)}])),
    ?assertEqual({error, read_only}, barrel_vectordb:persist_index(?STORE)),
    ?assertEqual({error, read_only},
                 barrel_vectordb_server:bm25_compact(?STORE)),
    ?assertEqual(ok, barrel_vectordb:checkpoint(?STORE)),
    ok = barrel_vectordb:stop(?STORE),
    %% OpenForReadOnly: no WAL replay flush, LOG, MANIFEST or OPTIONS
    ?assertEqual(Before, dir_digest(Dir)).

missing_store(Dir) ->
    process_flag(trap_exit, true),
    ?assertMatch({error, {db_open_failed, {read_only_store_missing, _}}},
                 barrel_vectordb:start_link(#{name => ?STORE, path => Dir,
                                              dimension => ?DIM,
                                              read_only => true})),
    ?assertNot(filelib:is_dir(Dir)).

%% A store written before a column family existed needs a writable open.
missing_cf(Dir) ->
    Cfs = ["default", "vectors", "metadata", "text", "hnsw_graph"],
    {ok, Db, Handles} = rocksdb:open(Dir, [], [{N, []} || N <- Cfs]),
    ok = rocksdb:drop_column_family(Db, lists:last(Handles)),
    ok = rocksdb:close(Db),
    Before = dir_digest(Dir),
    process_flag(trap_exit, true),
    ?assertMatch({error, {db_open_failed,
                          {read_only_upgrade_needed,
                           #{missing_cfs := ["hnsw_graph"]}}}},
                 barrel_vectordb:start_link(#{name => ?STORE, path => Dir,
                                              dimension => ?DIM,
                                              read_only => true})),
    ?assertEqual(Before, dir_digest(Dir)),
    {ok, _} = barrel_vectordb:start_link(#{name => ?STORE, path => Dir,
                                           dimension => ?DIM}),
    ok = barrel_vectordb:stop(?STORE),
    {ok, _} = barrel_vectordb:start_link(#{name => ?STORE, path => Dir,
                                           dimension => ?DIM,
                                           read_only => true}),
    ok = barrel_vectordb:stop(?STORE).

vec(I) ->
    [math:sin(I * K) || K <- lists:seq(1, ?DIM)].

dir_digest(Dir) ->
    Files = filelib:fold_files(Dir, ".*", true, fun(F, Acc) -> [F | Acc] end, []),
    lists:sort([{F, crypto:hash(sha256, element(2, file:read_file(F)))}
                || F <- Files]).

