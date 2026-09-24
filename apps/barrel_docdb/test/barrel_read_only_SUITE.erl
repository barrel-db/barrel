%%%-------------------------------------------------------------------
%%% @doc Read-only databases and imported copies (TIMELINE kind
%%% import): reads work, writes are refused, no file is created or
%%% rewritten, identity comes from the sidecar.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_read_only_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([reads_work/1,
         writes_refused/1,
         concurrent_writes_refused/1,
         attachment_writes_refused/1,
         no_maintenance_timers/1,
         import_copy_under_new_name/1,
         files_untouched/1,
         missing_store_refused/1,
         missing_cf_needs_upgrade/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [reads_work, writes_refused, concurrent_writes_refused,
     attachment_writes_refused,
     no_maintenance_timers, import_copy_under_new_name,
     files_untouched, missing_store_refused, missing_cf_needs_upgrade].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = filename:join(?config(priv_dir, Config), "ro_data"),
    [{data_dir, DataDir} | Config].

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_testcase(Case, Config) ->
    Db = <<"ro_", (atom_to_binary(Case, utf8))/binary>>,
    DataDir = ?config(data_dir, Config),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir}),
    [{ok, _}, {ok, _}] = barrel_docdb:put_docs(Db, [
        #{<<"id">> => <<"a">>, <<"v">> => 1},
        #{<<"id">> => <<"b">>, <<"v">> => 2}]),
    ok = barrel_docdb:put_local_doc(Db, <<"_local/x">>, #{<<"k">> => 1}),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"a">>, <<"f.txt">>,
                                          <<"hello">>),
    ok = barrel_docdb:close_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir,
                                           read_only => true}),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel_docdb:delete_db(?config(db, Config),
                               #{data_dir => ?config(data_dir, Config)}),
    ok.

reads_work(Config) ->
    Db = ?config(db, Config),
    {ok, Info} = barrel_docdb:db_info(Db),
    ?assertEqual(true, maps:get(read_only, Info)),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"a">>),
    {ok, Rows, _} = barrel_docdb:find(Db, #{where => [{path, [<<"v">>], 2}]}),
    ?assertEqual([<<"b">>], [maps:get(<<"id">>, R) || R <- Rows]),
    {ok, #{<<"k">> := 1}} = barrel_docdb:get_local_doc(Db, <<"_local/x">>),
    {ok, <<"hello">>} = barrel_docdb:get_attachment(Db, <<"a">>, <<"f.txt">>),
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    ?assertEqual(2, length(Changes)).

writes_refused(Config) ->
    Db = ?config(db, Config),
    ?assertEqual({error, read_only},
                 barrel_docdb:put_doc(Db, #{<<"id">> => <<"c">>})),
    ?assertEqual({error, read_only},
                 barrel_docdb:put_docs(Db, [#{<<"id">> => <<"c">>}])),
    ?assertEqual({error, read_only}, barrel_docdb:delete_doc(Db, <<"a">>)),
    ?assertEqual({error, read_only},
                 barrel_docdb:put_local_doc(Db, <<"_local/y">>, #{})),
    ?assertEqual({error, read_only},
                 barrel_docdb:delete_local_doc(Db, <<"_local/x">>)),
    ?assertEqual({error, read_only}, barrel_docdb:sweep_retention(Db)),
    ?assertEqual({error, read_only},
                 barrel_docdb:set_doc_embedding(Db, <<"a">>, <<"x">>, [1.0])),
    {error, not_found} = barrel_docdb:get_doc(Db, <<"c">>).

%% Writers racing on a read-only database never form a write group.
concurrent_writes_refused(Config) ->
    Db = ?config(db, Config),
    {ok, #{write_groups := Before}} = barrel_docdb:db_info(Db),
    Self = self(),
    Pids = [spawn_link(fun() ->
                Id = <<"w", (integer_to_binary(I))/binary>>,
                R = case I rem 3 of
                    0 -> barrel_docdb:put_doc(Db, #{<<"id">> => Id}, #{sync => true});
                    1 -> barrel_docdb:put_docs(Db, [#{<<"id">> => Id}]);
                    2 -> barrel_docdb:delete_doc(Db, <<"a">>)
                end,
                Self ! {self(), R}
            end) || I <- lists:seq(1, 48)],
    Results = [receive {P, R} -> R end || P <- Pids],
    ?assertEqual([], [R || R <- Results, R =/= {error, read_only}]),
    {ok, #{write_groups := After}} = barrel_docdb:db_info(Db),
    ?assertEqual(Before, After),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"a">>).

attachment_writes_refused(Config) ->
    Db = ?config(db, Config),
    ?assertEqual({error, read_only},
                 barrel_docdb:put_attachment(Db, <<"a">>, <<"g">>, <<"x">>)),
    ?assertEqual({error, read_only},
                 barrel_docdb:delete_attachment(Db, <<"a">>, <<"f.txt">>)),
    ?assertEqual({error, read_only},
                 barrel_docdb:open_attachment_writer(Db, <<"a">>, <<"g">>,
                                                     <<"text/plain">>)),
    {ok, <<"hello">>} = barrel_docdb:get_attachment(Db, <<"a">>, <<"f.txt">>).

no_maintenance_timers(Config) ->
    Db = ?config(db, Config),
    {ok, Pid} = barrel_docdb:open_db(Db),
    %% compaction/retention/ttl timers are not armed on a read-only db
    State = sys:get_state(Pid),
    Timers = [element(I, State) || I <- lists:seq(2, tuple_size(State)),
                                   is_reference(element(I, State))],
    ?assertEqual([], Timers).

%% A directory copied under another name with an import sidecar reads
%% under the source keyspace, has no timeline parent, the source id the
%% sidecar carries, and cannot be branched.
import_copy_under_new_name(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    {ok, SrcId} = barrel_docdb:db_instance_id(Db),
    ok = barrel_docdb:close_db(Db),
    Copy = <<"ro_imported_copy">>,
    Src = filename:join(DataDir, binary_to_list(Db)),
    Dst = filename:join(DataDir, binary_to_list(Copy)),
    ok = copy_tree(Src, Dst),
    Sid = <<"0123456789abcdef">>,
    ok = barrel_keyspace:write_meta(Dst, #{
        keyspace => Db, parent => Db, kind => import, source_id => Sid,
        fork_hlc => barrel_hlc:encode(barrel_hlc:new_hlc())}),
    {ok, _} = barrel_docdb:create_db(Copy, #{data_dir => DataDir,
                                             read_only => true}),
    try
        {ok, Info} = barrel_docdb:db_info(Copy),
        ?assertEqual(Db, maps:get(keyspace, Info)),
        ?assertNot(maps:is_key(parent, Info)),
        {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Copy, <<"b">>),
        {ok, CopyId} = barrel_docdb:db_instance_id(Copy),
        ?assertNotEqual(SrcId, CopyId),
        ?assertEqual(Sid, CopyId),
        ?assertEqual({error, cannot_branch_a_branch},
                     barrel_docdb:branch_db(Copy, <<"ro_copy_branch">>, #{}))
    after
        _ = barrel_docdb:delete_db(Copy, #{data_dir => DataDir})
    end.

%% OpenForReadOnly: open, reads and close create, rewrite or remove no
%% file (no LOG, MANIFEST, OPTIONS or WAL flush).
files_untouched(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    ok = barrel_docdb:close_db(Db),
    Dir = filename:join(DataDir, binary_to_list(Db)),
    Before = tree(Dir),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir,
                                           read_only => true}),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"a">>),
    {ok, <<"hello">>} = barrel_docdb:get_attachment(Db, <<"a">>, <<"f.txt">>),
    {ok, _, _} = barrel_docdb:get_changes(Db, first),
    ok = barrel_docdb:close_db(Db),
    ?assertEqual(Before, tree(Dir)).

%% A read-only open never creates a database.
missing_store_refused(Config) ->
    DataDir = ?config(data_dir, Config),
    Absent = <<"ro_absent">>,
    ?assertMatch({error, {store_open_failed,
                          {read_only_store_missing, _}}},
                 barrel_docdb:create_db(Absent, #{data_dir => DataDir,
                                                  read_only => true})),
    ?assertNot(filelib:is_dir(filename:join(DataDir, "ro_absent/docs"))).

%% A store missing a column family (older version) needs one writable
%% open before it can be served read only.
missing_cf_needs_upgrade(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    ok = barrel_docdb:close_db(Db),
    Docs = filename:join([DataDir, binary_to_list(Db), "docs"]),
    {ok, #{ref := R, local_cf := Local} = Ref} =
        barrel_store_rocksdb:open(Docs, #{}),
    ok = rocksdb:drop_column_family(R, Local),
    ok = barrel_store_rocksdb:close(Ref),
    Before = tree(Docs),
    ?assertMatch({error, {store_open_failed,
                          {read_only_upgrade_needed,
                           #{missing_cfs := ["local"]}}}},
                 barrel_docdb:create_db(Db, #{data_dir => DataDir,
                                              read_only => true})),
    ?assertEqual(Before, tree(Docs)),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir}),
    ok = barrel_docdb:close_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir,
                                           read_only => true}),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"a">>).

tree(Dir) ->
    lists:sort(filelib:fold_files(
                 Dir, ".*", true,
                 fun(F, Acc) ->
                         {ok, B} = file:read_file(F),
                         [{F, erlang:md5(B)} | Acc]
                 end, [])).

copy_tree(Src, Dst) ->
    case filelib:is_dir(Src) of
        true ->
            ok = filelib:ensure_path(Dst),
            {ok, Names} = file:list_dir(Src),
            lists:foreach(fun(N) ->
                              ok = copy_tree(filename:join(Src, N),
                                             filename:join(Dst, N))
                          end, Names);
        false ->
            {ok, _} = file:copy(Src, Dst),
            ok
    end.
