%%%-------------------------------------------------------------------
%%% @doc Timeline: keyspace identity for normal databases. Branch,
%%% lifecycle, and PITR cases join this suite in later steps.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_timeline_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    keyspace_identity/1,
    aliased_reads/1,
    aliased_writes/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [keyspace_identity, aliased_reads, aliased_writes].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_timeline",
    os:cmd("rm -rf " ++ DataDir),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    ok = application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

init_per_testcase(Case, Config) ->
    Db = <<"tl_", (atom_to_binary(Case, utf8))/binary>>,
    {ok, _} = barrel_docdb:create_db(Db, #{
        data_dir => ?config(data_dir, Config)
    }),
    [{db, Db} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

keyspace_identity(Config) ->
    Db = ?config(db, Config),
    {ok, Info} = barrel_docdb:db_info(Db),
    %% a normal database is its own keyspace and has no lineage
    ?assertEqual(Db, maps:get(keyspace, Info)),
    ?assertNot(maps:is_key(parent, Info)),
    ?assertNot(maps:is_key(fork_hlc, Info)),
    %% and nothing is registered in the keyspace registry
    ?assertEqual(Db, barrel_keyspace:resolve(Db)),
    ok.

%% A branch-shaped database made by hand: copy a populated db's dir
%% under another name and plant the TIMELINE sidecar. Every read
%% family must see the copied data through the keyspace indirection.
aliased_reads(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Alias = <<Db/binary, "_alias">>,
    Channels = #{<<"posts">> => [<<"type/post">>]},

    %% reopen the source with channels so the feed rows exist
    ok = barrel_docdb:close_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir,
                                           channels => Channels}),
    seed_docs(Db),
    ok = barrel_docdb:close_db(Db),

    plant_alias(DataDir, Db, Alias),
    {ok, _} = barrel_docdb:create_db(Alias, #{data_dir => DataDir,
                                              channels => Channels}),
    try
        {ok, Info} = barrel_docdb:db_info(Alias),
        ?assertEqual(Db, maps:get(keyspace, Info)),
        ?assertEqual(Db, maps:get(parent, Info)),
        ?assert(maps:is_key(fork_hlc, Info)),

        %% point + batch reads
        {ok, #{<<"type">> := <<"post">>}} =
            barrel_docdb:get_doc(Alias, <<"p1">>),
        [{ok, _}, {ok, _}] = barrel_docdb:get_docs(
            Alias, [<<"p1">>, <<"u1">>], #{}),
        %% query/find
        {ok, Rows, _} = barrel_docdb:find(Alias, #{
            where => [{path, [<<"type">>], <<"post">>}]}),
        ?assertEqual(1, length(Rows)),
        %% changes: full scan, path filter, channel
        %% three live docs plus the tombstone's feed row
        {ok, Changes, _} = barrel_docdb:get_changes(Alias, first),
        ?assertEqual(4, length(Changes)),
        {ok, PathChanges, _} = barrel_docdb:get_changes(
            Alias, first, #{paths => [<<"type/post">>]}),
        ?assertEqual(1, length(PathChanges)),
        {ok, ChanChanges, _} = barrel_docdb:get_changes(
            Alias, first, #{channel => <<"posts">>}),
        ?assertEqual(1, length(ChanChanges)),
        %% history + versions
        {ok, N} = barrel_docdb:fold_history(
            Alias, fun(_, Acc) -> {ok, Acc + 1} end, 0),
        ?assert(N >= 4),
        {ok, [_ | _]} = barrel_docdb:get_doc_versions(Alias, <<"p1">>),
        %% tombstone visible to replication reads
        {ok, #{deleted := true}} =
            barrel_docdb:get_doc_for_replication(Alias, <<"gone">>),
        %% outbox
        Pending = barrel_docdb:outbox_fold(
            Alias, <<"tag1">>, fun(E, Acc) -> {ok, [E | Acc]} end, []),
        ?assertEqual(1, length(Pending)),
        %% attachments
        {ok, <<"blob">>} = barrel_docdb:get_attachment(Alias, <<"p1">>,
                                                       <<"f">>),
        {ok, [AttEntry], _} = barrel_docdb:att_changes(Alias, first),
        ?assertEqual(<<"p1">>, maps:get(id, AttEntry)),
        %% local docs (copied: inherited but readable)
        {ok, #{<<"k">> := 1}} = barrel_docdb:get_local_doc(Alias,
                                                           <<"l1">>),
        %% conflicts API answers (none seeded)
        {ok, []} = barrel_docdb:get_conflicts(Alias, <<"p1">>)
    after
        _ = barrel_docdb:delete_db(Alias),
        {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir})
    end,
    ok.

%% Writes on the aliased database land under the keyspace and round
%% trip through every write family; its version author is its own.
aliased_writes(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Alias = <<Db/binary, "_alias">>,
    seed_docs(Db),
    SourceIdParent = persistent_term:get({barrel_source, Db}),
    ok = barrel_docdb:close_db(Db),
    plant_alias(DataDir, Db, Alias),
    {ok, _} = barrel_docdb:create_db(Alias, #{data_dir => DataDir}),
    try
        %% fresh author on the alias
        SourceIdAlias = persistent_term:get({barrel_source, Alias}),
        ?assertNotEqual(SourceIdParent, SourceIdAlias),
        %% create, update, read back
        {ok, _} = barrel_docdb:put_doc(Alias, #{<<"id">> => <<"new">>,
                                                <<"v">> => 1}),
        {ok, #{<<"_rev">> := Rev}} = barrel_docdb:get_doc(Alias,
                                                          <<"new">>),
        {ok, _} = barrel_docdb:put_doc(Alias, #{<<"id">> => <<"new">>,
                                                <<"v">> => 2,
                                                <<"_rev">> => Rev}),
        {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Alias, <<"new">>),
        %% the write is queryable and in the feed
        {ok, Rows, _} = barrel_docdb:find(Alias, #{
            where => [{path, [<<"v">>], 2}]}),
        ?assertEqual(1, length(Rows)),
        {ok, Changes, _} = barrel_docdb:get_changes(Alias, first),
        ?assert(lists:any(fun(C) -> maps:get(id, C) =:= <<"new">> end,
                          Changes)),
        %% update an inherited doc, then delete it
        {ok, #{<<"_rev">> := PRev}} = barrel_docdb:get_doc(Alias,
                                                           <<"p1">>),
        {ok, _} = barrel_docdb:delete_doc(Alias, <<"p1">>,
                                          #{rev => PRev}),
        {error, not_found} = barrel_docdb:get_doc(Alias, <<"p1">>),
        %% attachments + local docs write on the alias
        {ok, _} = barrel_docdb:put_attachment(Alias, <<"new">>,
                                              <<"na">>, <<"data">>),
        {ok, <<"data">>} = barrel_docdb:get_attachment(Alias, <<"new">>,
                                                       <<"na">>),
        ok = barrel_docdb:put_local_doc(Alias, <<"l2">>, #{<<"x">> => 1}),
        {ok, #{<<"x">> := 1}} = barrel_docdb:get_local_doc(Alias,
                                                           <<"l2">>)
    after
        _ = barrel_docdb:delete_db(Alias),
        {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir})
    end,
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Three docs (one per read family probe), one tombstone, an
%% attachment, an outbox-tagged write, and a local doc.
seed_docs(Db) ->
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"p1">>,
                                         <<"type">> => <<"post">>},
                                   #{outbox => [<<"tag1">>]}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"u1">>,
                                         <<"type">> => <<"user">>}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"n1">>,
                                         <<"n">> => 1}),
    {ok, #{<<"rev">> := GRev}} = barrel_docdb:put_doc(
        Db, #{<<"id">> => <<"gone">>}),
    {ok, _} = barrel_docdb:delete_doc(Db, <<"gone">>, #{rev => GRev}),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"p1">>, <<"f">>,
                                          <<"blob">>),
    ok = barrel_docdb:put_local_doc(Db, <<"l1">>, #{<<"k">> => 1}),
    ok.

plant_alias(DataDir, Db, Alias) ->
    Src = filename:join(DataDir, binary_to_list(Db)),
    Dst = filename:join(DataDir, binary_to_list(Alias)),
    os:cmd("rm -rf " ++ Dst),
    [] = os:cmd("cp -R " ++ Src ++ " " ++ Dst),
    ok = barrel_keyspace:write_meta(Dst, #{
        keyspace => Db,
        parent => Db,
        fork_hlc => barrel_hlc:encode(barrel_hlc:new_hlc())
    }).
