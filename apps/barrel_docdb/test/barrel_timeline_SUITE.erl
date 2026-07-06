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
    aliased_writes/1,
    fork_now_basic/1,
    fork_isolation/1,
    fork_source_id_fresh/1,
    fork_attachments_visible/1,
    fork_under_write_load/1,
    fork_branch_of_branch_rejected/1,
    fork_dir_exists_error/1,
    fork_channels_inherited/1,
    branch_reopen_resumes_channels/1,
    inherited_rep_checkpoints_inert/1,
    delete_closed_branch_removes_files/1,
    delete_parent_branch_survives/1,
    list_branches_open_only/1,
    branch_sweeper_independent_floor/1,
    merge_after_branch_sweep/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [keyspace_identity, aliased_reads, aliased_writes,
     fork_now_basic, fork_isolation, fork_source_id_fresh,
     fork_attachments_visible, fork_under_write_load,
     fork_branch_of_branch_rejected, fork_dir_exists_error,
     fork_channels_inherited, branch_reopen_resumes_channels,
     inherited_rep_checkpoints_inert, delete_closed_branch_removes_files,
     delete_parent_branch_survives, list_branches_open_only,
     branch_sweeper_independent_floor, merge_after_branch_sweep].

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

%%====================================================================
%% Fork (branch_db at => now)
%%====================================================================

fork_now_basic(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    seed_docs(Db),
    {ok, _Pid} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        %% the branch sees everything up to the fork
        {ok, #{<<"type">> := <<"post">>}} =
            barrel_docdb:get_doc(Branch, <<"p1">>),
        {ok, Changes, _} = barrel_docdb:get_changes(Branch, first),
        ?assertEqual(4, length(Changes)),
        {ok, Rows, _} = barrel_docdb:find(Branch, #{
            where => [{path, [<<"type">>], <<"post">>}]}),
        ?assertEqual(1, length(Rows)),
        %% lineage in db_info
        {ok, Info} = barrel_docdb:db_info(Branch),
        ?assertEqual(Db, maps:get(keyspace, Info)),
        ?assertEqual(Db, maps:get(parent, Info)),
        ?assert(is_tuple(maps:get(fork_hlc, Info)))
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.

fork_isolation(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"shared">>,
                                         <<"v">> => 1}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        %% post-fork parent writes invisible on the branch
        {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"ponly">>}),
        {error, not_found} = barrel_docdb:get_doc(Branch, <<"ponly">>),
        %% and vice versa
        {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"bonly">>}),
        {error, not_found} = barrel_docdb:get_doc(Db, <<"bonly">>),
        %% divergent updates of the shared doc stay separate
        {ok, #{<<"_rev">> := RevB}} = barrel_docdb:get_doc(Branch,
                                                           <<"shared">>),
        {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"shared">>,
                                                 <<"v">> => 2,
                                                 <<"_rev">> => RevB}),
        {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"shared">>),
        {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Branch, <<"shared">>)
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.

fork_source_id_fresh(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        ?assertNotEqual(persistent_term:get({barrel_source, Db}),
                        persistent_term:get({barrel_source, Branch})),
        %% a branch write is authored by the branch
        {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"b">>}),
        {ok, [#{version := Tok} | _]} =
            barrel_docdb:get_doc_versions(Branch, <<"b">>),
        BranchSource = persistent_term:get({barrel_source, Branch}),
        ?assertMatch({_, _}, binary:match(Tok, BranchSource))
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.

fork_attachments_visible(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    seed_docs(Db),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        {ok, <<"blob">>} = barrel_docdb:get_attachment(Branch, <<"p1">>,
                                                       <<"f">>),
        {ok, [_], _} = barrel_docdb:att_changes(Branch, first),
        %% attachment writes diverge too
        {ok, _} = barrel_docdb:put_attachment(Branch, <<"p1">>,
                                              <<"bnew">>, <<"br">>),
        {error, not_found} = barrel_docdb:get_attachment_info(
            Db, <<"p1">>, <<"bnew">>)
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.

fork_under_write_load(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    %% hammer the parent while forking
    Self = self(),
    Writer = spawn_link(fun() -> write_loop(Db, 1, Self) end),
    timer:sleep(50),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    Writer ! stop,
    receive {written, _N} -> ok after 5000 -> ct:fail(writer_hung) end,
    try
        {ok, Info} = barrel_docdb:db_info(Branch),
        ForkHlc = maps:get(fork_hlc, Info),
        %% every change on the branch is at or before the fork instant
        {ok, Changes, _} = barrel_docdb:get_changes(Branch, first),
        ?assert(length(Changes) >= 1),
        lists:foreach(
            fun(#{hlc := Hlc}) ->
                ?assertNot(barrel_hlc:less(ForkHlc, Hlc))
            end,
            Changes)
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.

write_loop(Db, N, Owner) ->
    receive
        stop -> Owner ! {written, N}
    after 0 ->
        Id = <<"load", (integer_to_binary(N))/binary>>,
        {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id}),
        write_loop(Db, N + 1, Owner)
    end.

fork_branch_of_branch_rejected(Config) ->
    Db = ?config(db, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        ?assertEqual({error, cannot_branch_a_branch},
                     barrel_docdb:branch_db(Branch, <<Db/binary, "_bb">>,
                                            #{}))
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.

fork_dir_exists_error(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = <<Db/binary, "_b">>,
    ok = filelib:ensure_path(filename:join(DataDir,
                                           binary_to_list(Branch))),
    ?assertEqual({error, branch_dir_exists},
                 barrel_docdb:branch_db(Db, Branch, #{})),
    ok = file:del_dir_r(filename:join(DataDir, binary_to_list(Branch))),
    ok.

fork_channels_inherited(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = <<Db/binary, "_b">>,
    Channels = #{<<"posts">> => [<<"type/post">>]},
    ok = barrel_docdb:close_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir,
                                           channels => Channels}),
    seed_docs(Db),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        %% pre-fork channel rows are visible
        {ok, [_], _} = barrel_docdb:get_changes(
            Branch, first, #{channel => <<"posts">>}),
        %% and post-fork branch writes keep feeding the channel
        {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"p2">>,
                                                 <<"type">> => <<"post">>}),
        {ok, Two, _} = barrel_docdb:get_changes(
            Branch, first, #{channel => <<"posts">>}),
        ?assertEqual(2, length(Two))
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.

branch_reopen_resumes_channels(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = <<Db/binary, "_b">>,
    Channels = #{<<"posts">> => [<<"type/post">>]},
    ok = barrel_docdb:close_db(Db),
    {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir,
                                           channels => Channels}),
    seed_docs(Db),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        ok = barrel_docdb:close_db(Branch),
        %% reopen contract: caller supplies config again
        {ok, _} = barrel_docdb:create_db(Branch, #{
            data_dir => DataDir, channels => Channels}),
        {ok, Info} = barrel_docdb:db_info(Branch),
        ?assertEqual(Db, maps:get(keyspace, Info)),
        {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"p2">>,
                                                 <<"type">> => <<"post">>}),
        {ok, Two, _} = barrel_docdb:get_changes(
            Branch, first, #{channel => <<"posts">>}),
        ?assertEqual(2, length(Two))
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.

inherited_rep_checkpoints_inert(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = <<Db/binary, "_b">>,
    Target = <<Db/binary, "_t">>,
    {ok, _} = barrel_docdb:create_db(Target, #{data_dir => DataDir}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"b">>}),
    %% parent replicates to the target and checkpoints
    {ok, #{docs_read := 2}} = barrel_rep:replicate(Db, Target),
    {ok, #{docs_read := 0}} = barrel_rep:replicate(Db, Target),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        %% the branch inherited the parent's checkpoint doc; its own
        %% rep id differs, so its run writes a SECOND checkpoint doc
        %% instead of resuming the parent's
        {ok, #{docs_written := 0}} = barrel_rep:replicate(Branch,
                                                          Target),
        %% two inherited (doc + att, parent rep id) and two fresh
        %% (branch rep id): four distinct ids, no resumption
        {ok, CkptIds} = barrel_docdb:fold_local_docs(
            Branch, <<"replication-checkpoint-">>,
            fun(Id, _Doc, Acc) -> [Id | Acc] end, []),
        ?assertEqual(4, length(lists:usort(CkptIds))),
        %% a branch-only write still flows on the next run
        {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"c">>}),
        {ok, #{docs_written := 1}} = barrel_rep:replicate(Branch,
                                                          Target),
        {ok, _} = barrel_docdb:get_doc(Target, <<"c">>)
    after
        _ = barrel_docdb:delete_db(Branch),
        _ = barrel_docdb:delete_db(Target)
    end,
    ok.

%%====================================================================
%% Lifecycle
%%====================================================================

delete_closed_branch_removes_files(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    ok = barrel_docdb:close_db(Branch),
    BranchPath = filename:join(DataDir, binary_to_list(Branch)),
    ?assert(filelib:is_dir(BranchPath)),
    %% a closed db's files are located under its data_dir and removed
    ok = barrel_docdb:delete_db(Branch, #{data_dir => DataDir}),
    ?assertNot(filelib:is_dir(BranchPath)),
    ok.

delete_parent_branch_survives(Config) ->
    Db = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = <<Db/binary, "_b">>,
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"a">>,
                                         <<"v">> => 1}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    try
        %% hard links make the branch independent of the parent's files
        ok = barrel_docdb:delete_db(Db),
        {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Branch, <<"a">>),
        {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"b">>}),
        %% and it survives a close + reopen after the parent is gone
        ok = barrel_docdb:close_db(Branch),
        {ok, _} = barrel_docdb:create_db(Branch,
                                         #{data_dir => DataDir}),
        {ok, _} = barrel_docdb:get_doc(Branch, <<"b">>)
    after
        _ = barrel_docdb:delete_db(Branch),
        {ok, _} = barrel_docdb:create_db(Db, #{data_dir => DataDir})
    end,
    ok.

list_branches_open_only(Config) ->
    Db = ?config(db, Config),
    Branch1 = <<Db/binary, "_b1">>,
    Branch2 = <<Db/binary, "_b2">>,
    {ok, _} = barrel_docdb:branch_db(Db, Branch1, #{}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch2, #{}),
    try
        ?assertEqual([Branch1, Branch2],
                     lists:sort(barrel_docdb:list_branches(Db))),
        ok = barrel_docdb:close_db(Branch2),
        ?assertEqual([Branch1], barrel_docdb:list_branches(Db)),
        ?assertEqual([], barrel_docdb:list_branches(Branch1))
    after
        _ = barrel_docdb:delete_db(Branch1),
        _ = barrel_docdb:delete_db(Branch2, #{data_dir =>
                                              ?config(data_dir, Config)})
    end,
    ok.

%%====================================================================
%% Retention interplay
%%====================================================================

branch_sweeper_independent_floor(Config) ->
    Db0 = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = <<Db0/binary, "_b">>,
    ok = barrel_docdb:close_db(Db0),
    {ok, _} = barrel_docdb:create_db(Db0, #{data_dir => DataDir,
                                            retention_period => 1}),
    {ok, _} = barrel_docdb:put_doc(Db0, #{<<"id">> => <<"a">>}),
    {ok, _} = barrel_docdb:branch_db(Db0, Branch, #{}),
    try
        timer:sleep(1100),
        %% sweeping the BRANCH advances its own floor only
        {ok, _} = barrel_docdb:sweep_retention(Branch),
        {ok, BInfo} = barrel_docdb:db_info(Branch),
        ?assertNotEqual(undefined,
                        maps:get(history_floor, BInfo, undefined)),
        {ok, PInfo} = barrel_docdb:db_info(Db0),
        ?assertEqual(undefined,
                     maps:get(history_floor, PInfo, undefined)),
        %% and sweeping the parent leaves the branch's floor alone
        {ok, _} = barrel_docdb:sweep_retention(Db0),
        {ok, BInfo2} = barrel_docdb:db_info(Branch),
        ?assertEqual(maps:get(history_floor, BInfo),
                     maps:get(history_floor, BInfo2))
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.

merge_after_branch_sweep(Config) ->
    Db0 = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = <<Db0/binary, "_b">>,
    ok = barrel_docdb:close_db(Db0),
    {ok, _} = barrel_docdb:create_db(Db0, #{data_dir => DataDir,
                                            retention_period => 1}),
    {ok, #{<<"rev">> := DR}} = barrel_docdb:put_doc(
        Db0, #{<<"id">> => <<"doomed">>}),
    {ok, _} = barrel_docdb:branch_db(Db0, Branch, #{}),
    try
        {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"kept">>,
                                                 <<"v">> => 1}),
        {ok, _} = barrel_docdb:delete_doc(Branch, <<"doomed">>,
                                          #{rev => DR}),
        timer:sleep(1100),
        %% the sweep forgets the expired tombstone but keeps live state
        {ok, _} = barrel_docdb:sweep_retention(Branch),
        {ok, Report} = barrel_docdb:merge_branch(Branch, #{}),
        %% live branch edits still converge after the sweep
        {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db0, <<"kept">>),
        %% the forgotten delete never ships: the documented hazard
        %% behind "merge at least once per retention window"
        ?assertMatch(#{docs_written := 1}, Report),
        {ok, _} = barrel_docdb:get_doc(Db0, <<"doomed">>)
    after
        _ = barrel_docdb:delete_db(Branch)
    end,
    ok.
