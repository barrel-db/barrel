%%%-------------------------------------------------------------------
%%% @doc Merge: a branch's edits ship to its parent through the VV
%%% protocol since the fork, incrementally across repeated merges,
%%% with conflicts resolving on the parent (LWW or its merger) and
%%% attachments riding along.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_timeline_merge_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    merge_basic/1,
    merge_idempotent/1,
    merge_incremental/1,
    merge_delete/1,
    merge_conflict_lww/1,
    merge_conflict_merger/1,
    merge_attachments/1,
    merge_pitr_branch/1,
    merge_after_parent_advanced/1,
    merge_not_a_branch/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [merge_basic, merge_idempotent, merge_incremental, merge_delete,
     merge_conflict_lww, merge_conflict_merger, merge_attachments,
     merge_pitr_branch, merge_after_parent_advanced,
     merge_not_a_branch].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DataDir = "/tmp/barrel_test_timeline_merge",
    os:cmd("rm -rf " ++ DataDir),
    [{data_dir, DataDir} | Config].

end_per_suite(Config) ->
    ok = application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(data_dir, Config)),
    ok.

init_per_testcase(Case, Config) ->
    Db = <<"mg_", (atom_to_binary(Case, utf8))/binary>>,
    {ok, _} = barrel_docdb:create_db(Db, #{
        data_dir => ?config(data_dir, Config)
    }),
    [{db, Db}, {branch, <<Db/binary, "_b">>} | Config].

end_per_testcase(_Case, Config) ->
    _ = barrel_docdb:delete_db(?config(branch, Config),
                               #{data_dir => ?config(data_dir, Config)}),
    _ = barrel_docdb:delete_db(?config(db, Config)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

merge_basic(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"pre">>}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"new">>,
                                             <<"v">> => 1}),
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Branch, <<"pre">>),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"pre">>,
                                             <<"edit">> => true,
                                             <<"_rev">> => R}),
    {ok, Report} = barrel_docdb:merge_branch(Branch, #{}),
    ?assertMatch(#{docs_written := 2}, Report),
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"new">>),
    {ok, #{<<"edit">> := true}} = barrel_docdb:get_doc(Db, <<"pre">>),
    ok.

merge_idempotent(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"a">>}),
    {ok, #{docs_written := 1}} = barrel_docdb:merge_branch(Branch, #{}),
    %% nothing new: the checkpointed merge writes nothing
    {ok, #{docs_written := 0}} = barrel_docdb:merge_branch(Branch, #{}),
    ok.

merge_incremental(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"a">>}),
    {ok, #{docs_written := 1}} = barrel_docdb:merge_branch(Branch, #{}),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"b">>}),
    {ok, #{docs_written := 1, docs_read := 1}} =
        barrel_docdb:merge_branch(Branch, #{}),
    {ok, _} = barrel_docdb:get_doc(Db, <<"a">>),
    {ok, _} = barrel_docdb:get_doc(Db, <<"b">>),
    ok.

merge_delete(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"doomed">>}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Branch,
                                                    <<"doomed">>),
    {ok, _} = barrel_docdb:delete_doc(Branch, <<"doomed">>,
                                      #{rev => R}),
    {ok, #{docs_written := 1}} = barrel_docdb:merge_branch(Branch, #{}),
    {error, not_found} = barrel_docdb:get_doc(Db, <<"doomed">>),
    {ok, #{deleted := true}} =
        barrel_docdb:get_doc_for_replication(Db, <<"doomed">>),
    ok.

merge_conflict_lww(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => <<"base">>}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    %% both sides edit: parent first, branch second (branch wins LWW)
    {ok, #{<<"_rev">> := PR}} = barrel_docdb:get_doc(Db, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => <<"parent">>,
                                         <<"_rev">> => PR}),
    {ok, #{<<"_rev">> := BR}} = barrel_docdb:get_doc(Branch, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"d">>,
                                             <<"v">> => <<"branch">>,
                                             <<"_rev">> => BR}),
    {ok, _} = barrel_docdb:merge_branch(Branch, #{}),
    %% deterministic LWW: the branch write is newer and wins; the
    %% parent's edit stays as a live conflict sibling
    {ok, #{<<"v">> := <<"branch">>}} = barrel_docdb:get_doc(Db,
                                                            <<"d">>),
    {ok, [_]} = barrel_docdb:get_conflicts(Db, <<"d">>),
    ok.

merge_conflict_merger(Config) ->
    Db0 = ?config(db, Config),
    DataDir = ?config(data_dir, Config),
    Branch = ?config(branch, Config),
    %% reopen the parent with a merger that unions the two bodies
    ok = barrel_docdb:close_db(Db0),
    Merger = fun(_Id, Local, Remote) ->
        {merge, maps:merge(Local, Remote)}
    end,
    {ok, _} = barrel_docdb:create_db(Db0, #{data_dir => DataDir,
                                            conflict_merger => Merger}),
    {ok, _} = barrel_docdb:put_doc(Db0, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:branch_db(Db0, Branch, #{}),
    {ok, #{<<"_rev">> := PR}} = barrel_docdb:get_doc(Db0, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Db0, #{<<"id">> => <<"d">>,
                                          <<"p">> => 1,
                                          <<"_rev">> => PR}),
    {ok, #{<<"_rev">> := BR}} = barrel_docdb:get_doc(Branch, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"d">>,
                                             <<"b">> => 1,
                                             <<"_rev">> => BR}),
    {ok, _} = barrel_docdb:merge_branch(Branch, #{}),
    %% merged body, no sibling left behind
    {ok, #{<<"p">> := 1, <<"b">> := 1}} = barrel_docdb:get_doc(Db0,
                                                               <<"d">>),
    {ok, []} = barrel_docdb:get_conflicts(Db0, <<"d">>),
    ok.

merge_attachments(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>}),
    {ok, _} = barrel_docdb:put_attachment(Db, <<"d">>, <<"pre">>,
                                          <<"old">>),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    {ok, _} = barrel_docdb:put_attachment(Branch, <<"d">>, <<"new">>,
                                          <<"fresh">>),
    ok = barrel_docdb:delete_attachment(Branch, <<"d">>, <<"pre">>),
    {ok, #{att_sync := AttStats}} = barrel_docdb:merge_branch(Branch,
                                                              #{}),
    %% only post-fork attachment work ships: one put, one delete
    ?assertMatch(#{atts_written := 1, atts_deleted := 1}, AttStats),
    {ok, <<"fresh">>} = barrel_docdb:get_attachment(Db, <<"d">>,
                                                    <<"new">>),
    {error, not_found} = barrel_docdb:get_attachment_info(Db, <<"d">>,
                                                          <<"pre">>),
    ok.

merge_pitr_branch(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 1}),
    {ok, _, T} = barrel_docdb:get_changes(Db, first),
    {ok, #{<<"_rev">> := R}} = barrel_docdb:get_doc(Db, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d">>,
                                         <<"v">> => 2,
                                         <<"_rev">> => R}),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{at => T}),
    %% the rewind itself is not merged: the parent keeps v2
    {ok, #{docs_written := 0}} = barrel_docdb:merge_branch(Branch, #{}),
    {ok, #{<<"v">> := 2}} = barrel_docdb:get_doc(Db, <<"d">>),
    %% a branch edit after the rewind ships normally (as a concurrent
    %% edit of v1 it conflicts with v2 and LWW picks the newer write)
    {ok, #{<<"_rev">> := BR}} = barrel_docdb:get_doc(Branch, <<"d">>),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"d">>,
                                             <<"v">> => 3,
                                             <<"_rev">> => BR}),
    {ok, #{docs_written := 1}} = barrel_docdb:merge_branch(Branch, #{}),
    {ok, #{<<"v">> := 3}} = barrel_docdb:get_doc(Db, <<"d">>),
    {ok, [_]} = barrel_docdb:get_conflicts(Db, <<"d">>),
    ok.

merge_after_parent_advanced(Config) ->
    Db = ?config(db, Config),
    Branch = ?config(branch, Config),
    {ok, _} = barrel_docdb:branch_db(Db, Branch, #{}),
    %% the parent moves on independently
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"ponly">>,
                                         <<"v">> => 1}),
    {ok, _} = barrel_docdb:put_doc(Branch, #{<<"id">> => <<"bonly">>}),
    {ok, #{docs_written := 1}} = barrel_docdb:merge_branch(Branch, #{}),
    %% merge never clobbers parent-only docs and ships branch-only ones
    {ok, #{<<"v">> := 1}} = barrel_docdb:get_doc(Db, <<"ponly">>),
    {ok, _} = barrel_docdb:get_doc(Db, <<"bonly">>),
    %% and the parent-only doc did not appear on the branch (one-way)
    {error, not_found} = barrel_docdb:get_doc(Branch, <<"ponly">>),
    ok.

merge_not_a_branch(Config) ->
    Db = ?config(db, Config),
    ?assertEqual({error, not_a_branch},
                 barrel_docdb:merge_branch(Db, #{})),
    ?assertEqual({error, not_found},
                 barrel_docdb:merge_branch(<<"nope">>, #{})),
    ok.
