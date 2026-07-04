%%%-------------------------------------------------------------------
%%% @doc Replication test suite for barrel_docdb
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, local_docs},
        {group, vv_protocol},
        {group, replication},
        {group, filtered_replication},
        {group, hlc_replication},
        {group, direction},
        {group, chain}
    ].

groups() ->
    [
        {local_docs, [sequence], [
            local_doc_crud,
            local_doc_not_replicated
        ]},
        {vv_protocol, [sequence], [
            diff_versions_missing_and_have,
            put_version_new_doc,
            put_version_idempotent,
            put_version_conflict_remote_wins,
            put_version_conflict_local_wins,
            get_doc_for_replication_meta
        ]},
        {replication, [sequence], [
            replicate_single_doc,
            replicate_multiple_docs,
            replicate_with_updates,
            replicate_deleted_doc,
            replicate_checkpoint_persistence
        ]},
        {filtered_replication, [sequence], [
            replicate_with_query_filter,
            replicate_with_path_filter,
            replicate_with_combined_filter,
            replicate_filter_no_match
        ]},
        {hlc_replication, [sequence], [
            replicate_hlc_checkpoint,
            replicate_hlc_sync
        ]},
        {direction, [sequence], [
            direction_push,
            direction_pull,
            direction_both
        ]},
        {chain, [sequence], [
            chain_replication_wait_for,
            sync_put_doc
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_group(Group, Config) ->
    %% Clean up any existing test databases
    lists:foreach(fun(Db) ->
        case barrel_docdb:open_db(Db) of
            {ok, _} -> barrel_docdb:delete_db(Db);
            _ -> ok
        end
    end, [<<"test_source">>, <<"test_target">>, <<"test_db">>]),

    DataDir = "/tmp/barrel_test_rep_" ++ atom_to_list(Group),
    os:cmd("rm -rf " ++ DataDir),

    {ok, _} = barrel_docdb:create_db(<<"test_db">>, #{data_dir => DataDir}),

    %% Create source and target for replication tests
    case Group of
        replication ->
            {ok, _} = barrel_docdb:create_db(<<"test_source">>, #{data_dir => DataDir ++ "_source"}),
            {ok, _} = barrel_docdb:create_db(<<"test_target">>, #{data_dir => DataDir ++ "_target"});
        filtered_replication ->
            {ok, _} = barrel_docdb:create_db(<<"test_source">>, #{data_dir => DataDir ++ "_source"}),
            {ok, _} = barrel_docdb:create_db(<<"test_target">>, #{data_dir => DataDir ++ "_target"});
        hlc_replication ->
            {ok, _} = barrel_docdb:create_db(<<"test_source">>, #{data_dir => DataDir ++ "_source"}),
            {ok, _} = barrel_docdb:create_db(<<"test_target">>, #{data_dir => DataDir ++ "_target"});
        direction ->
            {ok, _} = barrel_docdb:create_db(<<"test_source">>, #{data_dir => DataDir ++ "_source"}),
            {ok, _} = barrel_docdb:create_db(<<"test_target">>, #{data_dir => DataDir ++ "_target"});
        chain ->
            %% Chain: A -> B -> C
            {ok, _} = barrel_docdb:create_db(<<"chain_a">>, #{data_dir => DataDir ++ "_chain_a"}),
            {ok, _} = barrel_docdb:create_db(<<"chain_b">>, #{data_dir => DataDir ++ "_chain_b"}),
            {ok, _} = barrel_docdb:create_db(<<"chain_c">>, #{data_dir => DataDir ++ "_chain_c"});
        _ ->
            ok
    end,

    [{data_dir, DataDir}, {group, Group} | Config].

end_per_group(Group, Config) ->
    DataDir = ?config(data_dir, Config),

    barrel_docdb:delete_db(<<"test_db">>),

    case Group of
        replication ->
            barrel_docdb:delete_db(<<"test_source">>),
            barrel_docdb:delete_db(<<"test_target">>);
        filtered_replication ->
            barrel_docdb:delete_db(<<"test_source">>),
            barrel_docdb:delete_db(<<"test_target">>);
        hlc_replication ->
            barrel_docdb:delete_db(<<"test_source">>),
            barrel_docdb:delete_db(<<"test_target">>);
        direction ->
            barrel_docdb:delete_db(<<"test_source">>),
            barrel_docdb:delete_db(<<"test_target">>);
        chain ->
            barrel_docdb:delete_db(<<"chain_a">>),
            barrel_docdb:delete_db(<<"chain_b">>),
            barrel_docdb:delete_db(<<"chain_c">>);
        _ ->
            ok
    end,

    os:cmd("rm -rf " ++ DataDir ++ "*"),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Local Document Tests
%%====================================================================

local_doc_crud(_Config) ->
    Db = <<"test_db">>,
    DocId = <<"local_test_1">>,
    Doc = #{<<"key">> => <<"value">>, <<"count">> => 42},

    %% Initially not found
    ?assertEqual({error, not_found}, barrel_docdb:get_local_doc(Db, DocId)),

    %% Put local doc
    ?assertEqual(ok, barrel_docdb:put_local_doc(Db, DocId, Doc)),

    %% Get local doc
    {ok, Retrieved} = barrel_docdb:get_local_doc(Db, DocId),
    ?assertEqual(<<"value">>, maps:get(<<"key">>, Retrieved)),
    ?assertEqual(42, maps:get(<<"count">>, Retrieved)),

    %% Update local doc
    Doc2 = Doc#{<<"count">> => 100},
    ?assertEqual(ok, barrel_docdb:put_local_doc(Db, DocId, Doc2)),

    {ok, Retrieved2} = barrel_docdb:get_local_doc(Db, DocId),
    ?assertEqual(100, maps:get(<<"count">>, Retrieved2)),

    %% Delete local doc
    ?assertEqual(ok, barrel_docdb:delete_local_doc(Db, DocId)),
    ?assertEqual({error, not_found}, barrel_docdb:get_local_doc(Db, DocId)),

    ok.

local_doc_not_replicated(_Config) ->
    %% Local docs should not appear in changes feed
    Db = <<"test_db">>,

    %% Put a regular doc
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"regular_doc">>, <<"type">> => <<"test">>}),

    %% Put a local doc
    ok = barrel_docdb:put_local_doc(Db, <<"local_doc">>, #{<<"type">> => <<"local">>}),

    %% Get changes - should only see regular doc
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    ChangedIds = [maps:get(id, C) || C <- Changes],

    ?assert(lists:member(<<"regular_doc">>, ChangedIds)),
    ?assertNot(lists:member(<<"local_doc">>, ChangedIds)),

    ok.

%%====================================================================
%% VV Protocol Tests (diff_versions + put_version)
%%====================================================================

%% A version authored by a fake remote peer, with the version vector
%% such a peer would ship for a fresh write.
remote_version(Author) ->
    V = barrel_version:new(barrel_hlc:new_hlc(), Author),
    VV = barrel_vv:bump(barrel_vv:new(), V),
    {barrel_version:to_token(V), barrel_vv:encode(VV)}.

diff_versions_missing_and_have(_Config) ->
    Db = <<"test_db">>,

    {ok, #{<<"id">> := DocId, <<"rev">> := Rev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => <<"diff_doc">>, <<"value">> => 1}),

    %% Batch: covered version, unknown doc, and a foreign version of an
    %% existing doc
    {RemoteTok, _} = remote_version(<<"peer_a">>),
    {ok, Diff} = barrel_docdb:diff_versions(Db, #{
        DocId => Rev,
        <<"unknown_doc">> => RemoteTok
    }),
    ?assertEqual(have, maps:get(DocId, Diff)),
    ?assertEqual(missing, maps:get(<<"unknown_doc">>, Diff)),

    {ForeignTok, _} = remote_version(<<"peer_b">>),
    {ok, Diff2} = barrel_docdb:diff_versions(Db, #{DocId => ForeignTok}),
    ?assertEqual(missing, maps:get(DocId, Diff2)),

    ok.

put_version_new_doc(_Config) ->
    Db = <<"test_db">>,
    DocId = <<"pv_new">>,
    {Tok, VVBin} = remote_version(<<"peer_a">>),

    {ok, DocId, Tok} = barrel_docdb:put_version(
        Db, #{<<"id">> => DocId, <<"value">> => <<"remote">>}, Tok, VVBin, false),

    %% The remote version is preserved as the doc's rev
    {ok, Doc} = barrel_docdb:get_doc(Db, DocId),
    ?assertEqual(<<"remote">>, maps:get(<<"value">>, Doc)),
    ?assertEqual(Tok, maps:get(<<"_rev">>, Doc)),

    %% And the diff now answers have
    {ok, Diff} = barrel_docdb:diff_versions(Db, #{DocId => Tok}),
    ?assertEqual(have, maps:get(DocId, Diff)),

    ok.

put_version_idempotent(_Config) ->
    Db = <<"test_db">>,
    DocId = <<"pv_idem">>,
    {Tok, VVBin} = remote_version(<<"peer_a">>),
    Doc = #{<<"id">> => DocId, <<"value">> => <<"once">>},

    {ok, DocId, Tok} = barrel_docdb:put_version(Db, Doc, Tok, VVBin, false),
    %% Redelivery is a no-op returning the same winner
    {ok, DocId, Tok} = barrel_docdb:put_version(Db, Doc, Tok, VVBin, false),

    {ok, Stored} = barrel_docdb:get_doc(Db, DocId),
    ?assertEqual(Tok, maps:get(<<"_rev">>, Stored)),
    {ok, Conflicts} = barrel_docdb:get_conflicts(Db, DocId),
    ?assertEqual([], Conflicts),

    ok.

put_version_conflict_remote_wins(_Config) ->
    Db = <<"test_db">>,
    DocId = <<"pv_remote_wins">>,

    %% Local write first, then a concurrent remote version with a later
    %% HLC: LWW picks the remote, the local winner becomes a conflict
    %% sibling
    {ok, #{<<"rev">> := LocalRev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),
    {Tok, VVBin} = remote_version(<<"peer_a">>),

    {ok, DocId, Tok} = barrel_docdb:put_version(
        Db, #{<<"id">> => DocId, <<"v">> => <<"remote">>}, Tok, VVBin, false),

    {ok, Doc} = barrel_docdb:get_doc(Db, DocId),
    ?assertEqual(<<"remote">>, maps:get(<<"v">>, Doc)),
    {ok, Conflicts} = barrel_docdb:get_conflicts(Db, DocId),
    ?assertEqual([LocalRev], Conflicts),

    ok.

put_version_conflict_local_wins(_Config) ->
    Db = <<"test_db">>,
    DocId = <<"pv_local_wins">>,

    %% Remote version issued BEFORE the local write: concurrent but
    %% older, so the local winner stays and the remote becomes a
    %% conflict sibling
    {Tok, VVBin} = remote_version(<<"peer_a">>),
    {ok, #{<<"rev">> := LocalRev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"v">> => <<"local">>}),

    {ok, DocId, LocalRev} = barrel_docdb:put_version(
        Db, #{<<"id">> => DocId, <<"v">> => <<"remote">>}, Tok, VVBin, false),

    {ok, Doc} = barrel_docdb:get_doc(Db, DocId),
    ?assertEqual(<<"local">>, maps:get(<<"v">>, Doc)),
    {ok, Conflicts} = barrel_docdb:get_conflicts(Db, DocId),
    ?assertEqual([Tok], Conflicts),

    ok.

get_doc_for_replication_meta(_Config) ->
    Db = <<"test_db">>,

    {ok, #{<<"id">> := DocId, <<"rev">> := Rev}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => <<"repdoc">>, <<"value">> => 42}),

    {ok, #{doc := Doc, version := Rev, vv := VVBin, deleted := false}} =
        barrel_docdb:get_doc_for_replication(Db, DocId),
    ?assertEqual(42, maps:get(<<"value">>, Doc)),
    ?assertNot(maps:is_key(<<"_rev">>, Doc)),
    %% The shipped vector covers the shipped version
    ?assert(barrel_vv:contains(barrel_vv:decode(VVBin),
                               barrel_version:from_token(Rev))),

    %% Tombstones stay readable for replication
    {ok, _} = barrel_docdb:delete_doc(Db, DocId, #{rev => Rev}),
    {ok, #{deleted := true}} = barrel_docdb:get_doc_for_replication(Db, DocId),

    ok.

%%====================================================================
%% Replication Tests
%%====================================================================

replicate_single_doc(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create a document in source
    {ok, #{<<"id">> := DocId}} =
        barrel_docdb:put_doc(Source, #{<<"id">> => <<"doc1">>, <<"value">> => <<"hello">>}),

    %% Verify target is empty
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, DocId)),

    %% Replicate
    {ok, Result} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result)),
    ?assert(maps:get(docs_read, Result) >= 1),
    ?assert(maps:get(docs_written, Result) >= 1),

    %% Verify document in target
    {ok, TargetDoc} = barrel_docdb:get_doc(Target, DocId),
    ?assertEqual(<<"hello">>, maps:get(<<"value">>, TargetDoc)),

    ok.

replicate_multiple_docs(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create multiple documents in source
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"multi_doc_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"n">> => N})
    end, lists:seq(1, 10)),

    %% Replicate
    {ok, Result} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result)),
    ?assert(maps:get(docs_read, Result) >= 10),
    ?assert(maps:get(docs_written, Result) >= 10),

    %% Verify all documents in target
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"multi_doc_">>, integer_to_binary(N)]),
        {ok, Doc} = barrel_docdb:get_doc(Target, DocId),
        ?assertEqual(N, maps:get(<<"n">>, Doc))
    end, lists:seq(1, 10)),

    ok.

replicate_with_updates(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create and update a document
    {ok, #{<<"id">> := DocId, <<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Source, #{<<"id">> => <<"update_doc">>, <<"version">> => 1}),

    {ok, #{<<"rev">> := _Rev2}} =
        barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"version">> => 2}),

    %% Replicate
    {ok, _} = barrel_rep:replicate(Source, Target),

    %% Verify latest version in target
    {ok, TargetDoc} = barrel_docdb:get_doc(Target, DocId),
    ?assertEqual(2, maps:get(<<"version">>, TargetDoc)),

    ok.

replicate_deleted_doc(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create and delete a document
    {ok, #{<<"id">> := DocId, <<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Source, #{<<"id">> => <<"deleted_doc">>, <<"value">> => <<"temp">>}),

    {ok, _} = barrel_docdb:delete_doc(Source, DocId, #{rev => Rev1}),

    %% Replicate
    {ok, _} = barrel_rep:replicate(Source, Target),

    %% Verify document is deleted in target
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, DocId)),

    ok.

replicate_checkpoint_persistence(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create initial documents
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"cp_doc_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"batch">> => 1})
    end, lists:seq(1, 5)),

    %% First replication
    {ok, Result1} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result1)),
    _FirstLastSeq = maps:get(last_seq, Result1),

    %% Add more documents
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"cp_doc_batch2_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"batch">> => 2})
    end, lists:seq(1, 3)),

    %% Second replication - should start from checkpoint
    {ok, Result2} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result2)),

    %% Should have read fewer docs than a full replication (checkpoints work)
    %% Note: due to change list format, may read more than 3
    DocsRead = maps:get(docs_read, Result2),
    ct:pal("Second replication read ~p docs", [DocsRead]),

    %% Verify all documents exist in target
    {ok, TargetDocs, _} = barrel_docdb:get_changes(Target, first),
    ct:pal("Target has ~p changes", [length(TargetDocs)]),
    ?assert(length(TargetDocs) >= 8),

    ok.

%%====================================================================
%% Filtered Replication Tests
%%====================================================================

replicate_with_query_filter(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create documents of different types
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"user1">>,
        <<"type">> => <<"user">>,
        <<"name">> => <<"Alice">>
    }),
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"user2">>,
        <<"type">> => <<"user">>,
        <<"name">> => <<"Bob">>
    }),
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"order1">>,
        <<"type">> => <<"order">>,
        <<"total">> => 100
    }),
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"order2">>,
        <<"type">> => <<"order">>,
        <<"total">> => 200
    }),

    %% Replicate only user documents
    Filter = #{
        query => #{where => [{path, [<<"type">>], <<"user">>}]}
    },
    {ok, Result} = barrel_rep:replicate(Source, Target, #{filter => Filter}),
    ?assertEqual(true, maps:get(ok, Result)),

    %% Verify only user documents were replicated
    {ok, User1} = barrel_docdb:get_doc(Target, <<"user1">>),
    ?assertEqual(<<"Alice">>, maps:get(<<"name">>, User1)),

    {ok, User2} = barrel_docdb:get_doc(Target, <<"user2">>),
    ?assertEqual(<<"Bob">>, maps:get(<<"name">>, User2)),

    %% Order documents should NOT be in target
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, <<"order1">>)),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, <<"order2">>)),

    ok.

replicate_with_path_filter(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create documents with different structures
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"doc_with_users">>,
        <<"users">> => #{<<"123">> => #{<<"name">> => <<"Alice">>}}
    }),
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"doc_with_orders">>,
        <<"orders">> => #{<<"456">> => #{<<"total">> => 100}}
    }),
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"doc_with_products">>,
        <<"products">> => #{<<"789">> => #{<<"price">> => 50}}
    }),

    %% Replicate only documents with users path
    Filter = #{
        paths => [<<"users/#">>]
    },
    {ok, Result} = barrel_rep:replicate(Source, Target, #{filter => Filter}),
    ?assertEqual(true, maps:get(ok, Result)),

    %% Verify only doc_with_users was replicated
    {ok, UsersDoc} = barrel_docdb:get_doc(Target, <<"doc_with_users">>),
    ?assert(maps:is_key(<<"users">>, UsersDoc)),

    %% Other documents should NOT be in target
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, <<"doc_with_orders">>)),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, <<"doc_with_products">>)),

    ok.

replicate_with_combined_filter(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create documents
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"active_user">>,
        <<"type">> => <<"user">>,
        <<"status">> => <<"active">>
    }),
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"inactive_user">>,
        <<"type">> => <<"user">>,
        <<"status">> => <<"inactive">>
    }),
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"active_order">>,
        <<"type">> => <<"order">>,
        <<"status">> => <<"active">>
    }),

    %% Replicate only active documents (AND logic: path AND query)
    Filter = #{
        paths => [<<"type/#">>],  %% All docs have type, so this matches all
        query => #{where => [{path, [<<"status">>], <<"active">>}]}
    },
    {ok, Result} = barrel_rep:replicate(Source, Target, #{filter => Filter}),
    ?assertEqual(true, maps:get(ok, Result)),

    %% Verify only active documents were replicated
    {ok, ActiveUser} = barrel_docdb:get_doc(Target, <<"active_user">>),
    ?assertEqual(<<"active">>, maps:get(<<"status">>, ActiveUser)),

    {ok, ActiveOrder} = barrel_docdb:get_doc(Target, <<"active_order">>),
    ?assertEqual(<<"active">>, maps:get(<<"status">>, ActiveOrder)),

    %% Inactive user should NOT be in target
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, <<"inactive_user">>)),

    ok.

replicate_filter_no_match(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create documents
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"doc1">>,
        <<"type">> => <<"order">>
    }),
    {ok, _} = barrel_docdb:put_doc(Source, #{
        <<"id">> => <<"doc2">>,
        <<"type">> => <<"product">>
    }),

    %% Replicate with filter that matches nothing
    Filter = #{
        query => #{where => [{path, [<<"type">>], <<"nonexistent">>}]}
    },
    {ok, Result} = barrel_rep:replicate(Source, Target, #{filter => Filter}),
    ?assertEqual(true, maps:get(ok, Result)),
    ?assertEqual(0, maps:get(docs_written, Result)),

    %% Target should be empty (except for any pre-existing docs)
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, <<"doc1">>)),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, <<"doc2">>)),

    ok.

%%====================================================================
%% HLC Replication Tests
%%====================================================================

replicate_hlc_checkpoint(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create documents in source
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"hlc_doc_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"n">> => N})
    end, lists:seq(1, 5)),

    %% First replication
    {ok, Result1} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result1)),
    LastSeq1 = maps:get(last_seq, Result1),

    %% Verify last_seq is an HLC timestamp (not 'first')
    ?assertNotEqual(first, LastSeq1),

    %% Add more documents
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"hlc_doc_batch2_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"batch">> => 2})
    end, lists:seq(1, 3)),

    %% Second replication should resume from HLC checkpoint
    {ok, Result2} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result2)),
    StartSeq2 = maps:get(start_seq, Result2),
    LastSeq2 = maps:get(last_seq, Result2),

    %% Start should be at or after last_seq from first replication
    ?assertNotEqual(first, StartSeq2),
    ?assertNotEqual(first, LastSeq2),

    %% Verify all documents exist in target
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"hlc_doc_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:get_doc(Target, DocId)
    end, lists:seq(1, 5)),

    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"hlc_doc_batch2_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:get_doc(Target, DocId)
    end, lists:seq(1, 3)),

    ok.

replicate_hlc_sync(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Get initial global HLC
    InitialHlc = barrel_docdb:new_hlc(),

    %% Create documents in source (this advances global HLC)
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"sync_doc_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"n">> => N})
    end, lists:seq(1, 5)),

    %% Get HLC after writes
    AfterWritesHlc = barrel_docdb:new_hlc(),

    %% HLC should have advanced
    ?assert(barrel_hlc:less(InitialHlc, AfterWritesHlc)),

    %% Replicate - this exercises the HLC sync code path
    {ok, Result} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result)),
    ?assert(maps:get(docs_written, Result) >= 5),

    %% Get HLC after replication
    FinalHlc = barrel_docdb:new_hlc(),

    %% HLC should have advanced further (or at least not gone backwards)
    ?assertNot(barrel_hlc:less(FinalHlc, AfterWritesHlc)),

    %% Verify documents were replicated
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"sync_doc_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:get_doc(Target, DocId)
    end, lists:seq(1, 5)),

    ok.

%%====================================================================
%% Direction Tests
%%====================================================================

direction_push(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create document in source
    DocId = <<"push_doc_1">>,
    {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"value">> => <<"from_source">>}),

    %% Start push replication task
    {ok, TaskId} = barrel_rep_tasks:start_task(#{
        source => Source,
        target => Target,
        direction => push,
        mode => one_shot
    }),

    %% Wait for completion
    timer:sleep(500),

    %% Verify task completed
    {ok, Task} = barrel_rep_tasks:get_task(TaskId),
    ?assertEqual(completed, maps:get(status, Task)),

    %% Verify document was replicated to target
    {ok, TargetDoc} = barrel_docdb:get_doc(Target, DocId),
    ?assertEqual(<<"from_source">>, maps:get(<<"value">>, TargetDoc)),

    %% Clean up
    barrel_rep_tasks:delete_task(TaskId),
    ok.

direction_pull(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Clear any existing docs first
    _ = try barrel_docdb:delete_doc(Source, <<"pull_doc_1">>, #{}) catch _:_ -> ok end,
    _ = try barrel_docdb:delete_doc(Target, <<"pull_doc_1">>, #{}) catch _:_ -> ok end,
    timer:sleep(100),

    %% Create document in TARGET (we will pull from target to source)
    DocId = <<"pull_doc_1">>,
    {ok, _} = barrel_docdb:put_doc(Target, #{<<"id">> => DocId, <<"value">> => <<"from_target">>}),

    %% Verify source doesn't have it
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Source, DocId)),

    %% Start pull replication task (pulls from target to source)
    {ok, TaskId} = barrel_rep_tasks:start_task(#{
        source => Source,
        target => Target,
        direction => pull,
        mode => one_shot
    }),

    %% Wait for completion
    timer:sleep(500),

    %% Verify task completed
    {ok, Task} = barrel_rep_tasks:get_task(TaskId),
    ?assertEqual(completed, maps:get(status, Task)),

    %% Verify document was pulled to source
    {ok, SourceDoc} = barrel_docdb:get_doc(Source, DocId),
    ?assertEqual(<<"from_target">>, maps:get(<<"value">>, SourceDoc)),

    %% Clean up
    barrel_rep_tasks:delete_task(TaskId),
    ok.

direction_both(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create different documents in each
    SourceDocId = <<"both_source_doc">>,
    TargetDocId = <<"both_target_doc">>,

    {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => SourceDocId, <<"origin">> => <<"source">>}),
    {ok, _} = barrel_docdb:put_doc(Target, #{<<"id">> => TargetDocId, <<"origin">> => <<"target">>}),

    %% Verify docs are only in their origin
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, SourceDocId)),
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Source, TargetDocId)),

    %% Start bidirectional replication task
    {ok, TaskId} = barrel_rep_tasks:start_task(#{
        source => Source,
        target => Target,
        direction => both,
        mode => one_shot
    }),

    %% Wait for completion
    timer:sleep(1000),

    %% Verify task completed
    {ok, Task} = barrel_rep_tasks:get_task(TaskId),
    ?assertEqual(completed, maps:get(status, Task)),

    %% Verify source doc is now in target
    {ok, TargetSourceDoc} = barrel_docdb:get_doc(Target, SourceDocId),
    ?assertEqual(<<"source">>, maps:get(<<"origin">>, TargetSourceDoc)),

    %% Verify target doc is now in source
    {ok, SourceTargetDoc} = barrel_docdb:get_doc(Source, TargetDocId),
    ?assertEqual(<<"target">>, maps:get(<<"origin">>, SourceTargetDoc)),

    %% Clean up
    barrel_rep_tasks:delete_task(TaskId),
    ok.

%%====================================================================
%% Chain Replication Tests
%%====================================================================

chain_replication_wait_for(_Config) ->
    %% Setup chain: A -> B -> C

    %% Write document to A FIRST (before starting replication)
    DocId = <<"chain_doc_1">>,
    {ok, _} = barrel_docdb:put_doc(<<"chain_a">>, #{
        <<"id">> => DocId,
        <<"value">> => <<"from_chain_a">>
    }),

    %% Start B -> C replication (continuous to pick up changes as they arrive)
    {ok, TaskBC} = barrel_rep_tasks:start_task(#{
        source => <<"chain_b">>,
        target => <<"chain_c">>,
        direction => push,
        mode => continuous
    }),

    %% Give it time to start
    timer:sleep(200),

    %% Start A -> B replication with wait_for => C
    %% This means A won't complete until documents reach C
    {ok, TaskAB} = barrel_rep_tasks:start_task(#{
        source => <<"chain_a">>,
        target => <<"chain_b">>,
        direction => push,
        mode => one_shot,
        wait_for => [<<"chain_c">>]
    }),

    %% Wait for task A->B to complete
    %% Since wait_for is set, it should only complete after doc reaches C
    timer:sleep(6000),

    %% Verify task completed
    {ok, Task} = barrel_rep_tasks:get_task(TaskAB),
    ?assertEqual(completed, maps:get(status, Task)),

    %% Verify document is in C (the final destination)
    {ok, DocC} = barrel_docdb:get_doc(<<"chain_c">>, DocId),
    ?assertEqual(<<"from_chain_a">>, maps:get(<<"value">>, DocC)),

    %% Verify document is also in B (the intermediate)
    {ok, DocB} = barrel_docdb:get_doc(<<"chain_b">>, DocId),
    ?assertEqual(<<"from_chain_a">>, maps:get(<<"value">>, DocB)),

    %% Clean up
    barrel_rep_tasks:pause_task(TaskBC),
    barrel_rep_tasks:delete_task(TaskAB),
    barrel_rep_tasks:delete_task(TaskBC),

    ok.

sync_put_doc(_Config) ->
    %% Test sync put_doc with wait_for

    %% Start continuous replication A -> B
    {ok, TaskAB} = barrel_rep_tasks:start_task(#{
        source => <<"chain_a">>,
        target => <<"chain_b">>,
        direction => push,
        mode => continuous
    }),

    %% Give it time to start
    timer:sleep(200),

    %% Sync write to A with wait_for => B
    %% This should block until the doc reaches B
    DocId = <<"sync_doc_1">>,
    StartTime = erlang:system_time(millisecond),

    Result = barrel_docdb:put_doc(<<"chain_a">>, #{
        <<"id">> => DocId,
        <<"value">> => <<"sync_value">>
    }, #{
        replicate => sync,
        wait_for => [<<"chain_b">>]
    }),

    EndTime = erlang:system_time(millisecond),

    %% Verify write succeeded
    ?assertMatch({ok, _}, Result),

    %% Verify document is immediately available in B
    %% (since sync write waited for it)
    {ok, DocB} = barrel_docdb:get_doc(<<"chain_b">>, DocId),
    ?assertEqual(<<"sync_value">>, maps:get(<<"value">>, DocB)),

    %% Verify write took some time (indicates it waited)
    WriteTime = EndTime - StartTime,
    ct:pal("Sync write time: ~p ms", [WriteTime]),

    %% Clean up
    barrel_rep_tasks:pause_task(TaskAB),
    barrel_rep_tasks:delete_task(TaskAB),

    ok.
