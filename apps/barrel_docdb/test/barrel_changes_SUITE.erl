%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb changes feed
%%%
%%% Tests HLC-based changes API and streaming.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_changes_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("hlc/include/hlc.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases - barrel_changes
-export([
    changes_write_read/1,
    changes_fold/1,
    changes_get_list/1,
    changes_limit/1,
    changes_doc_ids_filter/1,
    changes_path_filter/1,
    changes_path_filter_wildcard/1,
    changes_path_hlc_wildcard_prefix/1,
    changes_path_and_docid_filter/1,
    changes_query_filter/1,
    changes_query_filter_compare/1,
    changes_query_and_path_filter/1,
    changes_style/1,
    changes_last_seq/1,
    changes_count_since/1,
    changes_has_changes_since/1,
    changes_chunked_pagination/1
]).

%% Test cases - barrel_changes_stream
-export([
    stream_iterate_mode/1,
    stream_push_mode/1,
    push_mode_idle_no_stall/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, changes}, {group, stream}].

groups() ->
    [
        {changes, [sequence], [
            changes_write_read,
            changes_fold,
            changes_get_list,
            changes_limit,
            changes_doc_ids_filter,
            changes_path_filter,
            changes_path_filter_wildcard,
            changes_path_hlc_wildcard_prefix,
            changes_path_and_docid_filter,
            changes_query_filter,
            changes_query_filter_compare,
            changes_query_and_path_filter,
            changes_style,
            changes_last_seq,
            changes_count_since,
            changes_has_changes_since,
            changes_chunked_pagination
        ]},
        {stream, [sequence], [
            stream_iterate_mode,
            stream_push_mode,
            push_mode_idle_no_stall
        ]}
    ].

init_per_suite(Config) ->
    %% Start the HLC clock for tests
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    TestDir = "/tmp/barrel_changes_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    [{test_dir, TestDir} | Config].

end_per_group(_Group, Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    os:cmd("rm -rf " ++ TestDir),
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - barrel_changes (HLC-based)
%%====================================================================

%% Helper to create HLC timestamps for testing
make_test_hlc(WallTime, Logical) ->
    #timestamp{wall_time = WallTime, logical = Logical}.

changes_write_read(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_write_read",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write a change with HLC
    Hlc = make_test_hlc(1000, 1),
    DocInfo = #{
        id => <<"doc1">>,
        rev => <<"1-abc">>,
        deleted => false
    },
    ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo),

    %% Read it back via fold
    {ok, Changes, LastHlc} = barrel_changes:fold_changes(
        StoreRef, DbName, first,
        fun(Change, Acc) -> {ok, [Change | Acc]} end,
        []
    ),

    ?assertEqual(1, length(Changes)),
    [Change] = Changes,
    ?assertEqual(<<"doc1">>, maps:get(id, Change)),
    ?assertEqual(Hlc, maps:get(hlc, Change)),
    ?assertEqual(Hlc, LastHlc),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_fold(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_fold",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write multiple changes with HLC
    Changes = [
        {<<"doc1">>, make_test_hlc(1000, 1)},
        {<<"doc2">>, make_test_hlc(1000, 2)},
        {<<"doc3">>, make_test_hlc(1000, 3)}
    ],
    lists:foreach(
        fun({DocId, Hlc}) ->
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        Changes
    ),

    %% Fold from beginning
    {ok, AllChanges, _} = barrel_changes:fold_changes(
        StoreRef, DbName, first,
        fun(Change, Acc) -> {ok, [maps:get(id, Change) | Acc]} end,
        []
    ),
    ?assertEqual([<<"doc3">>, <<"doc2">>, <<"doc1">>], AllChanges),

    %% Fold from specific HLC (exclusive)
    {ok, PartialChanges, _} = barrel_changes:fold_changes(
        StoreRef, DbName, make_test_hlc(1000, 1),
        fun(Change, Acc) -> {ok, [maps:get(id, Change) | Acc]} end,
        []
    ),
    ?assertEqual([<<"doc3">>, <<"doc2">>], PartialChanges),

    %% Fold with stop
    {ok, StoppedChanges, _} = barrel_changes:fold_changes(
        StoreRef, DbName, first,
        fun(Change, Acc) ->
            case maps:get(id, Change) of
                <<"doc2">> -> {stop, [maps:get(id, Change) | Acc]};
                _ -> {ok, [maps:get(id, Change) | Acc]}
            end
        end,
        []
    ),
    ?assertEqual([<<"doc2">>, <<"doc1">>], StoppedChanges),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_get_list(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_get_list",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with HLC
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    %% Get all changes
    {ok, Changes, LastHlc} = barrel_changes:get_changes(StoreRef, DbName, first, #{}),
    ?assertEqual(5, length(Changes)),
    ?assertEqual(make_test_hlc(1000, 5), LastHlc),

    %% Verify order (ascending by default)
    Ids = [maps:get(id, C) || C <- Changes],
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>, <<"doc4">>, <<"doc5">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_limit(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_limit",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write 10 changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 10)
    ),

    %% Get with limit
    {ok, Changes, LastHlc} = barrel_changes:get_changes(StoreRef, DbName, first, #{limit => 3}),
    ?assertEqual(3, length(Changes)),
    ?assertEqual(make_test_hlc(1000, 3), LastHlc),

    Ids = [maps:get(id, C) || C <- Changes],
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_doc_ids_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_doc_ids",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    %% Filter to specific doc_ids
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{doc_ids => [<<"doc2">>, <<"doc4">>]}
    ),

    ?assertEqual(2, length(Changes)),
    Ids = [maps:get(id, C) || C <- Changes],
    ?assertEqual([<<"doc2">>, <<"doc4">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_path_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_path_filter",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with documents of different types
    DocInfos = [
        {<<"user1">>, 1, #{<<"type">> => <<"user">>, <<"name">> => <<"Alice">>}},
        {<<"order1">>, 2, #{<<"type">> => <<"order">>, <<"total">> => 100}},
        {<<"user2">>, 3, #{<<"type">> => <<"user">>, <<"name">> => <<"Bob">>}},
        {<<"product1">>, 4, #{<<"type">> => <<"product">>, <<"price">> => 50}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Filter to documents with type=user path
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{paths => [<<"type/#">>]}
    ),

    %% All documents have a type field, so all should match type/#
    ?assertEqual(4, length(Changes)),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_path_filter_wildcard(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_path_wildcard",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with nested document structures
    DocInfos = [
        {<<"u1">>, 1, #{<<"users">> => #{<<"123">> => #{<<"name">> => <<"Alice">>}}}},
        {<<"u2">>, 2, #{<<"users">> => #{<<"456">> => #{<<"name">> => <<"Bob">>}}}},
        {<<"o1">>, 3, #{<<"orders">> => #{<<"789">> => #{<<"total">> => 100}}}},
        {<<"p1">>, 4, #{<<"products">> => #{<<"abc">> => #{<<"price">> => 50}}}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Filter to documents with users path (using # wildcard)
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{paths => [<<"users/#">>]}
    ),

    %% Should match u1 and u2
    ?assertEqual(2, length(Changes)),
    Ids = lists:sort([maps:get(id, C) || C <- Changes]),
    ?assertEqual([<<"u1">>, <<"u2">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

%% @doc Test path_hlc wildcard prefix matching via get_changes_by_path.
%% This tests the fix for the bug where "users/#" pattern would only match
%% exact topic "users" instead of all topics starting with "users".
changes_path_hlc_wildcard_prefix(Config) ->
    TestDir = case proplists:get_value(test_dir, Config) of
        undefined -> "/tmp/barrel_changes_test_" ++ integer_to_list(erlang:system_time(millisecond));
        Dir -> Dir
    end,
    DbPath = TestDir ++ "/changes_path_hlc_wildcard",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with path index entries for hierarchical topics.
    %% Path analyzer generates topics from leaf values, so documents need actual values.
    %% Topics generated:
    %% - doc1: "type/user" -> topic "type"
    %% - doc2: "users/123/name/Alice" -> topics "users", "users/123", "users/123/name"
    %% - doc3: "users/123/profile/age/30" -> topics "users", "users/123", "users/123/profile", etc.
    %% - doc4: "users/456/name/Bob" -> topics "users", "users/456", "users/456/name"
    %% - doc5: "orders/789/total/100" -> topics "orders", "orders/789", etc.
    DocInfos = [
        {<<"doc1">>, 1, #{<<"type">> => <<"user">>}},
        {<<"doc2">>, 2, #{<<"users">> => #{<<"123">> => #{<<"name">> => <<"Alice">>}}}},
        {<<"doc3">>, 3, #{<<"users">> => #{<<"123">> => #{<<"profile">> => #{<<"age">> => 30}}}}},
        {<<"doc4">>, 4, #{<<"users">> => #{<<"456">> => #{<<"name">> => <<"Bob">>}}}},
        {<<"doc5">>, 5, #{<<"orders">> => #{<<"789">> => #{<<"total">> => 100}}}}
    ],

    %% Write both change entries and path index entries
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            %% Write change entry
            ChangeOps = barrel_changes:write_change_ops(DbName, Hlc, DocInfo),
            %% Write path index entries
            PathOps = barrel_changes:write_path_index_ops(DbName, Hlc, DocInfo),
            ok = barrel_store_rocksdb:write_batch(StoreRef, ChangeOps ++ PathOps)
        end,
        DocInfos
    ),

    %% Test 1: Exact topic match - "users" should match docs with topic prefix "users"
    %% doc2, doc3, doc4 all have topics starting with "users"
    {ok, ExactChanges, _} = barrel_changes:get_changes_by_path(
        StoreRef, DbName, <<"users">>, first, #{}
    ),
    ExactIds = lists:sort([maps:get(id, C) || C <- ExactChanges]),
    %% All docs with "users" in their path hierarchy should match
    ?assertEqual([<<"doc2">>, <<"doc3">>, <<"doc4">>], ExactIds),

    %% Test 2: Wildcard prefix match - "users/#" should match all docs with topics
    %% starting with "users" (including "users", "users/123", "users/123/profile", etc.)
    {ok, WildcardChanges, _} = barrel_changes:get_changes_by_path(
        StoreRef, DbName, <<"users/#">>, first, #{}
    ),
    WildcardIds = lists:sort([maps:get(id, C) || C <- WildcardChanges]),
    %% Should match doc2, doc3, doc4 (all with users* topics)
    ?assertEqual([<<"doc2">>, <<"doc3">>, <<"doc4">>], WildcardIds),

    %% Test 3: More specific wildcard - "users/123/#" should match docs with
    %% topics like "users/123", "users/123/name", "users/123/profile", etc.
    {ok, NestedChanges, _} = barrel_changes:get_changes_by_path(
        StoreRef, DbName, <<"users/123/#">>, first, #{}
    ),
    NestedIds = lists:sort([maps:get(id, C) || C <- NestedChanges]),
    %% Should match doc2 and doc3 (topics "users/123/..." )
    ?assertEqual([<<"doc2">>, <<"doc3">>], NestedIds),

    %% Test 4: Non-matching wildcard - "products/#" should return empty
    {ok, NoMatchChanges, _} = barrel_changes:get_changes_by_path(
        StoreRef, DbName, <<"products/#">>, first, #{}
    ),
    ?assertEqual([], NoMatchChanges),

    %% Test 5: "orders/#" should only match doc5
    {ok, OrdersChanges, _} = barrel_changes:get_changes_by_path(
        StoreRef, DbName, <<"orders/#">>, first, #{}
    ),
    OrdersIds = [maps:get(id, C) || C <- OrdersChanges],
    ?assertEqual([<<"doc5">>], OrdersIds),

    %% Test 6: Verify that wildcard captures more than exact match for nested topics
    %% "users/456" exact should find doc4
    {ok, User456Changes, _} = barrel_changes:get_changes_by_path(
        StoreRef, DbName, <<"users/456">>, first, #{}
    ),
    User456Ids = [maps:get(id, C) || C <- User456Changes],
    ?assertEqual([<<"doc4">>], User456Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_path_and_docid_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_path_docid",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes
    DocInfos = [
        {<<"user1">>, 1, #{<<"type">> => <<"user">>}},
        {<<"user2">>, 2, #{<<"type">> => <<"user">>}},
        {<<"order1">>, 3, #{<<"type">> => <<"order">>}},
        {<<"order2">>, 4, #{<<"type">> => <<"order">>}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Filter to user1 with type path - both filters must match (AND)
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{doc_ids => [<<"user1">>, <<"order1">>], paths => [<<"type/#">>]}
    ),

    %% Both filters: doc_ids AND paths must match
    ?assertEqual(2, length(Changes)),
    Ids = [maps:get(id, C) || C <- Changes],
    ?assertEqual([<<"user1">>, <<"order1">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_query_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_query_filter",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with documents of different types
    DocInfos = [
        {<<"user1">>, 1, #{<<"type">> => <<"user">>, <<"name">> => <<"Alice">>}},
        {<<"order1">>, 2, #{<<"type">> => <<"order">>, <<"total">> => 100}},
        {<<"user2">>, 3, #{<<"type">> => <<"user">>, <<"name">> => <<"Bob">>}},
        {<<"product1">>, 4, #{<<"type">> => <<"product">>, <<"price">> => 50}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Query for type=user documents
    Query = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{query => Query}
    ),

    %% Should match user1 and user2
    ?assertEqual(2, length(Changes)),
    Ids = lists:sort([maps:get(id, C) || C <- Changes]),
    ?assertEqual([<<"user1">>, <<"user2">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_query_filter_compare(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_query_compare",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with numeric values
    DocInfos = [
        {<<"order1">>, 1, #{<<"type">> => <<"order">>, <<"total">> => 50}},
        {<<"order2">>, 2, #{<<"type">> => <<"order">>, <<"total">> => 100}},
        {<<"order3">>, 3, #{<<"type">> => <<"order">>, <<"total">> => 150}},
        {<<"order4">>, 4, #{<<"type">> => <<"order">>, <<"total">> => 200}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Query for orders with total > 100
    Query = #{where => [{compare, [<<"total">>], '>', 100}]},
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{query => Query}
    ),

    %% Should match order3 and order4
    ?assertEqual(2, length(Changes)),
    Ids = lists:sort([maps:get(id, C) || C <- Changes]),
    ?assertEqual([<<"order3">>, <<"order4">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_query_and_path_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_query_path",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with documents of different types
    DocInfos = [
        {<<"user1">>, 1, #{<<"type">> => <<"user">>, <<"status">> => <<"active">>}},
        {<<"user2">>, 2, #{<<"type">> => <<"user">>, <<"status">> => <<"inactive">>}},
        {<<"order1">>, 3, #{<<"type">> => <<"order">>, <<"status">> => <<"active">>}},
        {<<"order2">>, 4, #{<<"type">> => <<"order">>, <<"status">> => <<"pending">>}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Query for active users - both path and query must match (AND logic)
    Query = #{where => [{path, [<<"status">>], <<"active">>}]},
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{
            paths => [<<"type/#">>],
            query => Query
        }
    ),

    %% All have type path, but only user1 and order1 have status=active
    ?assertEqual(2, length(Changes)),
    Ids = lists:sort([maps:get(id, C) || C <- Changes]),
    ?assertEqual([<<"order1">>, <<"user1">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_style(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_style",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write a change with multiple revisions (simulating conflicts)
    Hlc = make_test_hlc(1000, 1),
    DocInfo = #{
        id => <<"doc1">>,
        rev => <<"2-winner">>,
        deleted => false,
        revtree => #{} %% Empty revtree means no conflicts in current impl
    },
    ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo),

    %% With main_only style
    {ok, [Change], _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{style => main_only}
    ),
    ?assertEqual(1, length(maps:get(changes, Change))),

    %% With all_docs style (default)
    {ok, [Change2], _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{style => all_docs}
    ),
    ?assert(length(maps:get(changes, Change2)) >= 1),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_last_seq(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_last_seq",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Empty database - last_seq returns encoded min HLC
    LastSeq1 = barrel_changes:get_last_seq(StoreRef, DbName),
    ?assert(is_binary(LastSeq1)),
    ?assertEqual(12, byte_size(LastSeq1)),  %% HLC encoded is 12 bytes

    %% Decode it to verify it's min HLC
    LastHlc1 = barrel_changes:get_last_hlc(StoreRef, DbName),
    ?assertEqual(barrel_hlc:min(), LastHlc1),

    %% After adding changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    LastSeq2 = barrel_changes:get_last_seq(StoreRef, DbName),
    ?assert(is_binary(LastSeq2)),

    LastHlc2 = barrel_changes:get_last_hlc(StoreRef, DbName),
    ?assertEqual(make_test_hlc(1000, 5), LastHlc2),

    %% Verify encoding roundtrip
    ?assertEqual(LastSeq2, barrel_hlc:encode(LastHlc2)),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_count_since(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_count",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 10)
    ),

    %% Count from beginning (min HLC)
    ?assertEqual(10, barrel_changes:count_changes_since(StoreRef, DbName, barrel_hlc:min())),

    %% Count from middle
    ?assertEqual(5, barrel_changes:count_changes_since(StoreRef, DbName, make_test_hlc(1000, 5))),

    %% Count from end
    ?assertEqual(0, barrel_changes:count_changes_since(StoreRef, DbName, make_test_hlc(1000, 10))),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_has_changes_since(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_has_since",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Before any writes, has_changes_since should return true (safe fallback)
    ?assertEqual(true, barrel_changes:has_changes_since(StoreRef, DbName, barrel_hlc:min())),

    %% Write changes with HLC - this also updates the bucket hints
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(2000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    %% After writes, has_changes_since with old HLC should return true
    %% (bucket hint shows max_hlc > since)
    OldHlc = make_test_hlc(1000, 1),  %% Before all changes
    ?assertEqual(true, barrel_changes:has_changes_since(StoreRef, DbName, OldHlc)),

    %% has_changes_since with min HLC should return true
    ?assertEqual(true, barrel_changes:has_changes_since(StoreRef, DbName, barrel_hlc:min())),

    %% has_changes_since with the last written HLC should return true
    %% (safe fallback when no changes after)
    LastHlc = make_test_hlc(2000, 5),
    ?assertEqual(true, barrel_changes:has_changes_since(StoreRef, DbName, LastHlc)),

    %% Verify the bucket was written by checking the key directly
    NowSecs = erlang:system_time(second),
    BucketTs = NowSecs div 60,  %% 60 second granularity
    BucketKey = barrel_store_keys:change_bucket(DbName, BucketTs),
    {ok, BucketValue} = barrel_store_rocksdb:get(StoreRef, BucketKey),

    %% Bucket format: <<MinHlc:12/binary, MaxHlc:12/binary, Count:32>>
    ?assertEqual(28, byte_size(BucketValue)),
    <<_MinBin:12/binary, MaxBin:12/binary, Count:32>> = BucketValue,
    ?assertEqual(5, Count),  %% 5 changes written

    %% Verify max HLC in bucket matches last written change
    MaxHlc = barrel_hlc:decode(MaxBin),
    ?assertEqual(make_test_hlc(2000, 5), MaxHlc),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_chunked_pagination(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_chunked",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write 10 changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 10)
    ),

    %% Get first chunk with limit 3
    {ok, Changes1, Info1} = barrel_changes:get_changes_chunked(
        StoreRef, DbName, first, #{limit => 3}
    ),
    ?assertEqual(3, length(Changes1)),
    ?assertEqual(true, maps:get(has_more, Info1)),
    ?assert(maps:is_key(continuation, Info1)),

    %% Verify first chunk has doc1, doc2, doc3
    Ids1 = [maps:get(id, C) || C <- Changes1],
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], Ids1),

    %% Get second chunk using continuation token
    Continuation1 = maps:get(continuation, Info1),
    ?assertEqual(12, byte_size(Continuation1)),  %% Encoded HLC is 12 bytes

    {ok, Changes2, Info2} = barrel_changes:get_changes_chunked(
        StoreRef, DbName, Continuation1, #{limit => 3}
    ),
    ?assertEqual(3, length(Changes2)),
    ?assertEqual(true, maps:get(has_more, Info2)),

    %% Verify second chunk has doc4, doc5, doc6
    Ids2 = [maps:get(id, C) || C <- Changes2],
    ?assertEqual([<<"doc4">>, <<"doc5">>, <<"doc6">>], Ids2),

    %% Get third chunk
    Continuation2 = maps:get(continuation, Info2),
    {ok, Changes3, Info3} = barrel_changes:get_changes_chunked(
        StoreRef, DbName, Continuation2, #{limit => 3}
    ),
    ?assertEqual(3, length(Changes3)),
    ?assertEqual(true, maps:get(has_more, Info3)),

    %% Verify third chunk has doc7, doc8, doc9
    Ids3 = [maps:get(id, C) || C <- Changes3],
    ?assertEqual([<<"doc7">>, <<"doc8">>, <<"doc9">>], Ids3),

    %% Get final chunk
    Continuation3 = maps:get(continuation, Info3),
    {ok, Changes4, Info4} = barrel_changes:get_changes_chunked(
        StoreRef, DbName, Continuation3, #{limit => 3}
    ),
    ?assertEqual(1, length(Changes4)),
    ?assertEqual(false, maps:get(has_more, Info4)),
    ?assertNot(maps:is_key(continuation, Info4)),

    %% Verify final chunk has doc10
    Ids4 = [maps:get(id, C) || C <- Changes4],
    ?assertEqual([<<"doc10">>], Ids4),

    %% Verify last_hlc is set correctly
    LastHlc = maps:get(last_hlc, Info4),
    ?assertEqual(make_test_hlc(1000, 10), LastHlc),

    barrel_store_rocksdb:close(StoreRef),
    ok.

%%====================================================================
%% Test Cases - barrel_changes_stream
%%====================================================================

stream_iterate_mode(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/stream_iterate",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write some changes with HLC
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 3)
    ),

    %% Start iterate stream
    {ok, Stream} = barrel_changes_stream:start_link(
        StoreRef, DbName,
        #{mode => iterate, since => first}
    ),

    %% Read changes one by one
    {ok, C1} = barrel_changes_stream:next(Stream),
    ?assertEqual(<<"doc1">>, maps:get(id, C1)),

    {ok, C2} = barrel_changes_stream:next(Stream),
    ?assertEqual(<<"doc2">>, maps:get(id, C2)),

    {ok, C3} = barrel_changes_stream:next(Stream),
    ?assertEqual(<<"doc3">>, maps:get(id, C3)),

    %% No more changes
    done = barrel_changes_stream:next(Stream),

    barrel_store_rocksdb:close(StoreRef),
    ok.

stream_push_mode(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/stream_push",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write some changes with HLC
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 3)
    ),

    %% Start push stream
    {ok, Stream} = barrel_changes_stream:start_link(
        StoreRef, DbName,
        #{mode => push, since => first, owner => self(), batch_size => 10}
    ),

    %% Wait for changes to be pushed
    {ReqId, Changes} = barrel_changes_stream:await(Stream, 1000),
    ?assert(is_reference(ReqId)),
    ?assertEqual(3, length(Changes)),

    %% Acknowledge
    ok = barrel_changes_stream:ack(Stream, ReqId),

    %% Stop
    ok = barrel_changes_stream:stop(Stream),

    barrel_store_rocksdb:close(StoreRef),
    ok.

%% @doc Test push mode does not stall when stream is idle for multiple poll cycles
%% This verifies the fix for push mode idle handling where empty batches were not
%% correctly scheduling the next poll, causing the stream to stall
push_mode_idle_no_stall(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/push_idle_db",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"push_idle_test">>,

    %% Start push stream with short interval
    {ok, StreamPid} = barrel_changes_stream:start_link(
        StoreRef, DbName,
        #{mode => push, owner => self(), interval => 50, since => first, batch_size => 10}
    ),

    %% Wait for several poll cycles without any documents (empty batches)
    timer:sleep(500),  %% 10 poll cycles with no data

    %% Stream should still be alive
    true = is_process_alive(StreamPid),

    %% Now insert a document
    Hlc = make_test_hlc(1000, 0),
    barrel_changes:write_change(StoreRef, DbName, Hlc, #{
        id => <<"test_doc">>,
        rev => <<"1-abc">>,
        deleted => false
    }),

    %% Should receive the change (not stalled)
    receive
        {changes, _ReqId, Changes} ->
            1 = length(Changes),
            [#{id := <<"test_doc">>}] = Changes
    after 1000 ->
        ct:fail(stream_stalled)
    end,

    barrel_changes_stream:stop(StreamPid),
    barrel_store_rocksdb:close(StoreRef),
    ok.
