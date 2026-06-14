%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb storage layer
%%%
%%% Tests the storage behaviour, key encoding, and RocksDB backend.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_store_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("hlc/include/hlc.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    %% Key encoding tests
    key_db_meta/1,
    key_doc_info/1,
    key_encoding_order/1,

    %% Path index key tests
    path_encode_decode_roundtrip/1,
    path_binary_ordering/1,
    path_integer_ordering/1,
    path_float_ordering/1,
    path_mixed_types_ordering/1,
    path_index_key_structure/1,

    %% RocksDB backend tests
    rocksdb_open_close/1,
    rocksdb_put_get/1,
    rocksdb_multi_get/1,
    rocksdb_delete/1,
    rocksdb_batch/1,
    rocksdb_fold/1,
    rocksdb_fold_range/1,
    rocksdb_snapshot/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, keys}, {group, path_keys}, {group, rocksdb}].

groups() ->
    [
        {keys, [sequence], [
            key_db_meta,
            key_doc_info,
            key_encoding_order
        ]},
        {path_keys, [parallel], [
            path_encode_decode_roundtrip,
            path_binary_ordering,
            path_integer_ordering,
            path_float_ordering,
            path_mixed_types_ordering,
            path_index_key_structure
        ]},
        {rocksdb, [sequence], [
            rocksdb_open_close,
            rocksdb_put_get,
            rocksdb_multi_get,
            rocksdb_delete,
            rocksdb_batch,
            rocksdb_fold,
            rocksdb_fold_range,
            rocksdb_snapshot
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(rocksdb, Config) ->
    %% Create a temporary directory for RocksDB
    TestDir = "/tmp/barrel_store_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    [{test_dir, TestDir} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(rocksdb, Config) ->
    %% Cleanup test directory
    TestDir = proplists:get_value(test_dir, Config),
    os:cmd("rm -rf " ++ TestDir),
    Config;
end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Key Encoding
%%====================================================================

key_db_meta(_Config) ->
    DbName = <<"testdb">>,

    %% Test UID key
    UidKey = barrel_store_keys:db_uid(DbName),
    ?assert(is_binary(UidKey)),

    %% Test docs count key
    DocsCountKey = barrel_store_keys:db_docs_count(DbName),
    ?assert(is_binary(DocsCountKey)),

    %% Test del count key
    DelCountKey = barrel_store_keys:db_del_count(DbName),
    ?assert(is_binary(DelCountKey)),

    %% Keys should be different
    ?assertNotEqual(UidKey, DocsCountKey),
    ?assertNotEqual(DocsCountKey, DelCountKey),

    ok.

key_doc_info(_Config) ->
    DbName = <<"testdb">>,
    DocId = <<"doc1">>,

    %% Test doc_info key
    InfoKey = barrel_store_keys:doc_info(DbName, DocId),
    ?assert(is_binary(InfoKey)),

    %% Test prefix
    Prefix = barrel_store_keys:doc_info_prefix(DbName),
    ?assert(is_binary(Prefix)),

    %% Key should start with prefix
    PrefixLen = byte_size(Prefix),
    <<Prefix:PrefixLen/binary, _/binary>> = InfoKey,

    %% Test decode
    DecodedDocId = barrel_store_keys:decode_doc_id(DbName, InfoKey),
    ?assertEqual(DocId, DecodedDocId),

    ok.

key_encoding_order(_Config) ->
    DbName = <<"testdb">>,

    %% Different key types should sort properly
    MetaKey = barrel_store_keys:db_uid(DbName),
    DocInfoKey = barrel_store_keys:doc_info(DbName, <<"doc1">>),
    DocRevKey = barrel_store_keys:doc_rev(DbName, <<"doc1">>, <<"1-abc">>),
    LocalKey = barrel_store_keys:local_doc(DbName, <<"_local/test">>),
    DocHlcKey = barrel_store_keys:doc_hlc(DbName, #timestamp{wall_time = 1000, logical = 1}),

    %% Meta keys should come first (prefix 0x01)
    ?assert(MetaKey < DocInfoKey),

    %% Doc info before rev (prefix 0x02 < 0x03)
    ?assert(DocInfoKey < DocRevKey),

    %% Rev before local (prefix 0x03 < 0x05)
    ?assert(DocRevKey < LocalKey),

    %% Local before HLC (prefix 0x05 < 0x0D)
    ?assert(LocalKey < DocHlcKey),

    ok.

%%====================================================================
%% Test Cases - RocksDB Backend
%%====================================================================

rocksdb_open_close(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/open_close_test",

    %% Open database
    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),
    ?assert(is_map(DbRef)),

    %% Close database
    ok = barrel_store_rocksdb:close(DbRef),

    ok.

rocksdb_put_get(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/put_get_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    Key = <<"test_key">>,
    Value = <<"test_value">>,

    %% Put value
    ok = barrel_store_rocksdb:put(DbRef, Key, Value),

    %% Get value
    {ok, RetrievedValue} = barrel_store_rocksdb:get(DbRef, Key),
    ?assertEqual(Value, RetrievedValue),

    %% Get non-existent key
    not_found = barrel_store_rocksdb:get(DbRef, <<"nonexistent">>),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_multi_get(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/multi_get_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    %% Put multiple values
    ok = barrel_store_rocksdb:put(DbRef, <<"key1">>, <<"value1">>),
    ok = barrel_store_rocksdb:put(DbRef, <<"key2">>, <<"value2">>),
    ok = barrel_store_rocksdb:put(DbRef, <<"key3">>, <<"value3">>),

    %% Multi-get existing keys
    Results = barrel_store_rocksdb:multi_get(DbRef, [<<"key1">>, <<"key2">>, <<"key3">>]),
    ?assertEqual([{ok, <<"value1">>}, {ok, <<"value2">>}, {ok, <<"value3">>}], Results),

    %% Multi-get with some non-existent keys
    Results2 = barrel_store_rocksdb:multi_get(DbRef, [<<"key1">>, <<"nonexistent">>, <<"key3">>]),
    ?assertEqual([{ok, <<"value1">>}, not_found, {ok, <<"value3">>}], Results2),

    %% Multi-get empty list
    Results3 = barrel_store_rocksdb:multi_get(DbRef, []),
    ?assertEqual([], Results3),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_delete(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/delete_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    Key = <<"delete_key">>,
    Value = <<"delete_value">>,

    %% Put then delete
    ok = barrel_store_rocksdb:put(DbRef, Key, Value),
    {ok, Value} = barrel_store_rocksdb:get(DbRef, Key),

    ok = barrel_store_rocksdb:delete(DbRef, Key),
    not_found = barrel_store_rocksdb:get(DbRef, Key),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_batch(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/batch_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    %% Write batch of operations
    Operations = [
        {put, <<"key1">>, <<"value1">>},
        {put, <<"key2">>, <<"value2">>},
        {put, <<"key3">>, <<"value3">>}
    ],

    ok = barrel_store_rocksdb:write_batch(DbRef, Operations),

    %% Verify all keys exist
    {ok, <<"value1">>} = barrel_store_rocksdb:get(DbRef, <<"key1">>),
    {ok, <<"value2">>} = barrel_store_rocksdb:get(DbRef, <<"key2">>),
    {ok, <<"value3">>} = barrel_store_rocksdb:get(DbRef, <<"key3">>),

    %% Batch with delete
    DeleteOps = [
        {delete, <<"key2">>},
        {put, <<"key4">>, <<"value4">>}
    ],

    ok = barrel_store_rocksdb:write_batch(DbRef, DeleteOps),

    not_found = barrel_store_rocksdb:get(DbRef, <<"key2">>),
    {ok, <<"value4">>} = barrel_store_rocksdb:get(DbRef, <<"key4">>),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_fold(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/fold_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    %% Insert data with common prefix
    Prefix = <<"prefix:">>,
    Operations = [
        {put, <<Prefix/binary, "a">>, <<"val_a">>},
        {put, <<Prefix/binary, "b">>, <<"val_b">>},
        {put, <<Prefix/binary, "c">>, <<"val_c">>},
        {put, <<"other:x">>, <<"val_x">>}
    ],

    ok = barrel_store_rocksdb:write_batch(DbRef, Operations),

    %% Fold over prefix
    CollectFun = fun(Key, Value, Acc) ->
        {ok, [{Key, Value} | Acc]}
    end,

    Result = barrel_store_rocksdb:fold(DbRef, Prefix, CollectFun, []),

    %% Should have 3 items (not "other:x")
    ?assertEqual(3, length(Result)),

    %% Verify all items have the prefix
    lists:foreach(
        fun({Key, _Value}) ->
            PrefixLen = byte_size(Prefix),
            <<Prefix:PrefixLen/binary, _/binary>> = Key
        end,
        Result
    ),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_fold_range(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/fold_range_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    %% Insert data
    Operations = [
        {put, <<"a">>, <<"1">>},
        {put, <<"b">>, <<"2">>},
        {put, <<"c">>, <<"3">>},
        {put, <<"d">>, <<"4">>},
        {put, <<"e">>, <<"5">>}
    ],

    ok = barrel_store_rocksdb:write_batch(DbRef, Operations),

    %% Fold range b to d (exclusive end)
    CollectFun = fun(Key, Value, Acc) ->
        {ok, [{Key, Value} | Acc]}
    end,

    Result = barrel_store_rocksdb:fold_range(DbRef, <<"b">>, <<"d">>, CollectFun, []),

    %% Should have b and c (d is exclusive upper bound)
    ?assertEqual(2, length(Result)),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_snapshot(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/snapshot_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    Key = <<"snapshot_key">>,
    Value1 = <<"value1">>,
    Value2 = <<"value2">>,

    %% Put initial value
    ok = barrel_store_rocksdb:put(DbRef, Key, Value1),

    %% Create snapshot
    {ok, Snapshot} = barrel_store_rocksdb:snapshot(DbRef),

    %% Update value
    ok = barrel_store_rocksdb:put(DbRef, Key, Value2),

    %% Get with snapshot should return old value
    {ok, SnapshotValue} = barrel_store_rocksdb:get_with_snapshot(DbRef, Key, Snapshot),
    ?assertEqual(Value1, SnapshotValue),

    %% Get without snapshot should return new value
    {ok, CurrentValue} = barrel_store_rocksdb:get(DbRef, Key),
    ?assertEqual(Value2, CurrentValue),

    %% Release snapshot
    ok = barrel_store_rocksdb:release_snapshot(Snapshot),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

%%====================================================================
%% Test Cases - Path Index Key Encoding
%%====================================================================

path_encode_decode_roundtrip(_Config) ->
    %% Test various path types round-trip correctly
    Paths = [
        [<<"field">>, <<"value">>],
        [<<"a">>, <<"b">>, <<"c">>],
        [<<"items">>, 0, <<"sku">>, <<"ABC">>],
        [<<"nested">>, 1, 2, 3],
        [<<"bool">>, true],
        [<<"bool">>, false],
        [<<"null">>, null],
        [<<"int">>, 42],
        [<<"int">>, -42],
        [<<"int">>, 0],
        [<<"float">>, 3.14],
        [<<"float">>, -2.718],
        [<<"mixed">>, 1, <<"a">>, true, 3.14]
    ],
    lists:foreach(
        fun(Path) ->
            Encoded = barrel_store_keys:encode_path(Path),
            Decoded = barrel_store_keys:decode_path(Encoded),
            ?assertEqual(Path, Decoded, {roundtrip_failed, Path})
        end,
        Paths
    ),
    ok.

path_binary_ordering(_Config) ->
    %% Binary paths should sort lexicographically
    Paths = [
        [<<"a">>, <<"x">>],
        [<<"a">>, <<"y">>],
        [<<"b">>, <<"x">>],
        [<<"aa">>, <<"x">>]
    ],
    Encoded = [barrel_store_keys:encode_path(P) || P <- Paths],
    Sorted = lists:sort(Encoded),

    %% Should be: a/x < a/y < aa/x < b/x
    Expected = [
        barrel_store_keys:encode_path([<<"a">>, <<"x">>]),
        barrel_store_keys:encode_path([<<"a">>, <<"y">>]),
        barrel_store_keys:encode_path([<<"aa">>, <<"x">>]),
        barrel_store_keys:encode_path([<<"b">>, <<"x">>])
    ],
    ?assertEqual(Expected, Sorted),
    ok.

path_integer_ordering(_Config) ->
    %% Integers should sort numerically, not lexicographically
    Paths = [
        [<<"n">>, -1000],
        [<<"n">>, -100],
        [<<"n">>, -10],
        [<<"n">>, -1],
        [<<"n">>, 0],
        [<<"n">>, 1],
        [<<"n">>, 10],
        [<<"n">>, 100],
        [<<"n">>, 1000]
    ],
    Encoded = [barrel_store_keys:encode_path(P) || P <- Paths],
    Sorted = lists:sort(Encoded),

    %% Should maintain numeric order
    ExpectedOrder = Paths,
    DecodedSorted = [barrel_store_keys:decode_path(E) || E <- Sorted],
    ?assertEqual(ExpectedOrder, DecodedSorted),
    ok.

path_float_ordering(_Config) ->
    %% Floats should sort numerically
    Paths = [
        [<<"f">>, -3.14],
        [<<"f">>, -1.0],
        [<<"f">>, 0.0],
        [<<"f">>, 1.0],
        [<<"f">>, 2.718],
        [<<"f">>, 3.14]
    ],
    Encoded = [barrel_store_keys:encode_path(P) || P <- Paths],
    Sorted = lists:sort(Encoded),

    %% Should maintain numeric order
    ExpectedOrder = Paths,
    DecodedSorted = [barrel_store_keys:decode_path(E) || E <- Sorted],
    ?assertEqual(ExpectedOrder, DecodedSorted),
    ok.

path_mixed_types_ordering(_Config) ->
    %% Different types should have consistent ordering:
    %% null < false < true < negative ints < 0 < positive ints < floats < binaries
    Paths = [
        [<<"v">>, null],
        [<<"v">>, false],
        [<<"v">>, true],
        [<<"v">>, -1],
        [<<"v">>, 0],
        [<<"v">>, 1],
        [<<"v">>, 1.5],
        [<<"v">>, <<"a">>]
    ],
    Encoded = [barrel_store_keys:encode_path(P) || P <- Paths],
    Sorted = lists:sort(Encoded),

    %% Should maintain type order
    ExpectedOrder = Paths,
    DecodedSorted = [barrel_store_keys:decode_path(E) || E <- Sorted],
    ?assertEqual(ExpectedOrder, DecodedSorted),
    ok.

path_index_key_structure(_Config) ->
    DbName = <<"testdb">>,
    Path = [<<"type">>, <<"user">>],
    DocId = <<"doc123">>,

    %% Test full key
    Key = barrel_store_keys:path_index_key(DbName, Path, DocId),
    ?assert(is_binary(Key)),

    %% Test prefix
    Prefix = barrel_store_keys:path_index_prefix(DbName, Path),
    PrefixLen = byte_size(Prefix),
    <<Prefix:PrefixLen/binary, _/binary>> = Key,

    %% Test end marker
    EndKey = barrel_store_keys:path_index_end(DbName, Path),
    ?assert(Key < EndKey),

    %% Keys with same path but different docids should sort
    Key1 = barrel_store_keys:path_index_key(DbName, Path, <<"aaa">>),
    Key2 = barrel_store_keys:path_index_key(DbName, Path, <<"bbb">>),
    ?assert(Key1 < Key2),

    %% Test doc_paths key
    DocPathsKey = barrel_store_keys:doc_paths_key(DbName, DocId),
    ?assert(is_binary(DocPathsKey)),

    ok.
