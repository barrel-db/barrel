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
-export([rocksdb_deleted_run_reads/1, legacy_bitmap_cf_dropped/1,
         legacy_bitmap_cf_read_only/1, default_compression_zstd/1,
         cf_options_applied/1]).

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
            rocksdb_snapshot,
            rocksdb_deleted_run_reads,
            legacy_bitmap_cf_dropped,
            legacy_bitmap_cf_read_only,
            default_compression_zstd,
            cf_options_applied
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

%% A long run of deletes becomes one range delete at flush
%% (min_tombstones_for_range_conversion). Reads must see exactly the
%% live keys, with the option on (the default) and off, and a snapshot
%% taken before the deletes still sees every key.
rocksdb_deleted_run_reads(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    lists:foreach(
      fun(Min) ->
          DbPath = TestDir ++ "/deleted_run_" ++ integer_to_list(Min),
          {ok, DbRef} = barrel_store_rocksdb:open(
                          DbPath, #{min_tombstones_for_range_conversion => Min}),
          #{ref := Ref} = DbRef,
          Key = fun(I) -> <<"hist:", I:32>> end,
          [ok = barrel_store_rocksdb:put(DbRef, Key(I), <<"v">>) || I <- lists:seq(1, 1000)],
          ok = rocksdb:flush(Ref, [{wait, true}]),
          Snap = rocksdb:snapshot(Ref),
          {ok, SnapRef} = Snap,
          [ok = barrel_store_rocksdb:delete(DbRef, Key(I)) || I <- lists:seq(1, 900)],
          ok = rocksdb:flush(Ref, [{wait, true}]),
          Live = barrel_store_rocksdb:fold_range(
                   DbRef, <<"hist:">>, <<"hist;">>,
                   fun(K, _V, Acc) -> {ok, [K | Acc]} end, []),
          ?assertEqual([Key(I) || I <- lists:seq(1000, 901, -1)], Live),
          ?assertEqual(not_found, barrel_store_rocksdb:get(DbRef, Key(500))),
          ?assertEqual({ok, <<"v">>},
                       rocksdb:get(Ref, Key(500), [{snapshot, SnapRef}])),
          ok = rocksdb:release_snapshot(SnapRef),
          ok = barrel_store_rocksdb:close(DbRef)
      end, [0, 64]).

%% A store created before this release has a "bitmap" column family.
legacy_store(Path, BitmapRows) ->
    {ok, Ref, [_, BitmapCF | _]} =
        rocksdb:open(Path, [{create_if_missing, true},
                            {create_missing_column_families, true}],
                     [{"default", []}, {"bitmap", []}, {"posting", []},
                      {"bodies", []}, {"local", []}]),
    ok = rocksdb:put(Ref, <<"doc">>, <<"v">>, []),
    [ok = rocksdb:put(Ref, BitmapCF, K, <<"x">>, []) || K <- BitmapRows],
    ok = rocksdb:close(Ref).

legacy_bitmap_cf_dropped(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    Empty = TestDir ++ "/legacy_empty",
    ok = legacy_store(Empty, []),
    {ok, DbRef} = barrel_store_rocksdb:open(Empty, #{}),
    ?assertEqual({ok, <<"v">>}, barrel_store_rocksdb:get(DbRef, <<"doc">>)),
    ok = barrel_store_rocksdb:close(DbRef),
    {ok, Cfs} = rocksdb:list_column_families(Empty, []),
    ?assertNot(lists:member("bitmap", Cfs)),
    %% reopens without it
    {ok, DbRef2} = barrel_store_rocksdb:open(Empty, #{}),
    ok = barrel_store_rocksdb:close(DbRef2),
    %% a non-empty one (never written by barrel) is kept
    Full = TestDir ++ "/legacy_full",
    ok = legacy_store(Full, [<<"k">>]),
    {ok, DbRef3} = barrel_store_rocksdb:open(Full, #{}),
    ok = barrel_store_rocksdb:close(DbRef3),
    {ok, Cfs3} = rocksdb:list_column_families(Full, []),
    ?assert(lists:member("bitmap", Cfs3)).

%% zstd is the codec rocksdb always builds; snappy needs the system library.
default_compression_zstd(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    ?assert(lists:member(zstd, rocksdb:supported_compressions())),
    Store = TestDir ++ "/zstd_store",
    {ok, DbRef} = barrel_store_rocksdb:open(Store, #{}),
    ok = barrel_store_rocksdb:close(DbRef),
    assert_zstd_only(Store),
    Att = TestDir ++ "/zstd_att",
    {ok, AttRef} = barrel_att_store_blob:open(Att, #{}),
    ok = barrel_att_store_blob:close(AttRef),
    assert_zstd_only(Att).

assert_zstd_only(Dir) ->
    Pairs = options_pairs(Dir),
    Codecs = [V || {K, V} <- Pairs,
                   K =:= <<"compression">> orelse K =:= <<"bottommost_compression">>],
    ?assertNotEqual([], Codecs),
    ?assertEqual([<<"kZSTD">>], lists:usort(Codecs)),
    %% blob files exist only where enabled; those compress with zstd
    Blobs = lists:usort([V || {<<"blob_compression_type">>, V} <- Pairs]),
    ?assert(lists:member(<<"kZSTD">>, Blobs)),
    ?assertEqual([], Blobs -- [<<"kZSTD">>, <<"kNoCompression">>]).

%% Column family options reach every column family, not just the db options.
cf_options_applied(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    Store = TestDir ++ "/cf_opts",
    {ok, DbRef} = barrel_store_rocksdb:open(Store, #{max_write_buffer_number => 5,
                                                     bloom_bits => 12}),
    ok = barrel_store_rocksdb:close(DbRef),
    Pairs = options_pairs(Store),
    Cfs = [V || {<<"[CFOptions">>, V} <- Pairs],
    ?assertEqual(4, length(Cfs)),
    ?assertEqual([<<"5">>],
                 lists:usort([V || {<<"max_write_buffer_number">>, V} <- Pairs])),
    Filters = [V || {<<"filter_policy">>, V} <- Pairs],
    ?assertEqual(4, length(Filters)),
    ?assertEqual([<<"bloomfilter:12:false">>], lists:usort(Filters)).

options_pairs(Dir) ->
    [Opts | _] = lists:reverse(lists:sort(filelib:wildcard(Dir ++ "/OPTIONS-*"))),
    {ok, Bin} = file:read_file(Opts),
    lists:flatmap(
      fun(L0) ->
              L = string:trim(L0),
              case binary:split(L, <<" ">>) of
                  [<<"[CFOptions">>, Name] -> [{<<"[CFOptions">>, Name}];
                  _ ->
                      case binary:split(L, <<"=">>) of
                          [K, V] -> [{string:trim(K), string:trim(V)}];
                          _ -> []
                      end
              end
      end, binary:split(Bin, <<"\n">>, [global])).

legacy_bitmap_cf_read_only(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    Path = TestDir ++ "/legacy_ro",
    ok = legacy_store(Path, []),
    {ok, DbRef} = barrel_store_rocksdb:open(Path, #{read_only => true}),
    ?assertEqual({ok, <<"v">>}, barrel_store_rocksdb:get(DbRef, <<"doc">>)),
    ok = barrel_store_rocksdb:close(DbRef),
    {ok, Cfs} = rocksdb:list_column_families(Path, []),
    ?assert(lists:member("bitmap", Cfs)).
