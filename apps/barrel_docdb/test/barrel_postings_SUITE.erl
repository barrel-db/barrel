%% @doc Tests for barrel_postings module
-module(barrel_postings_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2
]).

%% Test cases
-export([
    open_and_keys/1,
    contains_exact/1,
    contains_bitmap/1,
    count_keys/1,
    to_binary_roundtrip/1,
    intersection_op/1,
    union_op/1,
    difference_op/1,
    intersect_all_op/1,
    intersection_count_op/1
]).

all() ->
    [{group, resource_api}].

groups() ->
    [
        {resource_api, [sequence], [
            open_and_keys,
            contains_exact,
            contains_bitmap,
            count_keys,
            to_binary_roundtrip,
            intersection_op,
            union_op,
            difference_op,
            intersect_all_op,
            intersection_count_op
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(rocksdb),
    %% Create a temporary database for testing
    DbPath = filename:join(?config(priv_dir, Config), "test_postings_db"),
    {ok, Db} = rocksdb:open(DbPath, [
        {create_if_missing, true},
        {merge_operator, posting_list_merge_operator}
    ]),
    [{db, Db}, {db_path, DbPath} | Config].

end_per_suite(Config) ->
    Db = ?config(db, Config),
    rocksdb:close(Db),
    ok.

init_per_group(resource_api, Config) ->
    Db = ?config(db, Config),
    %% Create test posting lists
    %% List A: doc1, doc2, doc3
    ok = rocksdb:merge(Db, <<"list_a">>, {posting_add, <<"doc1">>}, []),
    ok = rocksdb:merge(Db, <<"list_a">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"list_a">>, {posting_add, <<"doc3">>}, []),
    %% List B: doc2, doc3, doc4
    ok = rocksdb:merge(Db, <<"list_b">>, {posting_add, <<"doc2">>}, []),
    ok = rocksdb:merge(Db, <<"list_b">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"list_b">>, {posting_add, <<"doc4">>}, []),
    %% List C: doc3, doc5
    ok = rocksdb:merge(Db, <<"list_c">>, {posting_add, <<"doc3">>}, []),
    ok = rocksdb:merge(Db, <<"list_c">>, {posting_add, <<"doc5">>}, []),
    %% Get binaries
    {ok, BinA} = rocksdb:get(Db, <<"list_a">>, []),
    {ok, BinB} = rocksdb:get(Db, <<"list_b">>, []),
    {ok, BinC} = rocksdb:get(Db, <<"list_c">>, []),
    [{bin_a, BinA}, {bin_b, BinB}, {bin_c, BinC} | Config];
init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

%% Test cases

open_and_keys(Config) ->
    BinA = ?config(bin_a, Config),
    {ok, Postings} = barrel_postings:open(BinA),
    Keys = barrel_postings:keys(Postings),
    %% Keys should be sorted
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], Keys).

contains_exact(Config) ->
    BinA = ?config(bin_a, Config),
    {ok, Postings} = barrel_postings:open(BinA),
    ?assertEqual(true, barrel_postings:contains(Postings, <<"doc1">>)),
    ?assertEqual(true, barrel_postings:contains(Postings, <<"doc2">>)),
    ?assertEqual(true, barrel_postings:contains(Postings, <<"doc3">>)),
    ?assertEqual(false, barrel_postings:contains(Postings, <<"doc4">>)),
    ?assertEqual(false, barrel_postings:contains(Postings, <<"unknown">>)).

contains_bitmap(Config) ->
    BinA = ?config(bin_a, Config),
    {ok, Postings} = barrel_postings:open(BinA),
    %% Bitmap contains should match for present keys
    ?assertEqual(true, barrel_postings:bitmap_contains(Postings, <<"doc1">>)),
    ?assertEqual(true, barrel_postings:bitmap_contains(Postings, <<"doc2">>)),
    %% Note: bitmap_contains may have false positives for absent keys
    %% so we only verify it returns true for present keys.
    ?assertEqual(true, barrel_postings:bitmap_contains(Postings, <<"doc3">>)).

count_keys(Config) ->
    BinA = ?config(bin_a, Config),
    BinB = ?config(bin_b, Config),
    BinC = ?config(bin_c, Config),
    {ok, PostingsA} = barrel_postings:open(BinA),
    {ok, PostingsB} = barrel_postings:open(BinB),
    {ok, PostingsC} = barrel_postings:open(BinC),
    ?assertEqual(3, barrel_postings:count(PostingsA)),
    ?assertEqual(3, barrel_postings:count(PostingsB)),
    ?assertEqual(2, barrel_postings:count(PostingsC)).

to_binary_roundtrip(Config) ->
    BinA = ?config(bin_a, Config),
    {ok, Postings} = barrel_postings:open(BinA),
    %% Convert back to binary
    NewBin = barrel_postings:to_binary(Postings),
    %% Open the new binary and verify keys
    {ok, Postings2} = barrel_postings:open(NewBin),
    ?assertEqual(barrel_postings:keys(Postings), barrel_postings:keys(Postings2)).

intersection_op(Config) ->
    BinA = ?config(bin_a, Config),  %% doc1, doc2, doc3
    BinB = ?config(bin_b, Config),  %% doc2, doc3, doc4
    {ok, PostingsA} = barrel_postings:open(BinA),
    {ok, PostingsB} = barrel_postings:open(BinB),
    %% Intersection should be doc2, doc3
    {ok, Result} = barrel_postings:intersection(PostingsA, PostingsB),
    Keys = barrel_postings:keys(Result),
    ?assertEqual([<<"doc2">>, <<"doc3">>], Keys).

union_op(Config) ->
    BinA = ?config(bin_a, Config),  %% doc1, doc2, doc3
    BinB = ?config(bin_b, Config),  %% doc2, doc3, doc4
    {ok, PostingsA} = barrel_postings:open(BinA),
    {ok, PostingsB} = barrel_postings:open(BinB),
    %% Union should be doc1, doc2, doc3, doc4
    {ok, Result} = barrel_postings:union(PostingsA, PostingsB),
    Keys = barrel_postings:keys(Result),
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>, <<"doc4">>], Keys).

difference_op(Config) ->
    BinA = ?config(bin_a, Config),  %% doc1, doc2, doc3
    BinB = ?config(bin_b, Config),  %% doc2, doc3, doc4
    {ok, PostingsA} = barrel_postings:open(BinA),
    {ok, PostingsB} = barrel_postings:open(BinB),
    %% A - B should be doc1
    {ok, Result} = barrel_postings:difference(PostingsA, PostingsB),
    Keys = barrel_postings:keys(Result),
    ?assertEqual([<<"doc1">>], Keys),
    %% B - A should be doc4
    {ok, Result2} = barrel_postings:difference(PostingsB, PostingsA),
    Keys2 = barrel_postings:keys(Result2),
    ?assertEqual([<<"doc4">>], Keys2).

intersect_all_op(Config) ->
    BinA = ?config(bin_a, Config),  %% doc1, doc2, doc3
    BinB = ?config(bin_b, Config),  %% doc2, doc3, doc4
    BinC = ?config(bin_c, Config),  %% doc3, doc5
    {ok, PostingsA} = barrel_postings:open(BinA),
    {ok, PostingsB} = barrel_postings:open(BinB),
    {ok, PostingsC} = barrel_postings:open(BinC),
    %% Intersection of A, B, C should be doc3
    {ok, Result} = barrel_postings:intersect_all([PostingsA, PostingsB, PostingsC]),
    Keys = barrel_postings:keys(Result),
    ?assertEqual([<<"doc3">>], Keys),
    %% Also test with binaries directly
    {ok, Result2} = barrel_postings:intersect_all([BinA, BinB, BinC]),
    Keys2 = barrel_postings:keys(Result2),
    ?assertEqual([<<"doc3">>], Keys2).

intersection_count_op(Config) ->
    BinA = ?config(bin_a, Config),  %% doc1, doc2, doc3
    BinB = ?config(bin_b, Config),  %% doc2, doc3, doc4
    {ok, PostingsA} = barrel_postings:open(BinA),
    {ok, PostingsB} = barrel_postings:open(BinB),
    %% Intersection count should be 2 (doc2, doc3)
    Count = barrel_postings:intersection_count(PostingsA, PostingsB),
    ?assertEqual(2, Count).
