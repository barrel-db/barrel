%%% @doc Correctness coverage for barrel_docdb:find/2,3.
%%%
%%% Regression suite for the find/query fixes:
%%%  - P1: AND of two or more {compare,...} conditions.
%%%  - P2: {prefix,...} as a sole and combined condition; id prefix/range scans.
%%%  - P3: the flat => true result option.
%%%  - P4: top-level `_'-prefixed fields are reserved (not indexed).
%%% Plus a direct unit check of barrel_ars_index:docid_get_value/4.
-module(barrel_find_correctness_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([single_compare/1,
         two_compares_same_field/1,
         two_compares_diff_fields/1,
         equality_and_two_compares/1,
         three_compares/1,
         prefix_sole/1,
         prefix_with_compare/1,
         id_prefix_scan/1,
         id_range_scan/1,
         id_scan_skips_tombstones/1,
         flat_option/1,
         default_wrapper/1,
         include_docs_false/1,
         underscore_meta_not_indexed/1,
         docid_get_value_returns_scalar/1]).

all() ->
    [single_compare,
     two_compares_same_field,
     two_compares_diff_fields,
     equality_and_two_compares,
     three_compares,
     prefix_sole,
     prefix_with_compare,
     id_prefix_scan,
     id_range_scan,
     id_scan_skips_tombstones,
     flat_option,
     default_wrapper,
     include_docs_false,
     underscore_meta_not_indexed,
     docid_get_value_returns_scalar].

init_per_suite(Config) ->
    Dir = "/tmp/barrel_find_corr_" ++ integer_to_list(erlang:system_time(millisecond)),
    application:set_env(barrel_docdb, data_dir, Dir),
    {ok, _} = application:ensure_all_started(barrel_docdb),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    application:stop(barrel_docdb),
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

init_per_testcase(TC, Config) ->
    Db = atom_to_binary(TC, utf8),
    {ok, Pid} = barrel_docdb:create_db(Db, #{data_dir => ?config(dir, Config)}),
    [{db, Db}, {pid, Pid} | Config].

end_per_testcase(_TC, Config) ->
    try barrel_docdb:delete_db(?config(db, Config)) catch _:_ -> ok end,
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Seed 5 docs: id "1".."5", n=1..5, size=100..500, sub="Hello World N", mb=true.
seed_nums(Db) ->
    [begin
         Doc = #{<<"id">> => integer_to_binary(I),
                 <<"n">> => I,
                 <<"size">> => I * 100,
                 <<"mb">> => true,
                 <<"sub">> => <<"Hello World ", (integer_to_binary(I))/binary>>},
         {ok, _} = barrel_docdb:put_doc(Db, Doc)
     end || I <- [1, 2, 3, 4, 5]],
    ok.

%% Run a where-query (default wrapper results) and return the sorted `n' values.
ns(Db, Where) ->
    {ok, Rows, _} = barrel_docdb:find(Db, #{where => Where}),
    lists:sort([maps:get(<<"n">>, maps:get(<<"doc">>, R)) || R <- Rows]).

%% Run an id-scan (spec map) and return the sorted ids.
scan_ids(Db, Spec) ->
    {ok, Rows, _} = barrel_docdb:find(Db, Spec),
    lists:sort([maps:get(<<"id">>, R) || R <- Rows]).

store_ref(Config) ->
    {ok, StoreRef} = barrel_db_server:get_store_ref(?config(pid, Config)),
    StoreRef.

%%====================================================================
%% P1 - compare intersection
%%====================================================================

single_compare(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    ?assertEqual([2, 3, 4, 5], ns(Db, [{compare, [<<"n">>], '>=', 2}])).

two_compares_same_field(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    ?assertEqual([2, 3], ns(Db, [{compare, [<<"n">>], '>=', 2},
                                 {compare, [<<"n">>], '<', 4}])).

two_compares_diff_fields(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    ?assertEqual([2, 3], ns(Db, [{compare, [<<"n">>], '>=', 2},
                                 {compare, [<<"size">>], '=<', 300}])).

equality_and_two_compares(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    ?assertEqual([2, 3], ns(Db, [{path, [<<"mb">>], true},
                                 {compare, [<<"n">>], '>=', 2},
                                 {compare, [<<"size">>], '=<', 300}])).

three_compares(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    %% n >= 2 AND n < 5 AND size >= 300  ->  n in {3,4}
    ?assertEqual([3, 4], ns(Db, [{compare, [<<"n">>], '>=', 2},
                                 {compare, [<<"n">>], '<', 5},
                                 {compare, [<<"size">>], '>=', 300}])).

%%====================================================================
%% P2 - prefix and id scans
%%====================================================================

prefix_sole(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    ?assertEqual([1, 2, 3, 4, 5], ns(Db, [{prefix, [<<"sub">>], <<"Hello">>}])).

prefix_with_compare(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    ?assertEqual([3, 4, 5], ns(Db, [{prefix, [<<"sub">>], <<"Hello">>},
                                    {compare, [<<"n">>], '>=', 3}])).

id_prefix_scan(Config) ->
    Db = ?config(db, Config),
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"v">> => 1})
     || Id <- [<<"item:1">>, <<"user:1">>, <<"user:2">>, <<"zzz">>]],
    ?assertEqual([<<"user:1">>, <<"user:2">>],
                 scan_ids(Db, #{id_prefix => <<"user:">>})).

id_range_scan(Config) ->
    Db = ?config(db, Config),
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"v">> => 1})
     || Id <- [<<"a">>, <<"b">>, <<"m">>, <<"z">>]],
    %% Half-open [a, n): a, b, m  (z excluded)
    ?assertEqual([<<"a">>, <<"b">>, <<"m">>],
                 scan_ids(Db, #{id_range => {<<"a">>, <<"n">>}})),
    %% Open lower bound, exclusive upper "b": just "a"
    ?assertEqual([<<"a">>], scan_ids(Db, #{id_range => {undefined, <<"b">>}})),
    %% Open upper bound from "m": m, z
    ?assertEqual([<<"m">>, <<"z">>], scan_ids(Db, #{id_range => {<<"m">>, undefined}})).

id_scan_skips_tombstones(Config) ->
    Db = ?config(db, Config),
    [{ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => Id, <<"v">> => 1})
     || Id <- [<<"user:1">>, <<"user:2">>, <<"user:3">>]],
    {ok, _} = barrel_docdb:delete_doc(Db, <<"user:2">>),
    ?assertEqual([<<"user:1">>, <<"user:3">>],
                 scan_ids(Db, #{id_prefix => <<"user:">>})).

%%====================================================================
%% P3 - result shape
%%====================================================================

flat_option(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    {ok, [Row], _} = barrel_docdb:find(Db, #{where => [{path, [<<"n">>], 3}],
                                             flat => true}),
    ?assertNot(maps:is_key(<<"doc">>, Row)),
    ?assertEqual(<<"3">>, maps:get(<<"id">>, Row)),
    ?assertEqual(3, maps:get(<<"n">>, Row)).

default_wrapper(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    {ok, [Row], _} = barrel_docdb:find(Db, #{where => [{path, [<<"n">>], 3}]}),
    ?assert(maps:is_key(<<"doc">>, Row)),
    ?assertEqual(3, maps:get(<<"n">>, maps:get(<<"doc">>, Row))).

include_docs_false(Config) ->
    Db = ?config(db, Config),
    seed_nums(Db),
    {ok, [Row], _} = barrel_docdb:find(Db, #{where => [{path, [<<"n">>], 3}],
                                             include_docs => false}),
    ?assertEqual([<<"id">>], maps:keys(Row)),
    ?assertEqual(<<"3">>, maps:get(<<"id">>, Row)).

%%====================================================================
%% P4 - reserved top-level _-prefixed fields
%%====================================================================

underscore_meta_not_indexed(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"d1">>,
                                         <<"_meta">> => #{<<"x">> => 1},
                                         <<"meta">> => #{<<"x">> => 1}}),
    %% Top-level _meta is stripped before storage/indexing -> not findable.
    {ok, R1, _} = barrel_docdb:find(Db, #{where => [{path, [<<"_meta">>, <<"x">>], 1}]}),
    ?assertEqual(0, length(R1)),
    %% A normal nested field is indexed and findable.
    {ok, R2, _} = barrel_docdb:find(Db, #{where => [{path, [<<"meta">>, <<"x">>], 1}]}),
    ?assertEqual(1, length(R2)).

%%====================================================================
%% docid_get_value/4 unit check (P1 hardening)
%%====================================================================

docid_get_value_returns_scalar(Config) ->
    Db = ?config(db, Config),
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"k1">>, <<"n">> => 7}),
    StoreRef = store_ref(Config),
    ?assertEqual({ok, 7},
                 barrel_ars_index:docid_get_value(StoreRef, Db, <<"k1">>, [<<"n">>])),
    ?assertEqual(not_found,
                 barrel_ars_index:docid_get_value(StoreRef, Db, <<"k1">>, [<<"missing">>])).
