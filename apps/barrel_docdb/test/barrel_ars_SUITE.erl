%%%-------------------------------------------------------------------
%%% @doc Common Test suite for barrel_ars
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ars_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    analyze_empty_doc/1,
    analyze_flat_doc/1,
    analyze_nested_maps/1,
    analyze_arrays/1,
    analyze_arrays_with_objects/1,
    analyze_mixed_types/1,
    analyze_complex_doc/1,
    analyze_indexed_cbor/1,
    short_truncates_long_binary/1,
    short_preserves_short_binary/1,
    short_preserves_non_binary/1,
    diff_added_paths/1,
    diff_removed_paths/1,
    diff_both_added_removed/1,
    diff_no_changes/1,
    diff_empty_to_paths/1,
    diff_paths_to_empty/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, analyze},
        {group, short},
        {group, diff}
    ].

groups() ->
    [
        {analyze, [parallel], [
            analyze_empty_doc,
            analyze_flat_doc,
            analyze_nested_maps,
            analyze_arrays,
            analyze_arrays_with_objects,
            analyze_mixed_types,
            analyze_complex_doc,
            analyze_indexed_cbor
        ]},
        {short, [parallel], [
            short_truncates_long_binary,
            short_preserves_short_binary,
            short_preserves_non_binary
        ]},
        {diff, [parallel], [
            diff_added_paths,
            diff_removed_paths,
            diff_both_added_removed,
            diff_no_changes,
            diff_empty_to_paths,
            diff_paths_to_empty
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% analyze tests
%%====================================================================

analyze_empty_doc(_Config) ->
    Paths = barrel_ars:analyze(#{}),
    ?assertEqual([], Paths).

analyze_flat_doc(_Config) ->
    Doc = #{
        <<"a">> => 1,
        <<"b">> => <<"hello">>,
        <<"c">> => true
    },
    Paths = barrel_ars:analyze(Doc),
    %% Convert to sorted list for comparison
    Expected = lists:sort([
        {[<<"a">>, 1], <<>>},
        {[<<"b">>, <<"hello">>], <<>>},
        {[<<"c">>, true], <<>>}
    ]),
    ?assertEqual(Expected, lists:sort(Paths)).

analyze_nested_maps(_Config) ->
    Doc = #{
        <<"profile">> => #{
            <<"name">> => <<"Alice">>,
            <<"address">> => #{
                <<"city">> => <<"Paris">>
            }
        }
    },
    Paths = barrel_ars:analyze(Doc),
    Expected = lists:sort([
        {[<<"profile">>, <<"name">>, <<"Alice">>], <<>>},
        {[<<"profile">>, <<"address">>, <<"city">>, <<"Paris">>], <<>>}
    ]),
    ?assertEqual(Expected, lists:sort(Paths)).

analyze_arrays(_Config) ->
    Doc = #{
        <<"tags">> => [<<"a">>, <<"b">>, <<"c">>]
    },
    Paths = barrel_ars:analyze(Doc),
    Expected = lists:sort([
        {[<<"tags">>, 0, <<"a">>], <<>>},
        {[<<"tags">>, 1, <<"b">>], <<>>},
        {[<<"tags">>, 2, <<"c">>], <<>>}
    ]),
    ?assertEqual(Expected, lists:sort(Paths)).

analyze_arrays_with_objects(_Config) ->
    Doc = #{
        <<"items">> => [
            #{<<"sku">> => <<"ABC">>, <<"qty">> => 2},
            #{<<"sku">> => <<"XYZ">>, <<"qty">> => 1}
        ]
    },
    Paths = barrel_ars:analyze(Doc),
    Expected = lists:sort([
        {[<<"items">>, 0, <<"sku">>, <<"ABC">>], <<>>},
        {[<<"items">>, 0, <<"qty">>, 2], <<>>},
        {[<<"items">>, 1, <<"sku">>, <<"XYZ">>], <<>>},
        {[<<"items">>, 1, <<"qty">>, 1], <<>>}
    ]),
    ?assertEqual(Expected, lists:sort(Paths)).

analyze_mixed_types(_Config) ->
    Doc = #{
        <<"int">> => 42,
        <<"float">> => 3.14,
        <<"string">> => <<"hello">>,
        <<"bool">> => false,
        <<"null">> => null
    },
    Paths = barrel_ars:analyze(Doc),
    Expected = lists:sort([
        {[<<"int">>, 42], <<>>},
        {[<<"float">>, 3.14], <<>>},
        {[<<"string">>, <<"hello">>], <<>>},
        {[<<"bool">>, false], <<>>},
        {[<<"null">>, null], <<>>}
    ]),
    ?assertEqual(Expected, lists:sort(Paths)).

analyze_complex_doc(_Config) ->
    %% Same test document as the original barrel_ars_view
    Doc = #{
        <<"a">> => 1,
        <<"b">> => <<"2">>,
        <<"c">> => #{
            <<"a">> => 1,
            <<"b">> => [<<"a">>, <<"b">>, <<"c">>],
            <<"c">> => #{<<"a">> => 1, <<"b">> => 2}
        },
        <<"d">> => [<<"a">>, <<"b">>, <<"c">>],
        <<"e">> => [#{<<"a">> => 1}, #{<<"b">> => 2, <<"c">> => 3}]
    },
    Paths = barrel_ars:analyze(Doc),
    Expected = lists:sort([
        {[<<"a">>, 1], <<>>},
        {[<<"b">>, <<"2">>], <<>>},
        {[<<"c">>, <<"a">>, 1], <<>>},
        {[<<"c">>, <<"b">>, 0, <<"a">>], <<>>},
        {[<<"c">>, <<"b">>, 1, <<"b">>], <<>>},
        {[<<"c">>, <<"b">>, 2, <<"c">>], <<>>},
        {[<<"c">>, <<"c">>, <<"a">>, 1], <<>>},
        {[<<"c">>, <<"c">>, <<"b">>, 2], <<>>},
        {[<<"d">>, 0, <<"a">>], <<>>},
        {[<<"d">>, 1, <<"b">>], <<>>},
        {[<<"d">>, 2, <<"c">>], <<>>},
        {[<<"e">>, 0, <<"a">>, 1], <<>>},
        {[<<"e">>, 1, <<"b">>, 2], <<>>},
        {[<<"e">>, 1, <<"c">>, 3], <<>>}
    ]),
    ?assertEqual(Expected, lists:sort(Paths)).

analyze_indexed_cbor(_Config) ->
    %% Test that analyze works with indexed CBOR binary (not just maps)
    Map = #{
        <<"type">> => <<"user">>,
        <<"name">> => <<"Alice">>,
        <<"profile">> => #{
            <<"city">> => <<"Paris">>,
            <<"age">> => 30
        }
    },

    %% Analyze the map directly
    MapPaths = barrel_ars:analyze(Map),

    %% Convert to indexed CBOR and analyze
    IndexedCbor = barrel_doc:from_map(Map),
    ?assert(barrel_doc:is_indexed(IndexedCbor)),
    CborPaths = barrel_ars:analyze(IndexedCbor),

    %% Both should produce the same paths
    ?assertEqual(lists:sort(MapPaths), lists:sort(CborPaths)),

    %% Verify expected paths
    Expected = lists:sort([
        {[<<"type">>, <<"user">>], <<>>},
        {[<<"name">>, <<"Alice">>], <<>>},
        {[<<"profile">>, <<"city">>, <<"Paris">>], <<>>},
        {[<<"profile">>, <<"age">>, 30], <<>>}
    ]),
    ?assertEqual(Expected, lists:sort(CborPaths)).

%%====================================================================
%% short tests
%%====================================================================

short_truncates_long_binary(_Config) ->
    %% Create a binary longer than 100 bytes
    LongBin = binary:copy(<<"a">>, 150),
    ?assertEqual(150, byte_size(LongBin)),
    Result = barrel_ars:short(LongBin),
    ?assertEqual(100, byte_size(Result)),
    ?assertEqual(binary:copy(<<"a">>, 100), Result).

short_preserves_short_binary(_Config) ->
    ShortBin = <<"hello world">>,
    ?assertEqual(ShortBin, barrel_ars:short(ShortBin)).

short_preserves_non_binary(_Config) ->
    ?assertEqual(42, barrel_ars:short(42)),
    ?assertEqual(3.14, barrel_ars:short(3.14)),
    ?assertEqual(true, barrel_ars:short(true)),
    ?assertEqual(null, barrel_ars:short(null)).

%%====================================================================
%% diff tests
%%====================================================================

diff_added_paths(_Config) ->
    Old = [{[<<"a">>, 1], <<>>}],
    New = [{[<<"a">>, 1], <<>>}, {[<<"b">>, 2], <<>>}],
    {Added, Removed} = barrel_ars:diff(Old, New),
    ?assertEqual([{[<<"b">>, 2], <<>>}], Added),
    ?assertEqual([], Removed).

diff_removed_paths(_Config) ->
    Old = [{[<<"a">>, 1], <<>>}, {[<<"b">>, 2], <<>>}],
    New = [{[<<"a">>, 1], <<>>}],
    {Added, Removed} = barrel_ars:diff(Old, New),
    ?assertEqual([], Added),
    ?assertEqual([{[<<"b">>, 2], <<>>}], Removed).

diff_both_added_removed(_Config) ->
    Old = [{[<<"a">>, 1], <<>>}, {[<<"b">>, 2], <<>>}],
    New = [{[<<"a">>, 1], <<>>}, {[<<"c">>, 3], <<>>}],
    {Added, Removed} = barrel_ars:diff(Old, New),
    ?assertEqual([{[<<"c">>, 3], <<>>}], Added),
    ?assertEqual([{[<<"b">>, 2], <<>>}], Removed).

diff_no_changes(_Config) ->
    Paths = [{[<<"a">>, 1], <<>>}, {[<<"b">>, 2], <<>>}],
    {Added, Removed} = barrel_ars:diff(Paths, Paths),
    ?assertEqual([], Added),
    ?assertEqual([], Removed).

diff_empty_to_paths(_Config) ->
    New = [{[<<"a">>, 1], <<>>}, {[<<"b">>, 2], <<>>}],
    {Added, Removed} = barrel_ars:diff([], New),
    ?assertEqual(lists:sort(New), lists:sort(Added)),
    ?assertEqual([], Removed).

diff_paths_to_empty(_Config) ->
    Old = [{[<<"a">>, 1], <<>>}, {[<<"b">>, 2], <<>>}],
    {Added, Removed} = barrel_ars:diff(Old, []),
    ?assertEqual([], Added),
    ?assertEqual(lists:sort(Old), lists:sort(Removed)).
