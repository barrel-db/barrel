%%%-------------------------------------------------------------------
%%% @doc Comprehensive test suite for barrel_revtree_bin
%%%
%%% Tests for conflict handling, winner selection, and binary encoding
%%% to ensure replication and conflict resolution work correctly.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_revtree_bin_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, basic_operations},
        {group, conflict_detection},
        {group, winner_selection},
        {group, contains_history},
        {group, pruning},
        {group, binary_encoding},
        {group, map_conversion},
        {group, integration}
    ].

groups() ->
    [
        {basic_operations, [sequence], [
            new_tree_is_empty,
            add_single_root,
            add_child_to_root,
            add_multiple_children,
            add_deep_chain
        ]},
        {conflict_detection, [sequence], [
            no_conflicts_single_leaf,
            no_conflicts_linear_chain,
            conflicts_with_branch,
            conflicts_multiple_branches,
            conflicts_exclude_winner
        ]},
        {winner_selection, [sequence], [
            winner_single_leaf,
            winner_by_generation,
            winner_by_lexicographic,
            winner_deep_branch_wins,
            winner_deleted_leaf
        ]},
        {contains_history, [sequence], [
            contains_existing_rev,
            contains_missing_rev,
            history_single_rev,
            history_linear_chain,
            history_with_depth_limit,
            history_missing_rev
        ]},
        {pruning, [sequence], [
            prune_empty_tree,
            prune_nothing_to_prune,
            prune_linear_chain,
            prune_branched_tree,
            prune_preserves_conflicts,
            prune_deep_tree
        ]},
        {binary_encoding, [sequence], [
            encode_decode_empty,
            encode_decode_single_root,
            encode_decode_chain,
            encode_decode_branched,
            decode_winner_leaves_partial
        ]},
        {map_conversion, [sequence], [
            from_map_empty,
            from_map_single_rev,
            from_map_linear_chain,
            from_map_branched_tree,
            to_map_roundtrip,
            conflicts_from_map_no_conflicts,
            conflicts_from_map_with_conflicts
        ]},
        {integration, [sequence], [
            replication_scenario_with_conflicts,
            large_tree_performance
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Basic Operations Tests
%%====================================================================

new_tree_is_empty(_Config) ->
    RT = barrel_revtree_bin:new(),
    ?assertEqual([], barrel_revtree_bin:leaves(RT)),
    ?assertEqual([], barrel_revtree_bin:conflicts(RT)),
    ?assertEqual(undefined, barrel_revtree_bin:winner(RT)),
    ok.

add_single_root(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, 0} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    ?assertEqual([<<"1-abc">>], barrel_revtree_bin:leaves(RT1)),
    ?assertEqual(<<"1-abc">>, barrel_revtree_bin:winner(RT1)),
    ok.

add_child_to_root(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-def">>, <<"1-abc">>, 2, false),

    %% Only leaf is child
    ?assertEqual([<<"2-def">>], barrel_revtree_bin:leaves(RT2)),
    ?assertEqual(<<"2-def">>, barrel_revtree_bin:winner(RT2)),
    ok.

add_multiple_children(_Config) ->
    %% Create a tree with branches:
    %%        1-abc
    %%       /     \
    %%    2-bbb   2-aaa
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-bbb">>, <<"1-abc">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-aaa">>, <<"1-abc">>, 2, false),

    Leaves = lists:sort(barrel_revtree_bin:leaves(RT3)),
    ?assertEqual([<<"2-aaa">>, <<"2-bbb">>], Leaves),
    ok.

add_deep_chain(_Config) ->
    %% Create a linear chain: 1-a -> 2-b -> 3-c -> 4-d -> 5-e
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-a">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-b">>, <<"1-a">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"3-c">>, <<"2-b">>, 3, false),
    {ok, RT4, _} = barrel_revtree_bin:add_child(RT3, <<"4-d">>, <<"3-c">>, 4, false),
    {ok, RT5, _} = barrel_revtree_bin:add_child(RT4, <<"5-e">>, <<"4-d">>, 5, false),

    ?assertEqual([<<"5-e">>], barrel_revtree_bin:leaves(RT5)),
    ?assertEqual(<<"5-e">>, barrel_revtree_bin:winner(RT5)),
    ok.

%%====================================================================
%% Conflict Detection Tests
%%====================================================================

no_conflicts_single_leaf(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),

    ?assertEqual([], barrel_revtree_bin:conflicts(RT1)),
    ok.

no_conflicts_linear_chain(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-def">>, <<"1-abc">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"3-ghi">>, <<"2-def">>, 3, false),

    ?assertEqual([], barrel_revtree_bin:conflicts(RT3)),
    ok.

conflicts_with_branch(_Config) ->
    %% Tree:    1-abc
    %%         /     \
    %%      2-winner  2-conflict
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-zzz">>, <<"1-abc">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-aaa">>, <<"1-abc">>, 2, false),

    %% Winner is 2-zzz (lexicographically greater at same gen)
    ?assertEqual(<<"2-zzz">>, barrel_revtree_bin:winner(RT3)),
    ?assertEqual([<<"2-aaa">>], barrel_revtree_bin:conflicts(RT3)),
    ok.

conflicts_multiple_branches(_Config) ->
    %% Tree:       1-root
    %%           /   |   \
    %%       2-aaa 2-bbb 2-ccc
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-root">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-aaa">>, <<"1-root">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-bbb">>, <<"1-root">>, 2, false),
    {ok, RT4, _} = barrel_revtree_bin:add_child(RT3, <<"2-ccc">>, <<"1-root">>, 2, false),

    %% Winner is 2-ccc (lexicographically greatest at same gen)
    ?assertEqual(<<"2-ccc">>, barrel_revtree_bin:winner(RT4)),
    Conflicts = lists:sort(barrel_revtree_bin:conflicts(RT4)),
    ?assertEqual([<<"2-aaa">>, <<"2-bbb">>], Conflicts),
    ok.

conflicts_exclude_winner(_Config) ->
    %% Ensure conflicts always excludes the winner
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-xxx">>, <<"1-abc">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-yyy">>, <<"1-abc">>, 2, false),

    Winner = barrel_revtree_bin:winner(RT3),
    Conflicts = barrel_revtree_bin:conflicts(RT3),

    ?assertNot(lists:member(Winner, Conflicts)),
    ok.

%%====================================================================
%% Winner Selection Tests
%%====================================================================

winner_single_leaf(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-only">>, 1, false),

    ?assertEqual(<<"1-only">>, barrel_revtree_bin:winner(RT1)),
    ok.

winner_by_generation(_Config) ->
    %% Higher generation wins regardless of lexicographic order
    %% Tree:    1-abc
    %%         /     \
    %%      2-zzz   3-aaa  <- 3-aaa wins (higher gen)
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-zzz">>, <<"1-abc">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"3-aaa">>, <<"1-abc">>, 3, false),

    ?assertEqual(<<"3-aaa">>, barrel_revtree_bin:winner(RT3)),
    ok.

winner_by_lexicographic(_Config) ->
    %% Same generation: lexicographically greater wins
    %% Tree:    1-abc
    %%         /     \
    %%      2-abc   2-xyz  <- 2-xyz wins (lexicographic)
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-abc">>, <<"1-abc">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-xyz">>, <<"1-abc">>, 2, false),

    ?assertEqual(<<"2-xyz">>, barrel_revtree_bin:winner(RT3)),
    ok.

winner_deep_branch_wins(_Config) ->
    %% Tree:        1-root
    %%             /      \
    %%          2-a       2-b
    %%           |
    %%          3-c       <- 3-c wins (deeper branch)
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-root">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-a">>, <<"1-root">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-b">>, <<"1-root">>, 2, false),
    {ok, RT4, _} = barrel_revtree_bin:add_child(RT3, <<"3-c">>, <<"2-a">>, 3, false),

    ?assertEqual(<<"3-c">>, barrel_revtree_bin:winner(RT4)),
    %% Conflict is the other leaf
    ?assertEqual([<<"2-b">>], barrel_revtree_bin:conflicts(RT4)),
    ok.

winner_deleted_leaf(_Config) ->
    %% Deleted leaves can still be winners (needed for replication)
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-deleted">>, <<"1-abc">>, 2, true),

    ?assertEqual(<<"2-deleted">>, barrel_revtree_bin:winner(RT2)),
    ok.

%%====================================================================
%% Contains and History Tests
%%====================================================================

contains_existing_rev(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-def">>, <<"1-abc">>, 2, false),

    ?assertEqual(true, barrel_revtree_bin:contains(RT2, <<"1-abc">>)),
    ?assertEqual(true, barrel_revtree_bin:contains(RT2, <<"2-def">>)),
    ok.

contains_missing_rev(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),

    ?assertEqual(false, barrel_revtree_bin:contains(RT1, <<"2-missing">>)),
    ?assertEqual(false, barrel_revtree_bin:contains(RT1, <<"nonexistent">>)),
    ok.

history_single_rev(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),

    ?assertEqual([<<"1-abc">>], barrel_revtree_bin:history(RT1, <<"1-abc">>)),
    ok.

history_linear_chain(_Config) ->
    %% Chain: 1-a -> 2-b -> 3-c -> 4-d -> 5-e
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-a">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-b">>, <<"1-a">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"3-c">>, <<"2-b">>, 3, false),
    {ok, RT4, _} = barrel_revtree_bin:add_child(RT3, <<"4-d">>, <<"3-c">>, 4, false),
    {ok, RT5, _} = barrel_revtree_bin:add_child(RT4, <<"5-e">>, <<"4-d">>, 5, false),

    %% Full history from leaf
    ?assertEqual([<<"5-e">>, <<"4-d">>, <<"3-c">>, <<"2-b">>, <<"1-a">>],
                 barrel_revtree_bin:history(RT5, <<"5-e">>)),

    %% History from middle
    ?assertEqual([<<"3-c">>, <<"2-b">>, <<"1-a">>],
                 barrel_revtree_bin:history(RT5, <<"3-c">>)),
    ok.

history_with_depth_limit(_Config) ->
    %% Chain: 1-a -> 2-b -> 3-c -> 4-d -> 5-e
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-a">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-b">>, <<"1-a">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"3-c">>, <<"2-b">>, 3, false),
    {ok, RT4, _} = barrel_revtree_bin:add_child(RT3, <<"4-d">>, <<"3-c">>, 4, false),
    {ok, RT5, _} = barrel_revtree_bin:add_child(RT4, <<"5-e">>, <<"4-d">>, 5, false),

    %% Limited history
    ?assertEqual([<<"5-e">>], barrel_revtree_bin:history(RT5, <<"5-e">>, 1)),
    ?assertEqual([<<"5-e">>, <<"4-d">>], barrel_revtree_bin:history(RT5, <<"5-e">>, 2)),
    ?assertEqual([<<"5-e">>, <<"4-d">>, <<"3-c">>], barrel_revtree_bin:history(RT5, <<"5-e">>, 3)),
    ok.

history_missing_rev(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-abc">>, 1, false),

    ?assertEqual([], barrel_revtree_bin:history(RT1, <<"nonexistent">>)),
    ok.

%%====================================================================
%% Pruning Tests
%%====================================================================

prune_empty_tree(_Config) ->
    RT = barrel_revtree_bin:new(),
    {PrunedRT, Pruned} = barrel_revtree_bin:prune(RT, 3),

    ?assertEqual([], barrel_revtree_bin:leaves(PrunedRT)),
    ?assertEqual([], Pruned),
    ok.

prune_nothing_to_prune(_Config) ->
    %% Tree with only 2 revisions, depth limit 3
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-a">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-b">>, <<"1-a">>, 2, false),

    {PrunedRT, Pruned} = barrel_revtree_bin:prune(RT2, 3),

    ?assertEqual([], Pruned),
    ?assertEqual([<<"2-b">>], barrel_revtree_bin:leaves(PrunedRT)),
    ?assertEqual(true, barrel_revtree_bin:contains(PrunedRT, <<"1-a">>)),
    ok.

prune_linear_chain(_Config) ->
    %% Chain: 1-a -> 2-b -> 3-c -> 4-d -> 5-e
    %% Prune with depth 3 should keep: 5-e, 4-d, 3-c
    %% And prune: 1-a, 2-b
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-a">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-b">>, <<"1-a">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"3-c">>, <<"2-b">>, 3, false),
    {ok, RT4, _} = barrel_revtree_bin:add_child(RT3, <<"4-d">>, <<"3-c">>, 4, false),
    {ok, RT5, _} = barrel_revtree_bin:add_child(RT4, <<"5-e">>, <<"4-d">>, 5, false),

    {PrunedRT, Pruned} = barrel_revtree_bin:prune(RT5, 3),

    %% Check pruned revisions
    PrunedSorted = lists:sort(Pruned),
    ?assertEqual([<<"1-a">>, <<"2-b">>], PrunedSorted),

    %% Check remaining tree
    ?assertEqual([<<"5-e">>], barrel_revtree_bin:leaves(PrunedRT)),
    ?assertEqual(<<"5-e">>, barrel_revtree_bin:winner(PrunedRT)),
    ?assertEqual(true, barrel_revtree_bin:contains(PrunedRT, <<"5-e">>)),
    ?assertEqual(true, barrel_revtree_bin:contains(PrunedRT, <<"4-d">>)),
    ?assertEqual(true, barrel_revtree_bin:contains(PrunedRT, <<"3-c">>)),
    ?assertEqual(false, barrel_revtree_bin:contains(PrunedRT, <<"2-b">>)),
    ?assertEqual(false, barrel_revtree_bin:contains(PrunedRT, <<"1-a">>)),
    ok.

prune_branched_tree(_Config) ->
    %% Tree:       1-root
    %%            /      \
    %%         2-a       2-b
    %%          |
    %%         3-c
    %% Prune with depth 2 should keep: 3-c, 2-a (branch 1) and 2-b, 1-root (branch 2)
    %% Wait, actually with depth 2:
    %% - From 3-c: keep 3-c, 2-a
    %% - From 2-b: keep 2-b, 1-root
    %% So all nodes are kept!
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-root">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-a">>, <<"1-root">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-b">>, <<"1-root">>, 2, false),
    {ok, RT4, _} = barrel_revtree_bin:add_child(RT3, <<"3-c">>, <<"2-a">>, 3, false),

    %% With depth 1, should only keep leaves
    {PrunedRT1, Pruned1} = barrel_revtree_bin:prune(RT4, 1),
    Pruned1Sorted = lists:sort(Pruned1),
    ?assertEqual([<<"1-root">>, <<"2-a">>], Pruned1Sorted),

    Leaves1 = lists:sort(barrel_revtree_bin:leaves(PrunedRT1)),
    ?assertEqual([<<"2-b">>, <<"3-c">>], Leaves1),
    ok.

prune_preserves_conflicts(_Config) ->
    %% Tree:       1-root
    %%            /      \
    %%         2-aaa    2-zzz
    %% Both branches have depth 2, pruning with depth 2 should keep everything
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-root">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-aaa">>, <<"1-root">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-zzz">>, <<"1-root">>, 2, false),

    %% Pruning with depth 2 keeps everything
    {PrunedRT, Pruned} = barrel_revtree_bin:prune(RT3, 2),
    ?assertEqual([], Pruned),

    %% Conflicts should be preserved
    ?assertEqual([<<"2-aaa">>], barrel_revtree_bin:conflicts(PrunedRT)),
    ?assertEqual(<<"2-zzz">>, barrel_revtree_bin:winner(PrunedRT)),
    ok.

prune_deep_tree(_Config) ->
    %% Create a tree with 20 revisions in a chain
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-root">>, 1, false),

    RTFinal = lists:foldl(
        fun(N, RT) ->
            RevId = iolist_to_binary([integer_to_binary(N), <<"-hash">>]),
            ParentN = N - 1,
            ParentRev = if
                ParentN =:= 1 -> <<"1-root">>;
                true -> iolist_to_binary([integer_to_binary(ParentN), <<"-hash">>])
            end,
            {ok, NewRT, _} = barrel_revtree_bin:add_child(RT, RevId, ParentRev, N, false),
            NewRT
        end,
        RT1,
        lists:seq(2, 20)
    ),

    %% Prune to depth 5
    {PrunedRT, Pruned} = barrel_revtree_bin:prune(RTFinal, 5),

    %% Should have pruned 15 revisions (20 - 5 = 15)
    ?assertEqual(15, length(Pruned)),

    %% Should keep revisions 16-20
    ?assertEqual(true, barrel_revtree_bin:contains(PrunedRT, <<"20-hash">>)),
    ?assertEqual(true, barrel_revtree_bin:contains(PrunedRT, <<"16-hash">>)),
    ?assertEqual(false, barrel_revtree_bin:contains(PrunedRT, <<"15-hash">>)),
    ?assertEqual(false, barrel_revtree_bin:contains(PrunedRT, <<"1-root">>)),

    %% Winner should still be the same
    ?assertEqual(<<"20-hash">>, barrel_revtree_bin:winner(PrunedRT)),
    ok.

%%====================================================================
%% Binary Encoding Tests
%%====================================================================

encode_decode_empty(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    Bin = barrel_revtree_bin:encode(RT0),
    RT1 = barrel_revtree_bin:decode(Bin),

    ?assertEqual([], barrel_revtree_bin:leaves(RT1)),
    ok.

encode_decode_single_root(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-test">>, 1, false),

    Bin = barrel_revtree_bin:encode(RT1),
    RT2 = barrel_revtree_bin:decode(Bin),

    ?assertEqual([<<"1-test">>], barrel_revtree_bin:leaves(RT2)),
    ?assertEqual(<<"1-test">>, barrel_revtree_bin:winner(RT2)),
    ok.

encode_decode_chain(_Config) ->
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-a">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-b">>, <<"1-a">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"3-c">>, <<"2-b">>, 3, false),

    Bin = barrel_revtree_bin:encode(RT3),
    RT4 = barrel_revtree_bin:decode(Bin),

    ?assertEqual([<<"3-c">>], barrel_revtree_bin:leaves(RT4)),
    ?assertEqual(<<"3-c">>, barrel_revtree_bin:winner(RT4)),
    ?assertEqual([], barrel_revtree_bin:conflicts(RT4)),
    ok.

encode_decode_branched(_Config) ->
    %% Tree with conflicts
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-root">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-branch1">>, <<"1-root">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-branch2">>, <<"1-root">>, 2, false),

    OriginalWinner = barrel_revtree_bin:winner(RT3),
    OriginalConflicts = lists:sort(barrel_revtree_bin:conflicts(RT3)),

    Bin = barrel_revtree_bin:encode(RT3),
    RT4 = barrel_revtree_bin:decode(Bin),

    DecodedWinner = barrel_revtree_bin:winner(RT4),
    DecodedConflicts = lists:sort(barrel_revtree_bin:conflicts(RT4)),

    ?assertEqual(OriginalWinner, DecodedWinner),
    ?assertEqual(OriginalConflicts, DecodedConflicts),
    ok.

decode_winner_leaves_partial(_Config) ->
    %% Test partial decode for fast winner/leaves computation
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-root">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-aaa">>, <<"1-root">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-zzz">>, <<"1-root">>, 2, false),

    Bin = barrel_revtree_bin:encode(RT3),
    Result = barrel_revtree_bin:decode_winner_leaves(Bin),

    ?assertEqual(<<"2-zzz">>, maps:get(winner, Result)),
    ?assertEqual([<<"2-aaa">>], maps:get(conflicts, Result)),

    Leaves = lists:sort(maps:get(leaves, Result)),
    ?assertEqual([<<"2-aaa">>, <<"2-zzz">>], Leaves),
    ok.

%%====================================================================
%% Map Conversion Tests
%%====================================================================

from_map_empty(_Config) ->
    RT = barrel_revtree_bin:from_map(#{}),
    ?assertEqual([], barrel_revtree_bin:leaves(RT)),
    ok.

from_map_single_rev(_Config) ->
    Map = #{
        <<"1-abc">> => #{id => <<"1-abc">>, parent => undefined, deleted => false}
    },
    RT = barrel_revtree_bin:from_map(Map),

    ?assertEqual([<<"1-abc">>], barrel_revtree_bin:leaves(RT)),
    ?assertEqual(<<"1-abc">>, barrel_revtree_bin:winner(RT)),
    ok.

from_map_linear_chain(_Config) ->
    Map = #{
        <<"1-a">> => #{id => <<"1-a">>, parent => undefined, deleted => false},
        <<"2-b">> => #{id => <<"2-b">>, parent => <<"1-a">>, deleted => false},
        <<"3-c">> => #{id => <<"3-c">>, parent => <<"2-b">>, deleted => false}
    },
    RT = barrel_revtree_bin:from_map(Map),

    ?assertEqual([<<"3-c">>], barrel_revtree_bin:leaves(RT)),
    ?assertEqual(<<"3-c">>, barrel_revtree_bin:winner(RT)),
    ?assertEqual([], barrel_revtree_bin:conflicts(RT)),
    ok.

from_map_branched_tree(_Config) ->
    %% Simulate a replication conflict scenario
    Map = #{
        <<"1-root">> => #{id => <<"1-root">>, parent => undefined, deleted => false},
        <<"2-server1">> => #{id => <<"2-server1">>, parent => <<"1-root">>, deleted => false},
        <<"2-server2">> => #{id => <<"2-server2">>, parent => <<"1-root">>, deleted => false}
    },
    RT = barrel_revtree_bin:from_map(Map),

    Leaves = lists:sort(barrel_revtree_bin:leaves(RT)),
    ?assertEqual([<<"2-server1">>, <<"2-server2">>], Leaves),

    %% Winner should be lexicographically greater
    ?assertEqual(<<"2-server2">>, barrel_revtree_bin:winner(RT)),
    ?assertEqual([<<"2-server1">>], barrel_revtree_bin:conflicts(RT)),
    ok.

to_map_roundtrip(_Config) ->
    %% Build tree, convert to map, convert back, verify same results
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-root">>, 1, false),
    {ok, RT2, _} = barrel_revtree_bin:add_child(RT1, <<"2-child1">>, <<"1-root">>, 2, false),
    {ok, RT3, _} = barrel_revtree_bin:add_child(RT2, <<"2-child2">>, <<"1-root">>, 2, true),

    OriginalLeaves = lists:sort(barrel_revtree_bin:leaves(RT3)),
    OriginalWinner = barrel_revtree_bin:winner(RT3),

    Map = barrel_revtree_bin:to_map(RT3),
    RT4 = barrel_revtree_bin:from_map(Map),

    RoundtripLeaves = lists:sort(barrel_revtree_bin:leaves(RT4)),
    RoundtripWinner = barrel_revtree_bin:winner(RT4),

    ?assertEqual(OriginalLeaves, RoundtripLeaves),
    ?assertEqual(OriginalWinner, RoundtripWinner),
    ok.

conflicts_from_map_no_conflicts(_Config) ->
    %% Linear chain has no conflicts
    Map = #{
        <<"1-a">> => #{id => <<"1-a">>, parent => undefined, deleted => false},
        <<"2-b">> => #{id => <<"2-b">>, parent => <<"1-a">>, deleted => false}
    },

    ?assertEqual([], barrel_revtree_bin:conflicts_from_map(Map)),
    ok.

conflicts_from_map_with_conflicts(_Config) ->
    %% Branched tree has conflicts
    Map = #{
        <<"1-root">> => #{id => <<"1-root">>, parent => undefined, deleted => false},
        <<"2-branch1">> => #{id => <<"2-branch1">>, parent => <<"1-root">>, deleted => false},
        <<"2-branch2">> => #{id => <<"2-branch2">>, parent => <<"1-root">>, deleted => false}
    },

    Conflicts = barrel_revtree_bin:conflicts_from_map(Map),
    %% Winner is 2-branch2 (lexicographically greater), so conflict is 2-branch1
    ?assertEqual([<<"2-branch1">>], Conflicts),
    ok.

%%====================================================================
%% Integration Tests
%%====================================================================

replication_scenario_with_conflicts(_Config) ->
    %% Simulate a real replication conflict scenario:
    %% 1. Document created on server A
    %% 2. Same doc updated on server A and server B independently
    %% 3. After replication, should have correct winner and conflicts

    %% Server A: doc created, then updated
    MapA = #{
        <<"1-initial">> => #{id => <<"1-initial">>, parent => undefined, deleted => false},
        <<"2-serverA">> => #{id => <<"2-serverA">>, parent => <<"1-initial">>, deleted => false}
    },

    %% Server B: got initial, then updated independently
    MapB = #{
        <<"1-initial">> => #{id => <<"1-initial">>, parent => undefined, deleted => false},
        <<"2-serverB">> => #{id => <<"2-serverB">>, parent => <<"1-initial">>, deleted => false}
    },

    %% After replication: merge both revision trees
    MergedMap = maps:merge(MapA, MapB),

    RT = barrel_revtree_bin:from_map(MergedMap),

    %% Should have 2 leaves (the two conflicting updates)
    Leaves = lists:sort(barrel_revtree_bin:leaves(RT)),
    ?assertEqual([<<"2-serverA">>, <<"2-serverB">>], Leaves),

    %% Winner is lexicographically greater
    ?assertEqual(<<"2-serverB">>, barrel_revtree_bin:winner(RT)),

    %% One conflict
    ?assertEqual([<<"2-serverA">>], barrel_revtree_bin:conflicts(RT)),

    %% Verify binary encoding preserves this
    Bin = barrel_revtree_bin:encode(RT),
    Result = barrel_revtree_bin:decode_winner_leaves(Bin),
    ?assertEqual(<<"2-serverB">>, maps:get(winner, Result)),
    ?assertEqual([<<"2-serverA">>], maps:get(conflicts, Result)),

    ok.

large_tree_performance(_Config) ->
    %% Test performance with a moderately large tree
    %% Build a tree with 100 revisions (linear chain)
    RT0 = barrel_revtree_bin:new(),
    {ok, RT1, _} = barrel_revtree_bin:add_root(RT0, <<"1-root">>, 1, false),

    %% Add 99 more revisions in a chain
    RTFinal = lists:foldl(
        fun(N, RT) ->
            RevId = iolist_to_binary([integer_to_binary(N), "-hash", integer_to_binary(N)]),
            ParentN = N - 1,
            ParentRev = if
                ParentN =:= 1 -> <<"1-root">>;
                true -> iolist_to_binary([integer_to_binary(ParentN), "-hash", integer_to_binary(ParentN)])
            end,
            {ok, NewRT, _} = barrel_revtree_bin:add_child(RT, RevId, ParentRev, N, false),
            NewRT
        end,
        RT1,
        lists:seq(2, 100)
    ),

    %% Operations should be fast
    Leaves = barrel_revtree_bin:leaves(RTFinal),
    ?assertEqual(1, length(Leaves)),

    Winner = barrel_revtree_bin:winner(RTFinal),
    ?assertMatch(<<"100-", _/binary>>, Winner),

    %% Binary encoding should work
    Bin = barrel_revtree_bin:encode(RTFinal),
    ct:pal("Binary size for 100-rev tree: ~p bytes", [byte_size(Bin)]),

    %% Partial decode should work
    Result = barrel_revtree_bin:decode_winner_leaves(Bin),
    ?assertMatch(<<"100-", _/binary>>, maps:get(winner, Result)),

    ok.
