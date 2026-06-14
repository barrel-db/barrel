%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_sub subscription manager
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_sub_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

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
    subscribe_exact_pattern/1,
    subscribe_single_wildcard/1,
    subscribe_multi_wildcard/1,
    subscribe_multiple_patterns/1,
    subscribe_multiple_subscribers/1,
    unsubscribe_removes_subscription/1,
    process_exit_cleanup/1,
    match_no_subscribers/1,
    match_exact/1,
    match_single_wildcard/1,
    match_multi_wildcard/1,
    match_multiple_paths/1,
    invalid_pattern_rejected/1,
    list_subscriptions/1,
    %% Integration tests
    put_doc_notifies_subscriber/1,
    delete_doc_notifies_subscriber/1,
    update_doc_notifies_subscriber/1,
    public_api_subscribe_unsubscribe/1
]).

%%====================================================================
%% Common Test callbacks
%%====================================================================

all() ->
    [{group, subscribe}, {group, match}, {group, cleanup}, {group, integration}].

groups() ->
    [
        {subscribe, [parallel], [
            subscribe_exact_pattern,
            subscribe_single_wildcard,
            subscribe_multi_wildcard,
            subscribe_multiple_patterns,
            subscribe_multiple_subscribers,
            invalid_pattern_rejected,
            list_subscriptions
        ]},
        {match, [parallel], [
            match_no_subscribers,
            match_exact,
            match_single_wildcard,
            match_multi_wildcard,
            match_multiple_paths
        ]},
        {cleanup, [sequence], [
            unsubscribe_removes_subscription,
            process_exit_cleanup
        ]},
        {integration, [sequence], [
            put_doc_notifies_subscriber,
            delete_doc_notifies_subscriber,
            update_doc_notifies_subscriber,
            public_api_subscribe_unsubscribe
        ]}
    ].

init_per_suite(Config) ->
    {ok, _Apps} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_testcase(_TestCase, Config) ->
    DbName = iolist_to_binary([<<"test_sub_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    [{db_name, DbName} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Subscribe test cases
%%====================================================================

subscribe_exact_pattern(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef} = barrel_sub:subscribe(DbName, <<"users/123">>, self()),
    ?assert(is_reference(SubRef)),
    ok = barrel_sub:unsubscribe(SubRef, self()).

subscribe_single_wildcard(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef} = barrel_sub:subscribe(DbName, <<"users/+/profile">>, self()),
    ?assert(is_reference(SubRef)),
    ok = barrel_sub:unsubscribe(SubRef, self()).

subscribe_multi_wildcard(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef} = barrel_sub:subscribe(DbName, <<"orders/#">>, self()),
    ?assert(is_reference(SubRef)),
    ok = barrel_sub:unsubscribe(SubRef, self()).

subscribe_multiple_patterns(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef1} = barrel_sub:subscribe(DbName, <<"users/#">>, self()),
    {ok, SubRef2} = barrel_sub:subscribe(DbName, <<"orders/#">>, self()),
    ?assertNotEqual(SubRef1, SubRef2),
    ok = barrel_sub:unsubscribe(SubRef1, self()),
    ok = barrel_sub:unsubscribe(SubRef2, self()).

subscribe_multiple_subscribers(Config) ->
    DbName = ?config(db_name, Config),
    Parent = self(),

    Pid1 = spawn_link(fun() ->
        {ok, SubRef} = barrel_sub:subscribe(DbName, <<"users/#">>, self()),
        Parent ! {subscribed, self(), SubRef},
        receive stop -> ok end
    end),

    Pid2 = spawn_link(fun() ->
        {ok, SubRef} = barrel_sub:subscribe(DbName, <<"users/#">>, self()),
        Parent ! {subscribed, self(), SubRef},
        receive stop -> ok end
    end),

    receive {subscribed, Pid1, _SubRef1} -> ok end,
    receive {subscribed, Pid2, _SubRef2} -> ok end,

    %% Both should be matched
    Pids = barrel_sub:match(DbName, [<<"users/123">>]),
    ?assertEqual(2, length(Pids)),
    ?assert(lists:member(Pid1, Pids)),
    ?assert(lists:member(Pid2, Pids)),

    Pid1 ! stop,
    Pid2 ! stop.

invalid_pattern_rejected(Config) ->
    DbName = ?config(db_name, Config),
    %% Pattern with # not at the end is invalid
    Result = barrel_sub:subscribe(DbName, <<"users/#/profile">>, self()),
    ?assertEqual({error, invalid_pattern}, Result).

list_subscriptions(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef1} = barrel_sub:subscribe(DbName, <<"users/#">>, self()),
    {ok, SubRef2} = barrel_sub:subscribe(DbName, <<"orders/+">>, self()),

    Subs = barrel_sub:list_subscriptions(DbName),
    ?assertEqual(2, length(Subs)),

    SubRefs = [Ref || {Ref, _, _} <- Subs],
    ?assert(lists:member(SubRef1, SubRefs)),
    ?assert(lists:member(SubRef2, SubRefs)),

    ok = barrel_sub:unsubscribe(SubRef1, self()),
    ok = barrel_sub:unsubscribe(SubRef2, self()).

%%====================================================================
%% Match test cases
%%====================================================================

match_no_subscribers(Config) ->
    DbName = ?config(db_name, Config),
    Pids = barrel_sub:match(DbName, [<<"nonexistent/path">>]),
    ?assertEqual([], Pids).

match_exact(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef} = barrel_sub:subscribe(DbName, <<"users/123">>, self()),

    %% Exact match
    Pids1 = barrel_sub:match(DbName, [<<"users/123">>]),
    ?assertEqual([self()], Pids1),

    %% No match
    Pids2 = barrel_sub:match(DbName, [<<"users/456">>]),
    ?assertEqual([], Pids2),

    ok = barrel_sub:unsubscribe(SubRef, self()).

match_single_wildcard(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef} = barrel_sub:subscribe(DbName, <<"users/+/profile">>, self()),

    %% Matches
    Pids1 = barrel_sub:match(DbName, [<<"users/123/profile">>]),
    ?assertEqual([self()], Pids1),

    Pids2 = barrel_sub:match(DbName, [<<"users/abc/profile">>]),
    ?assertEqual([self()], Pids2),

    %% No match - wrong level
    Pids3 = barrel_sub:match(DbName, [<<"users/123/settings">>]),
    ?assertEqual([], Pids3),

    %% No match - too deep
    Pids4 = barrel_sub:match(DbName, [<<"users/123/profile/avatar">>]),
    ?assertEqual([], Pids4),

    ok = barrel_sub:unsubscribe(SubRef, self()).

match_multi_wildcard(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef} = barrel_sub:subscribe(DbName, <<"orders/#">>, self()),

    %% Matches various depths
    Pids1 = barrel_sub:match(DbName, [<<"orders/123">>]),
    ?assertEqual([self()], Pids1),

    Pids2 = barrel_sub:match(DbName, [<<"orders/123/items">>]),
    ?assertEqual([self()], Pids2),

    Pids3 = barrel_sub:match(DbName, [<<"orders/123/items/1/product">>]),
    ?assertEqual([self()], Pids3),

    %% No match - different prefix
    Pids4 = barrel_sub:match(DbName, [<<"users/123">>]),
    ?assertEqual([], Pids4),

    ok = barrel_sub:unsubscribe(SubRef, self()).

match_multiple_paths(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef1} = barrel_sub:subscribe(DbName, <<"users/#">>, self()),
    {ok, SubRef2} = barrel_sub:subscribe(DbName, <<"orders/#">>, self()),

    %% Match on one path
    Pids1 = barrel_sub:match(DbName, [<<"users/123">>]),
    ?assertEqual([self()], Pids1),

    %% Match on both paths - should return pid once
    Pids2 = barrel_sub:match(DbName, [<<"users/123">>, <<"orders/456">>]),
    ?assertEqual([self()], Pids2),

    ok = barrel_sub:unsubscribe(SubRef1, self()),
    ok = barrel_sub:unsubscribe(SubRef2, self()).

%%====================================================================
%% Cleanup test cases
%%====================================================================

unsubscribe_removes_subscription(Config) ->
    DbName = ?config(db_name, Config),
    {ok, SubRef} = barrel_sub:subscribe(DbName, <<"users/#">>, self()),

    %% Verify match works
    Pids1 = barrel_sub:match(DbName, [<<"users/123">>]),
    ?assertEqual([self()], Pids1),

    %% Unsubscribe
    ok = barrel_sub:unsubscribe(SubRef, self()),

    %% Verify no longer matching
    Pids2 = barrel_sub:match(DbName, [<<"users/123">>]),
    ?assertEqual([], Pids2).

process_exit_cleanup(Config) ->
    DbName = ?config(db_name, Config),
    Parent = self(),

    %% Spawn a subscriber that exits
    Pid = spawn(fun() ->
        {ok, SubRef} = barrel_sub:subscribe(DbName, <<"cleanup/#">>, self()),
        Parent ! {subscribed, SubRef},
        receive after 100 -> ok end
    end),

    receive {subscribed, _SubRef} -> ok end,

    %% Verify match works while process is alive
    Pids1 = barrel_sub:match(DbName, [<<"cleanup/test">>]),
    ?assertEqual([Pid], Pids1),

    %% Wait for process to exit
    timer:sleep(200),

    %% Verify cleanup happened
    Pids2 = barrel_sub:match(DbName, [<<"cleanup/test">>]),
    ?assertEqual([], Pids2).

%%====================================================================
%% Integration test cases
%%====================================================================

put_doc_notifies_subscriber(_Config) ->
    %% Create a test database
    DbName = iolist_to_binary([<<"test_int_put_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    {ok, _} = barrel_docdb:create_db(DbName),

    try
        %% Subscribe to user type documents
        {ok, SubRef} = barrel_docdb:subscribe(DbName, <<"type/#">>),

        %% Create a document
        Doc = #{
            <<"id">> => <<"user1">>,
            <<"type">> => <<"user">>,
            <<"name">> => <<"Alice">>
        },
        {ok, #{<<"id">> := DocId, <<"rev">> := Rev}} = barrel_docdb:put_doc(DbName, Doc),

        %% Should receive notification
        receive
            {barrel_change, DbName, Change} ->
                ?assertEqual(DocId, maps:get(id, Change)),
                ?assertEqual(Rev, maps:get(rev, Change)),
                ?assertEqual(false, maps:get(deleted, Change)),
                ?assert(is_list(maps:get(paths, Change)))
        after 1000 ->
            ct:fail("Did not receive change notification")
        end,

        ok = barrel_docdb:unsubscribe(SubRef)
    after
        barrel_docdb:delete_db(DbName)
    end.

delete_doc_notifies_subscriber(_Config) ->
    %% Create a test database
    DbName = iolist_to_binary([<<"test_int_del_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    {ok, _} = barrel_docdb:create_db(DbName),

    try
        %% Create a document first
        Doc = #{
            <<"id">> => <<"user1">>,
            <<"type">> => <<"user">>,
            <<"name">> => <<"Alice">>
        },
        {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

        %% Subscribe after creation
        {ok, SubRef} = barrel_docdb:subscribe(DbName, <<"user1">>),

        %% Delete the document
        {ok, #{<<"rev">> := Rev2}} = barrel_docdb:delete_doc(DbName, <<"user1">>, #{rev => Rev1}),

        %% Should receive delete notification
        receive
            {barrel_change, DbName, Change} ->
                ?assertEqual(<<"user1">>, maps:get(id, Change)),
                ?assertEqual(Rev2, maps:get(rev, Change)),
                ?assertEqual(true, maps:get(deleted, Change))
        after 1000 ->
            ct:fail("Did not receive delete notification")
        end,

        ok = barrel_docdb:unsubscribe(SubRef)
    after
        barrel_docdb:delete_db(DbName)
    end.

update_doc_notifies_subscriber(_Config) ->
    %% Create a test database
    DbName = iolist_to_binary([<<"test_int_upd_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    {ok, _} = barrel_docdb:create_db(DbName),

    try
        %% Create a document first
        Doc = #{
            <<"id">> => <<"user1">>,
            <<"type">> => <<"user">>,
            <<"name">> => <<"Alice">>
        },
        {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc),

        %% Subscribe to name changes
        {ok, SubRef} = barrel_docdb:subscribe(DbName, <<"name/#">>),

        %% Update the document
        UpdatedDoc = Doc#{<<"_rev">> => Rev1, <<"name">> => <<"Bob">>},
        {ok, #{<<"rev">> := Rev2}} = barrel_docdb:put_doc(DbName, UpdatedDoc),

        %% Should receive update notification
        receive
            {barrel_change, DbName, Change} ->
                ?assertEqual(<<"user1">>, maps:get(id, Change)),
                ?assertEqual(Rev2, maps:get(rev, Change)),
                ?assertEqual(false, maps:get(deleted, Change))
        after 1000 ->
            ct:fail("Did not receive update notification")
        end,

        ok = barrel_docdb:unsubscribe(SubRef)
    after
        barrel_docdb:delete_db(DbName)
    end.

public_api_subscribe_unsubscribe(_Config) ->
    %% Create a test database
    DbName = iolist_to_binary([<<"test_int_api_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    {ok, _} = barrel_docdb:create_db(DbName),

    try
        %% Subscribe using public API
        {ok, SubRef} = barrel_docdb:subscribe(DbName, <<"test/#">>),
        ?assert(is_reference(SubRef)),

        %% Create a document
        {ok, _} = barrel_docdb:put_doc(DbName, #{
            <<"id">> => <<"doc1">>,
            <<"test">> => <<"value">>
        }),

        %% Receive notification
        receive
            {barrel_change, DbName, _Change} -> ok
        after 1000 ->
            ct:fail("Did not receive notification")
        end,

        %% Unsubscribe
        ok = barrel_docdb:unsubscribe(SubRef),

        %% Create another document - should NOT receive notification
        {ok, _} = barrel_docdb:put_doc(DbName, #{
            <<"id">> => <<"doc2">>,
            <<"test">> => <<"value2">>
        }),

        receive
            {barrel_change, DbName, _} ->
                ct:fail("Should not receive notification after unsubscribe")
        after 200 ->
            ok
        end
    after
        barrel_docdb:delete_db(DbName)
    end.
