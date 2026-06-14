%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_query_sub query subscription manager
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_query_sub_SUITE).

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
    subscribe_simple_query/1,
    subscribe_complex_query/1,
    unsubscribe_removes_subscription/1,
    process_exit_cleanup/1,
    match_notifies_subscriber/1,
    no_match_no_notification/1,
    path_optimization_works/1,
    multiple_queries_same_db/1,
    %% Integration tests
    put_doc_matching_query/1,
    put_doc_not_matching_query/1,
    update_doc_changes_match/1,
    public_api_works/1
]).

%%====================================================================
%% Common Test callbacks
%%====================================================================

all() ->
    [{group, subscribe}, {group, match}, {group, integration}].

groups() ->
    [
        {subscribe, [parallel], [
            subscribe_simple_query,
            subscribe_complex_query,
            unsubscribe_removes_subscription,
            process_exit_cleanup
        ]},
        {match, [parallel], [
            match_notifies_subscriber,
            no_match_no_notification,
            path_optimization_works,
            multiple_queries_same_db
        ]},
        {integration, [sequence], [
            put_doc_matching_query,
            put_doc_not_matching_query,
            update_doc_changes_match,
            public_api_works
        ]}
    ].

init_per_suite(Config) ->
    {ok, _Apps} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_testcase(_TestCase, Config) ->
    DbName = iolist_to_binary([<<"test_qsub_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    [{db_name, DbName} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Subscribe test cases
%%====================================================================

subscribe_simple_query(Config) ->
    DbName = ?config(db_name, Config),
    Query = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, SubRef} = barrel_query_sub:subscribe(DbName, Query, self()),
    ?assert(is_reference(SubRef)),
    ok = barrel_query_sub:unsubscribe(SubRef).

subscribe_complex_query(Config) ->
    DbName = ?config(db_name, Config),
    Query = #{
        where => [
            {path, [<<"type">>], <<"order">>},
            {compare, [<<"total">>], '>', 100}
        ]
    },
    {ok, SubRef} = barrel_query_sub:subscribe(DbName, Query, self()),
    ?assert(is_reference(SubRef)),
    ok = barrel_query_sub:unsubscribe(SubRef).

unsubscribe_removes_subscription(Config) ->
    DbName = ?config(db_name, Config),
    Query = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, SubRef} = barrel_query_sub:subscribe(DbName, Query, self()),
    ok = barrel_query_sub:unsubscribe(SubRef),
    %% Should not crash if unsubscribe called again
    ok = barrel_query_sub:unsubscribe(SubRef).

process_exit_cleanup(Config) ->
    DbName = ?config(db_name, Config),
    Parent = self(),
    Query = #{where => [{path, [<<"type">>], <<"cleanup">>}]},

    _Pid = spawn(fun() ->
        {ok, SubRef} = barrel_query_sub:subscribe(DbName, Query, self()),
        Parent ! {subscribed, SubRef},
        receive after 100 -> ok end
    end),

    receive {subscribed, _SubRef} -> ok end,

    %% Wait for process to exit
    timer:sleep(200),

    %% Subscription should be cleaned up (no crash on notify)
    Doc = #{<<"type">> => <<"cleanup">>},
    barrel_query_sub:notify_change(DbName, <<"doc1">>, <<"1-abc">>, Doc),
    ok.

%%====================================================================
%% Match test cases
%%====================================================================

match_notifies_subscriber(Config) ->
    DbName = ?config(db_name, Config),
    Query = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, SubRef} = barrel_query_sub:subscribe(DbName, Query, self()),

    %% Notify with matching document
    Doc = #{<<"type">> => <<"user">>, <<"name">> => <<"Alice">>},
    barrel_query_sub:notify_change(DbName, <<"user1">>, <<"1-abc">>, Doc),

    %% Should receive notification
    receive
        {barrel_query_change, DbName, #{id := <<"user1">>, rev := <<"1-abc">>}} ->
            ok
    after 500 ->
        ct:fail("Did not receive notification")
    end,

    ok = barrel_query_sub:unsubscribe(SubRef).

no_match_no_notification(Config) ->
    DbName = ?config(db_name, Config),
    Query = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, SubRef} = barrel_query_sub:subscribe(DbName, Query, self()),

    %% Notify with non-matching document
    Doc = #{<<"type">> => <<"order">>, <<"amount">> => 100},
    barrel_query_sub:notify_change(DbName, <<"order1">>, <<"1-abc">>, Doc),

    %% Should NOT receive notification
    receive
        {barrel_query_change, _, _} ->
            ct:fail("Should not receive notification for non-matching doc")
    after 200 ->
        ok
    end,

    ok = barrel_query_sub:unsubscribe(SubRef).

path_optimization_works(_Config) ->
    %% Test that path extraction works correctly for optimization
    Query1 = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, Plan1} = barrel_query:compile(Query1),
    Paths1 = barrel_query:extract_paths(Plan1),
    ?assertEqual([<<"type/#">>], Paths1),

    Query2 = #{where => [
        {path, [<<"type">>], <<"order">>},
        {compare, [<<"total">>], '>', 100}
    ]},
    {ok, Plan2} = barrel_query:compile(Query2),
    Paths2 = barrel_query:extract_paths(Plan2),
    ?assertEqual([<<"total/#">>, <<"type/#">>], lists:sort(Paths2)),

    ok.

multiple_queries_same_db(Config) ->
    DbName = ?config(db_name, Config),
    Query1 = #{where => [{path, [<<"type">>], <<"user">>}]},
    Query2 = #{where => [{path, [<<"type">>], <<"order">>}]},

    {ok, SubRef1} = barrel_query_sub:subscribe(DbName, Query1, self()),
    {ok, SubRef2} = barrel_query_sub:subscribe(DbName, Query2, self()),

    ?assertNotEqual(SubRef1, SubRef2),

    %% Notify with user doc
    UserDoc = #{<<"type">> => <<"user">>},
    barrel_query_sub:notify_change(DbName, <<"user1">>, <<"1-abc">>, UserDoc),

    receive
        {barrel_query_change, DbName, #{id := <<"user1">>}} ->
            ok
    after 500 ->
        ct:fail("Did not receive user notification")
    end,

    %% Notify with order doc
    OrderDoc = #{<<"type">> => <<"order">>},
    barrel_query_sub:notify_change(DbName, <<"order1">>, <<"1-def">>, OrderDoc),

    receive
        {barrel_query_change, DbName, #{id := <<"order1">>}} ->
            ok
    after 500 ->
        ct:fail("Did not receive order notification")
    end,

    ok = barrel_query_sub:unsubscribe(SubRef1),
    ok = barrel_query_sub:unsubscribe(SubRef2).

%%====================================================================
%% Integration test cases
%%====================================================================

put_doc_matching_query(_Config) ->
    DbName = iolist_to_binary([<<"test_int_qput_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    {ok, _} = barrel_docdb:create_db(DbName),

    try
        %% Subscribe to user type documents
        Query = #{where => [{path, [<<"type">>], <<"user">>}]},
        {ok, SubRef} = barrel_docdb:subscribe_query(DbName, Query),

        %% Create a matching document
        Doc = #{
            <<"id">> => <<"user1">>,
            <<"type">> => <<"user">>,
            <<"name">> => <<"Alice">>
        },
        {ok, #{<<"id">> := DocId, <<"rev">> := _Rev}} = barrel_docdb:put_doc(DbName, Doc),

        %% Should receive notification
        receive
            {barrel_query_change, DbName, #{id := DocId}} ->
                ok
        after 1000 ->
            ct:fail("Did not receive query change notification")
        end,

        ok = barrel_docdb:unsubscribe_query(SubRef)
    after
        barrel_docdb:delete_db(DbName)
    end.

put_doc_not_matching_query(_Config) ->
    DbName = iolist_to_binary([<<"test_int_qnomatch_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    {ok, _} = barrel_docdb:create_db(DbName),

    try
        %% Subscribe to user type documents only
        Query = #{where => [{path, [<<"type">>], <<"user">>}]},
        {ok, SubRef} = barrel_docdb:subscribe_query(DbName, Query),

        %% Create a non-matching document (order, not user)
        Doc = #{
            <<"id">> => <<"order1">>,
            <<"type">> => <<"order">>,
            <<"total">> => 100
        },
        {ok, _} = barrel_docdb:put_doc(DbName, Doc),

        %% Should NOT receive notification
        receive
            {barrel_query_change, DbName, _} ->
                ct:fail("Should not receive notification for non-matching doc")
        after 300 ->
            ok
        end,

        ok = barrel_docdb:unsubscribe_query(SubRef)
    after
        barrel_docdb:delete_db(DbName)
    end.

update_doc_changes_match(_Config) ->
    DbName = iolist_to_binary([<<"test_int_qupd_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    {ok, _} = barrel_docdb:create_db(DbName),

    try
        %% Create initial document (not matching)
        Doc1 = #{
            <<"id">> => <<"doc1">>,
            <<"type">> => <<"draft">>,
            <<"name">> => <<"Alice">>
        },
        {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(DbName, Doc1),

        %% Subscribe to user type documents
        Query = #{where => [{path, [<<"type">>], <<"user">>}]},
        {ok, SubRef} = barrel_docdb:subscribe_query(DbName, Query),

        %% Update document to match query
        Doc2 = Doc1#{<<"_rev">> => Rev1, <<"type">> => <<"user">>},
        {ok, _} = barrel_docdb:put_doc(DbName, Doc2),

        %% Should receive notification
        receive
            {barrel_query_change, DbName, #{id := <<"doc1">>}} ->
                ok
        after 1000 ->
            ct:fail("Did not receive query change notification")
        end,

        ok = barrel_docdb:unsubscribe_query(SubRef)
    after
        barrel_docdb:delete_db(DbName)
    end.

public_api_works(_Config) ->
    DbName = iolist_to_binary([<<"test_int_qapi_">>, integer_to_binary(erlang:unique_integer([positive]))]),
    {ok, _} = barrel_docdb:create_db(DbName),

    try
        %% Subscribe using public API
        Query = #{where => [{path, [<<"status">>], <<"active">>}]},
        {ok, SubRef} = barrel_docdb:subscribe_query(DbName, Query),
        ?assert(is_reference(SubRef)),

        %% Create a matching document
        {ok, _} = barrel_docdb:put_doc(DbName, #{
            <<"id">> => <<"item1">>,
            <<"status">> => <<"active">>
        }),

        %% Receive notification
        receive
            {barrel_query_change, DbName, _Change} -> ok
        after 1000 ->
            ct:fail("Did not receive notification")
        end,

        %% Unsubscribe
        ok = barrel_docdb:unsubscribe_query(SubRef),

        %% Create another matching doc - should NOT receive notification
        {ok, _} = barrel_docdb:put_doc(DbName, #{
            <<"id">> => <<"item2">>,
            <<"status">> => <<"active">>
        }),

        receive
            {barrel_query_change, DbName, _} ->
                ct:fail("Should not receive notification after unsubscribe")
        after 200 ->
            ok
        end
    after
        barrel_docdb:delete_db(DbName)
    end.
