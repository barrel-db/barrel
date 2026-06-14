%%%-------------------------------------------------------------------
%%% @doc Common Test suite for barrel_query
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_query_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases - compilation
-export([
    compile_simple_equality/1,
    compile_multiple_conditions/1,
    compile_with_variables/1,
    compile_comparison/1,
    compile_and_or/1,
    compile_in_operator/1,
    compile_exists_missing/1,
    compile_regex_prefix/1,
    compile_invalid_spec/1,
    compile_invalid_condition/1,
    compile_invalid_operator/1,
    matches_function/1
]).

%% Test cases - validation
-export([
    validate_valid_spec/1,
    validate_missing_where/1,
    validate_invalid_path/1,
    validate_invalid_regex/1
]).

%% Test cases - strategy
-export([
    strategy_index_seek/1,
    strategy_index_scan/1,
    strategy_multi_index/1,
    strategy_full_scan/1
]).

%% Test cases - execution
-export([
    execute_simple_equality/1,
    execute_multiple_conditions/1,
    execute_comparison_gt/1,
    execute_comparison_lt/1,
    execute_in_operator/1,
    execute_or_condition/1,
    execute_not_condition/1,
    execute_exists/1,
    execute_missing/1,
    execute_prefix/1,
    execute_regex/1,
    execute_nested_path/1,
    execute_with_limit/1,
    execute_with_offset/1,
    execute_with_order/1,
    execute_include_docs/1,
    execute_variable_binding/1,
    execute_multi_index_intersection/1,
    execute_multi_index_zero_cardinality/1,
    execute_multi_index_with_limit/1,
    execute_chunked_query/1,
    execute_chunked_with_include_docs/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, compilation},
        {group, validation},
        {group, strategy},
        {group, execution}
    ].

groups() ->
    [
        {compilation, [sequence], [
            compile_simple_equality,
            compile_multiple_conditions,
            compile_with_variables,
            compile_comparison,
            compile_and_or,
            compile_in_operator,
            compile_exists_missing,
            compile_regex_prefix,
            compile_invalid_spec,
            compile_invalid_condition,
            compile_invalid_operator,
            matches_function
        ]},
        {validation, [sequence], [
            validate_valid_spec,
            validate_missing_where,
            validate_invalid_path,
            validate_invalid_regex
        ]},
        {strategy, [sequence], [
            strategy_index_seek,
            strategy_index_scan,
            strategy_multi_index,
            strategy_full_scan
        ]},
        {execution, [sequence], [
            execute_simple_equality,
            execute_multiple_conditions,
            execute_comparison_gt,
            execute_comparison_lt,
            execute_in_operator,
            execute_or_condition,
            execute_not_condition,
            execute_exists,
            execute_missing,
            execute_prefix,
            execute_regex,
            execute_nested_path,
            execute_with_limit,
            execute_with_offset,
            execute_with_order,
            execute_include_docs,
            execute_variable_binding,
            execute_multi_index_intersection,
            execute_multi_index_zero_cardinality,
            execute_multi_index_with_limit,
            execute_chunked_query,
            execute_chunked_with_include_docs
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(execution, Config) ->
    %% Set up database with test documents for execution tests
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DbName = <<"query_test_db">>,
    {ok, Pid} = barrel_docdb:create_db(DbName, #{}),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),

    %% Insert test documents
    Docs = [
        #{<<"id">> => <<"user1">>, <<"type">> => <<"user">>, <<"name">> => <<"Alice">>,
          <<"age">> => 30, <<"status">> => <<"active">>, <<"org">> => <<"org1">>},
        #{<<"id">> => <<"user2">>, <<"type">> => <<"user">>, <<"name">> => <<"Bob">>,
          <<"age">> => 25, <<"status">> => <<"active">>, <<"org">> => <<"org1">>},
        #{<<"id">> => <<"user3">>, <<"type">> => <<"user">>, <<"name">> => <<"Charlie">>,
          <<"age">> => 35, <<"status">> => <<"inactive">>, <<"org">> => <<"org2">>},
        #{<<"id">> => <<"post1">>, <<"type">> => <<"post">>, <<"title">> => <<"Hello World">>,
          <<"author">> => <<"user1">>, <<"tags">> => [<<"intro">>, <<"welcome">>]},
        #{<<"id">> => <<"post2">>, <<"type">> => <<"post">>, <<"title">> => <<"Goodbye">>,
          <<"author">> => <<"user2">>},
        #{<<"id">> => <<"nested1">>, <<"type">> => <<"nested">>,
          <<"profile">> => #{<<"name">> => <<"Deep">>, <<"address">> => #{<<"city">> => <<"Paris">>}}}
    ],

    lists:foreach(
        fun(Doc) ->
            {ok, _} = barrel_docdb:put_doc(DbName, Doc)
        end,
        Docs
    ),

    [{db_name, DbName}, {store_ref, StoreRef} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(execution, Config) ->
    DbName = proplists:get_value(db_name, Config),
    ok = barrel_docdb:delete_db(DbName),
    ok = application:stop(barrel_docdb),
    Config;
end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Compilation
%%====================================================================

compile_simple_equality(_Config) ->
    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual([{path, [<<"type">>], <<"user">>}], maps:get(conditions, Explained)),
    ok.

compile_multiple_conditions(_Config) ->
    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(2, length(maps:get(conditions, Explained))),
    ok.

compile_with_variables(_Config) ->
    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"org">>], '?Org'}
        ],
        select => ['?Org']
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    Bindings = maps:get(bindings, Explained),
    ?assertEqual([<<"org">>], maps:get('?Org', Bindings)),
    ok.

compile_comparison(_Config) ->
    Spec = #{
        where => [
            {compare, [<<"age">>], '>', 18}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    [{compare, [<<"age">>], '>', 18}] = maps:get(conditions, Explained),
    ok.

compile_and_or(_Config) ->
    Spec = #{
        where => [
            {'and', [
                {path, [<<"type">>], <<"user">>},
                {'or', [
                    {path, [<<"status">>], <<"active">>},
                    {path, [<<"status">>], <<"pending">>}
                ]}
            ]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    [{'and', _}] = maps:get(conditions, Explained),
    ok.

compile_in_operator(_Config) ->
    Spec = #{
        where => [
            {in, [<<"status">>], [<<"active">>, <<"pending">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    [{in, [<<"status">>], [<<"active">>, <<"pending">>]}] = maps:get(conditions, Explained),
    ok.

compile_exists_missing(_Config) ->
    Spec = #{
        where => [
            {exists, [<<"email">>]},
            {missing, [<<"deleted">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(2, length(maps:get(conditions, Explained))),
    ok.

compile_regex_prefix(_Config) ->
    Spec = #{
        where => [
            {regex, [<<"name">>], <<"^A.*">>},
            {prefix, [<<"email">>], <<"admin@">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(2, length(maps:get(conditions, Explained))),
    ok.

compile_invalid_spec(_Config) ->
    ?assertEqual({error, {invalid_spec, not_a_map}}, barrel_query:compile(not_a_map)),
    ok.

compile_invalid_condition(_Config) ->
    Spec = #{
        where => [{invalid_op, [<<"field">>], <<"value">>}]
    },
    {error, {invalid_condition, _}} = barrel_query:compile(Spec),
    ok.

compile_invalid_operator(_Config) ->
    Spec = #{
        where => [{compare, [<<"age">>], 'invalid_op', 18}]
    },
    {error, {invalid_operator, invalid_op}} = barrel_query:compile(Spec),
    ok.

matches_function(_Config) ->
    %% Test matches/2 function - simple condition matching without compiled plan
    Doc = #{<<"type">> => <<"user">>, <<"age">> => 30, <<"name">> => <<"Alice">>},

    %% Test equality condition
    ?assert(barrel_query:matches(Doc, [{path, [<<"type">>], <<"user">>}])),
    ?assertNot(barrel_query:matches(Doc, [{path, [<<"type">>], <<"admin">>}])),

    %% Test comparison condition
    ?assert(barrel_query:matches(Doc, [{compare, [<<"age">>], '>', 18}])),
    ?assertNot(barrel_query:matches(Doc, [{compare, [<<"age">>], '>', 40}])),

    %% Test multiple conditions (AND)
    ?assert(barrel_query:matches(Doc, [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>=', 30}
    ])),

    %% Test empty conditions (always matches)
    ?assert(barrel_query:matches(Doc, [])),

    ok.

%%====================================================================
%% Test Cases - Validation
%%====================================================================

validate_valid_spec(_Config) ->
    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}]
    },
    ?assertEqual(ok, barrel_query:validate_spec(Spec)),
    ok.

validate_missing_where(_Config) ->
    Spec = #{
        select => ['*']
    },
    ?assertEqual({error, {missing_clause, where}}, barrel_query:validate_spec(Spec)),
    ok.

validate_invalid_path(_Config) ->
    Spec = #{
        where => [{path, not_a_list, <<"value">>}]
    },
    {error, {invalid_path, _, _}} = barrel_query:validate_spec(Spec),
    ok.

validate_invalid_regex(_Config) ->
    Spec = #{
        where => [{regex, [<<"field">>], <<"[invalid">>}]
    },
    {error, {invalid_regex, _}} = barrel_query:validate_spec(Spec),
    ok.

%%====================================================================
%% Test Cases - Strategy
%%====================================================================

strategy_index_seek(_Config) ->
    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(index_seek, maps:get(strategy, Explained)),
    ok.

strategy_index_scan(_Config) ->
    Spec = #{
        where => [{compare, [<<"age">>], '>', 18}]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(index_scan, maps:get(strategy, Explained)),
    ok.

strategy_multi_index(_Config) ->
    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(multi_index, maps:get(strategy, Explained)),
    ok.

strategy_full_scan(_Config) ->
    Spec = #{
        where => [
            {path, [<<"name">>], '?Name'}  % Only variable binding, no concrete value
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(full_scan, maps:get(strategy, Explained)),
    ok.

%%====================================================================
%% Test Cases - Execution
%%====================================================================

execute_simple_equality(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _LastSeq} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(3, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ?assert(lists:member(<<"user3">>, DocIds)),
    ok.

execute_multiple_conditions(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ok.

execute_comparison_gt(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {compare, [<<"age">>], '>', 28}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),  % age 30
    ?assert(lists:member(<<"user3">>, DocIds)),  % age 35
    ok.

execute_comparison_lt(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {compare, [<<"age">>], '<', 30}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"user2">>, maps:get(<<"id">>, Result)),  % age 25
    ok.

execute_in_operator(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {in, [<<"org">>], [<<"org1">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ok.

execute_or_condition(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {'or', [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"type">>], <<"post">>}
            ]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(5, length(Results)),  % 3 users + 2 posts

    %% Test OR condition with LIMIT - uses relaxed early limit
    Spec2 = #{
        where => [
            {'or', [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"type">>], <<"post">>}
            ]}
        ],
        limit => 2
    },
    {ok, Plan2} = barrel_query:compile(Spec2),
    {ok, Results2, _} = barrel_query:execute(StoreRef, DbName, Plan2),
    ?assertEqual(2, length(Results2)),

    %% Test equality + OR with LIMIT - remaining OR condition uses early limit
    Spec3 = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {'or', [
                {path, [<<"status">>], <<"active">>},
                {path, [<<"status">>], <<"inactive">>}
            ]}
        ],
        limit => 1
    },
    {ok, Plan3} = barrel_query:compile(Spec3),
    {ok, Results3, _} = barrel_query:execute(StoreRef, DbName, Plan3),
    ?assertEqual(1, length(Results3)),

    ok.

execute_not_condition(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {'not', {path, [<<"status">>], <<"inactive">>}}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),  % Only active users
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assertNot(lists:member(<<"user3">>, DocIds)),  % user3 is inactive
    ok.

execute_exists(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"post">>},
            {exists, [<<"tags">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"post1">>, maps:get(<<"id">>, Result)),
    ok.

execute_missing(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"post">>},
            {missing, [<<"tags">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"post2">>, maps:get(<<"id">>, Result)),
    ok.

execute_prefix(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {prefix, [<<"name">>], <<"A">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"user1">>, maps:get(<<"id">>, Result)),  % Alice
    ok.

execute_regex(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {regex, [<<"name">>], <<"^[AB].*">>}  % Names starting with A or B
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),  % Alice
    ?assert(lists:member(<<"user2">>, DocIds)),  % Bob
    ok.

execute_nested_path(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"profile">>, <<"address">>, <<"city">>], <<"Paris">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"nested1">>, maps:get(<<"id">>, Result)),
    ok.

execute_with_limit(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        limit => 2
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    ok.

execute_with_offset(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        offset => 1,
        limit => 2
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    ok.

execute_with_order(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        select => ['?Name'],
        order_by => {'?Name', asc}
    },
    %% First add variable binding for name
    SpecWithBinding = Spec#{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"name">>], '?Name'}
        ]
    },
    {ok, Plan} = barrel_query:compile(SpecWithBinding),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(3, length(Results)),
    Names = [maps:get(<<"?Name">>, R) || R <- Results],
    ?assertEqual([<<"Alice">>, <<"Bob">>, <<"Charlie">>], Names),
    ok.

execute_include_docs(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        include_docs => true,
        limit => 1
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assert(maps:is_key(<<"doc">>, Result)),
    Doc = maps:get(<<"doc">>, Result),
    ?assert(maps:is_key(<<"type">>, Doc)),
    ok.

execute_variable_binding(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"org">>], '?Org'},
            {path, [<<"name">>], '?Name'}
        ],
        select => ['?Org', '?Name']
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(3, length(Results)),
    lists:foreach(
        fun(R) ->
            ?assert(maps:is_key(<<"?Org">>, R)),
            ?assert(maps:is_key(<<"?Name">>, R))
        end,
        Results
    ),
    ok.

execute_multi_index_intersection(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"org">>], <<"org1">>},
            {path, [<<"status">>], <<"active">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ok.

%% Test that 3+ condition queries with a zero-cardinality condition
%% short-circuit and return empty results immediately
execute_multi_index_zero_cardinality(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    %% Query with nonexistent value - should short-circuit and return empty
    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"org">>], <<"org1">>},
            {path, [<<"status">>], <<"nonexistent">>}  %% No docs have this status
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(0, length(Results)),
    ok.

%% Test multi-index intersection with limit for early termination
execute_multi_index_with_limit(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    %% Query with multiple conditions and limit
    %% Should use early termination in intersection
    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ],
        limit => 1
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    %% Should return exactly 1 result due to limit
    ?assertEqual(1, length(Results)),

    %% Verify the result is a valid user with active status
    [Result] = Results,
    DocId = maps:get(<<"id">>, Result),
    ?assert(is_binary(DocId)),

    %% Test with limit 2
    Spec2 = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ],
        limit => 2
    },
    {ok, Plan2} = barrel_query:compile(Spec2),
    {ok, Results2, _} = barrel_query:execute(StoreRef, DbName, Plan2),
    ?assertEqual(2, length(Results2)),

    ok.

%% Test chunked query execution with continuation tokens
execute_chunked_query(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    %% Pure equality query with small chunk size
    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        include_docs => false
    },
    {ok, Plan} = barrel_query:compile(Spec),

    %% First chunk with size 2 (we have 3 users)
    {ok, Results1, Meta1} = barrel_query:execute(StoreRef, DbName, Plan, #{chunk_size => 2}),

    ?assertEqual(2, length(Results1)),
    ?assertEqual(true, maps:get(has_more, Meta1)),
    ?assert(maps:is_key(continuation, Meta1)),
    ?assert(maps:is_key(last_seq, Meta1)),

    %% Get continuation token
    Token = maps:get(continuation, Meta1),

    %% Second chunk should get remaining docs
    {ok, Results2, Meta2} = barrel_query:execute(StoreRef, DbName, Plan, #{continuation => Token}),

    ?assertEqual(1, length(Results2)),
    ?assertEqual(false, maps:get(has_more, Meta2)),
    ?assertNot(maps:is_key(continuation, Meta2)),

    %% Verify all users were returned across chunks
    AllDocIds = [maps:get(<<"id">>, R) || R <- Results1 ++ Results2],
    ?assertEqual(3, length(AllDocIds)),
    ?assert(lists:member(<<"user1">>, AllDocIds)),
    ?assert(lists:member(<<"user2">>, AllDocIds)),
    ?assert(lists:member(<<"user3">>, AllDocIds)),

    ok.

execute_chunked_with_include_docs(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    %% Pure equality query with include_docs and small chunk size
    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        include_docs => true
    },
    {ok, Plan} = barrel_query:compile(Spec),

    %% First chunk with size 2 (we have 3 users)
    {ok, Results1, Meta1} = barrel_query:execute(StoreRef, DbName, Plan, #{chunk_size => 2}),

    ?assertEqual(2, length(Results1)),
    ?assertEqual(true, maps:get(has_more, Meta1)),
    ?assert(maps:is_key(continuation, Meta1)),

    %% Verify docs are included in results
    [R1, R2] = Results1,
    ?assert(maps:is_key(<<"doc">>, R1)),
    ?assert(maps:is_key(<<"doc">>, R2)),
    Doc1 = maps:get(<<"doc">>, R1),
    ?assertEqual(<<"user">>, maps:get(<<"type">>, Doc1)),

    %% Get continuation token
    Token = maps:get(continuation, Meta1),

    %% Second chunk should get remaining doc with include_docs
    {ok, Results2, Meta2} = barrel_query:execute(StoreRef, DbName, Plan, #{continuation => Token}),

    ?assertEqual(1, length(Results2)),
    ?assertEqual(false, maps:get(has_more, Meta2)),

    %% Verify doc is included
    [R3] = Results2,
    ?assert(maps:is_key(<<"doc">>, R3)),

    %% Verify all users were returned across chunks
    AllDocIds = [maps:get(<<"id">>, R) || R <- Results1 ++ Results2],
    ?assertEqual(3, length(AllDocIds)),

    ok.
