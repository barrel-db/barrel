%%%-------------------------------------------------------------------
%%% @doc barrel_bql:format_error/1 renders the error a user reads when a
%%% query is rejected. It is pure, it is the whole uncovered half of
%%% barrel_bql, and nothing else exercises its clauses: a typo in a message
%%% ships silently. Pin every reason.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bql_format_error_tests).

-include_lib("eunit/include/eunit.hrl").

fmt(Reason) ->
    barrel_bql:format_error({invalid_query, undefined, Reason}).

%%====================================================================
%% Location prefix
%%====================================================================

no_location_prefix_test() ->
    ?assertEqual(<<"ORDER BY supports a single key">>,
                 fmt({unsupported, multi_key_order_by})).

location_prefix_test() ->
    ?assertEqual(<<"line 3 column 7: ORDER BY supports a single key">>,
                 barrel_bql:format_error(
                     {invalid_query, {3, 7}, {unsupported, multi_key_order_by}})).

parse_error_test() ->
    ?assertEqual(<<"syntax error: unexpected token">>,
                 barrel_bql:format_error(
                     {parse_error, undefined, "unexpected token"})),
    ?assertEqual(<<"line 1 column 5: syntax error: unexpected token">>,
                 barrel_bql:format_error(
                     {parse_error, {1, 5}, "unexpected token"})).

%%====================================================================
%% Parameters
%%====================================================================

params_test() ->
    ?assertEqual(<<"parameter $limit is not bound">>,
                 fmt({unbound_param, <<"limit">>})),
    ?assertEqual(<<"parameter $ids must be a scalar value">>,
                 fmt({invalid_param, <<"ids">>})).

%%====================================================================
%% Aliases and output names
%%====================================================================

alias_test() ->
    ?assertEqual(<<"a table function source needs an alias (AS v)">>,
                 fmt({alias_required, table_fn})),
    ?assertEqual(<<"a projection ending in an array index needs AS">>,
                 fmt({alias_required, indexed_projection})),
    ?assertEqual(<<"alias 'v' is used twice">>,
                 fmt({duplicate_alias, <<"v">>})),
    ?assertEqual(<<"duplicate output column 'title'; add AS">>,
                 fmt({duplicate_output_name, <<"title">>})).

%%====================================================================
%% Predicates
%%====================================================================

predicate_test() ->
    ?assertEqual(<<"field '_seq' is reserved and not queryable">>,
                 fmt({reserved_field, <<"_seq">>})),
    ?assertEqual(<<"comparisons with NULL never match; "
                   "use IS NULL or IS MISSING">>,
                 fmt({use_is_null, ignored})),
    ?assertEqual(<<"predicates must reference a document field">>,
                 fmt({constant_predicate, ignored})).

%%====================================================================
%% Table functions
%%====================================================================

unknown_table_function_test() ->
    ?assertEqual(<<"unknown table function knn_top_k "
                   "(vector_top_k, bm25_top_k, hybrid_top_k)">>,
                 fmt({unknown_table_function, <<"knn_top_k">>})).

table_fn_arg_test() ->
    ?assertEqual(<<"vector_top_k needs a string query as its first argument">>,
                 fmt({invalid_table_fn_arg, vector_top_k, query})),
    ?assertEqual(<<"duplicate option k for bm25_top_k">>,
                 fmt({invalid_table_fn_arg, bm25_top_k, {duplicate, <<"k">>}})),
    ?assertEqual(<<"unknown option depth for hybrid_top_k">>,
                 fmt({invalid_table_fn_arg, hybrid_top_k, <<"depth">>})),
    %% The catch-all clause: a non-binary "what" falls through to io_lib.
    ?assertEqual(<<"invalid k option for vector_top_k">>,
                 fmt({invalid_table_fn_arg, vector_top_k, k})).

%%====================================================================
%% Paths, ordering, subscribe
%%====================================================================

path_and_order_test() ->
    ?assertEqual(<<"UNNEST needs a document array path">>,
                 fmt({invalid_unnest_path, ignored})),
    ?assertEqual(<<"cannot ORDER BY the source itself">>,
                 fmt({invalid_order_key, ignored})).

subscribe_test() ->
    ?assertEqual(<<"ORDER BY is not supported with SUBSCRIBE">>,
                 fmt({unsupported_with_subscribe, <<"ORDER BY">>})).

%%====================================================================
%% Unsupported constructs
%%====================================================================

unsupported_test() ->
    ?assertEqual(<<"wildcard [*] paths are not supported; use UNNEST">>,
                 fmt({unsupported, wildcard_path, use_unnest})),
    ?assertEqual(<<"the document id only supports =, range comparisons "
                   "and LIKE 'prefix%'">>,
                 fmt({unsupported, id_condition})),
    ?assertEqual(<<"document id conditions cannot appear inside OR or NOT">>,
                 fmt({unsupported, id_in_disjunction})),
    ?assertEqual(<<"_score and _distance can be selected and ordered by, "
                   "not filtered on">>,
                 fmt({unsupported, score_in_where})),
    ?assertEqual(<<"unsupported: GROUP BY">>,
                 fmt({unsupported, <<"GROUP BY">>})).

%%====================================================================
%% Catch-all
%%====================================================================

unknown_reason_test() ->
    ?assertEqual(<<"{surprise,42}">>, fmt({surprise, 42})).
