%% Registered BQL FROM sources (barrel_bql_source): lowering, argument
%% validation, WHERE re-applied after pushdown, order, limits,
%% projection and chunked reads.
-module(barrel_bql_source_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([unknown_source/1, registry_rules/1, argument_errors/1,
         where_reapplied/1, pushdown_narrows/1, order_limit_projection/1,
         served_order_streams/1, chunked_reads/1, fold_reads/1,
         sort_cap/1, subscribe_refused/1, unregister_hides/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(SRC, barrel_bql_test_source).
-define(DB, <<"bql_source_db">>).

all() ->
    [unknown_source, registry_rules, argument_errors, where_reapplied,
     pushdown_narrows, order_limit_projection, served_order_streams,
     chunked_reads, fold_reads, sort_cap, subscribe_refused, unregister_hides].

init_per_suite(Config) ->
    Dir = filename:join(?config(priv_dir, Config), "data"),
    _ = application:load(barrel_docdb),
    ok = application:set_env(barrel_docdb, data_dir, Dir),
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = barrel_docdb:create_db(?DB),
    Config.

end_per_suite(_Config) ->
    _ = barrel_docdb:delete_db(?DB),
    ok = application:stop(barrel_docdb).

init_per_testcase(_Case, Config) ->
    ok = barrel_bql:register_source(<<"rows">>, ?SRC),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = barrel_bql:unregister_source(<<"rows">>),
    case whereis(bql_test_source_probe) of
        undefined -> ok;
        _ -> unregister(bql_test_source_probe)
    end,
    ok.

%%====================================================================
%% Cases
%%====================================================================

unknown_source(_Config) ->
    {error, {invalid_query, _, {unknown_table_function, <<"nope">>}} = Err} =
        barrel_bql:compile(<<"SELECT * FROM nope(1) AS v">>),
    Text = barrel_bql:format_error(Err),
    ?assertNotEqual(nomatch, binary:match(Text, <<"rows">>)),
    {error, {invalid_query, _, {unknown_table_function, <<"nope">>}}} =
        barrel_docdb:query(?DB, <<"SELECT * FROM nope(1) AS v">>).

registry_rules(_Config) ->
    {error, builtin} = barrel_bql:register_source(<<"vector_top_k">>, ?SRC),
    {error, invalid_name} = barrel_bql:register_source(<<"Rows">>, ?SRC),
    {error, invalid_name} = barrel_bql:register_source(<<"1rows">>, ?SRC),
    ?assert(lists:member(<<"rows">>, barrel_bql_source:names())).

argument_errors(_Config) ->
    {error, {invalid_query, _, {invalid_source_arg, <<"rows">>, count_required}}} =
        barrel_bql:compile(<<"SELECT * FROM rows('x') AS v">>),
    {error, {invalid_query, _, {invalid_source_arg, <<"rows">>, {unknown_options, [<<"k">>]}}}} =
        barrel_bql:compile(<<"SELECT * FROM rows(3, k => 1) AS v">>),
    {error, {invalid_query, _, {invalid_source_arg, <<"rows">>, {duplicate, <<"push">>}}}} =
        barrel_bql:compile(<<"SELECT * FROM rows(3, push => true, push => false) AS v">>),
    {error, {invalid_query, _, {invalid_source_arg, <<"rows">>, positional_after_named}}} =
        barrel_bql:compile(<<"SELECT * FROM rows(push => true, 3) AS v">>),
    {error, {invalid_query, _, {alias_required, table_fn}}} =
        barrel_bql:compile(<<"SELECT * FROM rows(3)">>),
    %% params substitute into source args like table-function args
    {ok, #{source := {source, <<"rows">>, ?SRC, #{n := 4}}}} =
        barrel_bql:compile(<<"SELECT * FROM rows($n) AS v">>,
                           #{params => #{<<"n">> => 4}}).

%% The source ignores WHERE; the executor still filters its rows.
where_reapplied(_Config) ->
    {ok, Rows, #{has_more := false}} = barrel_docdb:query(
        ?DB, <<"SELECT v.n AS n FROM rows(6) AS v WHERE v.n >= 3 AND v.key[0] = 'g'">>),
    ?assertEqual([3, 4, 5, 6], [N || #{<<"n">> := N} <- Rows]).

%% A source that narrows returns the same rows as one that does not.
pushdown_narrows(_Config) ->
    Bql = fun(Push) ->
        iolist_to_binary(["SELECT * FROM rows(9", Push, ") AS v WHERE v.n = 4"])
    end,
    {ok, Plain, _} = barrel_docdb:query(?DB, Bql("")),
    register(bql_test_source_probe, self()),
    {ok, Pushed, _} = barrel_docdb:query(?DB, Bql(", push => true")),
    receive {source_run, #{where := [{path, [<<"n">>], 4}]}} -> ok
    after 1000 -> ct:fail(no_where_passed)
    end,
    ?assertEqual(Plain, Pushed),
    ?assertMatch([#{<<"id">> := <<"4">>}], Pushed).

order_limit_projection(_Config) ->
    {ok, Desc, _} = barrel_docdb:query(
        ?DB, <<"SELECT v.id AS id FROM rows(5) AS v ORDER BY v.n DESC LIMIT 2 OFFSET 1">>),
    ?assertEqual([#{<<"id">> => <<"4">>}, #{<<"id">> => <<"3">>}], Desc),
    {ok, [#{<<"first">> := <<"g">>}], _} = barrel_docdb:query(
        ?DB, <<"SELECT v.key[0] AS first FROM rows(5) AS v LIMIT 1">>).

%% ORDER BY the source's own order streams without a sort buffer and
%% stops reading once LIMIT rows are collected.
served_order_streams(_Config) ->
    register(bql_test_source_probe, self()),
    {ok, Rows, _} = barrel_docdb:query(
        ?DB, <<"SELECT v.n AS n FROM rows(5000) AS v ORDER BY v.n LIMIT 3">>, #{max_rows => 10}),
    ?assertEqual([1, 2, 3], [N || #{<<"n">> := N} <- Rows]),
    ?assertEqual(1, drain_runs(0)).

chunked_reads(_Config) ->
    Bql = <<"SELECT v.n AS n FROM rows(5) AS v">>,
    {ok, R1, #{has_more := true, continuation := C1}} =
        barrel_docdb:query(?DB, Bql, #{chunk_size => 2}),
    {ok, R2, #{has_more := true, continuation := C2}} =
        barrel_docdb:query(?DB, Bql, #{chunk_size => 2, continuation => C1}),
    {ok, R3, #{has_more := false}} =
        barrel_docdb:query(?DB, Bql, #{chunk_size => 2, continuation => C2}),
    ?assertEqual([1, 2, 3, 4, 5], [N || #{<<"n">> := N} <- R1 ++ R2 ++ R3]),
    %% a sorting plan cannot resume from a continuation
    {error, {unsupported, continuation}} = barrel_docdb:query(
        ?DB, <<"SELECT * FROM rows(5) AS v ORDER BY v.n DESC">>, #{continuation => 1}).

fold_reads(_Config) ->
    {ok, Plan} = barrel_bql:compile(<<"SELECT v.n AS n FROM rows(7) AS v WHERE v.n > 2">>),
    {ok, Seen, #{has_more := false}} = barrel_bql_exec:fold(
        ?DB, Plan, #{chunk_size => 2},
        fun(#{<<"n">> := N}, Acc) -> {ok, [N | Acc]} end, []),
    ?assertEqual([3, 4, 5, 6, 7], lists:reverse(Seen)),
    {ok, Stopped, #{has_more := true}} = barrel_bql_exec:fold(
        ?DB, Plan, #{chunk_size => 2},
        fun(#{<<"n">> := N}, Acc) when N >= 4 -> {stop, [N | Acc]};
           (#{<<"n">> := N}, Acc) -> {ok, [N | Acc]} end, []),
    ?assertEqual([3, 4], lists:reverse(Stopped)).

sort_cap(_Config) ->
    {error, {too_many_rows, 10}} = barrel_docdb:query(
        ?DB, <<"SELECT * FROM rows(50) AS v ORDER BY v.n DESC">>, #{max_rows => 10}).

subscribe_refused(_Config) ->
    {error, {invalid_query, _, {unsupported_with_subscribe, table_function}}} =
        barrel_bql:compile(<<"SELECT * FROM rows(3) AS v SUBSCRIBE">>).

unregister_hides(_Config) ->
    ok = barrel_bql:unregister_source(<<"rows">>),
    {error, {invalid_query, _, {unknown_table_function, <<"rows">>}}} =
        barrel_bql:compile(<<"SELECT * FROM rows(3) AS v">>).

%%====================================================================
%% Helpers
%%====================================================================

drain_runs(N) ->
    receive {source_run, _} -> drain_runs(N + 1)
    after 0 -> N
    end.
