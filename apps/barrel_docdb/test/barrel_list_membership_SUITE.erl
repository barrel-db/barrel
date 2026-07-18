%%%-------------------------------------------------------------------
%%% @doc find/2 path queries on list-valued fields mean membership and
%%% return well-formed rows (issue #5).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_list_membership_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).
-export([
    t_ars_membership_paths/1,
    t_find_membership/1,
    t_find_non_member_empty/1,
    t_find_contains_op/1,
    t_find_positional/1,
    t_find_scalar_equality/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DB, <<"lmdb">>).

all() ->
    [t_ars_membership_paths, t_find_membership, t_find_non_member_empty,
     t_find_contains_op, t_find_positional, t_find_scalar_equality].

init_per_suite(Config) ->
    application:load(barrel_docdb),
    application:set_env(barrel_docdb, data_dir, ?config(priv_dir, Config)),
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_docdb),
    ok.

init_per_testcase(_TC, Config) ->
    {ok, _} = barrel_docdb:create_db(?DB),
    {ok, _} = barrel_docdb:put_doc(?DB, #{
        <<"id">> => <<"obs_0001">>,
        <<"tags">> => [<<"postgres">>, <<"pooling">>],
        <<"kind">> => <<"note">>}),
    {ok, _} = barrel_docdb:put_doc(?DB, #{
        <<"id">> => <<"obs_0002">>,
        <<"tags">> => [<<"redis">>],
        <<"kind">> => <<"note">>}),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = barrel_docdb:delete_db(?DB),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% Lists are analyzed as membership paths (no positional index).
t_ars_membership_paths(_Config) ->
    Paths = [P || {P, _} <-
                 barrel_ars:analyze(#{<<"tags">> =>
                                          [<<"postgres">>, <<"pooling">>]})],
    ?assert(lists:member([<<"tags">>, <<"postgres">>], Paths)),
    ?assert(lists:member([<<"tags">>, <<"pooling">>], Paths)),
    ?assertNot(lists:any(fun(P) -> lists:any(fun erlang:is_integer/1, P) end,
                         Paths)),
    ok.

%% The exact repro from #5: membership returns one well-formed row (an id
%% with a doc, no space prefix, no spurious deleted flag).
t_find_membership(_Config) ->
    {ok, Rows, _} = barrel_docdb:find(
        ?DB, #{where => [{path, [<<"tags">>], <<"postgres">>}]}),
    ?assertMatch([_], Rows),
    [Row] = Rows,
    ?assertEqual(<<"obs_0001">>, maps:get(<<"id">>, Row)),
    ?assert(maps:is_key(<<"doc">>, Row)),
    ?assertNot(maps:is_key(<<"deleted">>, Row)),
    ok.

t_find_non_member_empty(_Config) ->
    {ok, Rows, _} = barrel_docdb:find(
        ?DB, #{where => [{path, [<<"tags">>], <<"nope">>}]}),
    ?assertEqual([], Rows),
    ok.

%% The explicit {contains, ...} condition works too.
t_find_contains_op(_Config) ->
    {ok, Rows, _} = barrel_docdb:find(
        ?DB, #{where => [{contains, [<<"tags">>], <<"pooling">>}]}),
    ?assertEqual([<<"obs_0001">>], ids(Rows)),
    ok.

%% Positional access still works (via scan).
t_find_positional(_Config) ->
    {ok, Rows, _} = barrel_docdb:find(
        ?DB, #{where => [{path, [<<"tags">>, 0], <<"postgres">>}]}),
    ?assertEqual([<<"obs_0001">>], ids(Rows)),
    ok.

%% Scalar equality is unchanged.
t_find_scalar_equality(_Config) ->
    {ok, Rows, _} = barrel_docdb:find(
        ?DB, #{where => [{path, [<<"kind">>], <<"note">>}]}),
    ?assertEqual([<<"obs_0001">>, <<"obs_0002">>], ids(Rows)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

ids(Rows) ->
    lists:sort([maps:get(<<"id">>, R) || R <- Rows]).
