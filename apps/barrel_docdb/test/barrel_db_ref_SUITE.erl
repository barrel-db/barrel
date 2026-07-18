%%%-------------------------------------------------------------------
%%% @doc Name-only functions accept the pid returned by create_db/2
%%% (resolved to the name), and fail fast on a dead/foreign pid instead
%%% of silently no-op'ing (issue #4).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_db_ref_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_subscribe_by_pid_fires/1,
    t_subscribe_by_name_fires/1,
    t_subscribe_dead_pid_errors/1,
    t_branch_by_pid/1,
    t_list_branches_by_pid/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_subscribe_by_pid_fires, t_subscribe_by_name_fires,
     t_subscribe_dead_pid_errors, t_branch_by_pid, t_list_branches_by_pid].

init_per_suite(Config) ->
    application:load(barrel_docdb),
    application:set_env(barrel_docdb, data_dir,
                        filename:join(?config(priv_dir, Config), "data")),
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_docdb),
    ok.

%%====================================================================
%% Cases
%%====================================================================

%% #4 repro: subscribing with the pid from create_db/2 now delivers
%% changes (previously a silent no-op).
t_subscribe_by_pid_fires(_Config) ->
    Name = uname(<<"ref_pid_">>),
    {ok, Pid} = barrel_docdb:create_db(Name),
    {ok, _Ref} = barrel_docdb:subscribe(Pid, <<"type/#">>),
    put_user(Name),
    expect_change(Name),
    ok = barrel_docdb:delete_db(Name).

%% Subscribing by name still works (regression).
t_subscribe_by_name_fires(_Config) ->
    Name = uname(<<"ref_name_">>),
    {ok, _} = barrel_docdb:create_db(Name),
    {ok, _Ref} = barrel_docdb:subscribe(Name, <<"type/#">>),
    put_user(Name),
    expect_change(Name),
    ok = barrel_docdb:delete_db(Name).

%% A dead/removed pid fails fast rather than no-op'ing.
t_subscribe_dead_pid_errors(_Config) ->
    Name = uname(<<"ref_dead_">>),
    {ok, Pid} = barrel_docdb:create_db(Name),
    ok = barrel_docdb:close_db(Name),
    ?assertEqual({error, invalid_db_ref},
                 barrel_docdb:subscribe(Pid, <<"type/#">>)),
    ok = barrel_docdb:delete_db(Name).

%% branch_db accepts the pid.
t_branch_by_pid(_Config) ->
    Name = uname(<<"ref_branch_">>),
    {ok, Pid} = barrel_docdb:create_db(Name),
    Branch = uname(<<"ref_branchchild_">>),
    ?assertMatch({ok, _}, barrel_docdb:branch_db(Pid, Branch, #{})),
    ok = barrel_docdb:delete_db(Branch),
    ok = barrel_docdb:delete_db(Name).

%% list_branches accepts the pid.
t_list_branches_by_pid(_Config) ->
    Name = uname(<<"ref_lb_">>),
    {ok, Pid} = barrel_docdb:create_db(Name),
    Branch = uname(<<"ref_lbchild_">>),
    {ok, _} = barrel_docdb:branch_db(Pid, Branch, #{}),
    ?assert(lists:member(Branch, barrel_docdb:list_branches(Pid))),
    ok = barrel_docdb:delete_db(Branch),
    ok = barrel_docdb:delete_db(Name).

%%====================================================================
%% Helpers
%%====================================================================

uname(Prefix) ->
    iolist_to_binary([Prefix,
                      integer_to_binary(erlang:unique_integer([positive]))]).

put_user(Name) ->
    {ok, _} = barrel_docdb:put_doc(
        Name, #{<<"id">> => <<"u1">>, <<"type">> => <<"user">>}).

expect_change(Name) ->
    receive
        {barrel_change, Name, _Change} -> ok
    after 2000 ->
        ct:fail("no change notification delivered")
    end.
