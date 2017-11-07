%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. Nov 2017 12:50
%%%-------------------------------------------------------------------
-module(barrel_rpc_SUITE).
-author("benoitc").

-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  basic/1,
  changes_feed/1,
  write_and_get_system_docs/1,
  revsdiff/1
]).

all() ->
  [
    basic,
    changes_feed,
    write_and_get_system_docs,
    revsdiff
  ].


init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, RemoteNode} = start_slave(test_barrel2),
  [{remote, RemoteNode} | Config].
end_per_suite(Config) ->
  ok = stop_slave(test_barrel2),
  Config.

init_per_testcase(_, Config) ->
  ok = create_remote_db(Config),
  Config.
end_per_testcase(_, Config) ->
  ok = delete_remote_db(Config),
  ok.


basic(Config) ->
  Remote = remote(Config),
  D1 = #{<<"id">> => <<"a">>, <<"v">> => 1},
  [{ok, <<"a">>, Rev}] = barrel_rpc:update_docs(Remote, <<"testdb">>, [{post, D1}], #{}),
  
  Result = barrel_rpc:fetch_docs(
    Remote, <<"testdb">>,
    fun(Doc, Meta, Acc) ->
      [{Doc, Meta} | Acc]
    end,
    [],
    [<<"a">>],
    #{}
  ),
  [{#{<<"id">> := <<"a">>, <<"v">> := 1}, #{ <<"rev">> := Rev }}] = Result,
  ok.
  
changes_feed(Config) ->
  Remote = remote(Config),
  D1 = #{<<"id">> => <<"a">>, <<"v">> => 1},
  D2 = #{<<"id">> => <<"b">>, <<"v">> => 1},
  [{ok, <<"a">>, _Rev}] = barrel_rpc:update_docs(Remote, <<"testdb">>, [{post, D1}], #{}),
  Stream = barrel_rpc:subscribe_changes(Remote, <<"testdb">>, #{}),
  #{ <<"id">> := <<"a">>, <<"seq">> := 1 } = barrel_rpc:await_changes(Stream, 5000),
  [{ok, <<"b">>, _RevB}] = barrel_rpc:update_docs(Remote, <<"testdb">>, [{post, D2}], #{}),
  #{ <<"id">> := <<"b">>, <<"seq">> := 2 } = barrel_rpc:await_changes(Stream, 5000),
  ok = barrel_rpc:unsubscribe_changes(Remote, Stream),
  ok.


write_and_get_system_docs(Config) ->
  Remote = remote(Config),
  Doc = #{<<"v">> => 1},
  ok = barrel_rpc:put_system_doc(Remote, <<"testdb">>, <<"a">>, Doc),
  {ok, Doc} = barrel_rpc:get_system_doc(Remote, <<"testdb">>, <<"a">>),
  ok = barrel_rpc:delete_system_doc(Remote, <<"testdb">>, <<"a">>),
  {error, not_found} = barrel_rpc:get_system_doc(Remote, <<"testdb">>, <<"a">>),
  ok.

revsdiff(Config) ->
  Remote = remote(Config),
  Doc = #{ <<"id">> => <<"revsdiff">>, <<"v">> => 1},
  [{ok, <<"revsdiff">>, RevId}] = barrel_rpc:update_docs(Remote, <<"testdb">>, [{post, Doc}], #{}),
  Doc2 = Doc#{<<"v">> => 2},
  [{ok, <<"revsdiff">>, _RevId3}] = barrel_rpc:update_docs(Remote, <<"testdb">>, [{put, Doc2, RevId}], #{}),
  {ok, [<<"1-missing">>], []} = barrel_rpc:revsdiff(Remote, <<"testdb">>, <<"revsdiff">>, [<<"1-missing">>]),
  ok.


%% ==============================
%% internal helpers

remote(Config) ->
  Remote = proplists:get_value(remote, Config),
  Remote.

start_slave(Node) ->
  {ok, HostNode} = ct_slave:start(Node,
                                  [{kill_if_fail, true}, {monitor_master, true},
                                   {init_timeout, 3000}, {startup_timeout, 3000}]),
  pong = net_adm:ping(HostNode),
  CodePath = filter_rebar_path(code:get_path()),
  true = rpc:call(HostNode, code, set_path, [CodePath]),
  {ok,_} = rpc:call(HostNode, application, ensure_all_started, [barrel]),
  ct:print("\e[32m ---> Node ~p [OK] \e[0m", [HostNode]),
  {ok, HostNode}.

stop_slave(Node) ->
  {ok, _} = ct_slave:stop(Node),
  ok.

create_remote_db(Config) ->
  {ok, _} = rpc:call(
    remote(Config),
    barrel, create_database, [#{ <<"database_id">> => <<"testdb">>}]
  ),
  ok.

delete_remote_db(Config) ->
  rpc:call(remote(Config), barrel, delete_database, [<<"testdb">>] ).


%% a hack to filter rebar path
%% see https://github.com/erlang/rebar3/issues/1182
filter_rebar_path(CodePath) ->
  lists:filter(
    fun(P) ->
      case string:str(P, "rebar3") of
        0 -> true;
        _ -> false
      end
    end,
    CodePath
  ).