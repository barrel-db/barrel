%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Jun 2017 15:46
%%%-------------------------------------------------------------------
-module(barrel_remote_basic_SUITE).
-author("benoitc").

%% API
%% API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  basic/1,
  named_channel/1
]).

all() ->
  [
    basic,
    named_channel
  ].


init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, RemoteNode} = start_slave(barrel_test1),
  [{remote, RemoteNode} | Config].

end_per_suite(Config) ->
  ok = stop_slave(barrel_test1),
  Config.

init_per_testcase(_, Config) -> Config.

end_per_testcase(_, _Config) -> ok.

basic(Config) ->
  RemoteNode = proplists:get_value(remote, Config),
  {ok, ChPid} = barrel_remote:connect(#{ type => direct, endpoint => RemoteNode }),
  [] = barrel_remote:database_names(ChPid),
  {ok, _} = barrel_remote:create_database(ChPid, #{ <<"database_id">> => <<"testdb">> }),
  [<<"testdb">>] = barrel_remote:database_names(ChPid),
  {error, not_found} = barrel_remote:get(ChPid, <<"testdb">>, <<"a">>, #{}),
  Doc = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, RevId} = barrel_remote:post(ChPid, <<"testdb">>, Doc, #{}),
  {ok, Doc, #{<<"rev">> := RevId}=Meta} = barrel_remote:get(ChPid, <<"testdb">>, <<"a">>, #{}),
  false = maps:is_key(<<"deleted">>, Meta),
  {ok, <<"a">>, _RevId2} = barrel_remote:delete(ChPid, <<"testdb">>, <<"a">>, #{rev =>RevId}),
  {error, not_found} = barrel_remote:get(ChPid, <<"testdb">>, <<"a">>, #{}),
  ok = barrel_remote:delete_database(ChPid, <<"testdb">>),
  [] = barrel_remote:database_names(ChPid),
  ok = barrel_remote:disconnect(ChPid).

named_channel(Config) ->
  RemoteNode = proplists:get_value(remote, Config),
  {ok, ChPid} = barrel_remote:connect(#{ type => direct, endpoint => RemoteNode, channel => mychannel }),
  %% connection should be reused
  {ok, ChPid} = barrel_remote:connect(#{ type => direct, endpoint => RemoteNode, channel => mychannel }),
  true = (ChPid =:= barrel_channel:channel_pid(mychannel)),
  [] = barrel_remote:database_names(mychannel),
  {ok, _} = barrel_remote:create_database(mychannel, #{ <<"database_id">> => <<"testdb">> }),
  [<<"testdb">>] = barrel_remote:database_names(mychannel),
  {error, not_found} = barrel_remote:get(mychannel, <<"testdb">>, <<"a">>, #{}),
  Doc = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, RevId} = barrel_remote:post(mychannel, <<"testdb">>, Doc, #{}),
  {ok, Doc, #{<<"rev">> := RevId}=Meta} = barrel_remote:get(mychannel, <<"testdb">>, <<"a">>, #{}),
  false = maps:is_key(<<"deleted">>, Meta),
  {ok, <<"a">>, _RevId2} = barrel_remote:delete(mychannel, <<"testdb">>, <<"a">>, #{rev =>RevId}),
  {error, not_found} = barrel_remote:get(mychannel, <<"testdb">>, <<"a">>, #{}),
  ok = barrel_remote:delete_database(mychannel, <<"testdb">>),
  [] = barrel_remote:database_names(mychannel),
  ok = barrel_remote:disconnect(mychannel),
  false = erlang:is_process_alive(ChPid),
  undefined = barrel_channel:channel_pid(mychannel).

%% ==============================
%% internal helpers

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