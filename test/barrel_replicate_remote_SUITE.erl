%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Jun 2017 15:46
%%%-------------------------------------------------------------------
-module(barrel_replicate_remote_SUITE).
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
  one_doc/1
]).

all() ->
  [
    one_doc
  ].

channel(Config) -> proplists:get_value(channel, Config).

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, RemoteNode} = start_slave(barrel_test1),
  {ok, ChPid} = barrel_remote:start_channel(#{ type => direct, node => RemoteNode, channel_name => rep_test_channel }),
  [{remote, RemoteNode}, {channel, ChPid} | Config].

end_per_suite(Config) ->
  _ = barrel_remote:close_channel(channel(Config)),
  ok = stop_slave(barrel_test1),
  Config.

init_per_testcase(_, Config) ->
  {ok, _} = barrel_local:create_database(#{ <<"database_id">> => <<"source">> }),
  {ok, _} = barrel_remote:create_database(rep_test_channel, #{ <<"database_id">> => <<"testdb">> }),
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel_remote:delete_database(rep_test_channel, <<"testdb">>),
  ok = barrel_store:delete_db(<<"source">>),
  ok.

target(Config) ->
  Remote = proplists:get_value(remote, Config),
  #{ proto => rpc,
     type => direct,
     node => Remote,
     channel_name => rep_test_channel,
     db => <<"testdb">> }.

one_doc(Config) ->
  Ch = channel(Config),
  Options = [{metrics_freq, 100}],
  RepConfig = #{
    <<"source">> => <<"source">>,
    <<"target">> => target(Config)
  },
  {ok, #{<<"replication_id">> := RepId}} = barrel_replicate:start_replication(RepConfig, Options),
  Doc = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, _RevId} = barrel:post(<<"source">>, Doc, []),
  timer:sleep(200),
  {ok, Doc2, _} = barrel:get(<<"source">>, <<"a">>, []),
  {ok, Doc2, _} = barrel_remote:get(Ch, <<"testdb">>, <<"a">>, []),
  ok = barrel_replicate:stop_replication(RepId),
  ok = delete_doc(<<"source">>, <<"a">>),
  ok = delete_doc(Ch, <<"testdb">>, <<"b">>),
  ok.

delete_doc(Db, DocId) ->
  _ = barrel:delete(Db, DocId, []),
  ok.

delete_doc( Ch, Db, DocId) ->
  _ = barrel_remote:delete(Ch, Db, DocId, #{}),
  ok.

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
