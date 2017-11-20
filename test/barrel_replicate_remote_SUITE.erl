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

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, RemoteNode} = start_slave(barrel_test1),
  [{remote, RemoteNode} | Config].

end_per_suite(Config) ->
  ok = stop_slave(barrel_test1),
  Config.

init_per_testcase(_, Config) ->
  {ok, _} = barrel:create_database(#{ <<"database_id">> => <<"sourcedb">> }),
  ok = create_remote_db(Config),
  Config.

end_per_testcase(_, Config) ->
  ok = barrel:delete_database(<<"sourcedb">>),
  ok = delete_remote_db(Config),
  ok.

one_doc(Config) ->
  TargetDb = target(Config),
  RepConfig = #{
    source => <<"sourcedb">>,
    target => TargetDb,
    options => #{ metrics_freq => 100 }
  },
  {ok, #{ id := RepId }} = barrel_replicate:start_replication(RepConfig),
  Doc = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, _RevId} = barrel:post(<<"sourcedb">>, Doc, #{}),
  timer:sleep(200),
  {ok, Doc2, _} = barrel:get(<<"sourcedb">>, <<"a">>, #{}),
  {ok, Doc2, _} = barrel_replicate_api_wrapper:get(TargetDb, <<"a">>, #{}),
  ok = barrel_replicate:stop_replication(RepId),
  {ok, <<"a">>, _RevId_1} = delete_doc(<<"sourcedb">>, <<"a">>),
  
  {error, not_found} = delete_doc(TargetDb, <<"b">>),
  ok.


%% ==============================
%% internal helpers

remote(Config) ->
  Remote = proplists:get_value(remote, Config),
  Remote.


target(Config) ->
  Remote = proplists:get_value(remote, Config),
  {Remote, <<"targetdb">>}.

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
    barrel, create_database, [#{ <<"database_id">> => <<"targetdb">>}]
  ),
  ok.

delete_remote_db(Config) ->
  rpc:call(remote(Config), barrel, delete_database, [<<"targetdb">>] ).

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

delete_doc({Node, DbName}, DocId) ->
  [Res] = barrel_rpc:update_docs(Node, DbName, [{delete, DocId}], #{}),
  Res;
delete_doc(Db, DocId) ->
  barrel:delete(Db, DocId, #{}).
