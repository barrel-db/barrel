%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Apr 2018 22:49
%%%-------------------------------------------------------------------
-module(barrel_remote_SUITE).
-author("benoitc").

-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  save_doc/1,
  update_non_existing_doc/1,
  delete_doc/1
]).

all() ->
  [
    save_doc,
    update_non_existing_doc,
    delete_doc
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, RemoteNode} = start_slave(test_barrel2),
  [{remote, RemoteNode} | Config].
end_per_suite(Config) ->
  ok = stop_slave(test_barrel2),
  Config.

init_per_testcase(_, Config) ->
  ok = create_remote_barrel(Config),
  Config.
end_per_testcase(_, Config) ->
  ok = delete_remote_barrel(Config),
  ok.


save_doc(Config) ->
  Db = remote_db(Config),
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, Rev} = barrel:save_doc(Db, Doc0),
  {ok, #{ <<"_rev">> := Rev } = Doc1} = barrel:fetch_doc(Db, <<"a">>, #{}),
  {ok, <<"a">>, Rev2} = barrel:save_doc(Db, Doc1#{ <<"v">> => 2 }),
  {ok, #{ <<"_rev">> := Rev2 }} = barrel:fetch_doc(Db, <<"a">>, #{}),
  {error, {{conflict, revision_conflict}, <<"a">>}} =  barrel:save_doc(Db, Doc1#{ <<"v">> => 2 }),
  {error, {{conflict, doc_exists}, <<"a">>}} = barrel:save_doc(Db, Doc0),
  ok.

update_non_existing_doc(Config) ->
  Db = remote_db(Config),
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1, <<"_rev">> => <<"1-AAAAAAAAAAA">>},
  {error, {not_found, <<"a">>}} = barrel:save_doc(Db, Doc0).

delete_doc(Config) ->
  Db = remote_db(Config),
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, Rev} = barrel:save_doc(Db, Doc0),
  {ok, #{<<"_rev">> := Rev } } = barrel:fetch_doc(Db, <<"a">>, #{}),
  {ok, _, Rev2} = barrel:delete_doc(Db, <<"a">>, Rev),
  {error, not_found} = barrel:fetch_doc(Db, <<"a">>, #{}),
  {ok, _Doc2} = barrel:fetch_doc(Db, <<"a">>, #{ rev => Rev2}),
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
  {ok, _} = rpc:call(HostNode, barrel_store_sup, start_store, [default, barrel_memory_storage, #{}]),
  ct:print("\e[32m ---> Node ~p [OK] \e[0m", [HostNode]),
  {ok, HostNode}.

stop_slave(Node) ->
  {ok, _} = ct_slave:stop(Node),
  ok.


remote_db(Config) ->
  {<<"rtestdb">>, remote(Config)}.

create_remote_barrel(Config) ->
  barrel:create_barrel(remote_db(Config), #{}).


delete_remote_barrel(Config) ->
  barrel:delete_barrel(remote_db(Config)).


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
