%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 29. Jun 2017 15:46
%%%-------------------------------------------------------------------
-module(barrel_remote_db_SUITE).
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
  update_doc/1,
  create_doc/1,
  system_docs/1,
  multi_get/1,
  put_rev/1,
  revision_conflict/1,
  write_batch/1
]).

all() ->
  [
    update_doc,
    create_doc,
    system_docs,
    multi_get,
    put_rev,
    revision_conflict,
    write_batch
  ].


init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, RemoteNode} = start_slave(barrel_test1),
  {ok, ChPid} = barrel_remote:start_channel(#{ type => direct, node => RemoteNode }),
  [{remote, RemoteNode}, {channel, ChPid} | Config].

end_per_suite(Config) ->
  _ = barrel_remote:close_channel(channel(Config)),
  ok = stop_slave(barrel_test1),
  Config.

init_per_testcase(_, Config) ->
  Ch = channel(Config),
  {ok, _} = barrel_remote:create_database(Ch, #{ <<"database_id">> => <<"testdb">> }),
  Config.

end_per_testcase(_, Config) ->
  _ =  barrel_remote:delete_database(channel(Config), <<"testdb">>),
  ok.


update_doc(Config) ->
  Ch = channel(Config),
  Doc = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, RevId} = barrel_remote:insert(Ch, <<"testdb">>, Doc, #{}),
  {ok, Doc, _Meta2} = barrel_remote:get(Ch, <<"testdb">>, <<"a">>, []),
  Doc2 = Doc#{ v => 2},
  {ok, <<"a">>, RevId2} = barrel_remote:put(Ch, <<"testdb">>, Doc2, #{}),
  true = (RevId =/= RevId2),
  {ok, Doc2, _Meta4} = barrel_remote:get(Ch, <<"testdb">>, <<"a">>, []),
  {ok, <<"a">>, _RevId2} = barrel_remote:delete(Ch, <<"testdb">>, <<"a">>, #{rev => RevId2}),
  {error, not_found} = barrel_remote:get(Ch, <<"testdb">>, <<"a">>, []),
  {ok, <<"a">>, _RevId3} = barrel_remote:insert(Ch, <<"testdb">>, Doc, #{}).

create_doc(Config) ->
  Ch = channel(Config),
  Doc = #{<<"v">> => 1},
  {ok, DocId, _RevId} = barrel_remote:insert(Ch, <<"testdb">>, Doc, #{}),
  {ok, CreatedDoc, _} = barrel_remote:get(Ch, <<"testdb">>, DocId, []),
  {error, {conflict, doc_exists}} = barrel_remote:insert(Ch, <<"testdb">>, CreatedDoc, #{}),
  {ok, _, _} = barrel_remote:insert(Ch, <<"testdb">>, CreatedDoc, #{is_upsert => true}),
  Doc2 = #{<<"id">> => <<"b">>, <<"v">> => 1},
  {ok, <<"b">>, _RevId2} = barrel_remote:insert(Ch, <<"testdb">>, Doc2, #{}).

system_docs(Config) ->
  Ch = channel(Config),
  Doc = #{<<"v">> => 1},
  ok = barrel_remote:put_system_doc(Ch, <<"testdb">>, <<"a">>, Doc),
  {ok, Doc} = barrel_remote:get_system_doc(Ch, <<"testdb">>, <<"a">>),
  ok = barrel_remote:delete_system_doc(Ch, <<"testdb">>, <<"a">>),
  {error, not_found} = barrel_remote:get_system_doc(Ch, <<"testdb">>, <<"a">>),
  ok.

multi_get(Config) ->
  Ch = channel(Config),
  %% create some docs
  Kvs = [{<<"a">>, 1},
    {<<"b">>, 2},
    {<<"c">>, 3}],
  Docs = [#{ <<"id">> => K, <<"v">> => V} || {K,V} <- Kvs],
  [ {ok,_,_} = barrel_remote:insert(Ch, <<"testdb">>, D, #{}) || D <- Docs ],
  
  %% the "query" to get the id/rev
  Mget = [ Id || {Id, _} <- Kvs],
  
  %% a fun to parse the results
  %% the parameter is the same format as the regular get function output
  Fun=
    fun(Doc, Meta, Acc) ->
      #{<<"id">> := DocId} = Doc,
      #{<<"rev">> := RevId} = Meta,
      [#{<<"id">> => DocId, <<"rev">> => RevId, <<"doc">>  => Doc }|Acc]
    end,
  
  %% let's process it
  Results = barrel_remote:multi_get(Ch, <<"testdb">>, Fun, [], Mget, []),
  
  %% check results
  [#{<<"doc">> := #{<<"id">> := <<"a">>, <<"v">> := 1},
    <<"id">> := <<"a">>,
    <<"rev">> := _},
    #{<<"doc">> := #{<<"id">> := <<"b">>, <<"v">> := 2}},
    #{<<"doc">> := #{<<"id">> := <<"c">>, <<"v">> := 3}}] = lists:reverse(Results).

put_rev(Config) ->
  Ch = channel(Config),
  Doc = #{<<"v">> => 1},
  {ok, DocId, RevId} = barrel_remote:insert(Ch, <<"testdb">>, Doc, #{}),
  {ok, Doc2, _} = barrel_remote:get(Ch, <<"testdb">>, DocId, []),
  Doc3 = Doc2#{ v => 2},
  {ok, DocId, RevId2} = barrel_remote:put(Ch, <<"testdb">>, Doc3, #{rev => RevId}),
  Doc4 = Doc2#{ v => 3 },
  {Pos, _} = barrel_doc:parse_revision(RevId),
  NewRev = barrel_doc:revid(Pos +1, RevId, barrel_doc:make_doc(Doc4, RevId, false)),
  History = [NewRev, RevId],
  Deleted = false,
  {ok, DocId, _RevId3} = barrel_remote:put_rev(Ch, <<"testdb">>, Doc4, History, Deleted, #{}),
  {ok, _Doc5, Meta} = barrel_remote:get(Ch, <<"testdb">>, DocId, [{history, true}]),
  Revisions = [RevId2, RevId],
  io:format("revisions: ~p~nparsed:~p~n", [Revisions, barrel_doc:parse_revisions(Meta)]),
  Revisions = barrel_doc:parse_revisions(Meta).

revision_conflict(Config) ->
  Ch = channel(Config),
  Doc = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, _, RevId} = barrel_remote:insert(Ch, <<"testdb">>, Doc, #{}),
  {ok, Doc1, _} = barrel_remote:get(Ch, <<"testdb">>, <<"a">>, []),
  Doc2 = Doc1#{ <<"v">> => 2 },
  {ok, <<"a">>, _RevId2} = barrel_remote:put(Ch, <<"testdb">>, Doc2, #{rev => RevId}),
  {error, {conflict, revision_conflict}} = barrel_remote:put(Ch, <<"testdb">>, Doc2, #{rev => RevId}),
  ok.

write_batch(Config) ->
  Ch = channel(Config),
  %% create resources
  D1 = #{<<"id">> => <<"a">>, <<"v">> => 1},
  D2 = #{<<"id">> => <<"b">>, <<"v">> => 1},
  D3 = #{<<"id">> => <<"c">>, <<"v">> => 1},
  D4 = #{<<"id">> => <<"d">>, <<"v">> => 1},
  {ok, _, Rev1_1} = barrel_remote:insert(Ch, <<"testdb">>, D1, #{}),
  {ok, _, Rev3_1} = barrel_remote:insert(Ch, <<"testdb">>, D3, #{}),
  OPs =  [
    { put, D1#{ <<"v">> => 2 }, Rev1_1},
    { post, D2, false},
    { delete, <<"c">>, Rev3_1},
    { put, D4, <<>>}
  ],
  
  {ok, #{ <<"v">> := 1}, _} = barrel_remote:get(Ch, <<"testdb">>, <<"a">>, []),
  {error, not_found} = barrel_remote:get(Ch, <<"testdb">>, <<"b">>, []),
  {ok, #{ <<"v">> := 1}, _} = barrel_remote:get(Ch, <<"testdb">>, <<"c">>, []),
  
  Results = barrel_remote:write_batch(Ch, <<"testdb">>, OPs, #{}),
  true = is_list(Results),
  
  [ {ok, <<"a">>, _},
    {ok, <<"b">>, _},
    {ok, <<"c">>, _},
    {error, not_found} ] = Results,
  
  {ok, #{ <<"v">> := 2}, _} = barrel_remote:get(Ch, <<"testdb">>, <<"a">>, []),
  {ok, #{ <<"v">> := 1}, _} = barrel_remote:get(Ch, <<"testdb">>, <<"b">>, []),
  {error, not_found} = barrel_remote:get(Ch, <<"testdb">>, <<"c">>, []).

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

channel(Config) -> proplists:get_value(channel, Config).