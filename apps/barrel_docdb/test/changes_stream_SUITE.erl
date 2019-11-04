-module(changes_stream_SUITE).

-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export(
   [
    basic/1,
    batch_size/1,
    iterate/1,
    proxy/1,
    stop/1
   ]
  ).

all() ->
  [
   basic,
   batch_size,
   iterate,
   proxy,
   stop
  ].


init_per_suite(Config) ->
  _ = application:load(barrel),
  application:set_env(barrel, data_dir, "/tmp/default_rocksdb_test"),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  {ok, _} = application:ensure_all_started(barrel_docdb),
  Config.


init_per_testcase(_, Config) ->
  ok = barrel:create_barrel(<<"test">>),
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel:delete_barrel(<<"test">>),
  ok.

end_per_suite(Config) ->
  %Dir = barrel_config:get(rocksdb_root_dir),
  ok = application:stop(barrel_docdb),
  ok = application:stop(barrel),
  %  ok = rocksdb:destroy(Dir, []),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  Config.


basic(_Config) ->
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  Docs = [
          #{ <<"id">> => <<"a">>, <<"v">> => 1},
          #{ <<"id">> => <<"b">>, <<"v">> => 2},
          #{ <<"id">> => <<"c">>, <<"v">> => 3},
          #{ <<"id">> => <<"d">>, <<"v">> => 4},
          #{ <<"id">> => <<"e">>, <<"v">> => 5}
         ],
  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  5 = length(_Saved),
  Fun = fun(Change, Acc) -> {ok, [ Change | Acc ]} end,
  {ok, Changes, _LastSeq} = barrel:fold_changes(Barrel, first, Fun, [], #{}),
  5 = length(Changes),

  {ok, StreamPid} = barrel_changes_stream:start_link(<<"test">>, self(), #{}),
  {ReqId, Changes_1} = barrel_changes_stream:await(StreamPid),
  ok = barrel_changes_stream:ack(StreamPid, ReqId),

  true = (Changes_1 =:= Changes),
  ok.


batch_size(_Config) ->
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  Docs = [
          #{ <<"id">> => <<"a">>, <<"v">> => 1},
          #{ <<"id">> => <<"b">>, <<"v">> => 2},
          #{ <<"id">> => <<"c">>, <<"v">> => 3},
          #{ <<"id">> => <<"d">>, <<"v">> => 4},
          #{ <<"id">> => <<"e">>, <<"v">> => 5}
         ],
  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  5 = length(_Saved),

  {ok, StreamPid} = barrel_changes_stream:start_link(<<"test">>, self(), #{ batch_size => 2 }),
  {ReqId,
   [#{ <<"id">> := <<"a">> },
    #{ << "id">> := <<"b">> }]} = barrel_changes_stream:await(StreamPid),

  {ReqId_1,
   [#{ <<"id">> := <<"c">> },
    #{ << "id">> := <<"d">> }]} = barrel_changes_stream:await(StreamPid),

  {ReqId_2,
   [#{ <<"id">> := <<"e">> }]} = barrel_changes_stream:await(StreamPid),

  ok = barrel_changes_stream:ack(StreamPid, ReqId),
  ok = barrel_changes_stream:ack(StreamPid, ReqId_1),
  ok = barrel_changes_stream:ack(StreamPid, ReqId_2),
  ok.


iterate(_Config) ->
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  Docs = [
          #{ <<"id">> => <<"a">>, <<"v">> => 1},
          #{ <<"id">> => <<"b">>, <<"v">> => 2},
          #{ <<"id">> => <<"c">>, <<"v">> => 3},
          #{ <<"id">> => <<"d">>, <<"v">> => 4},
          #{ <<"id">> => <<"e">>, <<"v">> => 5}
         ],
  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  5 = length(_Saved),

  {ok, StreamPid} = barrel_changes_stream:start_link(<<"test">>, self(), #{ stream_mode => iterate }),
  {ok, #{ <<"id">> := <<"a">> }} = barrel_changes_stream:next(StreamPid),
  {ok, #{ <<"id">> := <<"b">> }} = barrel_changes_stream:next(StreamPid),
  {ok, #{ <<"id">> := <<"c">> }} = barrel_changes_stream:next(StreamPid),
  {ok, #{ <<"id">> := <<"d">> }} = barrel_changes_stream:next(StreamPid),
  {ok, #{ <<"id">> := <<"e">> }} = barrel_changes_stream:next(StreamPid),
  invalid = barrel_changes_stream:next(StreamPid),

  try barrel_changes_stream:next(StreamPid)
  catch
    exit:timeout -> ok
  end,
  ok.


proxy(_Config) ->
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  Docs = [
          #{ <<"id">> => <<"a">>, <<"v">> => 1},
          #{ <<"id">> => <<"b">>, <<"v">> => 2},
          #{ <<"id">> => <<"c">>, <<"v">> => 3},
          #{ <<"id">> => <<"d">>, <<"v">> => 4},
          #{ <<"id">> => <<"e">>, <<"v">> => 5}
         ],
  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  5 = length(_Saved),

   Fun = fun(Change, Acc) -> {ok, [ Change | Acc ]} end,
  {ok, Changes, _LastSeq} = barrel:fold_changes(Barrel, first, Fun, [], #{}),
  5 = length(Changes),

  {ok, StreamPid} = barrel_changes_streams:subscr(node(), <<"test">>, #{}),
  {ReqId, Changes_1} = barrel_changes_stream:await(StreamPid),
  ok = barrel_changes_stream:ack(StreamPid, ReqId),

  true = (Changes_1 =:= Changes),
  ok.


stop(_Config) ->
  {ok, StreamPid} = barrel_changes_stream:start_link(<<"test">>, self(), #{}),
  MRef = erlang:monitor(process, StreamPid),
  ok = barrel_changes_stream:stop(StreamPid),
  receive
    {'DOWN', MRef, process, _, normal} ->
     ok
  end.


