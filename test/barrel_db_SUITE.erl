%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Nov 2018 10:30
%%%-------------------------------------------------------------------
-module(barrel_db_SUITE).
-author("benoitc").

%% API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  create_and_delete_barrel/1,
  create_barrel_once/1,
  barrel_id_is_unique/1,
  open_same_barrel/1,
  reopen_same_barrel/1,
  barrel_info/1
]).

all() ->
  [
    create_and_delete_barrel,
    create_barrel_once,
    barrel_id_is_unique,
    open_same_barrel,
    reopen_same_barrel,
    barrel_info
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, _} = application:ensure_all_started(barrel_rocksdb),
  _ = os:cmd("rm -rf /tmp/rocksdb_test"),
  ok = barrel:start_store(testing, barrel_rocksdb, #{ path => "/tmp/rocksdb_test" }),
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, _Config) ->
  ok.

end_per_suite(Config) ->
  ok = barrel:stop_store(testing),
  ok = rocksdb:destroy("/tmp/rocksdb_test", []),
  ok = application:stop(barrel),
  Config.

create_and_delete_barrel(_Config) ->
  ok = barrel_db:create_barrel(testing, <<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(testing, <<"testbarrel">>),
  true = is_map(Barrel),
  #{ name := <<"testbarrel">>, id := _Id, provider := {barrel_rocksdb, testing} } = Barrel,
  ok = barrel_db:delete_barrel(testing, <<"testbarrel">>),
  {error, db_not_found} = barrel_db:open_barrel(testing, <<"testbarrel">>).

create_barrel_once(_Config) ->
  ok = barrel_db:create_barrel(testing, <<"testbarrel">>),
  {error, db_already_exists} = barrel_db:create_barrel(testing, <<"testbarrel">>),
  ok = barrel_db:delete_barrel(testing, <<"testbarrel">>).

barrel_id_is_unique(_Config) ->
  ok = barrel_db:create_barrel(testing, <<"testbarrel">>),
  {ok, #{ id := Id }}  = barrel_db:open_barrel(testing, <<"testbarrel">>),
  %% new barrel get a new ID
  ok = barrel_db:create_barrel(testing, <<"testbarrel1">>),
  {ok, #{ id := Id1 }}  = barrel_db:open_barrel(testing, <<"testbarrel1">>),
  true = (Id =/= Id1),
  %% recreating a barrel gives it a new ID
  ok = barrel_db:delete_barrel(testing, <<"testbarrel">>),
  ok = barrel_db:create_barrel(testing, <<"testbarrel">>),
  {ok, #{ id := Id2 }}  = barrel_db:open_barrel(testing, <<"testbarrel">>),
  true = (Id1 =/= Id2),
  true = (Id =/= Id2),
  ok = barrel_db:delete_barrel(testing, <<"testbarrel">>),
  ok = barrel_db:delete_barrel(testing, <<"testbarre1">>).

open_same_barrel(_Config) ->
  ok = barrel_db:create_barrel(testing, <<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(testing, <<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(testing, <<"testbarrel">>),
  ok = barrel_db:delete_barrel(testing, <<"testbarrel">>).

reopen_same_barrel(_Config) ->
  ok = barrel_db:create_barrel(testing, <<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(testing, <<"testbarrel">>),
  ok = barrel_db:close_barrel(Barrel),
  {error, db_already_exists} = barrel_db:create_barrel(testing, <<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(testing, <<"testbarrel">>),
  ok = barrel_db:delete_barrel(testing, <<"testbarrel">>).

barrel_info(_Config) ->
  ok = barrel_db:create_barrel(testing, <<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(testing, <<"testbarrel">>),
  {ok, Info} = barrel_db:barrel_infos(Barrel),
  #{ name := <<"testbarrel">>, updated_seq := 0, purge_seq := 0, docs_count := 0, docs_del_count := 0} = Info,
  ok = barrel_db:delete_barrel(testing, <<"testbarrel">>),
  {error, db_not_found} = barrel_db:barrel_infos(Barrel).