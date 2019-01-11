%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. Nov 2018 10:30
%%%-------------------------------------------------------------------
-module(barrel_store_SUITE).
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
  open_same_barrel/1,
  reopen_same_barrel/1,
  barrel_info/1
]).

-include_lib("eunit/include/eunit.hrl").


all() ->
  [
    create_and_delete_barrel,
    create_barrel_once,
    open_same_barrel,
    reopen_same_barrel,
    barrel_info
  ].

init_per_suite(Config) ->
  _ = application:load(barrel),
  application:set_env(barrel, docs_store_path, "/tmp/default_rocksdb_test"),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  {ok, _} = application:ensure_all_started(barrel),
  {ok, _} = application:ensure_all_started(barrel_rocksdb),
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel_db:delete_barrel(<<"testbarrel">>),
  ok.

end_per_suite(Config) ->
  ok = barrel:stop_store(testing),
  ok = application:stop(barrel),
  ok = rocksdb:destroy("/tmp/default_rocksdb_test", []),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  Config.

create_and_delete_barrel(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  true = is_map(Barrel),
  #{ name := <<"testbarrel">>, ref := _Ref, store_mod := barrel_rocksdb} = Barrel,
  ok = barrel_db:delete_barrel(<<"testbarrel">>),
  {error, barrel_not_found} = barrel_db:open_barrel(<<"testbarrel">>).

create_barrel_once(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>),
  {error, barrel_already_exists} = barrel_db:create_barrel(<<"testbarrel">>),
  ok = barrel_db:delete_barrel(<<"testbarrel">>).

open_same_barrel(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  ok = barrel_db:delete_barrel(<<"testbarrel">>).

reopen_same_barrel(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  ok = barrel_db:close_barrel(Barrel),
  {error, barrel_already_exists} = barrel_db:create_barrel(<<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  ok = barrel_db:delete_barrel(<<"testbarrel">>).

barrel_info(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>),
  {ok, Info} = barrel_db:barrel_infos(<<"testbarrel">>),
  #{ updated_seq := 0, purge_seq := 0, docs_count := 0, docs_del_count := 0} = Info,
  ok = barrel_db:delete_barrel(<<"testbarrel">>),
  {error, barrel_not_found} = barrel_db:barrel_infos(<<"testbarrel">>).