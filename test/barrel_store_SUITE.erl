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
  no_default_store/1,
  default_store/1,
  create_and_delete_barrel/1,
  create_barrel_once/1,
  barrel_id_is_unique/1,
  open_same_barrel/1,
  reopen_same_barrel/1,
  barrel_info/1
]).

-include_lib("eunit/include/eunit.hrl").


all() ->
  [
    no_default_store,
    default_store,
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

no_default_store(_Config) ->
  ?assertError(bad_provider_config, barrel_db:create_barrel(<<"testbarrel">>, #{})).

default_store(_Config) ->
  _ = os:cmd("rm -rf /tmp/default_rocksdb_test"),
  ok = barrel:start_store(default, barrel_rocksdb, #{ path => "/tmp/default_rocksdb_test" }),
  ok = barrel_db:create_barrel(<<"testbarrel">>, #{}),
  ok = barrel_db:delete_barrel(<<"testbarrel">>),
  ok = barrel:stop_store(default),
  ok = rocksdb:destroy("/tmp/default_rocksdb_test", []).

create_and_delete_barrel(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>, #{ store_provider => testing }),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  true = is_map(Barrel),
  #{ name := <<"testbarrel">>, id := _Id, store_mod := barrel_rocksdb, store_name := testing } = Barrel,
  ok = barrel_db:delete_barrel(<<"testbarrel">>),
  {error, barrel_not_found} = barrel_db:open_barrel(<<"testbarrel">>).

create_barrel_once(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>, #{ store_provider => testing }),
  {error, barrel_already_exists} = barrel_db:create_barrel(<<"testbarrel">>, #{ store_provider => testing }),
  ok = barrel_db:delete_barrel(<<"testbarrel">>).

barrel_id_is_unique(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>, #{ store_provider => testing }),
  {ok, #{ id := Id }}  = barrel_db:open_barrel(<<"testbarrel">>),
  %% new barrel get a new ID
  ok = barrel_db:create_barrel(<<"testbarrel1">>, #{ store_provider => testing }),
  {ok, #{ id := Id1 }}  = barrel_db:open_barrel(<<"testbarrel1">>),
  true = (Id =/= Id1),
  %% recreating a barrel gives it a new ID
  ok = barrel_db:delete_barrel(<<"testbarrel">>),
  ok =barrel_db:create_barrel(<<"testbarrel">>, #{ store_provider => testing }),
  {ok, #{ id := Id2 }}  = barrel_db:open_barrel(<<"testbarrel">>),
  true = (Id1 =/= Id2),
  true = (Id =/= Id2),
  ok = barrel_db:delete_barrel(<<"testbarrel">>),
  ok = barrel_db:delete_barrel(<<"testbarrel1">>).

open_same_barrel(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>, #{ store_provider => testing }),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  ok = barrel_db:delete_barrel(<<"testbarrel">>).

reopen_same_barrel(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>, #{ store_provider => testing }),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  ok = barrel_db:close_barrel(Barrel),
  {error, barrel_already_exists} = barrel_db:create_barrel(<<"testbarrel">>, #{ store_provider => testing }),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  ok = barrel_db:delete_barrel(<<"testbarrel">>).

barrel_info(_Config) ->
  ok = barrel_db:create_barrel(<<"testbarrel">>, #{ store_provider => testing }),
  {ok, Barrel}  = barrel_db:open_barrel(<<"testbarrel">>),
  {ok, Info} = barrel_db:barrel_infos(Barrel),
  #{ updated_seq := 0, purge_seq := 0, docs_count := 0, docs_del_count := 0} = Info,
  ok = barrel_db:delete_barrel(<<"testbarrel">>),
  {error, barrel_not_found} = barrel_db:barrel_infos(Barrel).