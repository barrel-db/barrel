%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Apr 2018 11:04
%%%-------------------------------------------------------------------
-module(barrel_local_SUITE).
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
  local_doc/1,
  create_and_delete_db/1,
  list_barrels/1
]).

-define(ITERATIONS, 1000).
-define(DB_PREFIX, <<"local_test">>).

all() ->
  [
    local_doc,
    create_and_delete_db,
    list_barrels
  ].

init_per_suite(Config) ->
  _ = application:load(barrel),
  application:set_env(barrel, data_dir, "/tmp/default_rocksdb_test"),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  {ok, _} = application:ensure_all_started(barrel),
  Config.


init_per_testcase(_, Config) ->
  ok = barrel:create_barrel(<<"test">>),
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel:delete_barrel(<<"test">>),
  ok.

end_per_suite(Config) ->
  Dir = barrel_config:get(rocksdb_root_dir),
  ok = application:stop(barrel),
  ok = rocksdb:destroy(Dir, []),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  Config.

local_doc(_Config) ->
  {ok, Barrel} = barrel_db:open_barrel(<<"test">>),
  LocalDoc = #{ <<"id">> => <<"adoc">>, <<"value">> => <<"local">> },
  RepDoc = #{ <<"id">> => <<"adoc">>, <<"value">> => <<"replicated">> },
  ok = barrel_local:put_doc(Barrel, <<"adoc">>, LocalDoc),
  {ok, _, Rev} = barrel:save_doc(Barrel, RepDoc),
  {ok, #{ <<"value">> := <<"local">> }} = barrel_local:get_doc(Barrel, <<"adoc">>),
  {ok, #{ <<"value">> := <<"replicated">>, <<"_rev">> := Rev }} = barrel:fetch_doc(Barrel, <<"adoc">>, #{}),
  {ok, _, _} = barrel:delete_doc(Barrel, <<"adoc">>, Rev),
  {error, not_found} = barrel:fetch_doc(Barrel, <<"adoc">>, #{}),
  {ok, #{ <<"value">> := <<"local">> }} = barrel_local:get_doc(Barrel, <<"adoc">>),
  ok = barrel_local:delete_doc(Barrel, <<"adoc">>),
  {error, not_found} = barrel_local:get_doc(Barrel, <<"adoc">>).

create_and_delete_db(Config) ->
    barrel:create_barrel(?DB_PREFIX),
    _ = barrel_db:open_barrel(?DB_PREFIX),
    {ok, _ } = barrel:barrel_infos(?DB_PREFIX),
    create_and_delete_db_loop(Config, ?ITERATIONS),
    barrel:delete_barrel(?DB_PREFIX).

create_and_delete_db_loop(_, 0) ->
    ok;
create_and_delete_db_loop(Config, Int) when Int > 0 ->
    error_logger:info_msg("Starting iteration ~p of database creation and deletion",
                          [?ITERATIONS - Int + 1]),
    %% Do something here.
    Name = <<(?DB_PREFIX)/binary, $_, (barrel_id:binary_id(62))/binary >>,
    ok = barrel:create_barrel(Name),
    ok = barrel:delete_barrel(Name),
    {ok, _ } = barrel:barrel_infos(?DB_PREFIX),
    create_and_delete_db_loop(Config, Int - 1).


list_barrels(_Config) ->
  [<<"test">>] = barrel:all_names(),
  ok = barrel:create_barrel(<<"test1">>),
  [<<"test">>, <<"test1">>] = barrel:all_names(),
  ok = barrel:delete_barrel(<<"test1">>),
  [<<"test">>] = barrel:all_names(),
  ok.
