%% @doc test default storage initialization

-module(rocksdb_SUITE).


%% API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([restart_test/1]).

all() ->
  [
   restart_test
  ].

init_per_suite(Config) ->
  _ = application:load(barrel),
  application:set_env(barrel, data_dir, "/tmp/default_rocksdb_test"),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  Config.


init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, _Config) ->
  ok.

end_per_suite(Config) ->
  Dir = barrel_config:get(rocksdb_root_dir),
  ok = rocksdb:destroy(Dir, []),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  Config.


restart_test(_Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  ok = barrel:create_barrel(<<"test">>),
  ok = application:stop(barrel),
  ok = application:start(barrel),
  ok = barrel:create_barrel(<<"test">>),
  ok = barrel:create_barrel(<<"test2">>),
  ok = application:stop(barrel),
  ok.
