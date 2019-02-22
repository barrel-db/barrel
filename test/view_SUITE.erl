-module(view_SUITE).
-author("benoitc").

%% API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([basic_test/1]).

all() ->
  [
   basic_test
  ].

init_per_suite(Config) ->
  _ = application:load(barrel),
  application:set_env(barrel, docs_store_path, "/tmp/default_rocksdb_test"),
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
  ok = barrel:stop_store(default),
  ok = application:stop(barrel),
  ok = rocksdb:destroy("/tmp/default_rocksdb_test", []),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  Config.


basic_test(_Config) ->
  {ok, Barrel} = barrel:open_barrel(<<"test">>),
  {ok, _} = barrel:start_view(<<"test">>, <<"ars">>, barrel_ars_view, #{}),

  Docs = [
    #{ <<"id">> => <<"a">>, <<"v">> => 1, <<"o">> => #{ <<"o1">> => 1, << "o2">> => 1}}
  ],
  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  timer:sleep(1000),

  [<<"a">>] = barrel:fold_view(<<"test">>, <<"ars">>,
                               fun(#{ id := Id }, Acc) ->
                                   {ok, [Id | Acc]}
                               end,
                               [],
                               #{ begin_key => [<<"id">>, <<"a">>],
                                  end_key => [<<"id">>, << 16#ff, 16#ff >>] }),

  ok.
