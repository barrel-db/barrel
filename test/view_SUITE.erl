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

-export([basic_test/1,
         fwd_test/1,
         rev_test/1]).

all() ->
  [
   basic_test,
   fwd_test,

   rev_test
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


basic_test(_Config) ->
  {ok, Barrel} = barrel:open_barrel(<<"test">>),
  {ok, ViewPid} = barrel:start_view(<<"test">>, <<"ars">>, barrel_ars_view, #{}),

  Docs = [
    #{ <<"id">> => <<"a">>, <<"v">> => 1, <<"o">> => #{ <<"o1">> => 1, << "o2">> => 1}}
  ],
  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  barrel_view:await_refresh(<<"test">>, <<"ars">>),
  [<<"a">>] = barrel:fold_view(<<"test">>, <<"ars">>,
                               fun(#{ id := Id }, Acc) ->
                                   {ok, [Id | Acc]}
                               end,
                               [],
                               #{ begin_key => [<<"id">>, <<"a">>],
                                  end_key => [<<"id">>, << 16#ff, 16#ff >>] }),

  supervisor:terminate_child(barrel_view_sup_sup, ViewPid),

  ok.


fwd_test(_Config) ->
   {ok, Barrel} = barrel:open_barrel(<<"test">>),
   {ok, ViewPid} = barrel:start_view(<<"test">>, <<"ars">>, barrel_ars_view, #{}),

  Docs = [
    #{ <<"id">> => <<"a">> },
    #{ <<"id">> => <<"b">> },
    #{ <<"id">> => <<"c">> },
    #{ <<"id">> => <<"d">> },
    #{ <<"id">> => <<"e">> }
  ],

  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  5 = length(_Saved),
   Fun = fun(#{ <<"id">> := Id }, Acc) -> {ok, [ Id | Acc ]} end,
  [<<"e">>,
   <<"d">>,
   <<"c">>,
   <<"b">>,
   <<"a">>] = barrel:fold_docs(Barrel, Fun, [], #{}),

   barrel_view:await_refresh(<<"test">>, <<"ars">>),

   [<<"e">>, <<"d">>] = try barrel:fold_view(<<"test">>, <<"ars">>,
                                             fun(#{ id := Id }, Acc) ->
                                                 {ok, [Id | Acc]}
                                             end,
                                             [],
                                             #{ begin_key => [<<"id">>, <<"c">>],
                                               begin_or_equal => false })
                        after
                          supervisor:terminate_child(barrel_view_sup_sup, ViewPid)
                        end,

  ok.


rev_test(_Config) ->
   {ok, Barrel} = barrel:open_barrel(<<"test">>),
   {ok, ViewPid} = barrel:start_view(<<"test">>, <<"ars">>, barrel_ars_view, #{}),

  Docs = [
    #{ <<"id">> => <<"a">> },
    #{ <<"id">> => <<"b">> },
    #{ <<"id">> => <<"c">> },
    #{ <<"id">> => <<"d">> },
    #{ <<"id">> => <<"e">> }
  ],

  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  5 = length(_Saved),

  Fun = fun(#{ <<"id">> := Id }, Acc) -> {ok, [ Id | Acc ]} end,
  [<<"e">>,
   <<"d">>,
   <<"c">>,
   <<"b">>,
   <<"a">>] = barrel:fold_docs(Barrel, Fun, [], #{}),

   barrel_view:await_refresh(<<"test">>, <<"ars">>),

   [<<"a">>, <<"b">>] = try barrel:fold_view(<<"test">>, <<"ars">>,
                                             fun(#{ id := Id }, Acc) ->
                                                 {ok, [Id | Acc]}
                                             end,
                                             [],
                                             #{ begin_key => [<<"id">>], 
                                               end_key => [<<"id">>, <<"c">>],
                                               end_or_equal => false,
                                               reverse => true })
                        after
                          supervisor:terminate_child(barrel_view_sup_sup, ViewPid)
                        end,

   ok.





