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
         rev_test/1,
         limit_test/1,
         r1_test/1]).

all() ->
  [
   basic_test,
   fwd_test,
   rev_test,
   limit_test,
   r1_test
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
  {ok, ViewPid} = barrel:start_view(<<"test">>, <<"ars">>, barrel_ars_view, 1),

  Docs = [
    #{ <<"id">> => <<"a">>, <<"v">> => 1, <<"o">> => #{ <<"o1">> => 1, << "o2">> => 1}}
  ],
  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  ok = barrel_view:await_refresh(<<"test">>, <<"ars">>),
  [<<"a">>] = barrel:fold_view(<<"test">>, <<"ars">>,
                               fun(#{ id := Id }, Acc) ->
                                   {ok, [Id | Acc]}
                               end,
                               [],
                               #{ begin_key => [<<"id">>, <<"a">>],
                                  end_key => [<<"id">>, << 16#ff, 16#ff >>] }),

  supervisor:terminate_child(barrel_view_sup, ViewPid),

  ok.


fwd_test(_Config) ->
   {ok, Barrel} = barrel:open_barrel(<<"test">>),
   {ok, ViewPid} = barrel:start_view(<<"test">>, <<"ars">>, barrel_ars_view, 1),

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
                          supervisor:terminate_child(barrel_view_sup, ViewPid)
                        end,

  ok.


rev_test(_Config) ->
   {ok, Barrel} = barrel:open_barrel(<<"test">>),
   {ok, ViewPid} = barrel:start_view(<<"test">>, <<"ars">>, barrel_ars_view, 1),

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
                          supervisor:terminate_child(barrel_view_sup, ViewPid)
                        end,

   ok.


limit_test(_Config) ->
  {ok, Barrel} = barrel:open_barrel(<<"test">>),
   {ok, ViewPid} = barrel:start_view(<<"test">>, <<"ars">>, barrel_ars_view, 1),

  Docs = [
    #{ <<"id">> => <<"a">> },
    #{ <<"id">> => <<"b">> },
    #{ <<"id">> => <<"c">> },
    #{ <<"id">> => <<"d">> },
    #{ <<"id">> => <<"e">> },
    #{ <<"id">> => <<"f">> },
    #{ <<"id">> => <<"g">> },
    #{ <<"id">> => <<"h">> }

  ],

  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  8 = length(_Saved),

  barrel_view:await_refresh(<<"test">>, <<"ars">>),

  [<<"f">>, <<"g">>, <<"h">>] = try barrel:fold_view(<<"test">>, <<"ars">>,
                                             fun(#{ id := Id }, Acc) ->
                                                 {ok, [Id | Acc]}
                                             end,
                                             [],
                                             #{begin_key => [<<"id">>],
                                               limit => 3,
                                               reverse => true })
                        after
                          supervisor:terminate_child(barrel_view_sup, ViewPid)
                        end,

  [<<"c">>, <<"b">>, <<"a">>] = try barrel:fold_view(<<"test">>, <<"ars">>,
                                             fun(#{ id := Id }, Acc) ->
                                                 {ok, [Id | Acc]}
                                             end,
                                             [],
                                             #{begin_key => [<<"id">>], limit => 3})
                        after
                          supervisor:terminate_child(barrel_view_sup, ViewPid)
                        end,


   ok.


r1_test(_Config) ->
  {ok, Barrel} = barrel:open_barrel(<<"test">>),
  {ok, ViewPid} = barrel:start_view(<<"test">>, <<"ars">>, barrel_ars_view, 1),

  Ids = [<<"9gUOXd0V5JePkx3HCU">>,<<"9gUOXd0V5JePkx3HCV">>,<<"9gUOXd0V5JePkx3HCW">>,
 <<"9gUOXd0V5JePkx3HCX">>,<<"9gUOXd0V5JePkx3HCY">>,<<"9gUOXd0V5JePkx3HCZ">>,
 <<"9gUOXd0V5JePkx3HCa">>,<<"9gUOXd0V5JePkx3HCb">>,<<"9gUOXd0V5JePkx3HCc">>,
 <<"9gUOXd0V5JePkx3HCd">>,<<"9gUOXd0V5JePkx3HCe">>,<<"9gUOXd0V5JePkx3HCf">>,
 <<"9gUOXd0V5JePkx3HCg">>,<<"9gUOXd0V5JePkx3HCh">>,<<"9gUOXd0V5JePkx3HCi">>],


  Docs = [#{ <<"id">> => Id,
             <<"message">> => #{ <<"messageId">> => Id }
           } || Id <- Ids],

  {ok, Saved} = barrel:save_docs(Barrel, Docs),
  15 = length(Saved),


  barrel_view:await_refresh(<<"test">>, <<"ars">>),

  [<<"9gUOXd0V5JePkx3HCe">>,
   <<"9gUOXd0V5JePkx3HCf">>,
   <<"9gUOXd0V5JePkx3HCg">>,
   <<"9gUOXd0V5JePkx3HCh">>,
   <<"9gUOXd0V5JePkx3HCi">>] = try barrel:fold_view(<<"test">>, <<"ars">>,
                                                    fun(#{  id := Id }, Acc) ->
                                                        {ok, [Id | Acc]}
                                                    end,
                                                    [],
                                                    #{begin_key => [<<"message">>, <<"messageId">>],
                                                      limit => 5,
                                                      reverse => true })
                               after
                                 supervisor:terminate_child(barrel_view_sup, ViewPid)
                               end,

  [<<"9gUOXd0V5JePkx3HCU">>,
   <<"9gUOXd0V5JePkx3HCV">>,
   <<"9gUOXd0V5JePkx3HCW">>,
   <<"9gUOXd0V5JePkx3HCX">>,
   <<"9gUOXd0V5JePkx3HCY">>] =try barrel:fold_view(<<"test">>, <<"ars">>,
                                                   fun(#{ id := Id }, Acc) ->
                                                       {ok, Acc ++[Id]}
                                                   end,
                                                   [],
                                                   #{begin_key => [<<"message">>, <<"messageId">>],
                                                     limit => 5})
                              after
                                supervisor:terminate_child(barrel_view_sup, ViewPid)
                              end,
  ok.






