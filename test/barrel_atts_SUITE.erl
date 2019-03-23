%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Apr 2018 11:04
%%%-------------------------------------------------------------------
-module(barrel_atts_SUITE).
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
  basic_test/1
]).

all() ->
  [
    basic_test
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
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  Doc0 = #{ <<"id">> => <<"a">>,
            <<"v">> => 1,
            <<"_attachments">> => #{
                <<"att">> => #{ <<"data">> => <<"test">> }
               }
          },
  {ok, <<"a">>, Rev} = barrel:save_doc(Barrel, Doc0),
  {ok, #{ <<"_rev">> := Rev,
          <<"_attachments">> := #{
              <<"att">> := #{ 
                             <<"data">> := <<"test">>
                            }
             }
        } = Doc1 } = barrel:fetch_doc(Barrel, <<"a">>, #{}),

   {ok, <<"a">>, Rev2} = barrel:save_doc(Barrel, Doc1#{ <<"v">> => 2 }),
   {ok, #{ <<"_rev">> := Rev2} } = _Doc2 = barrel:fetch_doc(Barrel, <<"a">>, #{}),
   ok.
