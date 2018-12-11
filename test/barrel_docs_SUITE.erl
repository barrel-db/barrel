%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Apr 2018 11:04
%%%-------------------------------------------------------------------
-module(barrel_docs_SUITE).
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
  save_doc/1,
  update_non_existing_doc/1,
  replicate_none_existing_doc/1,
  delete_doc/1,
  save_docs/1,
  all_or_nothing/1,
  fold_docs/1,
  fold_changes/1
]).

all() ->
  [
    save_doc,
    update_non_existing_doc,
    replicate_none_existing_doc,
    delete_doc,
    save_docs,
    all_or_nothing,
    fold_docs,
    fold_changes
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, _} = application:ensure_all_started(barrel_rocksdb),
  _ = os:cmd("rm -rf /tmp/rocksdb_test"),
  ok = barrel:start_store(default, barrel_rocksdb, #{ path => "/tmp/rocksdb_test" }),
  Config.


init_per_testcase(_, Config) ->
  ok = barrel:create_barrel(<<"test">>, #{}),
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel:delete_barrel(<<"test">>),
  ok.

end_per_suite(Config) ->
  ok = barrel:stop_store(default),
  ok = rocksdb:destroy("/tmp/rocksdb_test", []),
  ok = application:stop(barrel),
  Config.


save_doc(_Config) ->
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, Rev} = barrel:save_doc(Barrel, Doc0),
  {ok, #{ <<"_rev">> := Rev } = Doc1} = barrel:fetch_doc(Barrel, <<"a">>, #{}),
  {ok, <<"a">>, Rev2} = barrel:save_doc(Barrel, Doc1#{ <<"v">> => 2 }),
  {ok, #{ <<"_rev">> := Rev2} } = _Doc2 = barrel:fetch_doc(Barrel, <<"a">>, #{}),
  {error, {conflict, revision_conflict}} =  barrel:save_doc(Barrel, Doc1#{ <<"v">> => 2 }),
  {error, {conflict, doc_exists}} = barrel:save_doc(Barrel, Doc0),
  ok.

update_non_existing_doc(_Config) ->
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1, <<"_rev">> => <<"1-76d70d853c9fcf1f83a6b4b6cf3776633d28f480cb0dd7ee8b68d5bfc434360a">>},
  {error, {conflict, revision_conflict}}  = barrel:save_doc(Barrel, Doc0).


replicate_none_existing_doc(_Config) ->
  Doc = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  RepDoc = #{ <<"id">> => <<"a">>,
              <<"history">> => [<<"1-76d70d853c9fcf1f83a6b4b6cf3776633d28f480cb0dd7ee8b68d5bfc434360a">>],
              <<"doc">> => Doc},
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  ok = barrel:save_replicated_docs(Barrel, [RepDoc]),
  {ok, #{ <<"_rev">> := Rev} } = barrel:fetch_doc(Barrel, <<"a">>, #{}),
  Rev = <<"1-76d70d853c9fcf1f83a6b4b6cf3776633d28f480cb0dd7ee8b68d5bfc434360a">>.

delete_doc(_Config) ->
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  {ok, <<"a">>, Rev} = barrel:save_doc(Barrel, Doc0),
  {ok, #{ <<"_rev">> := Rev }} = barrel:fetch_doc(Barrel, <<"a">>, #{}),
  {ok, <<"a">>, Rev2} = barrel:delete_doc(Barrel, <<"a">>, Rev),
  {error, not_found} = barrel:fetch_doc(Barrel, <<"a">>, #{}),
  {ok, Doc2} = barrel:fetch_doc(Barrel, <<"a">>, #{ rev => Rev2}),
  undefined = maps:get(<<"v">>, Doc2, undefined),
  ok.


save_docs(_Config) ->
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  Docs = [
    #{ <<"id">> => <<"a">>, <<"v">> => 1},
    #{ <<"id">> => <<"b">>, <<"v">> => 1}
  ],
  {ok, [
    {ok, <<"a">>, _Rev1},
    {ok, <<"b">>, _Rev2}
  ]} = barrel:save_docs(Barrel, Docs),
  ok.

all_or_nothing(_Config) ->
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, Rev1} = barrel:save_doc(Barrel, Doc0),
  {ok, [{ok, <<"a">>, Rev2}]} = barrel:save_docs(Barrel, [Doc0#{ <<"_rev">> => Rev1}], #{ all_or_nothing => true}),
  true = (Rev2 =/= Rev1),
  Doc1 = #{ <<"id">> => <<"b">>, <<"v">> => 1},
  {ok, [{ok, <<"b">>, _RevB}]} = barrel:save_docs(Barrel, [Doc1], #{ all_or_nothing => true}),
  ok.

fold_docs(_Config) ->
  Docs = [
    #{ <<"id">> => <<"a">>, <<"v">> => 1},
    #{ <<"id">> => <<"b">>, <<"v">> => 2},
    #{ <<"id">> => <<"c">>, <<"v">> => 3},
    #{ <<"id">> => <<"d">>, <<"v">> => 4},
    #{ <<"id">> => <<"e">>, <<"v">> => 5}
  ],
  {ok,  Barrel}= barrel_db:open_barrel(<<"test">>),
  {ok, _Saved} = barrel:save_docs(Barrel, Docs),
  5 = length(_Saved),
  Fun = fun(#{ <<"id">> := Id }, Acc) -> {ok, [ Id | Acc ]} end,
  Result1 = barrel:fold_docs(Barrel, Fun, [], #{}),
  [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>] = lists:reverse(Result1),
  {ok, #{ <<"_rev">> := RevC}} = barrel:fetch_doc(Barrel, <<"c">>, #{}),
  {ok, _, _} = barrel:delete_doc(Barrel, <<"c">>, RevC),
  {error, not_found} = barrel:fetch_doc(Barrel, <<"c">>, #{}),
  Result2 = barrel:fold_docs(Barrel, Fun, [], #{}),
  [<<"a">>, <<"b">>, <<"d">>, <<"e">>] = lists:reverse(Result2),
  Result3 = barrel:fold_docs(Barrel, Fun, [], #{include_deleted => true}),
  [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>] = lists:reverse(Result3),
  ok.

fold_changes(_Config) ->
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
  Fun = fun(#{ <<"id">> := Id }, Acc) -> {ok, [ Id | Acc ]} end,
  {ok, Changes1, LastSeq1} = barrel:fold_changes(Barrel, 0, Fun, [], #{}),
  5 = length(Changes1),
  5 = LastSeq1,
  [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>] = lists:reverse(Changes1),
  {ok, #{ <<"_rev">> := RevC}} = barrel:fetch_doc(Barrel, <<"c">>, #{}),
  {ok, _, _} = barrel:delete_doc(Barrel, <<"c">>, RevC),
  {error, not_found} = barrel:fetch_doc(Barrel, <<"c">>, #{}),
  {ok, Changes2, LastSeq2} = barrel:fold_changes(Barrel, LastSeq1, Fun, [], #{}),
  [<<"c">>] = lists:reverse(Changes2),
  6 = LastSeq2,
  {ok, [], 6} = barrel:fold_changes(Barrel, 6, Fun, [], #{include_deleted => true}),
  ok.
  