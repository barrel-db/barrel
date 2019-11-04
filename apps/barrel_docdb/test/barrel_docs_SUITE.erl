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
  delete_barrel/1,
  update_non_existing_doc/1,
  replicate_none_existing_doc/1,
  delete_doc/1,
  save_docs/1,
  fold_docs/1,
  all_or_nothing/1,
  fold_changes/1
]).

all() ->
  [
    save_doc,
    delete_barrel,
    update_non_existing_doc,
    replicate_none_existing_doc,
    delete_doc,
    save_docs,
    all_or_nothing,
    fold_docs,
    fold_changes
  ].

init_per_suite(Config) ->
  _ = application:load(barrel_docdb),
  application:set_env(barrel, data_dir, "/tmp/default_rocksdb_test"),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
  {ok, _} = application:ensure_all_started(barrel_docdb),
  Config.


init_per_testcase(_, Config) ->
  ok = barrel:create_barrel(<<"test">>),
  Config.

end_per_testcase(_, _Config) ->
  ok = barrel:delete_barrel(<<"test">>),
  ok.

end_per_suite(Config) ->
%%  Dir = barrel_config:get(rocksdb_root_dir),
  ok = application:stop(barrel_docdb),
  ok = application:stop(barrel),
%  ok = rocksdb:destroy(Dir, []),
  os:cmd("rm -rf /tmp/default_rocksdb_test"),
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

delete_barrel(_Config) ->
  ok = barrel:create_barrel(<<"test1">>),
  ok = barrel:create_barrel(<<"test2">>),
  {ok, Barrel} = barrel:open_barrel(<<"test">>),
  {ok, Barrel1} = barrel:open_barrel(<<"test1">>),
  {ok, Barrel2} = barrel:open_barrel(<<"test2">>),
  Doc0 = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  {ok, <<"a">>, _Rev} = barrel:save_doc(Barrel, Doc0),
  {ok, <<"a">>, _Rev1} = barrel:save_doc(Barrel1, Doc0),
  {ok, <<"a">>, _Rev2} = barrel:save_doc(Barrel2, Doc0),
  {ok, #{ <<"id">> := <<"a">> }} = barrel:fetch_doc(Barrel, <<"a">>, #{}),
  {ok, #{ <<"id">> := <<"a">> }} = barrel:fetch_doc(Barrel1, <<"a">>, #{}),
  {ok, #{ <<"id">> := <<"a">> }} = barrel:fetch_doc(Barrel2, <<"a">>, #{}),
  ok = barrel:delete_barrel(<<"test1">>),
  {error, barrel_not_found} = barrel:open_barrel(<<"test1">>),
  {error, not_found} = barrel:fetch_doc(Barrel1, <<"a">>, #{}),
  {ok, #{ <<"id">> := <<"a">> }} = barrel:fetch_doc(Barrel, <<"a">>, #{}),
  {ok, #{ <<"id">> := <<"a">> }} = barrel:fetch_doc(Barrel2, <<"a">>, #{}),
  ok = barrel:delete_barrel(<<"test2">>),
  {error, not_found} = barrel:fetch_doc(Barrel2, <<"a">>, #{}),
  {ok, #{ <<"id">> := <<"a">> }} = barrel:fetch_doc(Barrel, <<"a">>, #{}),
  ok = barrel:create_barrel(<<"test1">>),
  {ok, Barrel1_1} = barrel:open_barrel(<<"test1">>),
  {error, not_found} = barrel:fetch_doc(Barrel1_1, <<"a">>, #{}),
  {ok, #{ <<"id">> := <<"a">> }} = barrel:fetch_doc(Barrel, <<"a">>, #{}),
  ok = barrel:delete_barrel(<<"test1">>),
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
  {ok, [{ok, <<"a">>, Rev}]} = barrel:save_docs(Barrel, [RepDoc], #{ merge_policy => merge_with_conflict }),
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
  {ok, [{ok, <<"a">>, Rev2}]} =
    barrel:save_docs(Barrel, [Doc0#{ <<"_rev">> => Rev1}], #{ merge_policy => merge_with_conflict }),
  true = (Rev2 =/= Rev1),
  Doc1 = #{ <<"id">> => <<"b">>, <<"v">> => 1},
  {ok, [{ok, <<"b">>, _RevB}]} =
    barrel:save_docs(Barrel, [Doc1], #{ merge_policy => merge_with_conflict }),
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
  {ok, Changes1, LastSeq1} = barrel:fold_changes(Barrel, first, Fun, [], #{}),
  5 = length(Changes1),
  {_, SeqBin} = barrel_sequence:from_string(LastSeq1),
  {_, 5} = barrel_sequence:decode(SeqBin),
  [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>] = lists:reverse(Changes1),
  {ok, #{ <<"_rev">> := RevC}} = barrel:fetch_doc(Barrel, <<"c">>, #{}),
  {ok, _, _} = barrel:delete_doc(Barrel, <<"c">>, RevC),
  {error, not_found} = barrel:fetch_doc(Barrel, <<"c">>, #{}),
  {ok, Changes2, LastSeq2} = barrel:fold_changes(Barrel, LastSeq1, Fun, [], #{}),
  [<<"c">>] = lists:reverse(Changes2),
  {_, SeqBin2} =  barrel_sequence:from_string(LastSeq2),
  {_, 6} = barrel_sequence:decode(SeqBin2),

  {ok, [], LastSeq2} = barrel:fold_changes(Barrel, LastSeq2, Fun, [], #{include_deleted => true}),
  ok.
