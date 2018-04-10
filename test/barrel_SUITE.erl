%% Copyright (c) 2018. Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(barrel_SUITE).
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
  get_db/1,
  write_change/1,
  fetch_doc/1,
  fetch_revision/1,
  write_changes/1,
  write_conflict/1,
  purge_doc/1,
  local_doc/1
]).

all() ->
  [
    get_db,
    write_change,
    fetch_doc,
    fetch_revision,
    write_changes,
    write_conflict,
    purge_doc,
    local_doc
  ].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(barrel),
  {ok, _} = barrel_store_sup:start_store(default, barrel_memory_storage, #{}),
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, _Config) ->
  ok.

end_per_suite(Config) ->
  ok = application:stop(barrel),
  Config.


get_db(_Config) ->
  BarrelId = <<"someid">>,
  {error, not_found} = barrel:barrel_infos(BarrelId),
  ok = barrel:create_barrel(BarrelId, #{}),
  #{ updated_seq := 0, docs_count := 0 } = barrel:barrel_infos(BarrelId),
  ok = barrel:delete_barrel(BarrelId).
  

write_change(_Config) ->
  BarrelId = <<"testdb">>,
  Batch = [
    {create, #{ <<"id">> => <<"a">>, <<"k">> => <<"v">>}}
  ],
  ok = barrel:create_barrel(BarrelId, #{}),
  [Record] = barrel_db:write_changes(BarrelId, Batch),
  #{ <<"id">> := <<"a">>, <<"_rev">> := Rev } = Record,
  true = is_binary(Rev),
  #{ updated_seq := 1, docs_count := 1 } = barrel:barrel_infos(BarrelId),
   [#{<<"id">> := <<"a">>,
      <<"error">> := <<"conflict">>,
      <<"conflict">> := <<"doc_exists">>}] = barrel_db:write_changes(BarrelId, Batch),
  #{ updated_seq := 1, docs_count := 1 } = barrel:barrel_infos(BarrelId),
  Batch2 = [
    {replace, #{ <<"id">> => <<"a">>, <<"k">> => <<"v1">>, <<"_rev">> => Rev}}
  ],
  [#{ <<"id">> := <<"a">>,
      <<"_rev">> := Rev2,
      <<"k">> := <<"v1">> }] = barrel_db:write_changes(BarrelId, Batch2),
  true = (Rev =/= Rev2),
  #{ updated_seq := 2, docs_count := 1 } = barrel:barrel_infos(BarrelId),
  Batch3 = [{delete, <<"a">>, Rev2}],
  [#{ <<"id">> := <<"a">>,
      <<"_rev">> := Rev3,
      <<"_deleted">> := true }] = barrel_db:write_changes(BarrelId, Batch3),
  true = (Rev2 =/= Rev3),
  #{ updated_seq := 3, docs_count := 0 } = barrel:barrel_infos(BarrelId),
  [#{<<"id">> := <<"a">>, <<"_rev">> := Rev4 }] = barrel_db:write_changes(BarrelId, Batch),
  true = (Rev =/= Rev4),
  #{ updated_seq := 4, docs_count := 1 } = barrel:barrel_infos(BarrelId),
  ok = barrel:delete_barrel(BarrelId),
  ok.

fetch_doc(_Config) ->
  BarrelId = <<"testdb">>,
  Doc = #{ <<"id">> => <<"a">>, <<"k">> => <<"v">>},
  Batch = [
    {create, Doc}
  ],
  ok = barrel:create_barrel(BarrelId, #{}),
  [#{ <<"id">> := <<"a">>, <<"_rev">> := Rev }] = barrel_db:write_changes(BarrelId, Batch),
  {ok, Doc1} = barrel_db:fetch_doc(BarrelId, <<"a">>, #{}),
  Rev = maps:get(<<"_rev">>, Doc1),
  Batch2 = [
    {delete, <<"a">>, Rev}
  ],
  [#{ <<"id">> := <<"a">>, <<"_deleted">> := true }] = barrel_db:write_changes(BarrelId, Batch2),
  {error, not_found} = barrel:fetch_doc(BarrelId, <<"a">>, #{}),
  ok = barrel:delete_barrel(BarrelId),
  ok.


fetch_revision(_Config) ->
  BarrelId = <<"testdb">>,
  Batch = [
    {create, #{ <<"id">> => <<"a">>, <<"k">> => <<"v">>}}
  ],
  ok = barrel:create_barrel(BarrelId, #{}),
  [#{ <<"id">> := <<"a">>, <<"_rev">> := Rev }] = barrel_db:write_changes(BarrelId, Batch),
  Batch2 = [
    {replace, #{ <<"id">> => <<"a">>, <<"k">> => <<"v1">>, <<"_rev">> => Rev}}
  ],
  [#{ <<"id">> := <<"a">>, <<"_rev">> := Rev2 }] = barrel_db:write_changes(BarrelId, Batch2),
  {ok, #{<<"id">> := <<"a">>,
         <<"k">> := <<"v1">>,
         <<"_rev">> := Rev2 }} = barrel_db:fetch_doc(BarrelId, <<"a">>, #{}),
  {ok, #{<<"id">> := <<"a">>,
         <<"k">> := <<"v">>,
         <<"_rev">> := Rev }} = barrel_db:fetch_doc(BarrelId, <<"a">>, #{ rev => Rev }),
  {ok, #{<<"id">> := <<"a">>,
         <<"k">> := <<"v1">>,
         <<"_rev">> := Rev2 }} = barrel_db:fetch_doc(BarrelId, <<"a">>, #{ rev => Rev2 }),
  Batch3 = [
    {delete, <<"a">>, Rev2}
  ],
  [#{ <<"id">> := <<"a">>, <<"_rev">> := Rev3, <<"_deleted">> := true }] = barrel_db:write_changes(BarrelId, Batch3),
  {ok, #{<<"id">> := <<"a">>,
         <<"_rev">> := Rev3,
         <<"_deleted">> := true }} = barrel_db:fetch_doc(BarrelId, <<"a">>, #{ rev => Rev3 }),
  ok = barrel:delete_barrel(BarrelId),
  ok.

write_changes(_Config) ->
  BarrelId = <<"testdb">>,
  Batch = [
    {create, #{ <<"id">> => <<"a">>, <<"ka">> => <<"va">>}},
    {create, #{ <<"id">> => <<"b">>, <<"kb">> => <<"vb">>}}
  ],
  ok = barrel:create_barrel(BarrelId, #{}),
  [
    #{ <<"id">> := <<"a">>, <<"_rev">> := RevA1, <<"ka">> := <<"va">> },
    #{<<"id">> := <<"b">>, <<"_rev">> := RevB1, <<"kb">> := <<"vb">> }
  ] = barrel_db:write_changes(BarrelId, Batch),
  Batch2 = [
    {replace, #{ <<"id">> => <<"a">>, <<"_rev">> => RevA1, <<"ka">> => <<"va1">> }},
    {delete, <<"b">>, RevB1}
  ],
  [
    #{ <<"id">> := <<"a">>, <<"_rev">> := _RevA2, <<"ka">> := <<"va1">> } = Doc,
    #{<<"id">> := <<"b">>,  <<"_rev">> := _RevB2, <<"_deleted">> := true }
  ] = barrel_db:write_changes(BarrelId, Batch2),
  false = maps:is_key(<<"_deleted">>, Doc),
  false = maps:is_key(<<"kb">>, Doc),
  {ok, Doc} = barrel_db:fetch_doc(BarrelId, <<"a">>, #{}),
  {error, not_found} = barrel_db:fetch_doc(BarrelId, <<"b">>, #{}),
  ok = barrel:delete_barrel(BarrelId),
  ok.

write_conflict(_Config) ->
  BarrelId = <<"testdb">>,
  Batch = [
    {create, #{ <<"id">> => <<"a">>, <<"k">> => <<"v">>}}
  ],
  ok = barrel:create_barrel(BarrelId, #{}),
  [#{ <<"id">> := <<"a">>, <<"_rev">> := Rev }] = barrel_db:write_changes(BarrelId, Batch),
  Batch2 = [
    {create, #{ <<"id">> => <<"a">>, <<"k">> => <<"v">>, <<"_rev">> => Rev}}
  ],
  [#{ <<"id">> := <<"a">>, <<"_rev">> := Rev2 }] = barrel_db:write_changes(BarrelId, Batch2),
  true = (Rev =/= Rev2),
  [#{ <<"id">> := <<"a">>,
      <<"error">> := <<"conflict">>,
      <<"error_code">> := 409,
      <<"conflict">> := <<"revision_conflict">> }] = barrel_db:write_changes(BarrelId, Batch2),
  Batch3 = [
    {create, #{ <<"id">> => <<"a">>, <<"k">> => <<"v">>, <<"_rev">> => Rev2}}
  ],
  [#{ <<"id">> := <<"a">>, <<"_rev">> := _Rev3 }] = barrel_db:write_changes(BarrelId, Batch3),
  [#{ <<"id">> := <<"a">>,
      <<"error">> := <<"conflict">>,
      <<"error_code">> := 409,
      <<"conflict">> := <<"doc_exists">> }] = barrel_db:write_changes(BarrelId, Batch),
  ok = barrel:delete_barrel(BarrelId),
  ok.

purge_doc(_Config) ->
  BarrelId = <<"testdb">>,
  Batch = [
    {create, #{ <<"id">> => <<"a">>, <<"k">> => <<"v">>}}
  ],
  ok = barrel:create_barrel(BarrelId, #{}),
  [#{ <<"id">> := <<"a">> }] = barrel_db:write_changes(BarrelId, Batch),
  {ok, #{ <<"id">> := <<"a">> }} = barrel_db:fetch_doc(BarrelId, <<"a">>, #{}),
  [#{<<"id">> := <<"a">>, <<"purged">> := true }] = barrel_db:purge_docs(BarrelId, [<<"a">>]),
  {error, not_found} = barrel:fetch_doc(BarrelId, <<"a">>, #{}),
  ok = barrel:delete_barrel(BarrelId),
  ok.


local_doc(_Config) ->
  BarrelId = <<"testdb">>,
  ok = barrel:create_barrel(BarrelId, #{}),
  Doc = #{ <<"id">> => <<"a">>, <<"v">> => 1},
  ok = barrel_db:put_local_doc(BarrelId, <<"a">>, Doc),
  {ok, Doc} = barrel_db:get_local_doc(BarrelId, <<"a">>),
  ok = barrel_db:delete_local_doc(BarrelId, <<"a">>),
  {error, not_found} = barrel_db:get_local_doc(BarrelId, <<"a">>),
  ok = barrel:delete_barrel(BarrelId),
  ok.