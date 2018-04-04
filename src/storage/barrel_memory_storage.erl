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

-module(barrel_memory_storage).
-author("benoitc").

%% API
-export([
  init/2,
  close/1,
  init_barrel/2,
  close_barrel/2,
  destroy_barrel/2,
  has_barrel/2,
  clean_barrel/2
]).

%% documents
-export([
  get_revision/4,
  add_revision/5,
  delete_revision/4,
  delete_revisions/4,
  fetch_docinfo/3,
  write_docinfo/6,
  purge_doc/5
]).


%% local documents
-export([
  put_local_doc/4,
  get_local_doc/3,
  delete_local_doc/3
]).

-include("barrel.hrl").
-include_lib("stdlib/include/ms_transform.hrl").


init(_Name, _Config) ->
  ok.

close(_Name) ->
  ok.

tabname(StoreName, DbId) ->
  list_to_atom(
    barrel_lib:to_list(StoreName) ++ [$_|barrel_lib:to_list(DbId)]
  ).

init_barrel(StoreName, Id) ->
  Tab = tabname(StoreName, Id),
  case memstore:open(Tab, []) of
    ok ->
      InitState  = #{ updated_seq => 0, docs_count => 0 },
      _ = memstore:write_batch(Tab, [{put, '$update_seq', 0},
                                     {put, '$docs_count', 0}]),
      {ok, InitState};
    Error ->
      Error
  end.


close_barrel(StoreName, Id) ->
  Tab = tabname(StoreName, Id),
  memstore:close(Tab).

destroy_barrel(StoreName, Id) ->
  close_barrel(StoreName, Id).


clean_barrel(StoreName, Id) ->
  ok = close_barrel(StoreName, Id),
  init_barrel(StoreName, Id).

has_barrel(StoreName, Id) ->
  (ets:info(tabname(StoreName, Id), name) =/= undefined).


%% documents

get_revision(StoreName, Id, DocId, Rev) ->
  Tab = tabname(StoreName, Id),
  case memstore:get(Tab, {r, DocId, Rev}) of
    {ok, Doc} -> {ok, Doc};
    not_found ->
      _ = lager:error("not found ~p~n", [ets:lookup(Tab, {r, DocId, Rev})]),
      {error, not_found}
  end.

add_revision(StoreName, Id, DocId, RevId, Body) ->
  Tab = tabname(StoreName, Id),
  memstore:put(Tab, {r, DocId, RevId}, Body).

delete_revision(StoreName, Id, DocId, RevId) ->
  Tab = tabname(StoreName, Id),
  memstore:delete(Tab, {r, DocId, RevId}).

delete_revisions(StoreName, Id, DocId, RevIds) ->
  Tab = tabname(StoreName, Id),
  _ = [memstore:delete(Tab, {r, DocId, RevId}) || RevId <- RevIds],
  ok.

fetch_docinfo(StoreName, Id, DocId) ->
  Tab = tabname(StoreName, Id),
  case memstore:get(Tab, {d, DocId}) of
    {ok, Doc} -> {ok, Doc};
    not_found -> {error, not_found}
  end.

%% TODO: that part should be atomic, maybe we should add a transaction log
write_docinfo(StoreName, Id, DocId, NewSeq, OldSeq, DocInfo) ->
  Tab = tabname(StoreName, Id),
  case write_action(NewSeq, OldSeq) of
    new ->
      memstore:write_batch(
        Tab,
        [{put, {d, DocId}, DocInfo},
         {put, {c, NewSeq}, DocInfo}]
      );
    replace ->
      memstore:write_batch(
        Tab,
        [{put, {d, DocId}, DocInfo},
         {put, {c, NewSeq}, DocInfo},
         {delete, {c, OldSeq}}]
      );
    edit ->
      memstore:put(Tab, {d, DocId}, DocInfo)
  end.

write_action(_Seq, nil) -> new;
write_action(nil, _Seq) -> edit;
write_action(Seq, Seq) -> edit;
write_action(_, _) -> replace.

purge_doc(StoreName, Id, DocId, LastSeq, Revisions) ->
  Tab = tabname(StoreName, Id),
  Batch = lists:foldl(
    fun(RevId, Batch1) ->
      [{delete, {r, DocId, RevId}} | Batch1]
        end,
    [{delete, {c, LastSeq}}, {delete, {d, DocId}}],
    Revisions
  ),
  memstore:write_batch(Tab, Batch).

%% local documents

put_local_doc(StoreName, Id, DocId, Doc) ->
  Tab = tabname(StoreName, Id),
  memstore:put(Tab, {l, DocId}, Doc).

get_local_doc(StoreName, Id, DocId) ->
  Tab = tabname(StoreName, Id),
  case memstore:get(Tab, {l, DocId}) of
    {ok, Doc} -> {ok, Doc};
    not_found -> {error, not_found}
  end.

delete_local_doc(StoreName, Id, DocId) ->
  Tab = tabname(StoreName, Id),
  memstore:delete(Tab, {l, DocId}).
