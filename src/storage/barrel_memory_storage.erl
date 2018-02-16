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
  purge_doc/3
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
  case ets:info(Tab, name) of
    undefined  ->
      _  = ets:new(Tab, [named_table, ordered_set, public,
                         {read_concurrency, true},
                         {write_concurrency, true}]),
      InitState  = #{ updated_seq => 0, docs_count => 0 },
      _ = ets:insert(Tab, [{'$update_seq', 0}, {'$docs_count', 0}]),
      
      {ok, InitState};
    _ ->
      {error, already_exists}
  end.


close_barrel(StoreName, Id) ->
  Tab = tabname(StoreName, Id),
  _ = (catch ets:delete(Tab)),
  ok.

destroy_barrel(StoreName, Id) ->
  close_barrel(StoreName, Id).


clean_barrel(StoreName, Id) ->
  Tab = tabname(StoreName, Id),
  _ = (catch ets:delete(Tab)),
  _  = ets:new(Tab, [named_table, ordered_set, public,
                     {read_concurrency, true},
                     {write_concurrency, true}]),
  ok.

has_barrel(StoreName, Id) ->
  (ets:info(tabname(StoreName, Id), name) =/= undefined).


%% documents

get_revision(StoreName, Id, DocId, Rev) ->
  Tab = tabname(StoreName, Id),
  try
    Doc = ets:lookup_element(Tab, {r, DocId, Rev}, 2),
    {ok, Doc}
  catch
    error:badarg ->
      _ = lager:error("not found ~p~n", [ets:lookup(Tab, {r, DocId, Rev})]),
      {error, not_found}
  end.

add_revision(StoreName, Id, DocId, RevId, Body) ->
  Tab = tabname(StoreName, Id),
  ets:insert(Tab, {{r, DocId, RevId}, Body}),
  ok.

delete_revision(StoreName, Id, DocId, RevId) ->
  Tab = tabname(StoreName, Id),
  ets:delete(Tab, {r, DocId, RevId}),
  ok.

delete_revisions(StoreName, Id, DocId, RevIds) ->
  Tab = tabname(StoreName, Id),
  _ = [ets:delete(Tab, {DocId, RevId}) || RevId <- RevIds],
  ok.

fetch_docinfo(StoreName, Id, DocId) ->
  Tab = tabname(StoreName, Id),
  case ets:lookup(Tab, {d, DocId}) of
    [] -> {error, not_found};
    [{{d, DocId}, Doc}] -> {ok, Doc}
  end.

%% TODO: that part should be atomic, maybe we should add a transaction log
write_docinfo(StoreName, Id, DocId, NewSeq, OldSeq, DocInfo) ->
  Tab = tabname(StoreName, Id),
  case write_action(NewSeq, OldSeq) of
    new ->
      ets:insert(Tab, [
        {{d, DocId}, DocInfo},
        {{c, NewSeq}, DocId, DocInfo}
      ]);
    replace ->
      ets:insert(Tab, [
        {{d, DocId}, DocInfo},
        {{c, NewSeq}, DocId, DocInfo}
      ]),
      ets:delete(Tab, {c, OldSeq});
    edit ->
      ets:insert(Tab, {{d, DocId}, DocInfo})
  end,
  ok.

write_action(_Seq, nil) -> new;
write_action(nil, _Seq) -> edit;
write_action(Seq, Seq) -> edit;
write_action(_, _) -> replace.

purge_doc(StoreName, Id, DocId) ->
  Tab = tabname(StoreName, Id),
  MS = [
    {{{d,'$1'},'_'},[{'=:=','$1',{const,DocId}}],[true]},
    {{{r,'$1', '_'},'_'},[{'=:=','$1',{const,DocId}}],[true]},
    {{{c,'_'}, '$1','_'},[{'=:=','$1',{const,DocId}}],[true]}
  ],
  _ = ets:select_delete(Tab, MS),
  ok.


%% local documents

put_local_doc(StoreName, Id, DocId, Doc) ->
  Tab = tabname(StoreName, Id),
  _ = ets:insert(Tab, {{l, DocId}, Doc}),
  ok.

get_local_doc(StoreName, Id, DocId) ->
  Tab = tabname(StoreName, Id),
  try
    Doc = ets:lookup_element(Tab, {l, DocId}, 2),
    {ok, Doc}
  catch
    error:badarg -> {error, not_found}
  end.

delete_local_doc(StoreName, Id, DocId) ->
  _ = ets:delete(tabname(StoreName, Id), {l, DocId}),
  ok.




