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

-module(barrel).
-author("benoitc").

%% API
-export([
  create_barrel/2,
  delete_barrel/1,
  barrel_infos/1
]).

-export([
  fetch_doc/3,
  save_doc/2,
  delete_doc/3,
  purge_doc/2,
  save_docs/2,
  delete_docs/2,
  save_local_doc/3,
  delete_local_doc/2,
  get_local_doc/2
]).

-export([query/5]).


create_barrel(Name, Options) ->
  barrel_db:create_barrel(Name, Options).

delete_barrel(Name) ->
  barrel_db:delete_barrel(Name).
  

barrel_infos(DbRef) ->
  barrel_db:db_infos(DbRef).

fetch_doc(DbRef, DocId, Options) ->
  barrel_db:fetch_doc(DbRef, DocId, Options).

save_doc(Barrel, Doc) ->
  [Res] = save_doc1(Barrel, Doc),
  Res.

save_doc1(Barrel, Doc = #{ <<"_rev">> := _Rev}) ->
  barrel_db:write_changes(Barrel, [{replace, Doc}]);
save_doc1(Barrel,  Doc)  ->
  barrel_db:write_changes(Barrel, [{create, Doc}]).

delete_doc(Barrel, DocId, Rev) ->
  [Res] = barrel_db:write_changes(Barrel, [{delete, DocId, Rev}]),
  Res.

purge_doc(Barrel, DocId) ->
  [Res] = barrel_db:write_changes(Barrel, [{purge, DocId}]),
  Res.

save_docs(Barrel, Docs) ->
  Batch = lists:map(
    fun
      (Doc = #{ <<"_rev">> := _}) -> {replace, Doc};
      (Doc) -> {create, Doc}
    end,
    Docs
  ),
  barrel_db:write_changes(Barrel, Batch).

delete_docs(Barrel, DocsOrDocsRevId) ->
  Batch = lists:map(
    fun
      (#{ <<"id">> := _, <<"_deleted">> := true, <<"_rev">> := _}=Doc) -> {replace, Doc};
      ({DocId, Rev})-> {delete, DocId, Rev};
      (_) -> erlang:error(badarg)
    end,
    DocsOrDocsRevId
  ),
  barrel_db:write_changes(Barrel, Batch).

save_local_doc(Barrel, DocId, Doc) ->
  barrel_db:put_local_doc(Barrel, DocId, Doc).

get_local_doc(Barrel, DocId) ->
  barrel_db:get_local_doc(Barrel, DocId).

delete_local_doc(Barrel, DocId) ->
  barrel_db:delete_local_doc(Barrel, DocId).

query(Barrel, Path, Fun, Acc, Options) ->
  barrel_index:query(Barrel, Path, Fun, Acc, Options).

