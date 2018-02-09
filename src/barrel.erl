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
  get_barrel/2,
  has_barrel/2,
  destroy_barrel/2,
  barrel_infos/1
]).

-export([
  fetch_doc/3
]).

get_barrel(Store, Id) ->
  when_store(
    Store,
    fun() ->
      {ok,
       #{store => Store,
         id => Id}}
    end
  ).

has_barrel(Store, Id) ->
  when_store(
    Store,
    fun() -> barrel_storage:has_barrel(Store, Id) end
  ).

destroy_barrel(Store, Id) ->
  when_store(
    Store,
    fun() ->
      ok = barrel_db:close(#{ store => Store, id => Id }),
      barrel_storage:destroy_barrel(Store, Id)
    end
  ).

barrel_infos(DbRef) ->
  barrel_db:db_infos(DbRef).

fetch_doc(DbRef, DocId, Options) ->
  barrel_db:fetch_doc(DbRef, DocId, Options).


when_store(Store, Fun) ->
  case barrel_storage:has_storage_provider(Store) of
    true -> Fun();
    false ->
      {error, storage_provider_not_found}
  end.
  