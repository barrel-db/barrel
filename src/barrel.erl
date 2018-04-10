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
  fetch_doc/3
]).


create_barrel(Name, Options) ->
  barrel_db:create_barrel(Name, Options).

delete_barrel(Name) ->
  barrel_db:delete_barrel(Name).
  

barrel_infos(DbRef) ->
  barrel_db:db_infos(DbRef).

fetch_doc(DbRef, DocId, Options) ->
  barrel_db:fetch_doc(DbRef, DocId, Options).
