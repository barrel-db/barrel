%% Copyright 2018, Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.
-module(barrel_memory_index).
-author("benoitc").

%% API
-export([
  new_batch/0,
  insert/4,
  unindex/4,
  log/3,
  commit/2,
  get_log/2
]).


new_batch() ->
  #{ mod => ?MODULE, batch => ets:new(?MODULE, [ordered_set, public]) }.

insert(Path, DocId, Type, #{ batch := Batch } ) ->
  Key = case Type of
          fwd -> {i, Path, DocId};
          rev -> {ri, Path, DocId}
        end,
  ets:insert(Batch, {Key, {put, Key, <<>>}}).

unindex(Path, DocId, Type, #{ batch := Batch }) ->
  Key = case Type of
          fwd -> {i, Path, DocId};
          rev -> {ri, Path, DocId}
        end,
  ets:insert(Batch, {Key, {delete, Key}}).

log(DocId, Paths, #{ batch := Batch }) ->
  Key = {ilog, DocId},
  ets:insert(Batch, {Key, {put, Key, Paths}}).

commit(#{ batch := Batch }, #{ tab := Tab }) ->
  WriteBatch = [OP || {_Key, OP} <- ets:tab2list(Batch)],
  memstore:write_batch(Tab, WriteBatch).

get_log(DocId, #{ tab := Tab }) ->
  Key = {ilog, DocId},
  memstore:get(Tab, Key).