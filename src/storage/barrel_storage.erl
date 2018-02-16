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

-module(barrel_storage).
-author("benoitc").

%% API
-export([
  register_storage_provider/3,
  unregister_storage_provider/1,
  storage_providers/0,
  has_storage_provider/1,
  with_storage_provider/2
]).

%% init barrel
-export([
  init_barrel/2,
  close_barrel/2,
  has_barrel/2,
  destroy_barrel/2,
  clean_barrel/2
]).

%% doc
-export([
  fetch_docinfo/2,
  write_docinfo/5,
  get_revision/3,
  add_revision/4,
  delete_revision/3,
  delete_revisions/3,
  purge_doc/2
]).

%% local doc
-export([
  get_local_doc/3,
  put_local_doc/3,
  delete_local_doc/3
]).


-export([call/3]).

-export([start_link/0]).
-export([
  init/1,
  handle_call/3,
  terminate/2
]).

-include_lib("stdlib/include/ms_transform.hrl").

-define(TAB, barrel_storages).

register_storage_provider(Name, Module, Config) ->
  gen_server:call(?MODULE, {register, Name, Module, Config}).

unregister_storage_provider(Name) ->
  gen_server:call(?MODULE, {unregister, Name}).


storage_providers() ->
  ets:tab2list(?TAB).

has_storage_provider(Name) ->
  ets:member(?TAB, Name).


with_storage_provider(Name, Fun) ->
  case ets:lookup(?TAB, Name) of
    [] -> {error, storage_provider_not_found};
    [{Name, Module}] -> Fun(Name, Module)
  end.


init_barrel(Store, Id) ->
  call(Store, init_barrel, [Id]).

close_barrel(Store, Id) ->
  call(Store, close_barrel, [Id]).

has_barrel(Store, Id) ->
  call(Store, has_barrel, [Id]).

destroy_barrel(Store, Id) ->
  call(Store, destroy_barrel, [Id]).

clean_barrel(Store, Id) ->
  call(Store, clean_barrel, [Id]).


%% -----------
%% doc api


fetch_docinfo(#{ store := Store, id := Id }, DocId) ->
  call(Store, fetch_docinfo, [Id, DocId]).

write_docinfo(#{ store := Store, id := Id }, DocId, NewSeq, OldSeq, DocInfo) ->
  call(Store, write_docinfo, [Id, DocId, NewSeq, OldSeq, DocInfo]).



get_revision(#{ store := Store, id := Id }, DocId, Rev) ->
  call(Store, get_revision, [Id, DocId, Rev]).

add_revision(#{ store := Store, id := Id }, DocId, Rev, Body) ->
  call(Store, add_revision, [Id, DocId, Rev, Body]).

delete_revision(#{ store := Store, id := Id }, DocId, RevId) ->
  call(Store, delete_revision, [Id, DocId, RevId]).

delete_revisions(#{ store := Store, id := Id }, DocId, RevIds) ->
  call(Store, delete_revisions, [Id, DocId, RevIds]).

purge_doc(#{ store := Store, id := Id }, DocId) ->
  call(Store, purge_doc, [Id, DocId]).


%% -----------
%% local doc api

put_local_doc(Store, Id, Doc) ->
  call(Store, put_local_doc, [Id, Doc]).

get_local_doc(Store, Id, Doc) ->
  call(Store, put_local_doc, [Id, Doc]).

delete_local_doc(Store, Id, Doc) ->
  call(Store, put_local_doc, [Id, Doc]).


call(Name, Fun, Args) ->
  Mod = ets:lookup_element(?TAB, Name, 2),
  apply(Mod, Fun, [Name] ++ Args).

start_link() ->
  _ = init_tab(),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init_tab() ->
  case ets:info(?TAB, name) of
    undefined -> ets:new(?TAB, [named_table, set, public,
                                {read_concurrency, true}]);
    _ ->
      ok
  end.

init([]) ->
  {ok, #{}}.

handle_call({register, Name, Module, Config}, _From, State) ->
  case init_storage(Name, Module, Config) of
    ok ->
      ets:insert(?TAB, {Name, Module}),
      {reply, ok, State};
    Error ->
      {reply, Error, State}
  end;
handle_call({unregister, Name}, _From, State) ->
  case ets:take(?TAB, Name) of
    [] -> {reply, ok, State};
    [{Name, Module}] ->
      _ = close_storage(Module, Name),
      {reply, ok, State}
  end;
handle_call(Msg, _From, State) ->
  {reply, {bad_call, Msg}, State}.

terminate(_Reason, _State) ->
  ok = close_all_storages(),
  _ = ets:delete(?TAB),
  ok.

init_storage(Name, Module, Config) ->
  try Module:init(Name, Config)
  catch
    error:Reason -> {error, Reason}
  end.

close_storage(Module, Name) ->
  try Module:close(Name)
  catch
    error:Reason -> {error, Reason}
  end.

close_all_storages() ->
  Storages = ets:tab2list(?TAB),
  _ = [Module:close(Name) || {Name, Module} <- Storages],
  ok.
