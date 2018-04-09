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

%% doc
-export([
  fetch_docinfo/2,
  write_docinfo/5,
  get_revision/3,
  add_revision/4,
  delete_revision/3,
  delete_revisions/3,
  purge_doc/4,
  fold_changes/4,
  fold_docs/5
]).

%% local doc
-export([
  get_local_doc/2,
  put_local_doc/3,
  delete_local_doc/2
]).


-export([call/3]).

-export([
  start_link/3,
  has_barrel/2,
  create_barrel/3,
  delete_barrel/2,
  open_barrel/2,
  find_barrel/1
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  terminate/2
]).


-export([default_dir/0, data_dir/0]).

-include("barrel.hrl").

-include_lib("stdlib/include/ms_transform.hrl").


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

purge_doc(#{ store := Store, id := Id }, DocId, LastSeq, Revisions) ->
  call(Store, purge_doc, [Id, DocId, LastSeq, Revisions]).

fold_docs(#{ store := Store, id := Id }, Start, End, Fun, Acc) ->
  call(Store, fold_docs, [Id, Start, End, Fun, Acc]).

fold_changes(#{ store := Store, id := Id }, Since, Fun, Acc) ->
  call(Store, fold_changes, [Id, Since, Fun, Acc]).



%% -----------
%% local doc api

put_local_doc(#{ store := Store, id := Id }, DocId, Doc) ->
  call(Store, put_local_doc, [Id, DocId, Doc]).

get_local_doc(#{ store := Store, id := Id }, DocId) ->
  call(Store, get_local_doc, [Id, DocId]).

delete_local_doc(#{ store := Store, id := Id }, DocId) ->
  call(Store, delete_local_doc, [Id, DocId]).


call(Name, Fun, Args) ->
  Mod = ets:lookup_element(?TAB, Name, 2),
  apply(Mod, Fun, [Name] ++ Args).


default_dir() ->
  filename:join([?DATA_DIR, node()]).

-spec data_dir() -> string().
data_dir() ->
  Dir = application:get_env(barrel, data_dir, default_dir()),
  _ = filelib:ensure_dir(filename:join([".", Dir, "dummy"])),
  Dir.

store_key(Name) ->
  {n, l, {store, Name}}.

has_barrel(Store, Name) ->
  gen_server:call(store_key(Store), {has_barrel, Name}).

create_barrel(Store, Name, Options) ->
  gen_server:call(store_key(Store), {create_barrel, Name, Options}).

delete_barrel(Store, Name) ->
  gen_server:call(store_key(Store), {delete_barrel, Name}).

open_barrel(Store, Name) ->
  gen_server:call(store_key(Store), {open_barrel, Name}).

find_barrel(Name) ->
  fold_stores(
    fun ({Store, _Pid}, Acc) ->
      case barrel_storage:has_barrel(Store, Name) of
        true -> {stop, {ok, Store};
        false -> Acc
      end
    end,
    error
  ).


fold_stores(Fun, Acc) ->
  Res = gproc:select({l, n}, [{{n, l, {store, '_'}}, [],['$_']}]),
  fold_stores_1(Res, Fun, Acc).

fold_stores_1([{n, l, {store, Name}, Pid, _} | Rest], Fun, Acc0) ->
  case Fun({Name, Pid}, Acc0) of
    {ok, Acc1} -> fold_stores_1(Rest, Fun, Acc1);
    {stop, Acc1} -> Acc1;
    stop -> Acc0
  end.

%% start a storage
start_link(Name, Mod, Options) ->
  proc_lib:start_link(?MODULE, init, [[Name, Mod, Options]]).


init([Name, Mod, Options]) ->
  process_flag(trap_exit, true),
  case init_storage(Name, Mod, Options) of
    {ok, ModState} ->
      proc_lib:init_ack({ok, self()}),
      State = #{
        name => Name,
        mod => Mod,
        modstate => ModState
      },
      gen_server:enter_loop(?MODULE, [], State, {via, gproc, store_key(Name)});
    Error ->
      _ = lager:error("error while initializing storage ~p~n", [Name]),
      erlang:error(Error)
  end.

handle_call({has_barrel, Name}, _From, #{ name := Store, mod := Mod, modstate := ModState} = State) ->
  Reply = try_exec(Mod, has_barrel, [Store, Name, ModState]),
  {reply, Reply, State};

handle_call({create_barrel, Name, Options}, _From, #{ name := Store, mod := Mod, modstate := ModState} = State) ->
  {Reply, NewModState} = try_exec(Mod, create_barrel, [{Store, Name}, Options, ModState]),
  {reply, Reply, State#{ modstate => NewModState}};

handle_call({open_barrel, Name}, _From, #{ name := Store, mod := Mod, modstate := ModState} = State) ->
  Reply = try_exec(Mod, open_barrel, [{Store, Name}, ModState]),
  {reply, Reply, State};

handle_call({delete_barrel, Name}, _From, #{ name := Store, mod := Mod, modstate := ModState} = State) ->
  {Reply, NewModState} = try_exec(Mod, delete_barrel, [{Store, Name}, ModState]),
  {reply, Reply, State#{ modstate => NewModState}};

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

terminate(Reason, #{ mod := Mod, modstate := ModState}) ->
  Mod:terminate(Reason, ModState).

init_storage(Name, Mod, Options) ->
  try Mod:init(Name, Options)
  catch
    error:Error -> Error
  end.

try_exec(M, F, A) ->
  try erlang:apply(M, F, A)
  catch
    error:Error -> Error
  end.