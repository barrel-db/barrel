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
  terminate/2,
  create_barrel/3,
  open_barrel/2,
  delete_barrel/2,
  has_barrel/2,
  close_barrel/2
]).

%% documents
-export([
  get_revision/3,
  add_revision/4,
  delete_revision/3,
  delete_revisions/3,
  fetch_docinfo/2,
  write_docinfo/5,
  purge_doc/4
]).


%% local documents
-export([
  put_local_doc/3,
  get_local_doc/2,
  delete_local_doc/2
]).

-include("barrel.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%%
%% -- storage behaviour
%%

init(StoreName, _Options) ->
  {ok, #{ store => StoreName, barrels => #{} } }.

terminate(_, _) -> ok.


create_barrel(Name, Options, #{ store := StoreName, barrels := Barrels } = State) ->
  case maps:is_key(Name, Barrels) of
    true ->
      {{error, already_exists}, State};
    false ->
      Tab = tabname(StoreName, Name),
      case memstore:open(Tab, []) of
        ok ->
          case init_barrel(StoreName, Name) of
            {ok, Barrel} ->
              NewState = State#{ barrels => Barrels#{ Name => Options } },
              {{ok, Barrel}, NewState};
            Error ->
              {Error, State}
          end;
        Error ->
          {Error, State}
      end
  end.


open_barrel(Name,  #{ store := StoreName, barrels := Barrels }) ->
  case maps:is_key(Name, Barrels) of
    true ->
      Barrel = init_barrel(StoreName, Name),
      {ok, Barrel};
    false ->
      {error, not_found}
  end.

delete_barrel(Name, #{ store := StoreName, barrels := Barrels } = State) ->
  Tab = tabname(StoreName, Name),
  _ =  memstore:close(Tab),
  NewState = State#{ barrels => maps:remove(Name, Barrels) },
  {ok, NewState}.

close_barrel(Name, State) ->
  delete_barrel(Name, State).
  
has_barrel(Name, #{ barrels := Barrels } ) ->
  maps:is_key(Name, Barrels).

tabname(StoreName, DbId) ->
  list_to_atom(
    barrel_lib:to_list(StoreName) ++ [$_|barrel_lib:to_list(DbId)]
  ).

init_barrel(StoreName, Name) ->
  Tab = tabname(StoreName, Name),
  case memstore:open(Tab, []) of
    ok ->
      Barrel  = #{store => StoreName,
                  name => Name,
                  tab => Tab,
                  updated_seq => 0,
                  docs_count => 0 },
      _ = memstore:write_batch(Tab, [{put, '$update_seq', 0},
                                     {put, '$docs_count', 0}]),
      {ok, Barrel};
    Error ->
      Error
  end.

%%
%% -- document storage API
%%


%% documents

get_revision(DocId, Rev, #{ tab := Tab }) ->
  case memstore:get(Tab, {r, DocId, Rev}) of
    {ok, Doc} -> {ok, Doc};
    not_found ->
      _ = lager:error("not found ~p~n", [ets:lookup(Tab, {r, DocId, Rev})]),
      {error, not_found}
  end.

add_revision(DocId, RevId, Body, #{ tab := Tab }) ->
  memstore:put(Tab, {r, DocId, RevId}, Body).

delete_revision(DocId, RevId, #{ tab := Tab }) ->
  memstore:delete(Tab, {r, DocId, RevId}).

delete_revisions(DocId, RevIds, #{ tab := Tab }) ->
  _ = [memstore:delete(Tab, {r, DocId, RevId}) || RevId <- RevIds],
  ok.

fetch_docinfo(DocId, #{ tab := Tab }) ->
  case memstore:get(Tab, {d, DocId}) of
    {ok, Doc} -> {ok, Doc};
    not_found -> {error, not_found}
  end.

%% TODO: that part should be atomic, maybe we should add a transaction log
write_docinfo(DocId, NewSeq, OldSeq, DocInfo, #{ tab := Tab }) ->
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

purge_doc(DocId, LastSeq, Revisions, #{ tab := Tab }) ->
  Batch = lists:foldl(
    fun(RevId, Batch1) ->
      [{delete, {r, DocId, RevId}} | Batch1]
        end,
    [{delete, {c, LastSeq}}, {delete, {d, DocId}}],
    Revisions
  ),
  memstore:write_batch(Tab, Batch).

%% local documents

put_local_doc(DocId, Doc, #{ tab := Tab }) ->
  memstore:put(Tab, {l, DocId}, Doc).

get_local_doc(DocId, #{ tab := Tab }) ->
  case memstore:get(Tab, {l, DocId}) of
    {ok, Doc} -> {ok, Doc};
    not_found -> {error, not_found}
  end.

delete_local_doc(DocId, #{ tab := Tab }) ->
  memstore:delete(Tab, {l, DocId}).