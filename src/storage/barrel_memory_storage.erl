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

-export([
  get_snapshot/1,
  release_snapshot/1
]).

-export([
  save_state/1,
  last_indexed_seq/1
]).

-export([fold_changes/4]).

-export([
  index_path/3,
  unindex_path/3
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
                  indexed_seq => 0,
                  updated_seq => 0,
                  docs_count => 0 },
      _ = memstore:write_batch(Tab, [{put, '$update_seq', 0},
                                     {put, 'indexed_seq', 0},
                                     {put, '$docs_count', 0}]),
      {ok, Barrel};
    Error ->
      Error
  end.

save_state(#{ tab := Tab, updated_seq := Seq, docs_count := Count, indexed_seq := ISeq}) ->
  _ = memstore:write_batch(Tab, [{put, '$update_seq', Seq},
                                 {put, 'indexed_seq', ISeq},
                                 {put, '$docs_count', Count}]),
  ok.


last_indexed_seq(#{ indexed_seq := ISeq }) -> ISeq.


%%
%% -- document storage API
%%


%% documents

get_revision(DocId, Rev, #{ tab := Tab } = State) ->
  case memstore:get(Tab, {r, DocId, Rev}, read_options(State)) of
    {ok, Doc} -> {ok, Doc};
    not_found ->
      {error, not_found}
  end.

add_revision(DocId, RevId, Body, #{ tab := Tab }) ->
  memstore:put(Tab, {r, DocId, RevId}, Body).

delete_revision(DocId, RevId, #{ tab := Tab }) ->
  memstore:delete(Tab, {r, DocId, RevId}).

delete_revisions(DocId, RevIds, #{ tab := Tab }) ->
  _ = [memstore:delete(Tab, {r, DocId, RevId}) || RevId <- RevIds],
  ok.

fetch_docinfo(DocId, #{ tab := Tab } = State) ->
  case memstore:get(Tab, {d, DocId}, read_options(State)) of
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

get_local_doc(DocId, #{ tab := Tab } = State) ->
  case memstore:get(Tab, {l, DocId}, read_options(State)) of
    {ok, Doc} -> {ok, Doc};
    not_found -> {error, not_found}
  end.

delete_local_doc(DocId, #{ tab := Tab }) ->
  memstore:delete(Tab, {l, DocId}).


get_snapshot(#{ tab := Tab } = State) ->
  Snapshot = memstore:new_snapshot(Tab),
  State#{ snapshot => Snapshot}.

release_snapshot(#{ snapshot := Snapshot }) ->
  memstore:release_snapshot(Snapshot);
release_snapshot(_) ->
  erlang:error(badarg).

read_options(#{ snapshot := Snapshot }) -> [{snapshot, Snapshot}];
read_options(_) -> [].

fold_changes(Since, Fun, Acc, State) ->
  Tab = maps:get(tab, State),
  {ok, Itr} = memstore:iterator(Tab, read_options(State)),
  try fold_changes_loop(memstore:iterator_move(Itr, {seek, Since + 1}), Itr, Fun, Acc)
  after memstore:iterator_close(Itr)
  end.

fold_changes_loop({ok, _Seq, DocInfo}, Itr, Fun, Acc0) ->
  case Fun(DocInfo, Acc0) of
    {ok, Acc1} ->
      fold_changes_loop(memstore:iterator_move(Itr, next), Itr, Fun, Acc1);
    {stop, Acc1} ->
      Acc1
  end;
fold_changes_loop(Else, _, _, Acc) ->
  _ = lager:warning("folding changes unexpectedly stopped : ~p~n", [Else]),
  Acc.


index_path(Path, DocId, #{ tab := Tab }) ->
  memstore:write_batch(Tab, [{put, {Path, DocId}, <<>>},
                             {put, {lists:reverse(Path), DocId}}, <<>>]).

unindex_path(Path, DocId, #{ tab := Tab }) ->
  memstore:write_batch(Tab, [{delete, {Path, DocId}, <<>>},
                             {delete, {lists:reverse(Path), DocId}}, <<>>]).

