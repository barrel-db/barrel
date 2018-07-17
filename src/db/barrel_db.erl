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

-module(barrel_db).
-author("benoitc").

%% API
-export([
  create_barrel/2,
  drop_barrel/1,
  close_barrel/1,
  db_infos/1,
  fetch_doc/3,
  update_docs/4,
  revsdiff/3
]).


-export([
  do_for_ref/2
]).


-include("barrel.hrl").

-define(WRITE_BATCH_SIZE, 128).

db_infos(DbRef) ->
  do_for_ref(
    DbRef,
    fun(#{ name := Name, store := Store} = Db) ->
      #{
        name => Name,
        store => Store,
        docs_count => barrel_storage:docs_count(Db),
        del_docs_count => barrel_storage:del_docs_count(Db),
        updated_seq => barrel_storage:updated_seq(Db),
        indexed_seq => barrel_storage:indexed_seq(Db)
      }
    end
  ).

fetch_doc(DbRef, DocId = << ?LOCAL_DOC_PREFIX, _/binary >>, _Options) ->
  do_for_ref(
    DbRef,
    fun(Db) ->
      case barrel_storage:get_local_doc(Db, DocId) of
        {ok, #{ doc := Doc }} -> Doc;
        not_found -> {error, not_found}
      end
    end);

fetch_doc(DbRef, DocId, Options) ->
  do_for_ref(
    DbRef,
    fun(Db) ->
      do_fetch_doc(Db, DocId, Options)
    end
  ).


do_fetch_doc(Db, DocId, Options) ->
  UserRev = maps:get(rev, Options, <<"">>),
  case barrel_storage:get_doc_infos(Db, DocId) of
    {ok, #{ deleted := true }} when UserRev =:= <<>> ->
      {error, not_found};
    {ok, #{ rev := WinningRev, revtree := RevTree}} ->
      Rev = case UserRev of
              <<"">> -> WinningRev;
              _ -> UserRev
            end,
      case maps:find(Rev, RevTree) of
        {ok, RevInfo} ->
          Del = maps:get(deleted, RevInfo, false),
          case barrel_storage:get_revision(Db, DocId, Rev) of
            {ok, Doc} ->
              WithHistory = maps:get(history, Options, false),
              MaxHistory = maps:get(max_history, Options, ?IMAX1),
              Ancestors = maps:get(ancestors, Options, []),
              case WithHistory of
                false ->
                  {ok, maybe_add_deleted(Doc#{ <<"_rev">> => Rev }, Del)};
                true ->
                  History = barrel_revtree:history(Rev, RevTree),
                  EncodedRevs = barrel_doc:encode_revisions(History),
                  Revisions = barrel_doc:trim_history(EncodedRevs, Ancestors, MaxHistory),
                  {ok, maybe_add_deleted(Doc#{ <<"_rev">> => Rev, <<"_revisions">> => Revisions }, Del)}
              end;
            not_found ->
              {error, not_found};
            Error ->
              Error
          end;
        Error ->
          Error
      end;
    not_found ->
      {error, not_found};
    Error ->
      Error
  end.


revsdiff(DbRef, DocId, RevIds) ->
  do_for_ref(
    DbRef,
    fun(Db) ->
      do_revsdiff(Db, DocId, RevIds)
    end
  ).

do_revsdiff(Db, DocId, RevIds) ->
  case barrel_storage:get_doc_infos(Db, DocId) of
    {ok, #{revtree := RevTree}} ->
      {Missing, PossibleAncestors} = lists:foldl(
        fun(RevId, {M, A} = Acc) ->
          case barrel_revtree:contains(RevId, RevTree) of
            true -> Acc;
            false ->
              M2 = [RevId | M],
              {Gen, _} = barrel_doc:parse_revision(RevId),
              A2 = barrel_revtree:fold_leafs(
                fun(#{ id := Id}=RevInfo, A1) ->
                  Parent = maps:get(parent, RevInfo, <<"">>),
                  case lists:member(Id, RevIds) of
                    true ->
                      {PGen, _} = barrel_doc:parse_revision(Id),
                      if
                        PGen < Gen -> [Id | A1];
                        PGen =:= Gen, Parent =/= <<"">> -> [Parent | A1];
                        true -> A1
                      end;
                    false -> A1
                  end
                end, A, RevTree),
              {M2, A2}
          end
        end, {[], []}, RevIds),
      {ok, lists:reverse(Missing), lists:usort(PossibleAncestors)};
    not_found ->
      {ok, RevIds, []};
    Error ->
      Error
  end.


update_docs(DbRef, Docs, Options, UpdateType) ->
  do_for_ref(
    DbRef,
    fun(Db) ->
      update_docs_1(Db, Docs, Options, UpdateType)
    end
  ).


update_docs_1(Db, Docs, Options, interactive_edit) ->
  %% create records to store
  Records0 = [barrel_doc:make_record(Doc) || Doc <- Docs],
  %% split local docs from docs that can be replicated
  {LocalRecords, Records1} = lists:partition(
    fun
      (#{ id := << ?LOCAL_DOC_PREFIX, _/binary >> }) -> true;
      (_) -> false
    end,
    Records0
  ),

  %% TODO: add native attachment support
  AllOrNothing =  maps:get(all_or_nothing, Options, false),
  WritePolicy = case AllOrNothing of
                  true -> merge_with_conflict;
                  false -> merge
                end,

  %% group records by ID
  RecordsBuckets = group_records(Records1),
  IdsRevs = new_revs(RecordsBuckets),

  %% do writes
  {ok, CommitResults} = write_and_commit(Db, RecordsBuckets, LocalRecords, WritePolicy, Options),
  %% reorder results
  ResultsMap = lists:foldl(
    fun({Key, Resp}, M) -> maps:put(Key, Resp, M) end,
    IdsRevs,
    CommitResults
  ),
  UpdateResults = lists:map(
    fun(#{ ref := Ref }) -> maps:get(Ref, ResultsMap) end,
    Records1
  ),
  {ok, UpdateResults}.

new_revs(RecordsBuckets) ->
  lists:foldl(
    fun(Bucket, M) ->
      lists:foldl(
        fun(#{ id := Id, revs := [RevId | _], ref := Ref }, M1) -> M1#{ Ref => {ok, Id, RevId}} end,
        M,
        Bucket
      )
    end,
    #{},
    RecordsBuckets
  ).

%% TODO: this could be optimised, instead of grouping / bucket we
group_records(Records) ->
  RecordsGroups = barrel_lib:group_by(Records, fun(#{ id := Id }) -> Id end),
  [Bucket || {_Id, Bucket} <- dict:to_list(RecordsGroups)].

write_and_commit(#{ updater_pid := Pid }, RecordsBuckets, LocalRecords, WritePolicy, _Options) ->
  Pid ! {update_docs, self(), RecordsBuckets, LocalRecords, WritePolicy},
  MRef = erlang:monitor(process, Pid),
  try await_write_results(MRef, Pid, [])
  after erlang:demonitor(MRef, [flush])
  end.

await_write_results(MRef, Pid, Results) ->
  receive
    {result, Pid, Resp} ->
      await_write_results(MRef, Pid, [Resp | Results]);
    {done, Pid} ->
      {ok, Results};
    {'DOWN', MRef, _, _, Reason} ->
      exit(Reason)
  end.

create_barrel(DbRef, Options) ->
  case barrel_db_sup:start_db(DbRef, Options#{ create => true }) of
    {ok, _Pid} -> ok;
    {error, {already_started, _Pid}} -> {error, already_exists};
    {error, Error} -> Error
  end.

drop_barrel(DbRef) ->
  req(DbRef, drop_barrel).


close_barrel(DbRef) ->
  req(DbRef, close_barrel).

do_for_ref(DbRef, Fun) ->
  try
      case gproc:lookup_values(?barrel(DbRef)) of
        [{_Pid, Db}] ->
          Fun(Db);
        [] ->
          case barrel_db_sup:start_db(DbRef) of
            {ok, _Pid} ->
              do_for_ref(DbRef, Fun) ;
            {error, {already_started, _id}} ->
              do_for_ref(DbRef, Fun);
            Err = {error, _} ->
              Err;
            Error ->
              {error, Error}
          end
      end
  catch
      exit:Reason when Reason =:= normal  ->
        do_for_ref(DbRef, Fun)
  end.

maybe_add_deleted(Doc, true) -> Doc#{ <<"_deleted">> => true };
maybe_add_deleted(Doc, false) -> Doc.

req(Barrel, Request) ->
  do_for_ref(
    Barrel,
    fun(#{ updater_pid := Pid }) ->
      Tag = erlang:make_ref(),
      From = {self(), Tag},
      MRef = erlang:monitor(process, Pid),
      Pid ! ?BARREL_CALL(From, Request),
      await_req(Tag, MRef)
    end
  ).

await_req(Tag, MRef) ->
  receive
    {Tag, Resp} -> Resp;
    {'DOWN', MRef, _, _, Reason} ->
      exit(Reason)

  end.