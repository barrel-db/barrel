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
  revsdiff/3,
  fold_docs/4,
  fold_changes/5,
  fold_path/5
]).


-export([
  do_for_ref/2
]).


-include("barrel.hrl").
-include("barrel_logger.hrl").

-define(WRITE_BATCH_SIZE, 128).

db_infos(DbRef) ->
  do_for_ref(
    DbRef,
    fun(#{ name := Name, store := Store} = Db) ->
      #{
        name => Name,
        store => Store,
        id => barrel_storage:id(Db),
        docs_count => barrel_storage:docs_count(Db),
        del_docs_count => barrel_storage:del_docs_count(Db),
        updated_seq => barrel_storage:updated_seq(Db),
        indexed_seq => barrel_storage:indexed_seq(Db)
      }
    end
  ).

fetch_doc(Db, DocId, Options) when is_map(Db) ->
  do_fetch_doc(Db, DocId, Options);
fetch_doc(DbRef, DocId, Options) ->
  do_for_ref(
    DbRef,
    fun(Db) ->
      do_fetch_doc(Db, DocId, Options)
    end
  ).

do_fetch_doc (Db, DocId = << ?LOCAL_DOC_PREFIX, _/binary >>, _Options) ->
  case barrel_storage:get_local_doc(Db, DocId) of
    {ok, #{ doc := Doc }} -> Doc;
    not_found -> {error, not_found}
  end;
do_fetch_doc(Db, DocId, Options) ->
  UserRev = maps:get(rev, Options, <<"">>),
  case barrel_storage:get_doc_infos(Db, DocId) of
    {ok, #{ deleted := true } = _DI} when UserRev =:= <<>> ->
      {error, not_found};
    {ok, #{ rev := WinningRev, revtree := RevTree}=_DI} ->
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

fold_docs(DbRef, UserFun, UserAcc, Options) ->
  do_for_ref(
    DbRef,
    fun(Db) ->
      WrapperFun = fold_docs_fun(Db, UserFun, Options),
      barrel_storage:fold_docs(Db, WrapperFun, UserAcc, Options)
    end
  ).

fold_docs_fun(#{ name := Name } = Db, UserFun, Options) ->
  IncludeDeleted =  maps:get(include_deleted, Options, false),
  WithHistory = maps:get(history, Options, false),
  MaxHistory = maps:get(max_history, Options, ?IMAX1),
  fun(DocId, DI, Acc) ->
    case DI of
      #{ deleted := true } when IncludeDeleted =/= true -> skip;
      #{ rev := Rev, revtree := RevTree, deleted := Del } ->
        case barrel_storage:get_revision(Db, DocId, Rev) of
          {ok, Doc} ->
            case WithHistory of
              false ->
                UserFun(maybe_add_deleted(Doc#{ <<"_rev">> => Rev}, Del), Acc);
              true ->
                History = barrel_revtree:history(Rev, RevTree),
                EncodedRevs = barrel_doc:encode_revisions(History),
                Revisions = barrel_doc:trim_history(EncodedRevs, [], MaxHistory),
                Doc1 = maybe_add_deleted(Doc#{ <<"_rev">> => Rev, <<"_revisions">> => Revisions }, Del),
                UserFun(Doc1, Acc)
            end;
          not_found ->
            ?LOG_WARNING(
              "doc revision not found while folding docs: db=~p id=~p, rev=~p~n",
              [Name, DocId, Rev]
            ),
            skip;
          Error ->
            ?LOG_ERROR(
              "error while folding docs: db=~p id=~p, rev=~p error=p~n",
              [Name, DocId, Rev, Error]
            ),
            exit(Error)
        end
    end
  end.


fold_changes(DbRef, Since, UserFun, UserAcc, Options) ->
  do_for_ref(
    DbRef,
    fun(Db) ->
      fold_changes_1(Db, Since, UserFun, UserAcc, Options)
    end
  ).

fold_changes_1(Db, Since, UserFun, UserAcc, Options) ->
  %% get options
  IncludeDoc = maps:get(include_doc, Options, false),
  WithHistory = maps:get(with_history, Options, false),
  WrapperFun =
    fun
      (_, DI, {Acc0, _}) ->
        #{id := DocId,
          seq := Seq,
          deleted := Deleted,
          rev := Rev,
          revtree := RevTree } = DI,
        Changes = case WithHistory of
                    false -> [Rev];
                    true -> barrel_revtree:history(Rev, RevTree)
                  end,
        Change0 = #{
          <<"id">> => DocId,
          <<"seq">> => Seq,
          <<"rev">> => Rev,
          <<"changes">> => Changes
        },
        Change = change_with_doc(
          change_with_deleted(Change0, Deleted),
          DocId, Rev, Db, IncludeDoc
        ),
        case UserFun(Change, Acc0) of
          {ok, Acc1} ->
            {ok, {Acc1, Seq}};
          {stop, Acc1} ->
            {stop, {Acc1, Seq}};
          ok ->
            {ok, {Acc0, Seq}};
          stop ->
            {stop, {Acc0, Seq}};
          skip ->
            skip
        end
    end,
  AccIn = {UserAcc, Since},
  {AccOut, LastSeq} = barrel_storage:fold_changes(Db, Since, WrapperFun, AccIn),
  {ok, AccOut, LastSeq}.

change_with_deleted(Change, true) -> Change#{ <<"deleted">> => true };
change_with_deleted(Change, _) -> Change.


change_with_doc(Change, DocId, Rev, Db, true) ->
  case barrel_storage:get_revision(Db, DocId, Rev) of
    {ok, Doc} -> Change#{ <<"doc">> => Doc };
    _ -> Change#{ <<"doc">> => null }
  end;
change_with_doc(Change, _, _, _, _) ->
  Change.


fold_path(DbRef, Path0, UserFun, UserAcc, Options) ->
  Path1 = normalize_path(Path0),
  DecodedPath = decode_path(Path1, []),
  do_for_ref(
    DbRef,
    fun(Db) ->
      WrapperFun = fold_docs_fun(Db, UserFun, Options),
      barrel_storage:fold_path(Db, DecodedPath, WrapperFun, UserAcc, Options)
    end
  ).

normalize_path(<<>>) -> <<"/id">>;
normalize_path(<<"/">>) -> <<"/id">>;
normalize_path(<< "/", _/binary >> = P) ->  P;
normalize_path(P) ->  <<"/", P/binary >>.

decode_path(<<>>, Acc) ->
  [ << "$" >> | lists:reverse(Acc)];
decode_path(<< $/, Rest/binary >>, Acc) ->
  decode_path(Rest, [<<>> |Acc]);
decode_path(<< $[, Rest/binary >>, Acc) ->
  decode_path(Rest, [<<>> |Acc]);
decode_path(<< $], Rest/binary >>, [BinInt | Acc] ) ->
  case (catch binary_to_integer(BinInt)) of
    {'EXIT', _} ->
      erlang:error(bad_path);
    Int ->
      decode_path(Rest, [Int | Acc])
  end;
decode_path(<<Codepoint/utf8, Rest/binary>>, []) ->
  decode_path(Rest, [<< Codepoint/utf8 >>]);
decode_path(<<Codepoint/utf8, Rest/binary>>, [Current|Done]) ->
  decode_path(Rest, [<< Current/binary, Codepoint/utf8 >> | Done]).



update_docs(DbRef, Docs, Options, UpdateType) ->
  do_for_ref(
    DbRef,
    fun(Db) ->
      Docs2 = prepare_docs(Docs, []),
      update_docs_1(Db, Docs2, Options, UpdateType)
    end
  ).

prepare_docs([Doc = #{ <<"id">> := _Id } | Rest], Acc) ->
  prepare_docs(Rest, [Doc | Acc]);
prepare_docs([Doc | Rest], Acc) ->
  Doc2 = Doc#{ <<"id">> => barrel_id:binary_id(62)},
  prepare_docs(Rest, [Doc2 | Acc]);
prepare_docs([], Acc) ->
  lists:reverse(Acc).

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
  {ok, UpdateResults};

update_docs_1(Db, Docs, Options, replicated_changes) ->
  %% create records to store
  Records = [barrel_doc:make_record(Doc) || Doc <- Docs],
  %% group records by ID
  RecordsBuckets = group_records(Records),
  %% do writes
  {ok, _} = write_and_commit(Db, RecordsBuckets, [], merge_with_conflict, Options),
  ok.
  

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
