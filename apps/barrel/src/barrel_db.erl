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
-behaviour(gen_batch_server).

%% API

-export([
  create_barrel/2,
  open_barrel/2,
  close_barrel/1,
  delete_barrel/2,
  barrel_infos/1
]).

-export([
  drop_barrel/1,
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

-export([start_link/3]).

-export([
  init/1,
  handle_batch/2,
  terminate/2
]).


-include_lib("barrel/include/barrel.hrl").
-include_lib("barrel/include/barrel_logger.hrl").

-define(WRITE_BATCH_SIZE, 128).


create_barrel(Store, Id) ->
  LockId = {{Store, Id}, self()},
  global:trans(
    LockId,
    fun() ->
      case start_barrel(Store, Id, #{ create_barrel => true }) of
        {ok, _Pid} -> ok;
        {error, db_already_exists} -> {error, db_already_exists};
        {error,{already_started, _Pid}} -> {error, db_already_exists}
      end
    end
  ).

open_barrel(Store, Id) ->
  ResourceId = {Store, Id},
  try
      case gproc:lookup_values(?barrel(ResourceId)) of
        [{_Pid, Barrel}] ->
          {ok, Barrel};
        [] ->
          LockId = {ResourceId, self()},
          Res = global:trans(
            LockId,
            fun() ->
              start_barrel(Store, Id, #{})
            end
          ),
          case Res of
            {ok, _} -> open_barrel(Store, Id);
            {error,{already_started, _}} ->
              open_barrel(Store, Id);
            Error ->
              Error
          end
      end
  catch
    exit:Reason when Reason =:= normal ->
      timer:sleep(10),
      open_barrel(Store, Id)
  end.


close_barrel(#{ name := Id, provider := {_, Store} }) ->
  LockId = {{Store, Id}, self()},
  global:trans(
    LockId,
    fun() ->
      stop_barrel(Store, Id)
    end
  ).

delete_barrel(Store, Id) ->
  LockId = {{Store, Id}, self()},
  global:trans(
    LockId,
    fun() ->
      ok = stop_barrel(Store, Id),
      Mod = barrel_services:get_service_module(store, Store),
      Mod:delete_barrel(Store, Id)
    end
  ).

start_barrel(StoreName, BarrelName, Options) ->
  supervisor:start_child(barrel_dbs_sup, [StoreName, BarrelName, Options]).

stop_barrel(StoreName, BarrelName) ->
  case supervisor:terminate_child(barrel_dbs_sup, gproc:where(?barrel({StoreName, BarrelName}))) of
    ok -> ok;
    {error, simple_one_for_one} -> ok
  end.

barrel_infos(#{ name := Id, provider := {Mod, Store} }) ->
  try Mod:barrel_infos(Store, Id)
  catch
    error:badarg ->
      {error, db_not_found}
  end.
  


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

drop_barrel(DbRef) ->
  req(DbRef, drop_barrel).



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






start_link(StoreName, BarrelName, Options) ->
  Name = ?barrel({StoreName, BarrelName}),
  gen_batch_server:start_link({via, gproc, Name}, ?MODULE, [StoreName, BarrelName, Options]).

init([StoreName, BarrelName, Options]) ->
  case init_(StoreName, BarrelName, Options) of
    {ok, Barrel} ->
      %% we trap exit there to to handle barrel closes
      erlang:process_flag(trap_exit, true),
      gproc:set_value(?barrel({StoreName, BarrelName}), Barrel),
      {ok, Barrel};
    {error, Reason} ->
      {stop, Reason}
  end.

handle_batch(Batch, State) ->
  NewState = handle_ops(Batch, [], #{}, maps:get(updated_seq, State), State),
  {ok, NewState}.

terminate(_Reason, State) ->
  ok = try_close_barrel(State),
  ok.


init_(StoreName, BarrelName, #{ create_barrel := true }) ->
  Mod = barrel_services:get_service_module(store, StoreName),
   Mod:create_barrel(StoreName, BarrelName);
init_(StoreName, BarrelName, _Options) ->
  Mod = barrel_services:get_service_module(store, StoreName),
  Mod:open_barrel(StoreName, BarrelName).


try_close_barrel(#{ name := BarrelName, provider := {Mod, StoreName} }) ->
  case erlang:function_exported(Mod, close_barrel, 2) of
    true ->
      Mod:close_barrel(StoreName, BarrelName);
    false ->
      ok
  end.



handle_ops([{_, {update, From, #{id := Id} = Record, Policy}} | Rest], Clients, DocInfosMap, Seq, Barrel) ->
  Clients2 = [From | Clients],
  case maps:find(Id, DocInfosMap) of
    {ok, {DI, OldDI}} ->
      DI2 = merge_revtree(Record, DI, From, Policy),
      if
        DI2 /= DI ->
          Seq2 = Seq +1,
          DocInfosMap2 = DocInfosMap#{ Id => { DI2#{ seq => Seq2}, OldDI }},
          handle_ops(Rest, Clients2, DocInfosMap2, Seq2, Barrel);
        true ->
          handle_ops(Rest, Clients2, DocInfosMap, Seq, Barrel)
      end;
    error ->
      {Status, DI} = case get_doc_info(Id, Barrel) of
                       {ok, DI} ->
                         {found, DI#{ body_map => #{} }};
                       not_found ->
                         {not_found, new_docinfo(Id)}
                     end,
  
      DI2 = merge_revtree(Record, DI, From, Policy),
      if
        DI2 /= DI ->
          Seq2 = Seq +1,
          Pair = case Status of
                   found -> { DI2#{ seq => Seq }, DI };
                   not_found -> { DI2#{ seq => Seq }, not_found }
                 end,
          DocInfosMap2 = DocInfosMap#{ Id => Pair },
          handle_ops(Rest, Clients2, DocInfosMap2, Seq2, Barrel);
        true ->
          handle_ops(Rest, Clients2, DocInfosMap, Seq, Barrel)
      end
  end;
handle_ops([], Clients, DocInfosMap, Seq, Barrel) ->
  DiPairs = maps:values(DocInfosMap),
  ok = write_docs(DiPairs, Barrel),
  Barrel2 = Barrel#{ updated_seq => Seq },
  %% notify an event and eventually update the shared state
  ok = maybe_notify(Barrel2, Barrel),
  _ = complete_batch(Clients),
  Barrel2.


%% -----------------------------------------
%% merge doc infos

merge_revtree(Record, DocInfos, ClientPid, Policy) ->
  case Policy of
    merge -> merge_revtree(Record, DocInfos, ClientPid);
    merge_with_conflict -> merge_revtree_with_conflict(Record, DocInfos, ClientPid)
  end.


new_docinfo(DocId) ->
  #{id => DocId,
    rev => <<"">>,
    seq => 0,
    deleted => false,
    revtree => barrel_revtree:new(),
    body_map => #{}}.

merge_revtree(Record, #{ deleted := true } = DocInfo, ClientPid) ->
  #{ rev := WinningRev,  revtree := RevTree, body_map := BodyMap } = DocInfo,
  #{ revs := Revs, deleted := NewDeleted, doc := Doc } = Record,
  Depth = length(Revs),
  case Depth == 1 andalso not NewDeleted of
    true ->
      {Gen, _}  = barrel_doc:parse_revision(WinningRev),
      NewRevHash = barrel_doc:revision_hash(Doc, WinningRev, false),
      NewRev = << (integer_to_binary(Gen+1))/binary, "-", NewRevHash/binary  >>,
      RevInfo = #{  id => NewRev,  parent => WinningRev, deleted => false },
      RevTree2 = barrel_revtree:add(RevInfo, RevTree),
      DocInfo#{
        rev => NewRev,
        deleted => false,
        revtree => RevTree2 ,
        body_map => BodyMap#{ NewRev => Doc }
      };
    false ->
      %% revision conflict
      cast_merge_result(ClientPid, Record, {error, {conflict, revision_conflict}}),
      DocInfo
  end;
merge_revtree(Record, DocInfo, ClientPid) ->
  #{ revtree := RevTree, body_map := BodyMap } = DocInfo,
  #{ revs := Revs, deleted := NewDeleted, doc := Doc } = Record,
  case Revs of
    [NewRev] when map_size(RevTree) =:= 0  ->
      RevInfo = #{  id => NewRev, parent => <<>>, deleted => NewDeleted },
      RevTree1 = barrel_revtree:add(RevInfo, RevTree),
      DocInfo#{ rev => NewRev,
        revtree => RevTree1,
        deleted => NewDeleted,
        body_map => BodyMap#{ NewRev => Doc} };
    [_NewRev] ->
      %% doc exists, we will create a new branch
      cast_merge_result(ClientPid, Record, {error, {conflict, doc_exists}}),
      DocInfo;
    [NewRev, Rev | _] ->
      case barrel_revtree:is_leaf(Rev, RevTree) of
        true ->
          RevInfo = #{  id => NewRev, parent => Rev, deleted => NewDeleted },
          RevTree2 = barrel_revtree:add(RevInfo, RevTree),
          {WinningRev, _, _} = barrel_revtree:winning_revision(RevTree2),
          case NewDeleted of
            false ->
              DocInfo#{ rev => WinningRev,
                deleted => false,
                revtree => RevTree2,
                body_map => BodyMap#{ NewRev => Doc} };
            true ->
              DocInfo#{ rev => WinningRev,
                deleted => barrel_doc:is_deleted(RevTree2),
                revtree => RevTree2,
                body_map => BodyMap#{ NewRev => Doc} }
          end;
        false ->
          cast_merge_result(ClientPid, Record, {error, {conflict, revision_conflict}}),
          DocInfo
      end
  end.

merge_revtree_with_conflict(Record, DocInfo, _Client) ->
  #{ revtree := RevTree, body_map := BodyMap } = DocInfo,
  #{ revs := [LeafRev|Revs] ,  deleted := NewDeleted, doc := Doc  } = Record,
  
  case barrel_revtree:contains(LeafRev, RevTree) of
    true ->
      %% revision already stored. This only happen when doing all_or_nothing.
      %% TODO: check before sending it to writes?
      DocInfo;
    false ->
      %% Find the point where this doc's history branches from the current rev:
      {Parent, Path} = find_parent(Revs, RevTree, []),
      %% merge path in the revision tree
      {_, RevTree2} = lists:foldr(
        fun(RevId, {P, Tree}) ->
          Deleted = (NewDeleted =:= true andalso RevId =:= LeafRev),
          RevInfo = #{ id => RevId, parent => P, deleted => Deleted },
          {RevId, barrel_revtree:add(RevInfo, Tree)}
        end,
        {Parent, RevTree},
        [LeafRev|Path]
      ),
      {WinningRev, _, _} = barrel_revtree:winning_revision(RevTree2),
      %% update DocInfo, we always find is the doc is deleted there
      %% since we could have only updated an internal branch
      DocInfo#{
        rev => WinningRev,
        revtree => RevTree2,
        deleted => barrel_doc:is_deleted(RevTree2),
        body_map => BodyMap#{ LeafRev => Doc }
      }
  end.

find_parent([RevId | Rest], RevTree, Acc) ->
  case barrel_revtree:contains(RevId, RevTree) of
    true ->
      {RevId, Rest};
    false ->
      find_parent(Rest, RevTree, [RevId | Acc])
  end;
find_parent([], _RevTree, Acc) ->
  {<<"">>, lists:reverse(Acc)}.


%% -----------------------------------------
%% internals

maybe_notify(#{ updated_seq := Seq }, #{ updated_seq := Seq }) ->
  ok;
maybe_notify(Barrel = #{ name := Name, provider := {_, Store} }, _) ->
  gproc:set_value(?barrel({Store, Name}), Barrel),
  barrel_event:notify(Name, db_updated).


get_doc_info(DocId, #{ name := Name, provider := {Mod, Store} }) ->
  Mod:get_doc_infos(Store, Name, DocId).

write_docs(Pairs, #{ name := Id, provider := {Mod, Store} }) ->
  Mod:write_docs(Store, Id, Pairs).

complete_batch(Pids) ->
  _ = sets:fold(fun(Pid, _) -> catch Pid ! {done, self()} end, ok, sets:from_list(Pids)).

-compile({inline, [cast_merge_result/3]}).
-spec cast_merge_result(From :: {pid(), term()}, Record :: map(), Msg :: any()) -> ok.
cast_merge_result(Pid, #{ ref := Ref }, Msg) ->
  try Pid ! {result, self(), {Ref, Msg}} of
    _ ->
      ok
  catch
    _:_ -> ok
  end.