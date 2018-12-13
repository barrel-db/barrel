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
  open_barrel/1,
  close_barrel/1,
  delete_barrel/1,
  barrel_infos/1
]).

-export([
  fetch_doc/3,
  update_docs/4,
  revsdiff/3,
  fold_docs/4,
  fold_changes/5
]).


-export([start_link/2]).

-export([
  init/1,
  handle_batch/2,
  terminate/2
]).


-include_lib("barrel/include/barrel.hrl").
-include_lib("barrel/include/barrel_logger.hrl").

-define(WRITE_BATCH_SIZE, 128).


-define(BAD_PROVIDER_CONFIG(Store),
  try barrel_services:get_service_module(store, Store)
  catch
    error:badarg -> erlang:error(bad_provider_config)
  end).

create_barrel(Name, Params0 = #{ store_provider := Store}) ->
  % raise an error if the store doesn't exist
  _ = ?BAD_PROVIDER_CONFIG(Store),
  barrel_registry:with_locked_barrel(
    Name,
    fun() ->
      case barrel_registry:exists(Name) of
        true ->  {error, barrel_already_exists};
        false ->
          Id = barrel_registry:local_id(Name),
          Params1 = Params0#{ id => Id },
          case start_barrel(Name, Params1) of
            {ok, _Pid} -> ok;
            Error  -> Error
          end
      end
    end
  );
create_barrel(Name, Params) ->
  create_barrel(Name, Params#{ store_provider => default }).
  
open_barrel(Name) ->
  try
      case barrel_registry:reference_of(Name) of
        {ok, _} = OK ->
          OK;
        error ->
          Res = barrel_registry:with_locked_barrel(
            Name,
            fun() ->
              case barrel_registry:config_of(Name) of
                {ok, #{ store_provider := Store } = Params} ->
                  _ = ?BAD_PROVIDER_CONFIG(Store),
                  start_barrel(Name, Params);
                error ->
                  {error, barrel_not_found}
              end
            end
          ),
          case Res of
            {ok, _} ->
              open_barrel(Name);
            {error,{already_started, _}} ->
              open_barrel(Name);
            Error ->
              Error
          end
      end
  catch
    exit:Reason when Reason =:= normal ->
      timer:sleep(10),
      open_barrel(Name)
  end.

close_barrel(Name) ->
  stop_barrel(Name).
  

delete_barrel(Name) ->
  barrel_registry:with_locked_barrel(
    Name,
    fun() ->
      ok = stop_barrel(Name),
      case barrel_registry:config_of(Name) of
        {ok, #{ id := Id, store_provider := Store }} ->
          ok = barrel_registry:delete_config(Name),
          Mod = barrel_services:get_service_module(store, Store),
          Mod:delete_barrel(Store, Id);
        error ->
          ok
      end
    end
  ).

start_barrel(Name, Params) ->
  supervisor:start_child(barrel_dbs_sup, [Name, Params]).

stop_barrel(Name) ->
  case supervisor:terminate_child(barrel_dbs_sup, barrel_registry:where_is(Name)) of
    ok -> ok;
    {error, simple_one_for_one} -> ok
  end.

barrel_infos(#{ id := Id, store_mod := Mod, store_name := Store }) ->
  try Mod:barrel_infos(Store, Id)
  catch
    error:badarg ->
      {error, barrel_not_found}
  end.


with_ctx(#{ id := Id, store_mod := Mod, store_name := Store }, Fun) ->
  {ok, Ctx} = Mod:init_ctx(Store, Id, true),
  try Fun(Ctx)
  after Mod:release_ctx(Ctx)
  end.

fetch_doc(#{ store_mod := Mod } = Barrel, DocId, Options) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
      do_fetch_doc(Mod, Ctx, DocId, Options)
    end
  ).

do_fetch_doc(Mod, Ctx, DocId, Options) ->
  UserRev = maps:get(rev, Options, <<"">>),
  case Mod:get_doc_info(Ctx, DocId) of
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
          case Mod:get_doc_revision(Ctx, DocId, Rev) of
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
    Error ->
      Error
  end.

revsdiff(#{ store_mod := Mod } = Barrel, DocId, RevIds) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
      do_revsdiff(Mod, Ctx, DocId, RevIds)
    end
  ).

do_revsdiff(Mod, Ctx, DocId, RevIds) ->
  case Mod:get_doc_info(Ctx, DocId) of
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
    {error, not_found} ->
      {ok, RevIds, []};
    Error ->
      Error
  end.

fold_docs(#{ store_mod := Mod } = Barrel, UserFun, UserAcc, Options) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
      WrapperFun = fold_docs_fun(Mod, Ctx, UserFun, Options),
      Mod:fold_docs(Ctx, WrapperFun, UserAcc, Options)
    end
  ).

fold_docs_fun(Mod, Ctx, UserFun, Options) ->
  IncludeDeleted =  maps:get(include_deleted, Options, false),
  WithHistory = maps:get(history, Options, false),
  MaxHistory = maps:get(max_history, Options, ?IMAX1),
  fun(DocId, DI, Acc) ->
    case DI of
      #{ deleted := true } when IncludeDeleted =/= true -> skip;
      #{ rev := Rev, revtree := RevTree, deleted := Del } ->
        case Mod:get_doc_revision(Ctx, DocId, Rev) of
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
          {errorn, not_found} ->
            skip;
          Error ->
            exit(Error)
        end
    end
  end.


fold_changes(#{ store_mod := Mod } = Barrel, Since, UserFun, UserAcc, Options) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
      fold_changes_1(Mod,Ctx, Since, UserFun, UserAcc, Options)
    end
  ).

fold_changes_1(Mod, Ctx, Since, UserFun, UserAcc, Options) ->
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
          DocId, Rev, Mod, Ctx, IncludeDoc
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
  {AccOut, LastSeq} = Mod:fold_changes(Ctx, Since + 1, WrapperFun, AccIn),
  {ok, AccOut, LastSeq}.

change_with_deleted(Change, true) -> Change#{ <<"deleted">> => true };
change_with_deleted(Change, _) -> Change.

change_with_doc(Change, DocId, Rev, Mod, Ctx, true) ->
  case Mod:get_revision(Ctx, DocId, Rev) of
    {ok, Doc} -> Change#{ <<"doc">> => Doc };
    _ -> Change#{ <<"doc">> => null }
  end;
change_with_doc(Change, _, _, _, _, _) ->
  Change.
  
update_docs(Barrel, Docs, Options, UpdateType) ->
  with_ctx(Barrel, fun(Ctx) -> update_docs(Barrel, Ctx, Docs, Options, UpdateType) end).

update_docs(Barrel, Ctx, Docs, Options, UpdateType) ->
  #{ name := Name } = Barrel,
  WritePolicy = case UpdateType of
                  interactive_edit ->
                    AllOrNothing =  maps:get(all_or_nothing, Options, false),
                    case AllOrNothing of
                      true -> merge_with_conflict;
                      false -> merge
                    end;
                  replicated_changes ->
                    merge_with_conflict
                end,
  barrel_tx:transact(
    fun() ->
      {Batch, Results} = lists:foldl(
        fun(Doc, {Batch1, Results1}) ->
          Record = barrel_doc:make_record(Doc),
          case update_doc(Barrel, Ctx, Record, WritePolicy, 0) of
            {ignore, Result} -> {Batch1, [Result | Results1]};
            {update_doc, OP, #{ id := DocId, rev := Rev } = DI, Tags} ->
              Result = {ok, DocId, Rev},
              {[{update_doc, OP, DI, Tags} | Batch1], [Result | Results1]}
          end
        end,
        {[], []},
        Docs
      ),
      Tag = make_ref(),
      FinalBatch = lists:reverse([{end_batch, Tag, self()} | Batch]),
      Pid = barrel_registry:where_is(Name),
      MRef = erlang:monitor(process, Pid),
      ok = gen_batch_server:cast_batch(Pid, FinalBatch),
      receive
        {Pid, Tag, done} when UpdateType =:= interactive_edit ->
          erlang:demonitor(MRef, [flush]),
          barrel_tx:commit(),
          {ok, lists:reverse(Results)};
        {Pid, Tag, done} ->
          barrel_tx:commit(),
          erlang:demonitor(MRef, [flush]),
          ok;
        {'DOWN', MRef, _, _, Reason} ->
          barrel_tx:abort(),
          exit(Reason)
      end
    end).


update_doc(Barrel, Ctx, #{ id := << ?LOCAL_DOC_PREFIX, _/binary>>=DocId } = Record, Policy, Attempts) ->
  #{name := Name, store_mod := Mod } = Barrel,
  case barrel_tx:register_write({Name, DocId}) of
    true ->
      #{ doc := LocalDoc, deleted := Deleted } = Record,
      case Deleted of
        true ->
          ok = Mod:delete_local_doc(Ctx, DocId);
        false ->
          ok = Mod:insert_local_doc(Ctx, LocalDoc)
      end,
      {ignore, {ok, DocId}};
    false ->
      barrel_lib:log_and_backoff(debug, ?MODULE, Attempts, "local doc write conflict"),
      update_doc(Barrel, Ctx, Record, Policy, Attempts +1 )
  end;
update_doc(Barrel, Ctx, #{ id := DocId } = Record, Policy, Attempts) ->
  #{name := Name, store_mod := Mod } = Barrel,
  case barrel_tx:register_write({Name, DocId}) of
    true ->
      {DocStatus, DI} = case Mod:get_doc_info(Ctx, DocId) of
                          {ok, DI1} -> {found, DI1#{ body_map => #{} }};
                          {error, not_found} -> {not_found, new_docinfo(DocId)}
                        end,
      MergeFun = case Policy of
                   merge -> fun merge_revtree/2;
                   merge_with_conflict -> fun merge_revtree_with_conflict/2
                 end,
      case MergeFun(Record, DI) of
        {ok, DI2} ->
          OP = case {DocStatus, maps:get(deleted, DI2, false)} of
                 {found, false} -> update;
                 {found, deleted} -> delete;
                 _ -> add
               end,
          Tags = do_index(Mod, Ctx, DI, DI2),
          {update_doc, OP, flush_revisions(Barrel, Ctx, DI2), Tags};
        Error ->
          {ignore, Error}
      end;
    false ->
      barrel_lib:log_and_backoff(debug, ?MODULE, Attempts, "document write conflict"),
      update_doc(Barrel, Ctx, Record, Policy, Attempts +1 )
  end.

maybe_add_deleted(Doc, true) -> Doc#{ <<"_deleted">> => true };
maybe_add_deleted(Doc, false) -> Doc.

flush_revisions(#{ store_mod := Mod }, Ctx, #{id := DocId} = DI) ->
  {BodyMap, DI2} = maps:take(body_map, DI),
  _ = maps:fold(
    fun(DocRev, Body, _) ->
      ok = Mod:add_doc_revision(Ctx, DocId, DocRev, Body),
      ok
    end,
    ok,
    BodyMap
  ),
  DI2.

do_index(_Mod, _Ctx,  #{ rev := Rev }, #{ rev := Rev }) ->
  %% winning revision didn't change
  {[], []};
do_index(Mod, Ctx, #{ rev := <<"">> }, #{ id := DocId, rev := Rev, body_map := BodyMap }) ->
  NewDoc = case maps:find(Rev, BodyMap) of
             {ok, Doc} -> Doc;
             error ->
               {ok, Doc} = Mod:get_doc_revision(Ctx, DocId, Rev),
               Doc
           end,
  {barrel_json:encode_index_keys(NewDoc), []};
do_index(Mod, Ctx, #{ rev := OldRev }, #{ id := DocId, rev := Rev, body_map := BodyMap }) ->
  {ok, OldDoc} = Mod:get_doc_revision(Ctx, DocId, OldRev),
  NewDoc = case maps:find(Rev, BodyMap) of
             {ok, Doc} -> Doc;
             error ->
               {ok, Doc} = Mod:get_doc_revision(Ctx, DocId, Rev),
               Doc
           end,
  OldKeys = barrel_json:encode_index_keys(OldDoc),
  NewKeys = barrel_json:encode_index_keys(NewDoc),
  %% {Added, Removed}
  {NewKeys -- OldKeys, OldKeys -- NewKeys}.

start_link(Name, Params) ->
  gen_batch_server:start_link({via, barrel_registry, Name}, ?MODULE, [Name, Params]).

init([Name, Params]) ->
  case init_(Name, Params) of
    {ok, Barrel} ->
      %% we trap exit there to to handle barrel closes
      erlang:process_flag(trap_exit, true),
      ok = barrel_registry:store_config(Name, Params),
      gproc:set_value(?barrel(Name), Barrel),
      {ok, Barrel};
    {error, Reason} ->
      {stop, Reason}
  end.

handle_batch(Batch, State) ->
  {ok, Ctx} = init_ctx(State),
  NewState = try handle_ops(Batch, [], 0, 0, maps:get(updated_seq, State), Ctx, State)
             after release_ctx(Ctx, State)
             end,
  {ok, NewState}.

terminate(_Reason, State) ->
  ok = try_close_barrel(State),
  ok.


init_(Name, #{ id := Id, store_provider := Store }) ->
  Mod = barrel_services:get_service_module(store, Store),
  case Mod:init_barrel(Store, Id) of
    {ok, LastSeq} ->
      {ok, #{ name => Name,
              id => Id,
              store_mod => Mod,
              store_name => Store,
              updated_seq => LastSeq }};
    Error ->
      Error
  end.


try_close_barrel(#{ name := BarrelName, provider := {Mod, StoreName} }) ->
  case erlang:function_exported(Mod, close_barrel, 2) of
    true ->
      Mod:close_barrel(StoreName, BarrelName);
    false ->
      ok
  end.

handle_ops([{_, {end_batch, Tag, Pid}} | Rest], DocInfos, AddCount, DelCount, Seq, Ctx, State) ->
  #{ name := Name, store_mod := Mod} = State,
  NewState =
    case Mod:write_doc_infos(Ctx, DocInfos, AddCount, DelCount) of
      ok ->
        barrel_event:notify(Name, db_updated),
        complete_batch({Tag, Pid}, done),
        State#{updated_seq => Seq};
      Error ->
        _ = complete_batch({Tag, Pid}, Error),
        State
    end,
  handle_ops(Rest, [], 0, 0, Seq, Ctx, NewState);
handle_ops([{_, {update_doc, OP, DI, DocTags}} | Rest], DocInfos, AddCount, DelCount, Seq, Ctx, State) ->
  Seq2 = Seq + 1,
  DI2 = DI#{ seq => Seq2 },
  DocInfos2 = [{DI2, DocTags} | DocInfos],
  {AddCount2, DelCount2} = case OP of
                             add -> {AddCount + 1, DelCount};
                             delete -> {AddCount, DelCount + 1};
                             update -> {AddCount, DelCount}
                           end,
  handle_ops(Rest, DocInfos2, AddCount2, DelCount2, Seq2, Ctx, State);
handle_ops([], _, _, _, _, _, State) ->
  State.

%% -----------------------------------------
%% merge doc infos

new_docinfo(DocId) ->
  #{id => DocId,
    rev => <<"">>,
    seq => 0,
    deleted => false,
    revtree => barrel_revtree:new(),
    body_map => #{}}.

merge_revtree(Record, #{ deleted := true } = DocInfo) ->
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
      {ok, DocInfo#{
        rev => NewRev,
        deleted => false,
        revtree => RevTree2 ,
        body_map => BodyMap#{ NewRev => Doc }
      }};
    false ->
      %% revision conflict
      {error, {conflict, revision_conflict}}
  end;
merge_revtree(Record, DocInfo) ->
  #{ revtree := RevTree, body_map := BodyMap } = DocInfo,
  #{ revs := Revs, deleted := NewDeleted, doc := Doc } = Record,
  case Revs of
    [NewRev] when map_size(RevTree) =:= 0  ->
      RevInfo = #{  id => NewRev, parent => <<>>, deleted => NewDeleted },
      RevTree1 = barrel_revtree:add(RevInfo, RevTree),
      {ok, DocInfo#{ rev => NewRev,
        revtree => RevTree1,
        deleted => NewDeleted,
        body_map => BodyMap#{ NewRev => Doc} }};
    [_NewRev] ->
      %% doc exists, we will create a new branch
      {error, {conflict, doc_exists}};
    [NewRev, Rev | _] ->
      case barrel_revtree:is_leaf(Rev, RevTree) of
        true ->
          RevInfo = #{  id => NewRev, parent => Rev, deleted => NewDeleted },
          RevTree2 = barrel_revtree:add(RevInfo, RevTree),
          {WinningRev, _, _} = barrel_revtree:winning_revision(RevTree2),
          case NewDeleted of
            false ->
              {ok, DocInfo#{ rev => WinningRev,
                deleted => false,
                revtree => RevTree2,
                body_map => BodyMap#{ NewRev => Doc} }};
            true ->
              {ok, DocInfo#{ rev => WinningRev,
                deleted => barrel_doc:is_deleted(RevTree2),
                revtree => RevTree2,
                body_map => BodyMap#{ NewRev => Doc} }}
          end;
        false ->
          {error, {conflict, revision_conflict}}
      end
  end.

merge_revtree_with_conflict(Record, DocInfo) ->
  #{ revtree := RevTree, body_map := BodyMap } = DocInfo,
  #{ revs := [LeafRev|Revs] ,  deleted := NewDeleted, doc := Doc  } = Record,
  
  case barrel_revtree:contains(LeafRev, RevTree) of
    true ->
      %% revision already stored. This only happen when doing all_or_nothing.
      %% TODO: check before sending it to writes?
      {ok, DocInfo};
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
      {ok, DocInfo#{
        rev => WinningRev,
        revtree => RevTree2,
        deleted => barrel_doc:is_deleted(RevTree2),
        body_map => BodyMap#{ LeafRev => Doc }
      }}
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

init_ctx(#{ id := Id, store_mod := Mod, store_name := Store }) ->
  Mod:init_ctx(Store, Id, false).

release_ctx(Ctx, #{store_mod := Mod }) ->
  Mod:release_ctx(Ctx).

complete_batch({Tag, Pid}, Msg) ->
  catch Pid ! {self(), Tag, Msg};
complete_batch(Clients, Msg) when is_list(Clients) ->
  _ = [catch Pid ! {self(), Tag, Msg} || {Tag, Pid} <- Clients].
