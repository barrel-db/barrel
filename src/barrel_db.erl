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
  create_barrel/1,
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
  fold_changes/5,
  put_local_doc/3,
  delete_local_doc/2,
  get_local_doc/2
]).


-include_lib("barrel.hrl").
-include_lib("barrel_logger.hrl").

-define(WRITE_BATCH_SIZE, 128).


-define(BAD_PROVIDER_CONFIG(Store),
  try barrel_services:get_service_module(store, Store)
  catch
    error:badarg -> erlang:error(bad_provider_config)
  end).

create_barrel(Name) ->
  #{ mod := Mod, ref := Ref } = barrel_storage:get_store(),
  with_locked_barrel(
    Name,
    fun() ->
      case Mod:barrel_exists(Name, Ref) of
        true ->  {error, barrel_already_exists};
        false ->
          case start_barrel(Name) of
            {ok, _Pid} -> ok;
            Error  -> Error
          end
      end
    end
  ).

open_barrel(Name) ->
  try
    case barrel_registry:reference_of(Name) of
      {ok, _} = OK ->
        OK;
      error ->
        #{ mod := Mod, ref := Ref } = barrel_storage:get_store(),
        Res = barrel_registry:with_locked_barrel(
                Name,
                fun() ->
                    case Mod:barrel_exists(Name, Ref) of
                      true ->
                        start_barrel(Name);
                      false ->
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
  with_locked_barrel(
    Name,
    fun() ->
      ok = stop_barrel(Name),
      #{ mod := Mod, ref := Ref } = barrel_storage:get_store(),
      Mod:delete_barrel(Name, Ref)
    end
  ).

start_barrel(Name) ->
  supervisor:start_child(barrel_server_sup, [Name]).

stop_barrel(Name) ->
  case supervisor:terminate_child(barrel_server_sup, barrel_registry:where_is(Name)) of
    ok -> ok;
    {error, simple_one_for_one} -> ok
  end.

barrel_infos(Name) ->
  #{ mod := Mod, ref := Ref } = barrel_storage:get_store(),
  try Mod:barrel_infos(Name, Ref)
  catch
    error:badarg ->
      {error, barrel_not_found}
  end.


with_ctx(#{ store_mod := Mod, ref := Ref  }, Fun) ->
  {ok, Ctx} = Mod:init_ctx(Ref, true),
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
  WithSeq = maps:get(seq, Options, false),
  case Mod:get_doc_info(Ctx, DocId) of
    {ok, #{ deleted := true } = _DI} when UserRev =:= <<>> ->
      {error, not_found};
    {ok, #{ rev := WinningRev, revtree := RevTree, seq := Seq }=_DI} ->
      Rev = case UserRev of
              <<"">> -> WinningRev;
              _ -> UserRev
            end,
      case maps:find(Rev, RevTree) of
        {ok, RevInfo} ->
          Del = maps:get(deleted, RevInfo, false),
          case Mod:get_doc_revision(Ctx, DocId, Rev) of
            {ok, Doc} ->
              Doc1 = maybe_add_sequence(Doc, Seq, WithSeq),
              WithHistory = maps:get(history, Options, false),
              MaxHistory = maps:get(max_history, Options, ?IMAX1),
              Ancestors = maps:get(ancestors, Options, []),
              case WithHistory of
                false ->
                  {ok, maybe_add_deleted(Doc1#{ <<"_rev">> => Rev }, Del)};
                true ->
                  History = barrel_revtree:history(Rev, RevTree),
                  EncodedRevs = barrel_doc:encode_revisions(History),
                  Revisions = barrel_doc:trim_history(EncodedRevs, Ancestors, MaxHistory),
                  {ok, maybe_add_deleted(Doc1#{ <<"_rev">> => Rev, <<"_revisions">> => Revisions }, Del)}
              end;
            not_found ->
              {error, not_found};
            Error ->
              Error
          end;
        error ->
          {error, not_found}
      end;
    Error ->
      Error
  end.

maybe_add_sequence(Doc, _, false) -> Doc;
maybe_add_sequence(Doc, Seq, true) -> Doc#{ <<"_seq">> => Seq }.

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



prepare_docs([Doc | Rest], Refs, Records) ->
  #{ id := Id, ref := Ref } = Record = barrel_doc:make_record(Doc),
  prepare_docs(Rest, [Ref | Refs], dict:append(Id, Record, Records));
prepare_docs([], Refs, Records) ->
  {lists:reverse(Refs), Records}.

update_docs(#{ name := Name }, Docs, Options, UpdateType) ->
  MergePolicy = case UpdateType of
                  interactive_edit ->
                    AllOrNothing =  maps:get(all_or_nothing, Options, false),
                    case AllOrNothing of
                      true -> merge_with_conflict;
                      false -> merge
                    end;
                  replicated_changes ->
                    merge_with_conflict
                end,
  {Refs, Records} = prepare_docs(Docs, [], dict:new()),
  Server =  barrel_registry:where_is(Name),
  case barrel_server:update_docs(Server, Records, MergePolicy) of
    {ok, Results} ->
      FinalResults = [maps:get(Ref, Results) || Ref <- Refs],
      {ok, FinalResults};
    Error ->
      Error
  end.


put_local_doc(#{ name := Name}, DocId, Doc) ->
   Server =  barrel_registry:where_is(Name),
   gen_server:call(Server, {put_local_doc, DocId, Doc}).

delete_local_doc(#{ name := Name }, DocId) ->
  Server =  barrel_registry:where_is(Name),
  gen_server:call(Server, {delete_local_doc, DocId}).

get_local_doc(#{ store_mod := Mod } = Barrel, DocId) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
        Mod:get_local_doc(Ctx, DocId)
    end
   ).


maybe_add_deleted(Doc, true) -> Doc#{ <<"_deleted">> => true };
maybe_add_deleted(Doc, false) -> Doc.

%% TODO: replace with our own internal locking system?
-spec with_locked_barrel(barrel_name(), fun()) -> any().
with_locked_barrel(BarrelName, Fun) ->
  LockId = {{barrel, BarrelName}, self()},
  global:trans(LockId, Fun).
