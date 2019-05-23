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
  fold_changes/5
]).

-export([fetch_attachment/3]).


-include_lib("barrel.hrl").

-define(WRITE_BATCH_SIZE, 128).


-define(BAD_PROVIDER_CONFIG(Store),
  try barrel_services:get_service_module(store, Store)
  catch
    error:badarg -> erlang:error(bad_provider_config)
  end).

create_barrel(Name) ->
  ?start_span(#{ <<"log">> => <<"create barrel">>,
                 <<"barrel">> => Name }),
  with_locked_barrel(
    Name,
    fun() ->
        try ?STORE:create_barrel(Name)
        after
          ?end_span
        end

    end
  ).

open_barrel(Name) ->
  ?start_span(#{ <<"log">> => <<"open barrel">>,
                 <<"barrel">> => Name }),
  try
    case barrel_registry:reference_of(Name) of
      {ok, undefined} ->
        %% race condition, retry
        timer:sleep(10),
        open_barrel(Name);
      {ok, _} = OK ->
        OK;
      error ->
        Res = barrel_registry:with_locked_barrel(
                Name,
                fun() -> start_barrel(Name)  end
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
  after
    ?end_span
  end.

close_barrel(Name) ->
  ?start_span(#{ <<"log">> => <<"close barrel">>,
                 <<"barrel">> => Name }),
  try stop_barrel(Name)
  after
    ?end_span
  end.


delete_barrel(Name) ->
  ?start_span(#{ <<"log">> => <<"delete barrel">>,
                 <<"barrel">> => Name }),
  with_locked_barrel(
    Name,
    fun() ->
        try
          ok = stop_barrel(Name),
          ?STORE:delete_barrel(Name)
        after
          ?end_span
        end
    end
  ).

start_barrel(Name) ->
  supervisor:start_child(barrel_db_sup, [Name]).

stop_barrel(Name) ->
  case supervisor:terminate_child(barrel_db_sup, barrel_registry:where_is(Name)) of
    ok -> ok;
    {error, simple_one_for_one} -> ok
  end.

barrel_infos(Name) ->
  ?STORE:barrel_infos(Name).

with_ctx(#{ ref := Ref  }, Fun) ->
  {ok, Ctx} = ?STORE:init_ctx(Ref, true),
  try Fun(Ctx)
  after
    ?STORE:release_ctx(Ctx)
  end.


fetch_doc(Barrel, DocId, Options) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
        Start = erlang:timestamp(),
        ocp:record('barrel/db/fetch_doc_num', 1),
        ?start_span(#{ <<"log">> => <<"fetch a doc">> }),
        try do_fetch_doc(Ctx, DocId, Options)
        after
          ?end_span,
          ocp:record('barrel/db/fetch_doc_duration',
                     timer:now_diff(erlang:timestamp(), Start))
        end
    end
  ).

do_fetch_doc(Ctx, DocId, Options) ->
  UserRev = maps:get(rev, Options, <<"">>),
  WithSeq = maps:get(seq, Options, false),
  case ?STORE:get_doc_info(Ctx, DocId) of
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
          case ?STORE:get_doc_revision(Ctx, DocId, Rev) of
            {ok, Doc} ->
              WithAttachments = maps:get(attachments, Options, false),
              WithHistory = maps:get(history, Options, false),
              MaxHistory = maps:get(max_history, Options, ?IMAX1),
              Ancestors = maps:get(ancestors, Options, []),
              Doc1 = maybe_add_sequence(Doc, Seq, WithSeq),
              Doc2 = maybe_fetch_attachments(Ctx, DocId, RevInfo, Doc1, WithAttachments),
              case WithHistory of
                false ->
                  {ok, maybe_add_deleted(Doc2#{ <<"_rev">> => Rev }, Del)};
                true ->
                  History = barrel_revtree:history(Rev, RevTree),
                  EncodedRevs = barrel_doc:encode_revisions(History),
                  Revisions = barrel_doc:trim_history(EncodedRevs, Ancestors, MaxHistory),
                  {ok, maybe_add_deleted(Doc2#{ <<"_rev">> => Rev, <<"_revisions">> => Revisions }, Del)}
              end;
            not_found ->
              {error, not_found};
            Error ->
              ?LOG_ERROR("error fetch revision document docid=~p rev=~p error=~p~n",
                         [DocId, Rev, Error]),
              Error
          end;
        error ->
          {error, not_found}
      end;
    {error, not_found} ->
      {error, not_found};
    Error ->
      ?LOG_ERROR("error fetching docinfo docid=~p error=~p~n",
                 [DocId, Error]),
      Error
  end.

maybe_add_sequence(Doc, _, false) -> Doc;
maybe_add_sequence(Doc, Seq, true) -> Doc#{ <<"_seq">> => Seq }.

maybe_fetch_attachments(_Ctx, _DocId,
                        #{ attachments := Atts }, Doc, false) when map_size(Atts) > 0 ->
 Atts1 = maps:map(
            fun(_Name, AttRecord) ->
                #{ doc := AttDoc } = AttRecord,
                AttDoc#{ <<"stub">> => true }
            end, Atts),
  Doc#{ <<"_attachments">> => Atts1};
maybe_fetch_attachments(_Ctx, _DocId,
                        #{ attachments := Atts }, Doc, true) when map_size(Atts) > 0 ->
  ?start_span(#{ <<"log">> => <<"fetch attachments">> }),
  Atts1 = try do_fetch_attachments(Atts)
          after
            ?end_span
          end,
  Doc#{ <<"_attachments">> => Atts1};
maybe_fetch_attachments(_, _, _, Doc, _) ->
  Doc.

do_fetch_attachments(Atts) ->
  maps:map(
    fun(_Name, AttRecord) ->
        #{ attachment := Att, doc := AttDoc } = AttRecord,
        {ok, ReaderFun, Ctx} = barrel_fs_att:fetch_attachment(Att),
        {ok, AttBin} = read_data(ReaderFun, Ctx, <<>>),
        AttDoc#{ <<"data">> => AttBin }
    end, Atts).

fetch_attachment(Barrel, DocId, AttName) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
        ?start_span(#{ <<"log">> => <<"fetch attachment">> }),
        try do_fetch_attachment(Ctx, DocId, AttName)
        after
          ?end_span
        end
    end
   ).

do_fetch_attachment(Ctx, DocId, AttName) ->
  case ?STORE:get_doc_info(Ctx, DocId) of
    {ok, #{ deleted := true } = _DI} ->
      {error, not_found};
    {ok, #{ rev := WinningRev, revtree := RevTree }=_DI} ->
      case maps:find(WinningRev, RevTree) of
        {ok, #{ attachments := Atts }} ->
          case maps:find(AttName, Atts) of
            {ok, #{ attachment := Att }} ->
              {ok, ReaderFun, ReadCtx} = barrel_fs_att:fetch_attachment(Att),
              read_data(ReaderFun, ReadCtx, <<>>);
            error ->
              {error, not_found}
          end;
        error ->
          {error, not_found}
      end;
    Error ->
      Error
  end.

read_data(ReaderFun, Ctx0, Acc) ->
  ?start_span(#{ <<"log">> => <<"load attachment data">> }),
  try do_read_data(ReaderFun, Ctx0, Acc)
  after
    ?end_span
  end.

do_read_data(ReaderFun, Ctx0, Acc) ->
  case ReaderFun(Ctx0) of
    {ok, Bin, Ctx1} ->
      do_read_data(ReaderFun, Ctx1, << Acc/binary, Bin/binary >> );
    eob ->
      {ok, Acc};
    Error ->
      Error
  end.

revsdiff(Barrel, DocId, RevIds) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
      ?start_span(#{ <<"log">> => <<"get missing revisions">> }),
      try do_revsdiff(Ctx, DocId, RevIds)
      after
        ?end_span
      end
    end
  ).

do_revsdiff(Ctx, DocId, RevIds) ->
  case ?STORE:get_doc_info(Ctx, DocId) of
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

fold_docs(Barrel, UserFun, UserAcc, Options) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
      WrapperFun = fold_docs_fun(Ctx, UserFun, Options),
      ocp:record('barrel/db/fold_docs_num', 1),
      Start = erlang:timestamp(),
      ?start_span(#{ <<"log">> => <<"fold docs">> }),
      try ?STORE:fold_docs(Ctx, WrapperFun, UserAcc, Options)
      after
        ?end_span,
        ocp:record('barrel/docs/fold_docs_duration',
                   timer:now_diff(erlang:timestamp(), Start))
      end
    end
   ).

fold_docs_fun(Ctx, UserFun, Options) ->
  IncludeDeleted =  maps:get(include_deleted, Options, false),
  WithHistory = maps:get(history, Options, false),
  MaxHistory = maps:get(max_history, Options, ?IMAX1),
  fun(DocId, DI, Acc) ->
    case DI of
      #{ deleted := true } when IncludeDeleted =/= true -> skip;
      #{ rev := Rev, revtree := RevTree, deleted := Del } ->
        case ?STORE:get_doc_revision(Ctx, DocId, Rev) of
          {ok, Doc} ->
            case WithHistory of
              false ->
                UserFun(maybe_add_deleted(Doc#{ <<"_rev">> => Rev}, Del), Acc);
              true ->
                History = barrel_revtree:history(Rev, RevTree),
                EncodedRevs = barrel_doc:encode_revisions(History),
                Revisions = barrel_doc:trim_history(EncodedRevs, [], MaxHistory),
                Doc1 = maybe_add_deleted(Doc#{ <<"_rev">> => Rev,
                                               <<"_revisions">> => Revisions }, Del),
                UserFun(Doc1, Acc)
            end;
          {errorn, not_found} ->
            skip;
          Error ->
            ?LOG_ERROR("fold doc error while fetching document docid=~p rev=~p error=~p~n",
                 [DocId, Rev, Error]),

            exit(Error)
        end
    end
  end.


fold_changes(Barrel, Since, UserFun, UserAcc, Options) ->
  with_ctx(
    Barrel,
    fun(Ctx) ->
        ocp:record('barrel/db/fold_changes_num', 1),
        Start = erlang:timestamp(),
        ?start_span(#{ <<"log">> => <<"fold changes">> }),
        try fold_changes_1(Ctx, Since, UserFun, UserAcc, Options)
        after
          ?end_span,
          ocp:record('barrel/db/fold_change_duration',
                     timer:now_diff(erlang:timestamp(), Start))
        end
    end
   ).

fold_changes_1(Ctx, Since0, UserFun, UserAcc, Options) ->
  {SinceEpoch, SinceSeq}= Since = case Since0 of
                                    first -> {0, 0};
                                    {_, _} -> Since0;
                                    _ ->
                                      erlang:error(badarg)
                                  end,
  %% get options
  IncludeDoc = maps:get(include_doc, Options, false),
  WithHistory = maps:get(with_history, Options, false),
  WrapperFun =
    fun
      (_, DI, {Acc0, _}) ->
        #{id := DocId,
          seq := {Epoch, Seq}=LSN,
          deleted := Deleted,
          rev := Rev,
          revtree := RevTree } = DI,
        Changes = case WithHistory of
                    false -> [Rev];
                    true -> barrel_revtree:history(Rev, RevTree)
                  end,
        Change0 = #{
          <<"id">> => DocId,
          <<"seq">> => << (integer_to_binary(Epoch))/binary, "-", (integer_to_binary(Seq))/binary >> ,
          <<"rev">> => Rev,
          <<"changes">> => Changes
        },
        Change = change_with_doc(
          change_with_deleted(Change0, Deleted),
          DocId, Rev, Ctx, IncludeDoc
        ),
        case UserFun(Change, Acc0) of
          {ok, Acc1} ->
            {ok, {Acc1, LSN}};
          {stop, Acc1} ->
            {stop, {Acc1, LSN}};
          ok ->
            {ok, {Acc0, LSN}};
          stop ->
            {stop, {Acc0, LSN}};
          skip ->
            skip
        end
    end,
  AccIn = {UserAcc, Since},
  {AccOut, LastSeq} = ?STORE:fold_changes(Ctx, {SinceEpoch, SinceSeq + 1}, WrapperFun, AccIn),
  {ok, AccOut, LastSeq}.

change_with_deleted(Change, true) -> Change#{ <<"deleted">> => true };
change_with_deleted(Change, _) -> Change.

change_with_doc(Change, DocId, Rev, Ctx, true) ->
  {ok, Doc} = ?STORE:get_doc_revision(Ctx, DocId, Rev),
  Change#{ <<"doc">> => Doc };
change_with_doc(Change, _, _, _, _) ->
  Change.

update_docs(Barrel, Docs, Options, interactive_edit) ->
  AllOrNothing =  maps:get(all_or_nothing, Options, false),
  MergePolicy = case AllOrNothing of
                  true -> merge_with_conflict;
                  false -> merge
                end,
  barrel_db_writer:update_docs(Barrel, Docs, MergePolicy);
update_docs(Barrel, Docs, _Options, replicated_changes) ->
  barrel_db_writer:update_docs(Barrel, Docs, merge).

maybe_add_deleted(Doc, true) -> Doc#{ <<"_deleted">> => true };
maybe_add_deleted(Doc, false) -> Doc.

%% TODO: replace with our own internal locking system?
-spec with_locked_barrel(barrel_name(), fun()) -> any().
with_locked_barrel(BarrelName, Fun) ->
  ?start_span(#{ <<"log">> => <<"lock barrel">> }),
  LockId = {{barrel, BarrelName}, self()},
  try global:trans(LockId, Fun)
  after
    ?end_span
  end.
