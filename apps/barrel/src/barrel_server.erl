%% Copyright (c) 2019. Benoit Chesneau
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


-module(barrel_server).
-behavior(gen_server).

-export([update_docs/3]).

-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).


-include("barrel.hrl").
-include("barrel_logger.hrl").

update_docs(Server, Records, MergePolicy) ->
  gen_server:call(Server, {update_docs, Records, MergePolicy}).

start_link(Name) ->
  gen_server:start_link({via, barrel_registry, Name}, ?MODULE, [Name], []).

init([Name]) ->
  case init_(Name) of
    {ok, Barrel, LastSeq} ->
      %% we trap exit there to to handle barrel closes
      erlang:process_flag(trap_exit, true),
      gproc:set_value(?barrel(Name), Barrel),
      {ok, Barrel#{updated_seq => LastSeq}};
    {error, Reason} ->
      {stop, Reason}
  end.

handle_call({put_local_doc, DocId, Doc}, _From,
            #{ store_mod := Mod,
               ref := Ref} = State) ->
  Reply = Mod:put_local_doc(Ref, DocId, Doc),
  {reply, Reply, State};

handle_call({delete_local_doc, DocId}, _From,
            #{ store_mod := Mod,
               ref := Ref} = State) ->
  Reply = Mod:delete_local_doc(Ref, DocId),
  {reply, Reply, State};

handle_call({update_docs, Docs, MergePolicy}, _From,
            #{ store_mod := Mod,
               ref := Ref,
               name := Name,
               updated_seq := LastSeq } = State) ->
  {ok, RU0} = Mod:recovery_unit(Ref),
  {Results, RU, UpdatedSeq} = update_docs(Docs, MergePolicy, RU0, State),
  {Reply, NewState} = case Mod:commit(RU) of
                        ok ->
                          Mod:release_recovery_unit(RU),
                          if
                            UpdatedSeq =/= LastSeq ->
                              barrel_event:notify(Name, db_updated),
                              {{ok, Results}, State#{ updated_seq => UpdatedSeq }};
                            true ->
                              {{ok, Results}, State}
                          end;
                        Error ->
                          ?LOG_ERROR("update_docs db=~p error=~p~n", [Name, Error]),
                          {Error, State}
                      end,
  {reply, Reply, NewState};

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  ok = try_close_barrel(State),
  ok.


update_docs(GroupedRecords, MergePolicy, RU,
            #{ store_mod := Mod, updated_seq := LastSeq }) ->

  MergeFun = case MergePolicy of
               merge -> fun merge_revtree/2;
               merge_with_conflict -> fun merge_revtree_with_conflict/2
             end,
  dict:fold(
    fun(DocId, Records, {Results1, RU1, Seq1}) ->
        {DocStatus, #{ seq := OldSeq } = DI} = case Mod:get_doc_info(RU, DocId) of
                                                 {ok, DI1} -> {found, DI1};
                                                 {error, not_found} -> {not_found, new_docinfo(DocId)}
                                               end,
        {DI2, Results2, RU2} = merge_revtrees(Records, DI, Results1, Mod, RU1, MergeFun),
        if
          DI /= DI2 ->
            Seq2 = Seq1 + 1,
            RU3 = case DocStatus of
                    not_found ->
                      Mod:insert_doc_infos(RU2, DI2#{ seq => Seq2 });
                    found ->
                      Mod:update_doc_infos(RU2, DI2#{ seq => Seq2 }, OldSeq)
                  end,
            {Results2, RU3, Seq2};
          true ->
            {Results2, RU2, Seq1}
        end
    end,
    {#{}, RU, LastSeq},
    GroupedRecords).

merge_revtrees([#{ ref := Ref } = Record | Rest], #{ id := DocId } = DI, Results, Mod, RU, MergeFun) ->
        case MergeFun(Record, DI) of
          {ok, #{ rev := Rev } = DI2, DocRev, RevBody} ->
            RU2 = Mod:add_doc_revision(RU, DocId, DocRev, RevBody),
            Results2 = maps:put(Ref, {ok, DocId, Rev}, Results),
            merge_revtrees(Rest, DI2, Results2, Mod, RU2, MergeFun);
          Error ->
            Results2 = maps:put(Ref, Error, Results),
            merge_revtrees(Rest, DI, Results2, Mod, RU, MergeFun)
        end;
merge_revtrees([], DI, Results, _Mod, RU, _MergedFun) ->
        {DI, Results, RU}.

%% -----------------------------------------
%% merge doc infos

new_docinfo(DocId) ->
  #{id => DocId,
    rev => <<"">>,
    seq => 0,
    deleted => false,
    revtree => barrel_revtree:new()}.

merge_revtree(Record, #{ deleted := true } = DocInfo) ->
  #{ rev := WinningRev,  revtree := RevTree } = DocInfo,
  #{ revs := Revs, deleted := NewDeleted, doc := Doc } = Record,
  Depth = length(Revs),
  case Depth == 1 andalso not NewDeleted of
    true ->
      {Gen, _}  = barrel_doc:parse_revision(WinningRev),
      NewRevHash = barrel_doc:revision_hash(Doc, WinningRev, false),
      NewRev = << (integer_to_binary(Gen+1))/binary, "-", NewRevHash/binary  >>,
      RevInfo = #{  id => NewRev,  parent => WinningRev, deleted => false },
      RevTree2 = barrel_revtree:add(RevInfo, RevTree),
      {ok, DocInfo#{rev => NewRev,
                    deleted => false,
                    revtree => RevTree2 }, NewRev, Doc};
    false ->
      %% revision conflict
      {error, {conflict, revision_conflict}}
  end;
merge_revtree(Record, DocInfo) ->
  #{ revtree := RevTree} = DocInfo,
  #{ revs := Revs, deleted := NewDeleted, doc := Doc } = Record,
  case Revs of
    [NewRev] when map_size(RevTree) =:= 0  ->
      RevInfo = #{  id => NewRev, parent => <<>>, deleted => NewDeleted },
      RevTree1 = barrel_revtree:add(RevInfo, RevTree),
      {ok, DocInfo#{ rev => NewRev,
                     revtree => RevTree1,
                     deleted => NewDeleted }, NewRev, Doc};
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
                             revtree => RevTree2 }, NewRev, Doc};
            true ->
              {ok, DocInfo#{ rev => WinningRev,
                             deleted => barrel_doc:is_deleted(RevTree2),
                             revtree => RevTree2 }, NewRev, Doc}
          end;
        false ->
          {error, {conflict, revision_conflict}}
      end
  end.

merge_revtree_with_conflict(#{ revs := [LeafRev|Revs],
                               deleted := NewDeleted,
                               doc := Doc },
                            #{ revtree := RevTree } = DocInfo) ->
  case barrel_revtree:contains(LeafRev, RevTree) of
    true ->
      %% revision already stored. This only happen when doing all_or_nothing.
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
             deleted => barrel_doc:is_deleted(RevTree2)}, LeafRev, Doc}
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

%% init & terminate a barrel server

init_(Name) ->
  #{ mod := Mod, ref := Ref } = barrel_storage:get_store(),
  case Mod:open_barrel(Name, Ref) of
    {ok, BarrelRef, LastSeq} ->
      Store = #{ name => Name, ref => BarrelRef, store_mod => Mod},
      {ok, Store, LastSeq};
    Error ->
      Error
  end.


try_close_barrel(#{ name := BarrelName, store_mod := Mod }) ->
  case erlang:function_exported(Mod, close_barrel, 2) of
    true ->
      Mod:close_barrel(BarrelName);
    false ->
      ok
  end.
