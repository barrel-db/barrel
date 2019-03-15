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


-module(barrel_writer).
-behavior(gen_server).

-export([update_docs/3]).
-export([update_doc/3]).

-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).


-include("barrel.hrl").

-define(TIMEOUT, 5000).

update_docs(Server, Docs, MergePolicy) ->
  {ok, [update_doc(Server, Doc, MergePolicy) || Doc <- Docs]}.


update_doc(Server, Doc, MergePolicy) ->
   Start = erlang:timestamp(),
   #{ ref := Ref } = Record = barrel_doc:make_record(Doc),
   gen_server:cast(Server, {update_doc, self(), Record, MergePolicy}),
   receive
     {Ref, Result} ->
       Now = erlang:timestamp(),
       ocp:record('barrel/db/update_doc_duration', timer:now_diff(Now, Start)),
       ocp:record('barrel/db/update_doc_num', 1),
       Result
   after 5000 ->
           ocp:record('barrel/db/update_doc_timeout', 1),
           exit(timeout)
   end.



start_link(Name) ->
  gen_server:start_link({via, barrel_registry, Name}, ?MODULE, [Name], []).

init([Name]) ->
  case init_(Name) of
    {ok, Barrel, LastSeq} ->
      ocp:record('barrel/dbs/active_num', 1),
      %% we trap exit there to to handle barrel closes
      erlang:process_flag(trap_exit, true),
      gproc:set_value(?barrel(Name), Barrel),
      {ok, Barrel#{updated_seq => LastSeq}};
    {error, Reason} ->
      {stop, Reason}
  end.

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast({update_doc, From, #{ id := DocId, ref := Ref } = Record, MergePolicy},
            #{ name := Name, ref := BRef, updated_seq := Seq } = State) ->

  {DocStatus,
   #{ seq := OldSeq,
      deleted := OldDel } = DI} = case ?STORE:get_doc_info(BRef, DocId) of
                                    {ok, DI1} ->
                                      {found, DI1};
                                    {error, not_found} ->
                                      {not_found, new_docinfo(DocId)}
                                  end,

  MergeFun = case MergePolicy of
               merge -> fun merge_revtree/2;
               merge_with_conflict -> fun merge_revtree_with_conflict/2
             end,


  case MergeFun(Record, DI) of
    {ok, #{ rev := Rev } = DI2, DocRev, DocBody} when DI2 =/= DI ->
      Seq2 = Seq + 1,
      case DocStatus of
        not_found ->
          ?STORE:insert_doc(BRef, DI2#{ seq => Seq2 }, DocRev, DocBody);
        found ->
          ?STORE:update_doc(BRef, DI2#{ seq => Seq2 }, DocRev, DocBody, OldSeq, OldDel)
      end,
      barrel_event:notify(Name, db_updated),
      From ! {Ref, {ok, DocId, Rev}},
      {noreply, State#{ updated_seq => Seq2 }};
    {ok, #{ rev := Rev}, _DocRev, _DocBody} ->
      From ! {Ref, {ok, DocId, Rev}},
      {noreply, State};
    Error ->
      From ! {Ref, Error},
      {noreply, State}
  end;

handle_cast(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  ocp:record('barrel/dbs/active_num', -1),
  ok = try_close_barrel(State),
  ok.


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
  case ?STORE:open_barrel(Name) of
    {ok, BarrelRef, LastSeq} ->
      Store = #{ name => Name, ref => BarrelRef},
      {ok, Store, LastSeq};
    Error ->
      Error
  end.


try_close_barrel(#{ name := BarrelName }) ->
  case erlang:function_exported(?STORE, close_barrel, 2) of
    true ->
      ?STORE:close_barrel(BarrelName);
    false ->
      ok
  end.
