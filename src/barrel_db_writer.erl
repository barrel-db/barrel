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


-module(barrel_db_writer).
-behavior(gen_server).

-export([update_docs/3]).

-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).


-include("barrel.hrl").

-define(TIMEOUT, 5000).


-define(CHUNK_SIZE, 5 * 1024 * 1000). %% 5 MB.

update_docs(Barrel, Docs, Options) ->
  MergePolicy = maps:get(merge_policy, Options, fail_on_conflict),
  ?start_span(#{ <<"log">> => <<"update docs">>,
                 <<"batch_size">> => length(Docs),
                 <<"merge_policy">> => barrel_lib:to_binary(MergePolicy) }),
  TRef = erlang:send_after(5000, self(), update_timeout),
  Results = try
              [do_update_doc(Barrel, Doc, Options) || Doc <- Docs]
            after
              ?end_span
            end,
  _ = erlang:cancel_timer(TRef),
  {ok, Results}.

do_update_doc(#{ name := Name } = Barrel, Doc, Options) ->
  StartTime = erlang:timestamp(),
  Record0 = barrel_doc:make_record(Doc),
  {_, Record1} = flush_attachments(Name, Record0),
  jobs:run(barrel_write_queue,
           fun() ->
               update_doc_1(Barrel, Record1, Options, StartTime)
           end).



update_doc_1(#{ name := Name }, #{ ref := Ref } = Record, Options, StartTime) ->
  SpanCtx = ocp:current_span_ctx(),
  Tags = ocp:current_tags(),
  gen_server:cast({via, gproc, ?barrel(Name)},
                  {{update_doc, self(), Record, Options}, SpanCtx, Tags}),

  MRef = erlang:monitor(process, gproc:where(?barrel(Name))),
  receive
    {Ref, Result} ->
      erlang:demonitor(MRef, [flush]),
      Now = erlang:timestamp(),
      ocp:record('barrel/db/update_doc_duration', timer:now_diff(Now, StartTime)),
      ocp:record('barrel/db/update_doc_num', 1),
      Result;
    {'DOWN', MRef, process, _Pid, Reason} ->
      ?LOG_ERROR("merge process down, name=~p, reason=~p~n", [Name, Reason]),
      exit({merge_down, Reason});
    update_timeout ->
      ocp:record('barrel/db/update_doc_timeout', 1),
      erlang:demonitor(MRef, [flush]),
      exit(timeout)
  end.

flush_attachments(Name, Record) ->
  _ = ocp:with_child_span(?MFA_SPAN_NAME, #{ <<"log">> => <<"flush attachments" >> }),
  try do_flush_attachments(Name, Record)
  after
    ?end_span
  end.

do_flush_attachments(Name,
                     #{ id := DocId,
                        attachments := Atts0 } = Record) when map_size(Atts0) > 0 ->
  Atts1 = maps:map(
            fun(AttName, AttDoc0) ->
                {Data, AttDoc1} = maps:take(<<"data">>, AttDoc0),

                ReaderFun = fun
                              (Bin) when is_binary(Bin) ->
                                {ok, Bin, eob};
                              (eob) ->
                                {ok, eob, undefined}
                            end,

                {ok, AttRecord, _} =
                  jobs:run(barrel_write_queue,
                           fun() ->
                               barrel_fs_att:put_attachment(Name, DocId,
                                                            AttName, {ReaderFun, Data})
                           end),
                #{ attachment => AttRecord, doc =>AttDoc1 }
            end,
            Atts0),
  {true, Record#{ attachments => Atts1 }};
do_flush_attachments(_, Record) ->
  {false, Record}.

start_link(Name) ->
  gen_server:start_link({via, gproc, ?barrel(Name)}, ?MODULE, [Name], []).


init([Name]) ->
  erlang:process_flag(trap_exit, true),
  case init_(Name) of
    {ok, Barrel} ->
      StartSeq = barrel_sequence:init(Barrel),
      gproc:set_value(?barrel(Name), Barrel),
      ?LOG_INFO("barrel opened name=~p seq=~p~n", [Name, StartSeq]),
      ocp:record('barrel/dbs/active_num', 1),
      {ok, Barrel#{updated_seq => StartSeq}};
    {error, Reason} ->
      ?LOG_ERROR("error opening barrel name=~p error=~p~n", [Name, Reason]),
      {stop, Reason}
  end.

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast({{update_doc, From, #{ id := DocId, ref := Ref } = Record, Options},
             SpanCtx, Tags},
            #{ name := Name, ref := BarrelRef, updated_seq := LastSeq } = State) ->

  MergePolicy = maps:get(merge_policy, Options, fail_on_conflict),
  _ = ocp:with_span_ctx(SpanCtx),
  _ = ocp:with_tags(Tags),
  case get_docinfo(BarrelRef, DocId) of
    {ok, {DocStatus, DI}} ->
      #{ seq := OldSeq, deleted := OldDel } = DI,
      _ = ocp:with_child_span(
            <<"barrel_db_writer:do_merge/2">>,
            #{ <<"log">> => <<"merge document">>,
               <<"merge_policy">> => barrel_lib:to_binary(MergePolicy) }),
      MergeResult = try
                      do_merge(Record, DI, MergePolicy)
                    catch
                      C:E:T ->
                        ?LOG_ERROR("merge error, docid=~p error=~p~n", [DocId, E]),
                        ?LOG_DEBUG("merge error, docid=~p class=~p error=~p traceback= p~n",
                                   [DocId, C, E, T]),
                        {error, {merge_error, E}}
                    after
                      ocp:finish_span()
                    end,
      case MergeResult of
        {ok, #{ rev := Rev } = DI2, DocRev, DocBody} when DI2 =/= DI ->
          Seq = barrel_sequence:inc(LastSeq),
          case DocStatus of
            not_found ->
              ?STORE:insert_doc(
                 BarrelRef, DI2#{ seq => barrel_sequence:encode(Seq) }, DocRev, DocBody
                );
            found ->
              ?STORE:update_doc(
                 BarrelRef, DI2#{ seq => barrel_sequence:encode(Seq) }, DocRev, DocBody, OldSeq, OldDel
                )
          end,
          barrel_event:notify(Name, db_updated),
          From ! {Ref, {ok, DocId, Rev}},
          {noreply, State#{ updated_seq => Seq }};
        {ok, #{ rev := Rev}, _DocRev, _DocBody} ->
          From ! {Ref, {ok, DocId, Rev}},
          {noreply, State};
        Error ->
          From ! {Ref, Error},
          {noreply, State}
      end;
    Error ->
      From ! {Ref, Error},
          {noreply, State}
  end;

handle_cast(_Msg, State) ->
  {noreply, State}.

terminate(_Reason, #{ name := Name } = State) ->
  ?LOG_INFO("closing barrel=~p~n", [Name]),
  ocp:record('barrel/dbs/active_num', -1),
  ok = try_close_barrel(State),
  ok.


%% -----------------------------------------
%% merge doc infos

new_docinfo(DocId) ->
  #{id => DocId,
    rev => <<"">>,
    seq => {0, 0},
    deleted => false,
    revtree => barrel_revtree:new()}.

get_docinfo(BarrelRef, DocId) ->
 case ?STORE:get_doc_info(BarrelRef, DocId) of
   {ok, DI1} ->
     {ok, {found, DI1}};
   {error, not_found} ->
     {ok, {not_found, new_docinfo(DocId)}};
   Error ->
     ?LOG_ERROR("~s: error fetching docid=~p errror=~p~n", [Error]),
     Error
 end.

do_merge(Record, DI, fail_on_conflict) ->
  merge_revtree(Record, DI);
do_merge(Record, DI, merge_with_conflict) ->
  merge_revtree_with_conflict(Record, DI).


merge_revtree(Record, #{ deleted := true } = DocInfo) ->
  #{ rev := WinningRev,  revtree := RevTree } = DocInfo,
  #{ revs := Revs,
     deleted := NewDeleted,
     doc := Doc,
     attachments := Atts } = Record,
  Depth = length(Revs),
  case Depth == 1 andalso not NewDeleted of
    true ->
      {Gen, _}  = barrel_doc:parse_revision(WinningRev),
      NewRevHash = barrel_doc:revision_hash(Doc, WinningRev, false),
      NewRev = << (integer_to_binary(Gen+1))/binary, "-", NewRevHash/binary  >>,
      RevInfo = #{  id => NewRev,
                    parent => WinningRev,
                    deleted => false,
                    attachments => Atts },
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
  #{ revs := Revs,
     deleted := NewDeleted,
     doc := Doc,
     attachments := Atts } = Record,
  case Revs of
    [NewRev] when map_size(RevTree) =:= 0  ->
      RevInfo = #{  id => NewRev,
                    parent => <<>>,
                    deleted => NewDeleted,
                    attachments => Atts },
      RevTree1 = barrel_revtree:add(RevInfo, RevTree),
      {WinningRev, _, _} = barrel_revtree:winning_revision(RevTree1),
      {ok, DocInfo#{ rev => WinningRev,
                     revtree => RevTree1,
                     deleted => NewDeleted }, NewRev, Doc};
    [_NewRev] ->
      %% doc exists, we will create a new branch
      {error, {conflict, doc_exists}};
    [NewRev, Rev | _] ->
      case barrel_revtree:is_leaf(Rev, RevTree) of
        true ->
          RevInfo = #{  id => NewRev,
                        parent => Rev,
                        deleted => NewDeleted,
                    attachments => Atts },
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
                               doc := Doc,
                               attachments := Atts },
                            #{ revtree := RevTree } = DocInfo) ->
  case barrel_revtree:contains(LeafRev, RevTree) of
    true ->
      %% revision already stored. This only happen when doing all_or_nothing.
      {ok, DocInfo, LeafRev, Doc};
    false ->
      %% Find the point where this doc's history branches from the current rev:
      {Parent, Path} = find_parent(Revs, RevTree, []),
      %% merge path in the revision tree
      {_, RevTree2} = lists:foldr(
                        fun(RevId, {P, Tree}) ->
                            Deleted = (NewDeleted =:= true andalso RevId =:= LeafRev),
                            Atts1 =  if
                                       RevId =:= LeafRev -> Atts;
                                       true -> #{}
                                  end,
                            RevInfo = #{ id => RevId,
                                         parent => P,
                                         deleted => Deleted,
                                         attachments => Atts1 },
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
    {ok, BarrelRef} ->
      Store = #{ name => Name, ref => BarrelRef},
      {ok, Store};
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
