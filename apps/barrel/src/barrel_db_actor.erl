%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Jan 2018 11:27
%%%-------------------------------------------------------------------
-module(barrel_db_actor).
-author("benoitc").

%% API
-export([
  start_link/5
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).


-include("barrel.hrl").
-include("barrel_logger.hrl").



start_link(DbPid, Buffer, Tid, Store, UpdatedSeq) ->
  gen_server:start_link(?MODULE, [DbPid, Buffer, Tid, Store, UpdatedSeq], []).

init([DbPid, Buffer, Tid, Store, UpdatedSeq]) ->
  State =
    #{db_pid => DbPid,
      buffer => Buffer,
      update_tasks => Tid,
      store => Store,
      updated_seq => UpdatedSeq
    
    },
  
  {ok, State, hibernate}.


handle_call(_Msg, _From, State) -> {reply, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(wakup, State) ->
  do_process_entries(State).

do_process_entries(#{ db_pid := DbPid, buffer := Buffer } = State) ->
  case fetch_entry(Buffer, 0)  of
    {ok, {Ref, From, Timestamp, Op}} ->
      
      case handle_op(Op, From, State) of
        {ok, LastSeq} ->
          DbPid ! {updated, Ref, LastSeq},
          do_process_entries(State#{ updated_seq => LastSeq });
        Error ->
          exit(Error)
          
      end;
    hibernate ->
      {noreply, State, hibernate}
  end.


%% we try to fetch the entries in loop and if nothing happen after sometimes we enter in hibernation.
%% TODO: replace `timer:sleep/1' by a function catching a stop from the main barrel process
fetch_entry(Buffer, Attempts) ->
  case barrel_buffer_ets:out(Buffer) of
    {ok, Entry} -> {ok, Entry};
    empty ->
      %% All numbers below chosen by guess and check against a few random benchmarks.
      if
        Attempts < 4 -> fetch_entry(Buffer, Attempts + 1);
        Attempts < 10 ->
          timer:sleep(1),
          fetch_entry(Buffer, Attempts + 1);
        Attempts < 100 ->
          timer:sleep(5),
          fetch_entry(Buffer, Attempts + 1);
        Attempts < 200 ->
          timer:sleep(10),
          fetch_entry(Buffer, Attempts + 1);
        true ->
          hibernate
      end
  end.
  
handle_op({save_docs, RepDocs, LocalDocs, Policy}, From, #{ store := Store, updated_seq := UpdatedSeq }) ->
y

  


do_update_docs(Client, RepRecords, LocalRecords, Policy, #{ name := Name } = State0) ->
  Entries = group_records(RepRecords, Client, Policy, dict:new()),
  %%{Clients, Entries, LocalRecords} = case LocalRecords0 of
  %%                                     [] ->
  %%                                       TRef = erlang:send_after(1, self(), timeout),
  %%                                       collect_docs([Client], Entries0, Policy, TRef);
  %%                                     _ ->
  %%                                       {[Client], Entries0, LocalRecords0}
  %%                                   end,
  Clients = [Client],
  
  LocalRecords12 = update_local_records_rev([{Client, LocalRecord} || LocalRecord <- LocalRecords]),

  MergeFun = merge_fun(Policy),
  DIPairs = process_entries(Entries, MergeFun, [], State0),
  {DIPairs2, Indexed} = flush_revisions(DIPairs, [], [], State0),
  {ok, State1} = barrel_storage:write_docs_infos(State0, DIPairs2, LocalRecords12, []),
  State2 = barrel_storage:commit(State1),
  UpdatedSeq = barrel_storage:updated_seq(State2),

  FinalState = case Indexed of
                 true ->
                   barrel_storage:set_indexed_seq(State2, UpdatedSeq);
                 false ->
                   State2
               end,

  true = gproc:set_value(?barrel(Name), FinalState),

  maybe_notify(FinalState, State0),
  _ = [To ! {done, self()} || To <- Clients],
  FinalState.

flush_revisions([{DI, DI}| Rest], Flushed, ToIndex, State) ->
  flush_revisions(Rest, Flushed, ToIndex, State);
flush_revisions([{DI, OldDI}| Rest], Flushed, ToIndex, State) ->
  {BodyMap, DI2} = maps:take(body_map, DI),
  #{ id := Id, rev := WinningRev } = DI2,
  BodyMap2 = maps:filter(
    fun
      (Rev, Doc) when Rev =/= WinningRev ->
        barrel_storage:write_revision(State, Id, Rev, Doc),
        false;
      (Rev, Doc) ->
        barrel_storage:write_revision(State, Id, Rev, Doc),
        true
    end,
    BodyMap
  ),
  Flushed2 = [{DI2, OldDI} | Flushed],
  IRef = erlang:make_ref(),
  wpool:cast(barrel_index_pool, {index, IRef, State, DI2#{ body_map => BodyMap2}, OldDI}),
  ToIndex2 = [IRef | ToIndex],
  flush_revisions(Rest, Flushed2, ToIndex2, State);
flush_revisions([], Flushed, ToIndex, State) ->
  {Flushed, ToIndex /= [] }.

update_local_records_rev(Records) ->
  lists:map(
    fun({Client, Record}) ->
      #{ id := Id, revs := Revs, deleted := Del} = Record,
      PrevRev = case Revs of
                  [RevStr | _] ->
                    binary_to_integer(RevStr);
                  [] ->
                    0
                end,
      NewRev = case Del of
                 false -> PrevRev + 1;
                 true -> 0
               end,
      NewRecord = Record#{ revs => [NewRev]},
      send_result(Client, Record, {ok, Id, integer_to_binary(NewRev)}),
      NewRecord
    end,
    Records
  ).

maybe_notify(NewState, OldState) ->
  case {barrel_storage:updated_seq(NewState), barrel_storage:updated_seq(OldState)} of
    {Seq, Seq} ->
      ok;
    {_, _} ->
      barrel_event:notify(maps:get(name, NewState), db_updated),
      ok
  end.


group_records([Group | Rest], Client, Policy, D) ->
  [#{ id := Id } |_ ] = Group,
  D2 = lists:foldr(
    fun(Record, D1) ->
      dict:append(Id, {Record, Client, Policy}, D1)
      end,
    D,
    Group
  ),
  group_records(Rest, Client, Policy, D2);
group_records([], _, _, D) ->
  D.

process_entries(Entries, MergeFun, DIPairs, State) ->
  UpdatedSeq = barrel_storage:updated_seq(State),
  LastRID = barrel_storage:resource_id(State),
  {DIPairs2, _, _} = dict:fold(
    fun(DocId, Updates, {Pairs, Seq, Rid}) ->
      case barrel_storage:get_doc_infos(State, DocId) of
        {ok, #{ seq := OldSeq } = DI0} ->
          DI1 = DI0#{ body_map => #{} },
          DI2 = merge_revtrees(Updates, DI1, MergeFun),
          Seq2 = case DI2 =/= DI1 of
                   true ->
                     Seq + 1;
                   false -> OldSeq
                 end,
          {[{DI2#{ seq => Seq2 }, DI1} | Pairs], Seq2, Rid};
        not_found ->
          Rid2 = Rid + 1,
          DI = new_docinfo(DocId, Rid2),
          DI2 = merge_revtrees(Updates, DI, MergeFun),
          case DI2 =/= DI of
            true ->
              Seq2 = Seq +1,
              {[{DI2#{ seq => Seq2 }, not_found} | Pairs], Seq2, Rid2};
    
            false ->
              {[{not_found, not_found} | Pairs], Seq, Rid2}
  
          end
      end
    end,
    {DIPairs, UpdatedSeq, LastRID},
    Entries
  ),
  DIPairs2.

merge_fun(merge) ->
  fun merge_revtree/3;
merge_fun(merge_with_conflict) ->
  fun merge_revtree_with_conflict/3.

new_docinfo(DocId, Rid) ->
  #{id => DocId,
    rev => <<"">>,
    seq => 0,
    rid => Rid,
    deleted => false,
    revtree => barrel_revtree:new(),
    body_map => #{}}.

merge_revtrees([{Record, Client, _Policy} | Rest], DocInfo, MergeFun) ->
  DocInfo2 = MergeFun(Record, DocInfo, Client),
  merge_revtrees(Rest, DocInfo2, MergeFun);
merge_revtrees([], DocInfo, _MergeFun) ->
  DocInfo.


merge_revtree(Record, #{ deleted := true } = DocInfo, From) ->
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
      send_result(From, Record, {error, {conflict, revision_conflict}}),
      DocInfo
  end;
merge_revtree(Record, DocInfo, Client) ->
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
      send_result(Client, Record, {error, {conflict, doc_exists}}),
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
          send_result(Client, Record, {error, {conflict, revision_conflict}}),
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

-compile({inline, [send_result/3]}).
-spec send_result(Client :: pid(), Record :: map(), Result :: any()) -> ok.
send_result(Client, #{ ref := Ref }, Result) ->
  try Client ! {result, self(), {Ref, Result}} of
    _ ->
      ok
  catch
    _:_ -> ok
  end.


-compile({inline, [reply/2]}).
-spec reply({Pid :: pid(), Tag :: reference()}, Resp :: any()) -> ok.
reply({FromPid, Tag}, Resp) ->
  try FromPid ! {Tag, Resp} of
    _ ->
      ok
  catch
    _:_  -> ok
  end.

