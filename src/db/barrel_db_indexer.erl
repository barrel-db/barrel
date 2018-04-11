%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Apr 2018 14:09
%%%-------------------------------------------------------------------
-module(barrel_db_indexer).
-author("benoitc").
-behaviout(gen_statem).

%% API
-export([start_link/2]).

-export([
  init/1,
  callback_mode/0,
  terminate/3,
  code_change/4
]).

-export([
  active/3,
  idle/3,
  handle_event/4
]).

-export([index_worker/3]).

start_link(Name, DbPid) ->
  gen_statem:start_link(?MODULE, [Name, DbPid], []).


init([Name, Pid]) ->
  %% register to the changes
  _ = barrel_event:reg(Name),
  %% start the worker
  Worker = spawn_worker(Pid, refresh),
  Data = #{name => Name,
           db_pid => Pid,
           worker => Worker,
           pending => false},
  {ok, active, Data}.


callback_mode() -> state_functions.

terminate({shutdown, deleted}, _State, #{ store := Store, name := Name}) ->
  _ = barrel_storage:delete_barrel(Store, Name),
  ok;
terminate(_Reason, _State, #{ store := Store, name := Name}) ->
  _ = barrel_storage:close_barrel(Store, Name),
  ok.

code_change(_OldVsn, State, Data, _Extra) ->
  {ok, State, Data}.

active({call, From}, available, #{ pending := true } = Data ) ->
  {keep_state, Data#{ pending => false }, [{reply, From, refresh}]};

active({call, From}, available, #{ pending := false } = Data ) ->
  {next_state, idle, Data#{ pending => false }, [{reply, From, wait}]};

active(info, {'$barrel_event', Name, db_updated}, #{ name := Name } = Data) ->
  {keep_state, Data#{ pending => true }};

active(EventType, Event, Data) ->
  handle_event(EventType, Event, active, Data).

idle(info, {'$barrel_event', _, db_updated}, #{ worker := Worker } = Data) ->
  Worker ! refresh,
  {keep_state, Data#{ pending => true }};

idle(EventType, Event, Data) ->
  handle_event(EventType, Event, active, Data).


handle_event(info, {'EXIT', Pid, _}, State, #{ db_pid := DbPid, worker := Pid} = Data) ->
  Action = case State of
             active -> refresh;
             idle -> wait
           end,
  NewWorker = spawn_worker(DbPid, Action),
  {keep_state, Data#{worker => NewWorker }};
handle_event(_, _, _, Data) ->
  {keep_state, Data}.


spawn_worker(Pid, Action) ->
  spawn_link(?MODULE, index_worker, [self(), Pid, Action]).


index_worker(Parent, Pid, Action) ->
  case Action of
    refresh ->
      process_changes(Parent, Pid);
    _ ->
      wait_for_refresh(Parent, Pid)
  end.


%% TODO: make it parallel,
%% there is no reason there we can't process the change in //.
%% We can spawn processes that will analyze them in // and
%% collect the final result in order before writing it.
process_changes(Parent, Pid) ->
  {Mod, ModState} = barrel_db:get_state(Pid),
  Snapshot = Mod:get_snapshot(ModState),
  LastIndexedSeq = Mod:last_indexed_seq(Snapshot),
  NewSeq = try process_changes_1(LastIndexedSeq, Mod, Snapshot)
           after Mod:release_snapshot(Snapshot)
           end,
  if
    NewSeq =/=  LastIndexedSeq ->
      barrel_db:set_last_indexed_seq(Pid, NewSeq);
    true ->
      ok
  end,
  case gen_statem:call(Parent, available) of
    refresh ->
      process_changes(Parent, Pid);
    wait ->
      wait_for_refresh(Parent, Pid)
  end.


process_changes_1(LastIndexedSeq, Mod, Snapshot) ->
  Mod:fold_changes(
    LastIndexedSeq,
    fun(DI, _OldSeq) ->
      #{ id := DocId, seq := Seq, rev := Rev } = DI,
      OldRev = maps:get(old_rev, DI, undefined),
      NewDoc = idoc(DocId, Rev, Mod, Snapshot),
      OldDoc = idoc(DocId, OldRev, Mod, Snapshot),
      {Added, Removed} = barrel_index:diff(NewDoc, OldDoc),
      ok = update_index(Added, Removed, DocId, Mod, Snapshot),
      {ok, Seq}
    end,
    LastIndexedSeq,
    Snapshot).

  
wait_for_refresh(Parent, Pid) ->
  receive
    refresh ->
      process_changes(Parent, Pid)
  end.


idoc(_DocId, undefined, _Mod, _Snapshot) ->
  #{};
idoc(DocId, Rev, Mod, Snapshot) ->
  case Mod:get_revision(DocId, Rev, Snapshot) of
    {ok, NewDoc} -> NewDoc;
    _ -> #{}
  end.

update_index(Added, Removed, DocId, Mod, Snapshot) ->
  lists:foreach(
    fun(Path) ->
      Mod:index_path(Path, DocId, Snapshot)
    end,
    Added),
  
  lists:foreach(
    fun(Path) ->
      Mod:unindex_path(Path, DocId, Snapshot)
    end,
    Removed),
  ok.
