%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Dec 2018 05:45
%%%-------------------------------------------------------------------
-module(barrel_tx).
-author("benoitc").
-behaviour(gen_server).


%% API
-export([
  transact/1,
  register_write/1,
  commit/0, commit/1,
  abort/0
]).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include_lib("stdlib/include/ms_transform.hrl").

-define(uncommited_keys, barrel_tx_uncommited_keys).
-define(commited_keys, barrel_tx_commited_keys).


-spec transact(function()) -> any().
transact(Fun) ->
  _ = clean_transaction(),
  Tx = #{ snapshot_id => new_transaction(),
          written_keys => []},
  _ = erlang:put(barrel_transaction, Tx),
  try Fun()
  after clean_transaction()
  end.

-spec register_write(Key :: term()) -> true | false.
register_write(Key) ->
  case erlang:get(barrel_transaction) of
    undefined -> erlang:error(no_transaction);
    Tx ->
      #{ snapshot_id := SnapshotId, written_keys := Keys } = Tx,
      case lists:member(Key, Keys) of
        true ->
          true;
        false ->
          Result =
            gen_server:call(?MODULE, {register_write, Key, self(), SnapshotId}, 1000),
          case Result of
            true ->
              Keys2 = [Key |Keys],
              _ = erlang:put(barrel_transaction, Tx#{ written_keys => Keys2 }),
              true;
            false ->
              false
          end
      end
  end.

-spec abort() -> ok.
abort() ->
  clean_transaction().

-spec commit() -> ok.
commit() ->
  commit(infinity).

-spec commit(Timeout :: non_neg_integer() | infinity) -> ok.
commit(Timeout) ->
  case erlang:get(barrel_transaction) of
    undefined ->
      erlang:error(no_transaction);
    #{ snapshot_id := SnapshotId, written_keys := Keys } ->
      ok = gen_server:call(?MODULE, {commit, Keys, self(), SnapshotId}, Timeout),
      _ = erlang:erase(barrel_transaction),
      ok
  end.

new_transaction() ->
  SnapshotId = persistent_term:get({?MODULE, snapshot_id}),
  atomics:add_get(SnapshotId, 1, 1).


clean_transaction() ->
  case erlang:erase(barrel_transaction) of
    undefined ->
      ok;
    #{ written_keys := Keys } ->
      clean_uncommited_keys(self(), Keys)
  end.


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  ?uncommited_keys = ets:new(?uncommited_keys, [ordered_set, public, named_table,
    {write_concurrency, true}, {read_concurrency, true}]),
  ?commited_keys = ets:new(?commited_keys, [set, named_table]),
  SnapshotId = atomics:new(1, []),
  persistent_term:put({?MODULE, snapshot_id}, SnapshotId),

  {ok, #{ snapshot_id => 0, monitors => #{} } }.


handle_call({register_write, Key, Pid, SnapshotId}, _From, State) ->
  case is_commited_after_snapshot(Key, SnapshotId) of
    true ->
      {reply, false, State};
    false ->
      case is_uncommited(Key, Pid) of
        true ->
          ets:insert(?uncommited_keys, {Key, Pid}),
          ets:insert(?uncommited_keys, {{Pid, Key}, Key}),
          NewState = maybe_monitor(Pid, State),
          {reply, true, NewState};
        false ->
          {reply, false, State}
      end
  end;

handle_call({commit, Keys, Pid, SnapshotId}, _From, State) ->
  clean_uncommited_keys(Pid, Keys),
  lists:foreach(
    fun(Key) -> ets:insert(?commited_keys, {Key, SnapshotId}) end,
    Keys
  ),
  {reply, ok, State};

handle_call({abort, Pid, Keys}, _From, State) ->
  clean_uncommited_keys(Pid, Keys),
  {reply, ok, State}.

handle_cast({abort, Pid, Keys}, State) ->
  clean_uncommited_keys(Pid, Keys),
  {noreply, State};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
  NewState = process_is_down(Pid, State),
  {noreply, NewState}.


is_commited_after_snapshot(Key, SnapshotId) ->
  case ets:lookup(?commited_keys, Key) of
    [] -> false;
    [{_, Id}] when Id < SnapshotId -> false;
    _ -> true
  end.

is_uncommited(Key, Pid) ->
  case ets:lookup(?uncommited_keys, Key) of
    [] -> true;
    [{_, OtherPid}] when OtherPid =/= Pid -> false;
    _Else -> true
  end.

clean_uncommited_keys(Pid, Keys) ->
  lists:foreach(
    fun(Key) ->
      ets:delete(?uncommited_keys, Key),
      ets:delete(?uncommited_keys, {Pid, Key})
    end,
    Keys
  ).

maybe_monitor(Pid, #{ monitors := Monitors } = State) ->
  case maps:is_key(Pid, Monitors) of
    true -> State;
    false ->
      MRef = erlang:monitor(process, Pid),
      Monitors2 = Monitors#{ Pid => MRef },
      State#{ monitors => Monitors2 }
  end.
  
process_is_down(Pid, #{ monitors := Monitors} = State) ->
  case maps:take(Pid, Monitors) of
    {_, Monitors2} ->
      MS = ets:fun2ms(fun({{P, K}, _}) when P =:= Pid -> K end),
      Keys = ets:select(?uncommited_keys, MS),
      lists:foreach(
        fun(Key) ->
          ets:delete(?uncommited_keys, [Key, {Pid, Key}])
        end,
        Keys
      ),
    
      State#{ monitors => Monitors2 };
    error ->
      State
  end.

