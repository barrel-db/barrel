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

-define(active_snapshots, barrel_tx_active_snapshots).
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
      Batch = [{Key, SnapshotId} || Key <- Keys],
      ok = gen_server:call(?MODULE, {commit, self(), Batch, SnapshotId}, Timeout),
      _ = erlang:erase(barrel_transaction),
      ok
  end.

new_transaction() ->
  SnapshotId = persistent_term:get({?MODULE, snapshot_id}),
  Id = atomics:add_get(SnapshotId, 1, 1),
  Id.


clean_transaction() ->
  case erlang:erase(barrel_transaction) of
    undefined ->
      ok;
    #{ written_keys := _Keys } ->
      _ = clean_uncommited_keys(self()),
      _ = erlang:erase(barrel_transaction),
      ok
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
          ets:insert(?uncommited_keys, [{Key, Pid},  {{Pid, Key}, Key}]),
          NewState = maybe_monitor(Pid, State),
          {reply, true, NewState};
        false ->
          {reply, false, State}
      end
  end;

handle_call({commit, Pid, Batch, _SnapshotId}, _From, State) ->
  _  = clean_uncommited_keys(Pid),
  _  = ets:insert(?commited_keys, Batch),
  {reply, ok, State};

handle_call({abort, Pid}, _From, State) ->
  _  = clean_uncommited_keys(Pid),
  {reply, ok, State}.

handle_cast({abort, Pid}, State) ->
  _ = clean_uncommited_keys(Pid),
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

%% TODO: since only one process can do a transaction at a time we may not need to ckeck
%% who's the owner of the transaction but only if one is already running?
is_uncommited(Key, Pid) ->
  case ets:lookup(?uncommited_keys, Key) of
    [] -> true;
    [{_, OtherPid}] when OtherPid =/= Pid -> false;
    _Else -> true
  end.

-spec clean_uncommited_keys(pid()) -> non_neg_integer().
clean_uncommited_keys(Pid) ->
  MS = ets:fun2ms(fun
                    ({{P, _K}, _}) when P =:= Pid -> true;
                    ({_, P}) when P =:= Pid -> true
                  end),
  ets:select_delete(?uncommited_keys, MS).

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
      _ = clean_uncommited_keys(Pid),
      State#{ monitors => Monitors2 };
    error ->
      State
  end.

