-module(barrel_channel).
-behaviour(gen_statem).


%% PUBLIC Api
-export([
  start_link/2,
  connect/1,
  close/2
]).

%% Gen server API
-export([
  init/1,
  callback_mode/0,
  terminate/3,
  code_change/4
]).

%% states
-export([
  idle/3,
  connected/3
]).

-include_lib("stdlib/include/ms_transform.hrl").

-define(TIMEOUT, 5000).


%% API

connect(ConnPid) -> gen_statem:call(ConnPid, connect).

close(ConnPid, Timeout) -> gen_statem:stop(ConnPid, normal, Timeout).

%% ==============================
%% internal API

start_link(TypeSup, Params) ->
  gen_statem:start_link(?MODULE, {TypeSup, Params}, []).

%% ==============================
%% gen server API

init({TypeSup, Params}) ->
  process_flag(trap_exit, true),
  {ok, idle, {TypeSup, Params}}.

callback_mode() -> state_functions.

terminate(Reason, _State, #{ mod := Mod, mod_state := ModState }) ->
  Mod:terminate(Reason, ModState);
terminate(_Reason, _State, _Data) ->
  ok.

code_change(_OldVsn, State, Data, _Extra) -> {ok, State, Data}.

idle({call, From}, connect, {TypeSup, Params}) ->
  {Type, Mod} = barrel_channel_transport_sup:type_mod(Params),
  {ok, ModState} = Mod:init(),
  %% initialize the data
  Data = #{
    mod => Mod,
    mod_state => ModState,
    type => Type,
    streams => [],
    last_id => 0
  },
  %% dp the connection
  %% TODO: handle retry logic
  case Mod:connect(Params, TypeSup, ModState) of
    {ok, NewModState} ->
      %% TODO: revisit the way we handle streams ?
      Tid = ets:new(?MODULE, [set]),
      Data1 = Data#{ mod_state => NewModState, tab => Tid},
      Reply = {ok, self()},
      {next_state, connected, Data1, [{reply, From, Reply}]};
    Error ->
      {stop, {shutdown, Error}, Data}
  end;
idle(EventType, Event, Data) ->
  handle_event(EventType, idle, Event, Data).


connected(info, {request, StreamRef, Pid, Req}, Data = #{ last_id := Id } ) ->
  %% maybe monitor this stream
  ok = maybe_monitor(Pid, Data),
  %% prepare for HTTP2 support, use an even number on client side
  StreamId = Id + 2,
  ok = add_stream(StreamId, StreamRef, Pid, Data),
  Writer = get_writer(Data),
  _ = Writer ! {rpc, StreamId, Req},
  {keep_state, Data#{ last_id => StreamId }};
connected(info, {stream, StreamRef, ReqData}, Data) ->
  case stream_for_client(StreamRef, Data) of
    {ok, StreamId} ->
      Writer = get_writer(Data),
      _ = Writer ! {stream, StreamId, ReqData},
      {keep_state, Data};
    not_found ->
      {keep_state, Data}
  end;
connected(info, {rpc_stream, StreamId, Resp}, Data) ->
  ok = handle_response_stream(StreamId, Resp, Data),
  {keep_state, Data};
connected(info, {rpc_response, StreamId, Resp}, Data) ->
  ok = handle_response(StreamId, Resp, Data),
  {keep_state, Data};
connected(info, {'DOWN', _MRef, process, Pid, _Reason}=Info, Data) ->
  case client_is_down(Pid, Data) of
    true ->
      {keep_state, Data};
    false ->
      handle_message(Info, Data)
  end;
connected(EventType, Event, Data) ->
  handle_event(EventType, connected, Event, Data).


handle_event({call, From}, State, get_state, Data) ->
  {keep_state, Data, [{reply, From, State}]};
handle_event(info, _State, Info, Data) ->
  handle_message(Info, Data);
handle_event(_EventType, State, Event, Data) ->
  error_logger:error_msg(
    "~s: ~p, unknown event: ~p~n",
    [?MODULE_STRING, State, Event]
  ),
  {stop, unknown_event, Data}.


handle_message(Info, Data) ->
  #{ mod := Mod, mod_state := ModState } = Data,
  case Mod:handle_message(Info, ModState) of
    {ok, NewModState} ->
      {keep_state, Data#{ mod_state => NewModState }};
    {stop, Reason, NewModState} ->
      {stop, Reason, Data#{ mod_state => NewModState }}
  end.
%% ==============================
%% internals

maybe_monitor(Pid, #{ tab := Tab }) ->
  case ets:insert_new(Tab, {Pid, m}) of
    true ->
      _ = erlang:monitor(process, Pid),
      ok;
    false ->
      ok
  end.

add_stream(StreamId, StreamRef, Pid, #{ tab := Tab }) ->
  true = ets:insert(Tab, {StreamId, {StreamRef, Pid}}),
  true = ets:insert(Tab, {StreamRef, StreamId}),
  true = ets:insert(Tab, {{Pid, StreamId}, StreamRef}),
  ok.

client_for_stream(StreamId, #{ tab := Tab }) ->
  case ets:lookup(Tab, StreamId) of
    [] -> not_found;
    [{StreamId, {StreamRef, ClientPid}}] -> {ok, StreamRef, ClientPid}
  end.

stream_for_client(StreamRef, #{ tab := Tab }) ->
  case ets:lookup(Tab, StreamRef) of
    [] -> not_found;
    [{StreamRef, StreamId}] -> {ok, StreamId}
  end.

get_writer(#{ mod := Mod, mod_state := ModState}) ->
  Mod:get_writer(ModState).

delete_stream(StreamId, StreamRef, Pid, #{ tab := Tab }) ->
  _ = ets:delete(Tab, StreamId),
  _ = ets:delete(Tab, StreamRef),
  _ = ets:delete(Tab, {Pid, StreamId}),
  ok.

client_is_down(Pid, Data = #{ tab := Tab }) ->
  case ets:take(Tab, Pid) of
    [] -> false;
    _ ->
      MatchSpec = ets:fun2ms(fun({{P, S}, R}) when P =:= Pid -> {P, S, R} end),
      AllStreams = ets:select(Tab, MatchSpec),
      lists:foreach(
        fun({Pid1, StreamId, StreamRef}) ->
          delete_stream(StreamId, StreamRef, Pid1, Data)
        end,
        AllStreams),
      true
  end.

handle_response(StreamId, Resp, Data) ->
  case client_for_stream(StreamId, Data) of
    {ok, StreamRef, ClientPid} ->
      ClientPid ! {barrel_rpc_response, StreamRef, Resp},
      delete_stream(StreamId, StreamRef, ClientPid, Data);
    not_found ->
      ok
  end.

handle_response_stream(StreamId, Resp, Data) ->
  case client_for_stream(StreamId, Data) of
    {ok, StreamRef, ClientPid} ->
      ClientPid ! {barrel_rpc_stream, StreamRef, Resp},
      ok;
    not_found ->
      ok
  end.


