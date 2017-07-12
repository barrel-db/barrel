%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jun 2017 07:29
%%%-------------------------------------------------------------------
-module(barrel_server_channel).
-author("benoitc").

%% API
-export([start_link/2]).


%% gen_statem callbacks
-export([
  init/1,
  callback_mode/0,
  terminate/3,
  code_change/4
]).


%% gen_statem states

-export([
  wait_request/3
]).

%% internal
-export([
  worker/5
]).


start_link(ReaderPid, WriterPid) ->
  gen_statem:start_link(?MODULE, [ReaderPid, WriterPid], []).


%% keep stream information in a record so we can easily find
%% it in a list
-record(stream, {
  id,
  worker,
  monitor,
  service,
  method
}).

init([ReaderPid, WriterPid]) ->
  Data = #{
    reader => ReaderPid,
    writer => WriterPid,
    streams => []
  },
  {ok, wait_request, Data}.

callback_mode() -> state_functions.

terminate(_Reason, _State, _Data) ->
  _ = lager:info("~s: ~p terminated.", [?MODULE_STRING, self()]),
  ok.

code_change(_OldVsn, State, Data, _Extra) -> {ok, State, Data}.

wait_request(info, {rpc, StreamId, {_Service, _Method, _Args}=Call} , Data) ->
  handle_request(StreamId, Call, Data);
wait_request(info, {stream, StreamId, Msg}, Data) ->
  ok = handle_message(StreamId, Msg, Data),
  {keep_state, Data};
wait_request(EventType, Event, Data) ->
  handle_event(EventType, wait_request, Event, Data).

%% ==============================
%% internal handlers

handle_request(StreamId, {Service, Method, Args}, Data) ->
  case barrel_rpc:find_service(Service) of
    not_found ->
      _ = send_response(StreamId, {error, {service_not_found, Service}}, Data),
      {keep_state, Data};
    Mod ->
      %% TODO: maybe we should just link instead of monitoring the workers?
      WorkerPid = spawn_worker(StreamId, Mod, Method, Args, Data),
      Data1 = add_stream(StreamId, WorkerPid, Service, Method, Data),
      {keep_state, Data1}
  end.

handle_message(StreamId, Msg, Data) ->
  case get_worker(StreamId, Data) of
    {ok, WorkerPid} ->
      WorkerPid ! {rpc_stream, StreamId, Msg},
      ok;
    false ->
      ok
  end.

handle_event(info, State, {rst_stream, StreamId, Reason}, Data) ->
  {_Stream, Data1} = kill_stream(StreamId, Data),
  _ = lager:info(
    "~s: ~p, stream ~p exited remotely with error: ~p~n",
    [?MODULE_STRING, State, StreamId, Reason]
  ),
  {keep_state, Data1};
handle_event(info, _State, {'DOWN', _Ref, process, Pid, normal}, Data) ->
  {_Stream, Data1} = remove_worker(Pid, Data),
  {keep_state, Data1};
handle_event(info, State, {'DOWN', _Ref, process, Pid, Reason}, Data) ->
  {Stream, Data1} = remove_worker(Pid, Data),
  #stream{id = StreamId,
          service = Service,
          method = Method} = Stream,
  %% tell the client about the error so it can close the stream
  _ = send_response(StreamId, {error, {service_error, Reason}}, Data),
  _ = lager:error(
    "~s: ~p, stream ~p (~p:~p) error: ~p~n",
    [?MODULE_STRING, State, StreamId, Service, Method, Reason]
  ),
  {keep_state, Data1};
handle_event(_EventType, State, Event, Data) ->
  error_logger:error_msg(
    "~s: ~p, unknown event: ~p~n",
    [?MODULE_STRING, State, Event]
  ),
  {stop, unknown_event, Data}.

%% ==============================
%% internal helpers

spawn_worker(StreamId, Mod, Method, Args, Data) ->
  spawn(?MODULE, worker, [StreamId, Mod, Method, Args, Data]).

worker(StreamId, Mod, Method, Args, Data = #{ writer := Writer }) ->
  case Mod:execute(new_context(StreamId), Writer, Method, Args) of
    ignore -> ok;
    Result -> send_response(StreamId, Result, Data)
  end.

send_response(StreamId, Msg, #{ writer := Writer}) ->
  Msg1 = {rpc_response, StreamId, Msg},
  Writer ! Msg1.

add_stream(StreamId, WorkerPid, Service, Method, Data = #{ streams := Streams }) ->
  MRef = erlang:monitor(process, WorkerPid),
  Stream = #stream{id = StreamId,
                   worker = WorkerPid,
                   monitor = MRef,
                   service = Service,
                   method =  Method},
  Data1 = Data#{ streams => [Stream | Streams ]},
  Data1.

get_worker(StreamId, #{ streams := Streams }) ->
  case lists:keyfind(StreamId, #stream.id, Streams) of
    #stream{ worker = WorkerPid } -> {ok, WorkerPid};
    false -> false
  end.

remove_worker(WorkerPid, Data = #{ streams := Streams }) ->
  case lists:keytake(WorkerPid, #stream.worker, Streams) of
    false -> {undefined, Data};
    {value, Stream, NStreams} ->
      {Stream, Data#{ streams => NStreams }}
  end.

new_context(StreamId) ->
  #{ stream_id => StreamId, server => self() }.

kill_stream(StreamId, Data = #{ streams := Streams }) ->
  case lists:keytake(StreamId, #stream.id, Streams) of
    false -> {undefined, Data};
    {value, Stream, NStreams} ->
      Data2 = Data#{ streams => NStreams},
      #stream{ worker = Pid, monitor = MRef} = Stream,
      %% kill stream worker
      try
          catch exit(Pid, kill),
          receive
            {'DOWN', MRef, _, _, _} -> ok
          end
      after
        erlang:demonitor(MRef, [flush])
      end,
      {Stream, Data2}
  end.