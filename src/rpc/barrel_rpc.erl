%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Jun 2017 04:33
%%%-------------------------------------------------------------------
-module(barrel_rpc).
-author("benoitc").

-export([
  start_channel/1,
  close_channel/1, close_channel/2,
  request/2, request/3,
  await/2, await/3,
  stream/3,
  end_stream/2,
  response_stream/3,
  response_end_stream/2
]).


%% API
-export([
  find_service/1,
  load_services/1
]).

-define(CLOSE_TIMEOUT, 3000).

-define(TIMEOUT, 5000).

-type channel() :: pid() | term().

-export_types([channel/0]).


-spec start_channel(map()) -> {ok, pid()}.
start_channel(Params) ->
  {ok, _Sup, Connection} = barrel_channel_sup_sup:start_connection_sup(Params),
  barrel_channel:connect(Connection).


-spec close_channel(channel()) -> ok.
close_channel(Ch) -> close_channel(Ch, ?CLOSE_TIMEOUT).

-spec close_channel(channel(), non_neg_integer()) -> ok.
close_channel(Ch, Timeout) ->
  barrel_channel:close(barrel_channel:channel_pid(Ch), Timeout).

request(Ch, Req) -> request(Ch, Req, []).

request(Ch, {_Service, _Method, _Args}=Req, Options) ->
  with_channel(
    Ch,
    fun(ChPid) ->
      StreamRef = make_ref(),
      ReplyTo = proplists:get_value(reply_to, Options, self()),
      ChPid ! {request, StreamRef, ReplyTo, Req},
      StreamRef
    end
  ).

await(Ch, StreamRef) -> await(Ch, StreamRef, ?TIMEOUT).

await(Ch, StreamRef, Timeout) ->
  with_channel(
    Ch,
    fun(ChPid) ->
      MRef = erlang:monitor(process, ChPid),
      Res = await1(MRef, ChPid, StreamRef, Timeout),
      erlang:demonitor(MRef, [flush]),
      Res
    end
  ).

await1(MRef, ChPid, StreamRef, Timeout) ->
  receive
    {rpc_response, StreamRef, Resp} -> Resp;
    {rpc_stream, StreamRef, Resp} -> Resp;
    {'DOWN', MRef, process, ChPid, Reason} -> {error, Reason}
  after Timeout ->
    {error, rpc_timeout}
  end.

stream(Ch, StreamRef, Data) ->
  _ = send_channel(Ch, {stream, StreamRef, {data, Data}}),
  ok.

end_stream(Ch, StreamRef) ->
  _ = send_channel(Ch, {stream, StreamRef, end_stream}),
  ok.

response_stream(Writer, StreamId, Data) ->
  _ = Writer ! {rpc_stream, StreamId, {data, Data}},
  ok.

response_end_stream(Writer, StreamId) ->
  _ = Writer ! {rpc_stream, StreamId, end_stream},
  ignore.
  

find_service(Service) ->
  barrel_rpc_service:find_service(barrel_lib:to_atom(Service)).

%% TODO: validate mapping
load_services(Mapping) ->
  barrel_rpc_service:load_services(Mapping).


with_channel(Channel, Fun) ->
  case barrel_channel:channel_pid(Channel) of
    undefined -> erlang:error(badarg);
    ChPid -> Fun(ChPid)
  end.

send_channel(Ch, Msg) ->
  with_channel(
    Ch,
    fun(ChPid) ->
      ChPid ! Msg
    end
  ).