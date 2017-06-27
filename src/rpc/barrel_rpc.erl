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
  await/2, await/3
]).



%% API
-export([
  find_service/1,
  load_services/1
]).

-define(CLOSE_TIMEOUT, 3000).

-define(TIMEOUT, 5000).


-spec start_channel(map()) -> {ok, pid()}.
start_channel(Params) ->
  {ok, _Sup, Connection} = barrel_channel_sup_sup:start_connection_sup(Params),
  barrel_channel:connect(Connection).


-spec close_channel(pid()) -> ok.
close_channel(ChPid) -> close_channel(ChPid, ?CLOSE_TIMEOUT).

-spec close_channel(pid(), non_neg_integer()) -> ok.
close_channel(ChPid, Timeout) ->
  barrel_channel:close(ChPid, Timeout).

request(ChPid, Req) -> request(ChPid, Req, []).

request(ChPid, {_Service, _Method, _Args}=Req, Options) ->
  StreamRef = make_ref(),
  ReplyTo = proplists:get_value(reply_to, Options, self()),
  ChPid ! {request, StreamRef, ReplyTo, Req},
  StreamRef.

await(ChPid, StreamRef) -> await(ChPid, StreamRef, ?TIMEOUT).

await(ChPid, StreamRef, Timeout) ->
  MRef = erlang:monitor(process, ChPid),
  Res = await1(MRef, ChPid, StreamRef, Timeout),
  erlang:demonitor(MRef, [flush]),
  Res.

await1(MRef, ChPid, StreamRef, Timeout) ->
  receive
    {barrel_rpc_response, StreamRef, Resp} -> Resp;
    {rpc_end_stream, StreamRef} -> done;
    {'DOWN', MRef, process, ChPid, Reason} ->
      {error, Reason};
    Other ->
      lager:error(
        "~s: unknown stream message, ~p~n",
        [?MODULE_STRING, Other]
      )
  after Timeout ->
    {error, rpc_timeout}
  end.


find_service(Service) ->
  barrel_rpc_service:find_service(barrel_lib:to_atom(Service)).

%% TODO: validate mapping
load_services(Mapping) ->
  barrel_rpc_service:load_services(Mapping).

