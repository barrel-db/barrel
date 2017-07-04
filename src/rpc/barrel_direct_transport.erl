%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jun 2017 08:09
%%%-------------------------------------------------------------------

%% @sdoc connect to a remote database

-module(barrel_direct_transport).
-author("benoitc").
-behaviour(barrel_rpc_transport).

%% API
-export([
  init/0,
  connect/3,
  terminate/2,
  handle_message/2,
  get_writer/1,
  channel_id/1
]).


channel_id(Params) -> maps:get(node, Params).

init() -> {ok, #{}}.

connect(Params, _TypeSup, _State) ->
  #{ node :=  Node} = Params,
  State1 = #{
    node => Node,
    params => Params,
    connected_at => erlang:monotonic_time()
  },
  
  case
    rpc:call(Node, barrel_direct, connect, [#{}, self()])
  of
    {ok, ChannelPid} ->
      erlang:monitor(process, ChannelPid),
      State2 = State1#{ channel_pid => ChannelPid },
      {ok, State2};
    {error, _} = E ->
      E;
    {badrpc, nodedown} ->
      _ = lager:error("~s: badrpc: node ~p down", [?MODULE_STRING, Node]),
      {error, {nodedown, Node}}
  end.

terminate(_Reason, #{ node := Node, channel_pid := ChannelPid }) ->
  _ = rpc:call(Node, barrel_direct, disconnect, [ChannelPid]),
  ok.

handle_message({'DOWN', _MRef, process, ChPid, Reason}, #{ node := Node, channel_pid := ChPid } = State) ->
  _ = lager:info("~s: remote channel on ~p down: ~p", [?MODULE_STRING, Node, Reason]),
  {stop, {server_down, Reason}, State};

handle_message(Msg, State) ->
  {stop, {unexpected_msg, Msg}, State}.


get_writer(#{ channel_pid := ChannelPid }) ->
  ChannelPid.