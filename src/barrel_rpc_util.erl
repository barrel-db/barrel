%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Jun 2018 00:55
%%%-------------------------------------------------------------------
-module(barrel_rpc_util).
-author("benoitc").

%% API
-export([
  send/2
]).


%% @doc send a message as quickly as possible
send(Dest, Msg) ->
  case erlang:send(Dest, Msg, [noconnect, nosuspend]) of
    ok ->
      ok;
    _ ->
      % treat nosuspend and noconnect the same
      barrel_rpc_buffer:send(Dest, Msg)
  end.
