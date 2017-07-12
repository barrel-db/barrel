%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Jun 2017 15:06
%%%-------------------------------------------------------------------
-module(barrel_direct).
-author("benoitc").

%% API
-export([
  connect/2,
  disconnect/1
]).

%% TODO: add credential and connection limitation support
connect(_Creds, Pid) ->
  _ = lager:info("~s: ~p connected.", [?MODULE_STRING, Pid]),
  barrel_server_channel_sup:start_channel(Pid, Pid).

disconnect(Pid) ->
  gen_statem:stop(Pid).