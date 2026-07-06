%%%-------------------------------------------------------------------
%%% @doc Application entry point for the barrel agent layer.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_spaces_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    barrel_spaces_sup:start_link().

stop(_State) ->
    ok.
