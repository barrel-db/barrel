%%%-------------------------------------------------------------------
%%% @doc Application entry point for barrel_att_s3.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_s3_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    barrel_att_s3_sup:start_link().

stop(_State) ->
    ok.
