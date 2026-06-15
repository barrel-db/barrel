%%%-------------------------------------------------------------------
%%% @doc Application entry point for the barrel server.
%%%
%%% Starts the supervision tree: a database lifecycle manager
%%% ({@link barrel_server_dbs}) and the HTTP service
%%% ({@link barrel_server_http}). All request handlers call the `barrel'
%%% facade only; this app holds no database logic of its own.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    barrel_server_sup:start_link().

stop(_State) ->
    ok.
