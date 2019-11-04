%%%-------------------------------------------------------------------
%% @doc barrel_docdb public API
%% @end
%%%-------------------------------------------------------------------

-module(barrel_docdb_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    barrel_docdb_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
