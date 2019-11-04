%%%-------------------------------------------------------------------
%% @doc barrel_replicator public API
%% @end
%%%-------------------------------------------------------------------

-module(barrel_replicator_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    barrel_replicator_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
