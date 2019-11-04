%%%-------------------------------------------------------------------
%% @doc barrel_view public API
%% @end
%%%-------------------------------------------------------------------

-module(barrel_view_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    barrel_view_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
