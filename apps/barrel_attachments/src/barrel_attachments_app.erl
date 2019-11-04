%%%-------------------------------------------------------------------
%% @doc barrel_attachments public API
%% @end
%%%-------------------------------------------------------------------

-module(barrel_attachments_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    barrel_attachments_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
