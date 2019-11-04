%%%-------------------------------------------------------------------
%% @doc barrel_rocksdb public API
%% @end
%%%-------------------------------------------------------------------

-module(barrel_rocksdb_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    barrel_rocksdb_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
