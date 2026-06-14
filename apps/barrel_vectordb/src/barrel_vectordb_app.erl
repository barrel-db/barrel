%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb application module
%%%
%%% This module implements the OTP application behaviour for barrel_vectordb.
%%% It starts the top-level supervisor. barrel_vectordb is an embedded
%%% library; vector stores are started on demand via
%%% {@link barrel_vectordb:start_link/1}.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_app).
-behaviour(application).

-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the barrel_vectordb application.
%% @private
start(_StartType, _StartArgs) ->
    barrel_vectordb_sup:start_link().

%% @doc Stop the barrel_vectordb application.
%% @private
stop(_State) ->
    ok.
