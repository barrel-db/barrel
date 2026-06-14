%%%-------------------------------------------------------------------
%%% @doc barrel_rerank application module
%%%
%%% This module implements the OTP application behaviour for barrel_rerank.
%%% It starts the top-level supervisor and initializes the managed Python
%%% virtual environment for the rerank server.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rerank_app).
-behaviour(application).

-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the barrel_rerank application.
%% @private
start(_StartType, _StartArgs) ->
    %% Initialize managed Python venv for rerank server
    case barrel_rerank_venv:ensure_venv() of
        {ok, VenvPath} ->
            error_logger:info_msg("barrel_rerank: using venv at ~s~n", [VenvPath]);
        {error, Reason} ->
            error_logger:warning_msg("barrel_rerank: venv setup failed: ~p~n", [Reason])
    end,
    barrel_rerank_sup:start_link().

%% @doc Stop the barrel_rerank application.
%% @private
stop(_State) ->
    ok.
