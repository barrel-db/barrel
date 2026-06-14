%%%-------------------------------------------------------------------
%%% @doc barrel_embed application callback module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_app).

-behaviour(application).

-export([start/2, stop/1]).

%%====================================================================
%% Application callbacks
%%====================================================================

start(_StartType, _StartArgs) ->
    %% Ensure managed venv exists
    case barrel_embed_venv:ensure_venv() of
        {ok, VenvPath} ->
            error_logger:info_msg("barrel_embed: using venv at ~s~n", [VenvPath]);
        {error, Reason} ->
            error_logger:warning_msg(
                "barrel_embed: failed to create managed venv: ~p~n"
                "Providers will need explicit venv config~n",
                [Reason]
            )
    end,
    barrel_embed_sup:start_link().

stop(_State) ->
    ok.
