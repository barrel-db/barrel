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
    %% Managed venv bootstrap (opt out with managed_venv => false)
    case barrel_embed_venv:bootstrap() of
        {ok, VenvPath} ->
            error_logger:info_msg("barrel_embed: using venv at ~s~n", [VenvPath]);
        skipped ->
            error_logger:info_msg(
                "barrel_embed: managed venv disabled by config~n");
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
