%%%-------------------------------------------------------------------
%%% @doc Application entry point for the barrel server.
%%%
%%% Maps server config onto the underlying apps (data_dir), then
%%% starts the supervision tree: the HTTP service
%%% ({@link barrel_server_http}). All request handlers call the `barrel'
%%% facade only; open handles live in the facade's {@link barrel_dbs}
%%% manager, reached through the {@link barrel_server_dbs} shim.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_app).
-behaviour(application).

-export([start/2, prep_stop/1, stop/1]).

start(_Type, _Args) ->
    case application:get_env(barrel_server, data_dir) of
        {ok, Dir} -> application:set_env(barrel_docdb, data_dir, Dir);
        undefined -> ok
    end,
    barrel_server_sup:start_link().

prep_stop(State) ->
    %% close exactly the databases this server opened; other barrel_dbs
    %% tenants (embedded users, the agent layer) are untouched
    try barrel_dbs:close_owned(barrel_server)
    catch _:_ -> ok
    end,
    State.

stop(_State) ->
    ok.
