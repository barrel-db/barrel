%%%-------------------------------------------------------------------
%%% @doc Per-database supervisor: restarts a crashed writer with the
%%% same name and config (3 restarts in 60 s), and shuts down with it
%%% when the writer stops normally or the restart budget runs out.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_db_instance_sup).

-behaviour(supervisor).

-export([start_link/2, writer/1]).
-export([init/1]).

-spec start_link(binary(), map()) -> {ok, pid()} | {error, term()}.
start_link(Name, Config) ->
    supervisor:start_link(?MODULE, [Name, Config]).

%% @doc The running writer of this database supervisor, if any.
-spec writer(pid()) -> {ok, pid()} | {error, not_found}.
writer(Sup) ->
    try supervisor:which_children(Sup) of
        [{barrel_db_server, Pid, _, _}] when is_pid(Pid) -> {ok, Pid};
        _ -> {error, not_found}
    catch
        exit:_ -> {error, not_found}
    end.

init([Name, Config]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 3,
                 period => 60,
                 auto_shutdown => any_significant},
    Writer = #{id => barrel_db_server,
               start => {barrel_db_server, start_link, [Name, Config, self()]},
               restart => transient,
               significant => true,
               shutdown => 5000,
               type => worker,
               modules => [barrel_db_server]},
    {ok, {SupFlags, [Writer]}}.
