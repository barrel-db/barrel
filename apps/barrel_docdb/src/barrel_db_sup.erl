%%%-------------------------------------------------------------------
%%% @doc barrel_db_sup - Dynamic supervisor for database processes
%%%
%%% Manages individual database server processes using simple_one_for_one
%%% strategy for dynamic child management.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_db_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([start_db/2, stop_db/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the database supervisor
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a new database server
-spec start_db(binary(), map()) -> {ok, pid()} | {error, term()}.
start_db(Name, Config) ->
    supervisor:start_child(?SERVER, [Name, Config]).

%% @doc Stop a database server
-spec stop_db(binary()) -> ok | {error, term()}.
stop_db(Name) ->
    case whereis_db(Name) of
        undefined ->
            {error, not_found};
        Pid ->
            supervisor:terminate_child(?SERVER, Pid)
    end.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @doc Initialize the supervisor with simple_one_for_one strategy
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 5,
        period => 60
    },

    DbServer = #{
        id => barrel_db_server,
        start => {barrel_db_server, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [barrel_db_server]
    },

    {ok, {SupFlags, [DbServer]}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Find database server by name
-spec whereis_db(binary()) -> pid() | undefined.
whereis_db(Name) ->
    %% Use persistent_term or registry lookup
    %% For now, we use a simple approach - this will be enhanced
    case persistent_term:get({barrel_db, Name}, undefined) of
        undefined -> undefined;
        Pid when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true -> Pid;
                false -> undefined
            end
    end.
