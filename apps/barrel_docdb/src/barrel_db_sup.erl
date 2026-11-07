%%%-------------------------------------------------------------------
%%% @doc barrel_db_sup - Dynamic supervisor for database processes
%%%
%%% Starts one barrel_db_instance_sup per database (simple_one_for_one);
%%% that supervisor restarts the database's writer.
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

%% @doc Start a new database under its own supervisor; returns the
%% writer pid.
-spec start_db(binary(), map()) -> {ok, pid()} | {error, term()}.
start_db(Name, Config) ->
    case supervisor:start_child(?SERVER, [Name, Config]) of
        {ok, Sup} ->
            barrel_db_instance_sup:writer(Sup);
        {error, {shutdown, {failed_to_start_child, barrel_db_server, Reason}}} ->
            {error, Reason};
        {error, _} = Err ->
            Err
    end.

%% @doc Stop a database server
-spec stop_db(binary()) -> ok | {error, term()}.
stop_db(Name) ->
    case whereis_db(Name) of
        undefined ->
            {error, not_found};
        Pid ->
            barrel_db_server:stop(Pid)
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

    %% One supervisor per database: a crash-looping database never
    %% counts against its siblings.
    DbInstance = #{
        id => barrel_db_instance_sup,
        start => {barrel_db_instance_sup, start_link, []},
        restart => temporary,
        shutdown => infinity,
        type => supervisor,
        modules => [barrel_db_instance_sup]
    },

    {ok, {SupFlags, [DbInstance]}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Find database server by name
-spec whereis_db(binary()) -> pid() | undefined.
whereis_db(Name) ->
    case barrel_docdb:db_pid(Name) of
        {ok, Pid} -> Pid;
        {error, _} -> undefined
    end.
