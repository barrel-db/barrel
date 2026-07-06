%%%-------------------------------------------------------------------
%%% @doc Supervisor for record-mode indexers: one transient
%%% barrel_record_indexer child per record-mode database, started when
%%% the database opens and stopped when it closes. A crashed indexer
%%% restarts with the same config and re-drives pending outbox entries
%%% (processing is idempotent).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_record_sup).
-behaviour(supervisor).

-export([start_link/0, start_indexer/1, stop_indexer/1]).
-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start an indexer for a record-mode database.
%% Config: #{name, db, vstore, policy, embed} (see barrel_record_indexer).
-spec start_indexer(map()) -> {ok, pid()} | {error, term()}.
start_indexer(Config) ->
    supervisor:start_child(?SERVER, [Config]).

%% @doc Stop the indexer of a database, if running.
-spec stop_indexer(atom() | binary()) -> ok.
stop_indexer(Name) ->
    case barrel_record_indexer:whereis_pid(Name) of
        undefined -> ok;
        Pid -> supervisor:terminate_child(?SERVER, Pid)
    end.

%% @private
init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 60
    },
    Child = #{
        id => barrel_record_indexer,
        start => {barrel_record_indexer, start_link, []},
        restart => transient,
        shutdown => 5000
    },
    {ok, {SupFlags, [Child]}}.
