%% @doc Supervisor for live BQL queries: one temporary worker per
%% SUBSCRIBE statement.
-module(barrel_bql_live_sup).
-behaviour(supervisor).

-export([start_link/0, start_child/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child(map()) -> {ok, pid()} | {error, term()}.
start_child(Args) ->
    supervisor:start_child(?MODULE, [Args]).

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 5,
        period => 60
    },
    Child = #{
        id => barrel_bql_live,
        start => {barrel_bql_live, start_link, []},
        restart => temporary,
        shutdown => 5000
    },
    {ok, {SupFlags, [Child]}}.
