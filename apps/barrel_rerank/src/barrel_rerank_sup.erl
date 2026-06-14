%%%-------------------------------------------------------------------
%%% @doc barrel_rerank top-level supervisor
%%%
%%% Simple supervisor with no children. The rerank server is started
%%% on-demand by users calling barrel_rerank:start_link/1.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rerank_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.
