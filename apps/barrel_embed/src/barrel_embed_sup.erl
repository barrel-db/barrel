%%%-------------------------------------------------------------------
%%% @doc barrel_embed top level supervisor
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    %% No child processes - just ETS tables managed by python_queue
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.
