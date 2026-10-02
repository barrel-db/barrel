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
        strategy => rest_for_one,
        intensity => 10,
        period => 10
    },
    %% the model processes, then the pool that shares them
    ChildSpecs = [
        #{id => barrel_embed_pool_sup,
          start => {barrel_embed_pool_sup, start_link, []},
          type => supervisor},
        #{id => barrel_embed_pool,
          start => {barrel_embed_pool, start_link, []}}
    ],
    {ok, {SupFlags, ChildSpecs}}.
