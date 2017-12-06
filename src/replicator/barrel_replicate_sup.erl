%%%-------------------------------------------------------------------
%% @doc barrel_replicate top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(barrel_replicate_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
  Specs = [ %%  replication manager to store replication tasks
            #{id => barrel_replicate,
              start => {barrel_replicate, start_link, []},
              restart => permanent,
              shutdown => 2000,
              type => worker,
              modules => [barrel_replicate]},
            %% monitor replication nodes to pause the replication if needed
            #{id => monitor,
              start => {barrel_replicate_monitor, start_link, []},
              restart => permanent,
              shutdown => 2000,
              type => worker,
              modules => [barrel_replicator_monitor]
            },
            %% tasks supervisor
            #{id => barrel_replicate_task_sup,
              start => {barrel_replicate_task_sup, start_link, []},
              restart => permanent,
              shutdown => 2000,
              type => supervisor,
              modules => [barrel_replicate_task_sup]}
          ],

    {ok, {{one_for_all, 0, 1}, Specs}}.

%%====================================================================
%% Internal functions
%%====================================================================
