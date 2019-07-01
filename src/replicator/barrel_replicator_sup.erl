%%%-------------------------------------------------------------------
%% @doc barrel_replicator top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(barrel_replicator_sup).

-behaviour(supervisor).



%% API
-export([start_replication/3,
         stop_replication/2]).

-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).


start_replication(Name, Endpoint, Config) ->
  supervisor:start_child(?SERVER, replication_spec(Name, Endpoint, Config)).

stop_replication(Name, #{ id := Id }) ->
  case supervisor:terminate_child(?SERVER, {Id, Name}) of
    ok ->
      supervisor:delete_child(?SERVER, {Id, Name});
    Error ->
      Error
  end.


replication_spec(Name, #{ id := Id} = Endpoint, Config) ->
  #{ id => {Id, Name},
     start => {barrel_replication_sup, start_link, [Name, Endpoint, Config]},
     type => supervisor }.


start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).


init([]) ->
  SupFlags = #{ strategy => one_for_one  },
  {ok, {SupFlags, []}}.

