-module(barrel_db_stream_sup).
-author("benoitc").
-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  SupFlags = #{strategy => one_for_one,
               intensity => 0,
               period => 1},
  Specs = [
    %% broker
    #{id => barrel_db_stream_broker,
      start => {barrel_db_stream_broker, start_link, []},
      restart => permanent,
      type => worker,
      shutdown => 5000},
    %% agents
    #{id => barrel_db_stream_agent_sup,
      start => {barrel_db_stream_agent_sup, start_link, []},
      restart => permanent,
      type => supervisor,
      shutdown => 5000},
    %% stream manager
    #{id => barrel_db_stream_mgr,
      start => {barrel_db_stream_mgr, start_link, []},
      restart => permanent,
      type => worker,
      shutdown => 5000}],
  {ok, {SupFlags, Specs}}.