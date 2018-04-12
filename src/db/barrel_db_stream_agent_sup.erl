%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Apr 2018 00:25
%%%-------------------------------------------------------------------
-module(barrel_db_stream_agent_sup).
-author("benoitc").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  SupFlags = #{strategy => one_for_one,
               intensity => 5,
               period => 10},
  Specs = [agent_spec(N) || N <- lists:seq(1, num_agents())],
  {ok, {SupFlags, Specs}}.


num_agents() ->
  application:get_env(barrel, num_stream_agents, erlang:system_info(schedulers)).

agent_spec(N) ->
  #{id => {barrel_db_stream_agent, N},
    start => {barrel_db_stream_agent, start_link, []},
    restart => permanent,
    type => worker,
    shutdown => 5000,
    modules => [barrel_db_stream_agent]}.