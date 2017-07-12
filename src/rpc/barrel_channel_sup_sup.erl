%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Jun 2017 10:24
%%%-------------------------------------------------------------------
-module(barrel_channel_sup_sup).
-author("benoitc").
-behavior(supervisor).

%% API
-export([
  start_link/0,
  start_connection_sup/1
]).

%% supervisor callbacks
-export([init/1]).

%% API

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_connection_sup(Params) ->
  supervisor:start_child(?MODULE, [Params]).

%% Supervisor API

init([]) ->
  ChildSpec = #{
    id => channel_sup,
    start => {barrel_channel_sup, start_link, []},
    restart => temporary,
    shutdown => infinity,
    type => supervisor,
    modules => [barrel_channel_sup]
  },
  SupFlags = #{ strategy => simple_one_for_one, intensity => 0, period => 1},
  {ok, {SupFlags, [ChildSpec]}}.
