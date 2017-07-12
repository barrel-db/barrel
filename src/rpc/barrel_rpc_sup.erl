%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Jun 2017 04:45
%%%-------------------------------------------------------------------
-module(barrel_rpc_sup).
-author("benoitc").

-behaviour(supervisor).

%% API
-export([start_link/0]).

-export([init/1]).


start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_) ->
  SupClient = sup_child(barrel_channel_sup_sup, start_link, []),
  SupChannel = sup_child(barrel_server_channel_sup, start_link, []),
  Service = worker_child(barrel_rpc_service, start_link, []),
  SupFlags = one_for_one_flags(),
  {ok, {SupFlags, [SupClient, SupChannel, Service]}}.

one_for_one_flags() ->
  #{ strategy => one_for_one, intensity => 4, period => 3600}.

sup_child(M, F, A) ->
  #{
    id => M,
    start => {M, F, A},
    type => supervisor,
    modules => [M]
  }.

worker_child(M, F, A) ->
  #{
    id => M,
    start => {M, F, A},
    restart => permanent,
    type => worker,
    modules => [M]
  }.