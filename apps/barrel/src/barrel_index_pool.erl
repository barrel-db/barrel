%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 18. Jul 2018 14:20
%%%-------------------------------------------------------------------
-module(barrel_index_pool).
-author("benoitc").

%% API
-export([
  start_link/0
]).


start_link() ->
  PoolSize = application:get_env(barrel, index_workers, 4),
  PoolOptions = [
    {overrun_warning, 5000},
    {overrun_handler, {barrel_lib, report_overrun}},
    {workers, PoolSize},
    {worker, {barrel_index_worker, []}}
  ],
  wpool:start_pool(?MODULE, PoolOptions).
