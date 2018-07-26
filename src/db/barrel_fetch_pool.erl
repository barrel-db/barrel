%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Jul 2018 21:31
%%%-------------------------------------------------------------------
-module(barrel_fetch_pool).
-author("benoitc").

%% API
-export([
  start_link/0
]).


start_link() ->
  PoolSize = application:get_env(barrel, fetch_workers, 4),
  PoolOptions = [
                  {overrun_warning, 5000},
                  {overrun_handler, {barrel_lib, report_overrun}},
                  {workers, PoolSize},
                  {worker, {barrel_fetch_worker, []}}
                ],
  wpool:start_pool(?MODULE, PoolOptions).

