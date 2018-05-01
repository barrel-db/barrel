%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Dec 2017 13:14
%%%-------------------------------------------------------------------
-module(barrel_monitor_sup).
-author("benoitc").
-behaviour(supervisor).

%% API
-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).


init([]) ->
  {ok, MemoryWatermark} = application:get_env(vm_memory_high_watermark),
  {ok, DiskLimit} = application:get_env(disk_free_limit),

  Specs = [
    #{id => monitor_disk,
      start => {barrel_disk_monitor, start_link, [DiskLimit]},
      restart => permanent,
      shutdown => infinity,
      type => worker,
      modules => [barrel_disk_monitor]
    },

    #{id => monitor_memory,
      start => {barrel_vm_memory_monitor, start_link, [MemoryWatermark]},
      restart => permanent,
      shutdown => infinity,
      type => worker,
      modules => [barrel_vm_memory_monitor]
    }
  ],
  SupFlags = #{ strategy => one_for_all, intensity => 0, period =>1 },
  {ok, {SupFlags, Specs}}.
