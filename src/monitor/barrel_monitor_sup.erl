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
  {ok, Sup} = supervisor:start_link({local, ?SERVER}, ?MODULE, []),
  {ok, MemoryWatermark} = application:get_env(vm_memory_high_watermark),
  ok = start_restartable_child( barrel_vm_memory_monitor, [MemoryWatermark]),
  {ok, DiskLimit} = application:get_env(disk_free_limit),
  ok = start_delayed_restartable_child(barrel_disk_monitor, [DiskLimit]),
  {ok, Sup}.


init([]) ->
  SupFlags = #{ strategy => one_for_all, intensity => 0, period =>1 },
  {ok, {SupFlags, []}}.

%%----------------------------------------------------------------------------

start_restartable_child(M, A) -> start_restartable_child(M, A, false).
start_delayed_restartable_child(M, A) -> start_restartable_child(M, A,  true).

start_restartable_child(Mod, Args, Delay) ->
  child_reply(
    supervisor:start_child(?SERVER, restartable_child_spec(Mod, Args, Delay))
  ).

restartable_child_spec(Mod, Args, Delay) ->
  Name = list_to_atom(atom_to_list(Mod) ++ "_sup"),
  #{id => Name,
    start => {barrel_restartable_sup, start_link,
              [Name, {Mod, start_link, Args}, Delay]},
    restart => transient,
    shutdown => infinity,
    type => supervisor,
    modules => [barrel_restartable_sup]
  }.


child_reply({ok, _}) -> ok;
child_reply(X)       -> X.