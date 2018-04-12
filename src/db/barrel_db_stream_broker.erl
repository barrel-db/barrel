%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Apr 2018 05:20
%%%-------------------------------------------------------------------
-module(barrel_db_stream_broker).
-author("benoitc").


-export([start_link/0]).

-export([init/1]).

-include("barrel.hrl").

start_link() ->
  sbroker:start_link({local, ?db_stream_broker}, ?MODULE, [], [{read_time_after, 16}]).

init(_) ->
  QueueSpec = {sbroker_timeout_queue, #{out => out,
                                        timeout => infinity,
                                        drop => drop_r,
                                        min => 0,
                                        max => 128}},
  WorkerSpec = {sbroker_drop_queue, #{out => out,
                                      drop => drop,
                                      max => infinity}},
  {ok, {QueueSpec, WorkerSpec, []}}.