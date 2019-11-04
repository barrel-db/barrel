-module(barrel_local_epoch_store).
-behaviour(gen_server).

-export([new_epoch/1]).
-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).


new_epoch(Barrel) ->
  gen_server:call(?MODULE, {new_epoch, Barrel}).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  StartTime = os:system_time(nanosecond),
  {ok, StartTime}.


handle_call({new_epoch, _Barrel}, _From, Epoch) ->
  {reply, os:system_time(millisecond), Epoch}.

handle_cast(_Msg, Epoch) ->
  {noreply, Epoch}.

terminate(_Reason, _Epoch) ->
  ok.
