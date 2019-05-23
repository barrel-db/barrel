-module(barrel_local_epoch_store).
-behaviour(gen_server).

-export([get_epoch/0]).
-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         terminate/2]).


get_epoch() ->
  persistent_term:get({?MODULE, epoch}).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  StartTime = os:system_time(millisecond),
  persistent_term:put({?MODULE, epoch}, StartTime),
  {ok, StartTime}.


handle_call({get_epoch, _Barrel}, _From, Epoch) ->
  {reply, Epoch, Epoch}.

handle_cast(_Msg, Epoch) ->
  {noreply, Epoch}.

terminate(_Reason, _Epoch) ->
  persistent_term:erase({?MODULE, epoch}).
