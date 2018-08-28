%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Aug 2018 14:46
%%%-------------------------------------------------------------------
-module(barrel_q).
-author("benoitc").
-behaviour(gen_server).

%% API
-export([enqueue/1]).

-export([
  start_link/0
]).


-export([
  init/1,
  handle_call/3,
  handle_cast/2
]).


enqueue(Req) ->
  gen_server:call(?MODULE, {enqueue, ts(), Req}, infinity).

ts() -> erlang:timestamp().

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  {ok, #{}}.

handle_call({enqueue, Start, Req }, _From, State) ->
  Elapsed = timer:now_diff(ts(), Start),
  {reply, {Elapsed, Req}, State}.


handle_cast(_Msg, State) -> {ok, State}.
