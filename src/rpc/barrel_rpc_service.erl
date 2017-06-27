%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Jun 2017 04:17
%%%-------------------------------------------------------------------
-module(barrel_rpc_service).
-author("benoitc").
-behaviour(gen_server).

%% API
-export([
  start_link/0,
  load_services/1,
  find_service/1
]).


-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SERVER, ?MODULE).

-define(TAB, barrel_service).

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%% TODO: validate the service module
load_services(Mapping) ->
  _ = maps:map(
    fun(Service, Mod) ->
      true = ets:insert_new(?TAB, {barrel_lib:to_atom(Service), Mod})
    end, Mapping
  ),
  ok.

find_service(Service) ->
  try ets:lookup_element(?TAB, barrel_lib:to_atom(Service), 2)
  catch
    _:_ -> not_found
  end.

init([]) ->
  _ = ets:new(?TAB, [set, named_table, public, {read_concurrency, true}]),
  {ok, #{}}.

handle_call(_Msg, _From, State) -> {reply, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.