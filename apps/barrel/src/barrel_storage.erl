%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Jan 2019 05:43
%%%-------------------------------------------------------------------
-module(barrel_storage).
-author("benoitc").


%% API
-export([get_store/0]).
-export([start_link/0]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  terminate/2
]).

-type store() :: #{ mod := atom(), ref := any()}.

-spec get_store() -> store().
get_store() ->
  persistent_term:get(?MODULE).


start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
  erlang:process_flag(trap_exit, true),
  %% initialize the storage
  Mod = application:get_env(barrel, storage_service, barrel_rocksdb),
  {ok, Ref} = Mod:init_storage(),
  %% we store it as a persistent term as the creation of barrels is done asynchronously
  Store = #{ mod => Mod, ref => Ref },
  ok = persistent_term:put(?MODULE, Store),
  {ok, Store}.


handle_call(_Msg, _From, Store) ->
  {reply, ok, Store}.

handle_cast(_Msg, Store) ->
  {noreply, Store}.

terminate(Reason, #{ mod := Mod, ref := Ref}) ->
  Mod:terminate_storage(Reason, Ref).