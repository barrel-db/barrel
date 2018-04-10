%% Copyright (c) 2018. Benoit Chesneau
%%
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%    http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(barrel_storage).
-author("benoitc").

-export([
  start_link/3,
  has_barrel/2,
  create_barrel/3,
  delete_barrel/2,
  open_barrel/2,
  find_barrel/1,
  get_default/0,
  all_stores/0,
  close_barrel/2
]).

-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  terminate/2
]).


-export([default_dir/0, data_dir/0]).

-include("barrel.hrl").

-include_lib("stdlib/include/ms_transform.hrl").




default_dir() ->
  filename:join([?DATA_DIR, node()]).

-spec data_dir() -> string().
data_dir() ->
  Dir = application:get_env(barrel, data_dir, default_dir()),
  _ = filelib:ensure_dir(filename:join([".", Dir, "dummy"])),
  Dir.

has_barrel(Store, Name) ->
  gen_server:call(?via_store(Store), {has_barrel, Name}).

create_barrel(Store, Name, Options) ->
  gen_server:call(?via_store(Store), {create_barrel, Name, Options}).

delete_barrel(Store, Name) ->
  gen_server:call(?via_store(Store), {delete_barrel, Name}).

close_barrel(Store, Name) ->
  gen_server:call(?via_store(Store), {close_barrel, Name}).

open_barrel(Store, Name) ->
  gen_server:call(?via_store(Store), {open_barrel, Name}).


get_default() ->
  get_default_1(barrel_store_sup:all_stores()).

get_default_1([]) -> {error, no_stores};
get_default_1(Stores) ->
  case lists:member(default, Stores) of
    true -> default;
    false ->
      [Default | _] = Stores,
      Default
  end.
  

find_barrel(Name) ->
  fold_stores(
    fun ({Store, _Pid}, Acc) ->
      case barrel_storage:has_barrel(Store, Name) of
        true ->
          {stop, {ok, Store}};
        false ->
          {ok, Acc}
      end
    end,
    error
  ).

all_stores() ->
  fold_stores(
    fun({Name, _}, Acc) -> {ok, [Name | Acc]} end,
    []
  ).


-spec fold_stores(fun(({term(), pid()}, any()) -> {ok, any()} | {stop, any()}), any()) -> any().
fold_stores(Fun, Acc) ->
  Res =  gproc:select({l,n}, [{{{n, l, {barrel_storage, '_'}}, '_','_'},[],['$_'] }]),
  fold_stores_1(Res, Fun, Acc).

fold_stores_1([{{n, l, {barrel_storage, Name}}, Pid, _} | Rest], Fun, Acc0) ->
  case Fun({Name, Pid}, Acc0) of
    {ok, Acc1} -> fold_stores_1(Rest, Fun, Acc1);
    {stop, StopAcc} -> StopAcc
  end;
fold_stores_1([], _Fun, Acc) ->
  Acc.

%% start a storage
start_link(Name, Mod, Options) ->
  proc_lib:start_link(?MODULE, init, [[Name, Mod, Options]]).


init([Name, Mod, Options]) ->
  case init_storage(Name, Mod, Options) of
    {ok, ModState} ->
      process_flag(trap_exit, true),
      %% register the store and return
      gproc:register_name(?store(Name), self()),
      proc_lib:init_ack({ok, self()}),
      %% enter in  the storage process loop
      State = #{
        name => Name,
        mod => Mod,
        modstate => ModState
      },
      gen_server:enter_loop(?MODULE, [], State);
    Error ->
      _ = lager:error("error while initializing storage ~p~n", [Name]),
      erlang:error(Error)
  end.

handle_call({has_barrel, Name}, _From, #{ mod := Mod, modstate := ModState} = State) ->
  Reply = try_exec(Mod, has_barrel, [Name, ModState]),
  {reply, Reply, State};

handle_call({create_barrel, Name, Options}, _From, #{ mod := Mod, modstate := ModState} = State) ->
  {Reply, NewModState} = try_exec(Mod, create_barrel, [Name, Options, ModState]),
  {reply, {Reply, Mod}, State#{ modstate => NewModState}};

handle_call({open_barrel, Name}, _From, #{ mod := Mod, modstate := ModState} = State) ->
  Reply = try_exec(Mod, open_barrel, [Name, ModState]),
  {reply, {Reply, Mod}, State};

handle_call({delete_barrel, Name}, _From, #{ mod := Mod, modstate := ModState} = State) ->
  {Reply, NewModState} = try_exec(Mod, delete_barrel, [Name, ModState]),
  {reply, Reply, State#{ modstate => NewModState}};
handle_call({close_barrel, Name}, _From, #{  mod := Mod, modstate := ModState} = State) ->
  {Reply, NewModState} = try_exec(Mod, close_barrel, [Name, ModState]),
  {reply, Reply, State#{ modstate => NewModState}};

handle_call(_Msg, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

terminate(Reason, #{ mod := Mod, modstate := ModState}) ->
  Mod:terminate(Reason, ModState).

init_storage(Name, Mod, Options) ->
  try Mod:init(Name, Options)
  catch
    error:Error -> Error
  end.

try_exec(M, F, A) ->
  try erlang:apply(M, F, A)
  catch
    error:Error -> Error
  end.