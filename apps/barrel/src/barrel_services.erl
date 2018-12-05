%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 28. Nov 2018 10:28
%%%-------------------------------------------------------------------
-module(barrel_services).
-author("benoitc").

%% API
-export([
  start_service/1,
  stop_service/1,
  activate_service/4,
  deactivate_service/2,
  get_service_module/2
]).


-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2]).


start_service(Spec =  #{ id := ServiceId }) ->
  case supervisor:start_child(barrel_services_sup, Spec) of
    {ok, Pid} ->
      {ok, Pid};
    {error, already_present} ->
      supervisor:restart_child(barrel_services_sup, ServiceId);
    {error, {already_started, Pid}} ->
      %% TODO: check if spec is equal
      {ok, Pid}
  end.

stop_service(Name) ->
  case supervisor:terminate_child(barrel_services_sup, Name) of
    ok ->
      supervisor:delete_child(barrel_services_sup, Name);
    Error ->
      Error
  end.

activate_service(Type, Module, Name, Spec0) ->
  Spec = Spec0#{ id => {Type, Name}},
  case ets:insert_new(?MODULE, {{Type, Name}, Module}) of
    true ->
      {ok, _} = start_service(Spec),
      ok;
    false ->
      case supervisor:get_childspec(barrel_services_sup, Name) of
        {error, not_found} ->
          {ok, _} = start_service(Spec),
          ok;
        Error ->
          Error
      end
  end.

deactivate_service(Type, Name) ->
  _ = stop_service({Type, Name}),
  ets:delete(?MODULE, {Type, Name}),
  ok.

get_service_module(Type, Name) -> ets:lookup_element(?MODULE, {Type, Name}, 2).


start_link() ->
  gen_server:start_link(?MODULE, [], []).

init([]) ->
  _ = ets:new(?MODULE, [named_table, public, {read_concurrency, true}, {write_concurrency, true}]),
  {ok, #{}}.

handle_call(_Msg, _From, State) -> {reply, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.