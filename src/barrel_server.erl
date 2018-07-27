%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Jul 2018 15:46
%%%-------------------------------------------------------------------
-module(barrel_server).
-author("benoitc").

%% API
-export([start_link/0]).


-export([
  init/1,
  handle_call/3,
  handle_cast/2
]).

start_link() ->
  {ok, NodeId} = init_nodeid(),
  Config = [{node_id, NodeId}],
  ok = barrel_lib:load_config(barrel_server_config, Config),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [NodeId], []).

init([NodeId]) ->
  {ok, #{nodeid => NodeId}}.

handle_call(get_id, _From, #{ nodeid := NodeId }=State) ->
  {reply, NodeId, State}.

handle_cast(_Msg, State) -> {noreply, State}.

init_nodeid() ->
  case init:get_argument(setnodeid) of
    {ok, [[ID0]]} ->
      ID = list_to_binary(ID0),
      {ok, ID};
    _ ->
      case application:get_env(barrel, nodeid) of
        {ok, ID} ->
          {ok, barrel_lib:to_binary(ID)};
        undefined ->
          read_nodeid()
      end
  end.

read_nodeid() ->
  Name = filename:join(barrel_storage:data_dir(), "BARREL_NODEID"),
  ok = filelib:ensure_dir(Name),
  case read_file(Name) of
    {ok, ID} -> {ok, ID};
    {error, enoent} -> create_nodeid(Name);
    Error -> Error
  end.

read_file(Name) ->
  case file:read_file(Name) of
    {ok, Bin} ->
      Term = erlang:binary_to_term(Bin),
      {ok,  Term};
    Error ->
      Error
  end.

create_nodeid(Name) ->
  ID = barrel_lib:uniqid(),
  ok = file:write_file(Name, term_to_binary(ID)),
  {ok, ID}.