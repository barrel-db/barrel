%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Oct 2018 09:31
%%%-------------------------------------------------------------------
-module(barrel_config).
-author("benoitc").

%% API
-export([
  init/0,
  set/2,
  get/1, get/2
]).

-include("barrel.hrl").
-include("barrel_logger.hrl").


get(Key) ->
  persistent_term:get({?MODULE, Key}).

get(Key, Default) ->
  try
    persistent_term:get({?MODULE, Key})
  catch
      error:badarg  -> Default
  end.

set(Key, Value) ->
  application:set_env(?APP, Key, Value),
  persistent_term:put({?MODULE, Key}, Value).





init() ->
  %% Configure data dir
  DataDir = data_dir(),
  barrel_config:set(data_dir, DataDir),
  ok= barrel_registry:init(DataDir),
  
  [env_or_default(Key, Default) ||
    {Key, Default} <- [
      {fold_timeout, 5000}
    ]
  ],
  
  ok.

data_dir() ->
  Default = filename:join([?DATA_DIR, node()]),
  Dir = application:get_env(barrel, data_dir, Default),
  _ = filelib:ensure_dir(filename:join([".", Dir, "dummy"])),
  Dir.


env_or_default(Key, Default) ->
  case application:get_env(barrel, Key) of
    {ok, Value} ->
      set(Key, Value);
    undefined ->
      set(Key, Default)
  end.
