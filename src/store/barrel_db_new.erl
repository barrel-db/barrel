%%%-------------------------------------------------------------------
%%% @author benoitc
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Sep 2017 13:54
%%%-------------------------------------------------------------------
-module(barrel_db_new).
-author("benoitc").


%% API
-export([
  start_link/2,
  stop/1,
  delete_db/1,
  get_handle/1,
  get_config/1
]).

-export([db_key/1]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

start_link(OpenType, Config = #{ <<"database_id" >> := DbId }) ->
  ServerName = server_name(DbId),
  gen_server:start_link(ServerName, ?MODULE, [OpenType, Config], []).

stop(DbId) ->
  gen_server:call(server_name(DbId), stop, infinity).

delete_db(DbId) ->
  gen_server:call(server_name(DbId), delete_db, infinity).


get_handle(DbId) ->
  gen_server:call(server_name(DbId), get_handle, infinity).


get_config(DbId) ->
  gen_server:call(server_name(DbId), get_config, infinity).


server_name(DbId) ->
  {via, gproc, db_key(DbId)}.

db_key(DbId) -> {n, l, {barrel, db, DbId}}.

init([OpenType, Config]) ->
  case init_db(OpenType, Config) of
    {ok, DbHandle, Config2} ->
      {ok, #{ handle => DbHandle, config => Config2}};
    Error ->
      %% error opening the database
      {stop, Error}
  end.

handle_call(get_handle, _From, #{ handle := DbHandle } = State) ->
  {reply, DbHandle, State};

handle_call(get_config, _From, #{ config := Config } = State) ->
  {reply, Config, State};

handle_call(stop, _From, #{ handle := DbHandle, config := Config } = State) ->
  _ = lager:info(
    "~s, close database ~p~n",
    [?MODULE_STRING, maps:get(<<"database_id">>, Config)]
  ),
  ok = rocksdb:close(DbHandle),
  {stop, normal, ok, State#{ handle => nil }};
handle_call(delete_db, _From, #{ handle := DbHandle, config := Config } = State) ->
  _ = lager:info(
    "~s, delete database ~p~n",
    [?MODULE_STRING, maps:get(<<"database_id">>, Config)]
  ),
  ok = rocksdb:close(DbHandle),
  ok = delete_db_dir(Config),
  {stop, normal, ok, State#{ handle => nil }}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #{ handle := nil}) ->
  ok;
terminate(Reason, #{ handle := DbHandle, config := Config}) ->
  _  = lager:error(
    "~s, database ~p terminated with reason ~p~n",
    [?MODULE_STRING, maps:get(<<"database_id">>, Config), Reason]
  ),
  rocksdb:close(DbHandle).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


delete_db_dir(Path) ->
  _ = spawn(
    fun() ->
      _ = rocksdb:destroy(Path, [])
    end
  ),
  ok.


db_dir() ->
  Dir = filename:join(barrel_store:data_dir(), "dbs"),
  ok = filelib:ensure_dir([Dir, "dummy"]),
  Dir.

init_db(create, Config) ->
  Path = db_path(Config),
  Options = [
    {create_if_missing, true}, {error_if_exists, true} | db_options(Config)
  ],
  open_db(Path, Options, Config);
init_db(open, Config = #{ <<"_path">> := Path }) ->
  Options = db_options(Config),
  open_db(Path, Options, Config).


open_db(Path, Options, Config) ->
  RetriesLeft = application:get_env(barrel, db_open_retries, 30),
  open_db(Path, Options, Config, RetriesLeft, undefined).

open_db(_Path, _Options, _Config, 0, LastError) ->
  {error, LastError};

open_db(Path, Options, Config, RetriesLeft, _LastError) ->
  case rocksdb:open(Path, Options) of
    {ok, DbHandle} ->
      {ok, DbHandle, Config#{ <<"_path">> => Path }};
    %% Check specifically for lock error, this can be caused if
    %% a crashed instance takes some time to flush leveldb information
    %% out to disk.  The process is gone, but the NIF resource cleanup
    %% may not have completed.
    {error, {db_open, OpenErr}=Reason} ->
      case lists:prefix("IO error: lock ", OpenErr) of
        true ->
          SleepFor = application:get_env(barrel, db_open_retry_delay, 2000),
          _ = lager:debug(
            "~s: barrel rocksdb backend retrying ~p in ~p ms after error ~s\n",
            [?MODULE, Path, SleepFor, OpenErr]
          ),
          timer:sleep(SleepFor),
          open_db(Path, Options, Config, RetriesLeft - 1, Reason);
        false ->
          {error, Reason}
      end;
    Error ->
      Error
  end.

db_options(#{ <<"in_memory">> := true }) ->
  [{env, memenv} | default_rocksdb_options()];
db_options(_) ->
  default_rocksdb_options().

default_rocksdb_options() ->
  WriteBufferSize = 64 * 1024 * 1024, %% 64MB
  Cache = barrel_cache:get_cache(),
  BlockOptions = [{block_cache, Cache}],
  [
    {max_open_files, 1000},
    {write_buffer_size, WriteBufferSize}, %% 64MB
    {allow_concurrent_memtable_write, true},
    {enable_write_thread_adaptive_yield, true},
    {block_based_table_options, BlockOptions}
  ].

db_path(#{ << "database_id" >> := DbId })  ->
  Path = binary_to_list(
    filename:join(
      db_dir(),
      << DbId/binary, "-", (barrel_lib:uniqid())/binary >>
    )
  ),
  ok = filelib:ensure_dir(Path),
  Path.
