%% Copyright (c) 2017. Benoit Chesneau
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

-module(barrel_store).
-author("benoitc").

-behaviour(gen_server).

%% API
-export([
  create_db/1,
  create_db/2,
  delete_db/1,
  open_db/1,
  drop_db/1,
  load_db/1,
  databases/0,
  fold_databases/2
]).

-export([
  start_link/0,
  get_databases/0,
  whereis_db/1,
  db_pid/1,
  db_properties/1,
  data_dir/0
]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).


-include("barrel.hrl").

-define(CONF_VERSION, 1).

-deprecated([create_db/2]).

%%%===================================================================
%%% API
%%%===================================================================

create_db(DbId, Config) ->
  _ = lager:warning("barrel_db:create/2 is deprecated", []),
  create_db(Config#{ <<"database_id">> => DbId }).

create_db(#{ << "database_id">> := _DbId } = Config) ->
  gen_server:call(?MODULE, {create_db, Config});
create_db(Config) when is_map(Config) ->
  DbId = << "db-", (barrel_lib:uniqid())/binary >>,
  gen_server:call(?MODULE, {create_db, Config#{ <<"database_id">> => DbId}});
create_db(_) ->
  erlang:error(badarg).

delete_db(DbId) ->
  gen_server:call(?MODULE, {delete_db, DbId}).

drop_db(DbId) ->
  gen_server:call(?MODULE, {drop_db, DbId}).

load_db(Config) ->
  gen_server:call(?MODULE, {load_db, Config}).


open_db(DbId) ->
  try
    Db = gproc:lookup_value(barrel_db:db_key(DbId)),
    {ok, Db}
  catch
    error:badarg ->
      gen_server:call(?MODULE, {open_db, DbId}, infinity)
  end.

%% TODO: simply return all databases
databases() ->
  AllDbs = fold_databases(
    fun(#{ <<"database_id">> := DatabaseId }, Acc) -> {ok, [DatabaseId | Acc]} end,
    []
  ),
  lists:usort(AllDbs).

fold_databases(Fun, AccIn) ->
  {ok, Databases} = get_databases(),
  fold_databases_1(maps:values(Databases), Fun, AccIn).


fold_databases_1([Db | Rest], Fun, Acc) ->
  case Fun(Db, Acc) of
    {ok, Acc2} -> fold_databases_1(Rest, Fun, Acc2);
    {stop, Acc2} -> Acc2;
    stop -> Acc;
    ok -> Acc
  end;
fold_databases_1([], _Fun, Acc) ->
  Acc.

whereis_db(DbId) ->
  try
    gproc:lookup_value(barrel_db:db_key(DbId))
  catch
    error:badarg -> undefined
  end.

db_pid(DbId) ->
  gproc:where(barrel_db:db_key(DbId)).

db_properties(DbId) ->
  case whereis_db(DbId) of
    undefined -> undefined;
    Db ->
      #db{ pid = DbPid,
           last_rid = LastRid,
           updated_seq = UpdateSeq,
           docs_count = DocCount,
           system_docs_count = SystemDocsCount} = Db,
      Infos = erlang:process_info(DbPid),
      MsgLen = proplists:get_value(message_queue_len, Infos),
      PDict = proplists:get_value(dictionary, Infos),
      DocsUpdated = proplists:get_value(num_docs_updated, PDict),

      #{ last_rid => LastRid,
         last_update_seq => UpdateSeq,
         docs_count => DocCount,
         system_docs_count => SystemDocsCount,
         message_queue_len => MsgLen,
         num_docs_updated => DocsUpdated }
  end.

get_databases() -> gen_server:call(?MODULE, get_databases).

default_dir() ->
  filename:join([?DATA_DIR, node()]).

-spec data_dir() -> string().
data_dir() ->
  Dir = application:get_env(barrel, data_dir, default_dir()),
  _ = filelib:ensure_dir(filename:join([".", Dir, "dummy"])),
  Dir.

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  process_flag(trap_exit, true),
  {ok, RegExp} = re:compile("^[a-z][a-z0-9\\_\\$()\\+\\-\\/]*$"),
  %% initialize the cache
  %% TODO: make it optionnal for the peer
  BlockCacheSize = case application:get_env(barrel, block_cache_size, 0) of
                     0 ->
                       MaxSize = barrel_memory:get_total_memory(),
                       %% reserve 1GB for system and binaries, and use 30% of the rest
                       ((MaxSize - 1024 * 1024) * 0.3);
                     Sz ->
                       Sz * 1024 * 1024 * 1024
                   end,

  {ok, Cache} = rocksdb:new_lru_cache(trunc(BlockCacheSize)),
  InitState = #{ db_regexp => RegExp, cache => Cache },
  {ok, State} = load_config(InitState),
  {ok, State}.

handle_call({open_db, DbId}, _From, State) ->
  Reply = maybe_open_db(db_pid(DbId), DbId, State),
  {reply, Reply, State};

handle_call({create_db, Config=#{<<"database_id">> := DbId}}, _From, State) ->
  {Reply, NState} = maybe_create_db(db_pid(DbId), DbId, Config, State),
  {reply, Reply, NState};

handle_call({delete_db, DbId}, _From, State) ->
  {Reply, NState} = maybe_delete_db(db_pid(DbId), DbId, State),
  {reply, Reply, NState};

handle_call({drop_db, DbId}, _From, State) ->
  {Reply, NState} = maybe_drop_db(db_pid(DbId), DbId, State),
  {reply, Reply, NState};

handle_call({load_db, Config}, _From, State) ->
  {Reply, NState} = do_load_db(Config, State),
  {reply, Reply, NState};

handle_call(get_databases, _From, State = #{ databases := Databases}) ->
  {reply, {ok, Databases}, State};

handle_call(get_state, _From, State) ->
  {reply, State, State};

handle_call(_Request, _From, State) ->
  {reply, bad_call, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

db_options(#{ <<"in_memory">> := true }, _State) ->
  [{env, memenv} | default_rocksdb_options()];
db_options(_, #{ cache := Cache }) ->
  BlockOptions = [{block_cache, Cache}],
  [{block_based_table_options, BlockOptions} | default_rocksdb_options()].

default_rocksdb_options() ->
  WriteBufferSize = 64 * 1024 * 1024, %% 64MB
  [
    {max_open_files, 64},
    {write_buffer_size, WriteBufferSize}, %% 64MB
    {allow_concurrent_memtable_write, true},
    {enable_write_thread_adaptive_yield, true}
  ].

maybe_create_db(undefined, DbId, Config, State = #{ databases := Dbs }) ->
  case maps:find(DbId, Dbs) of
    {ok, _Config} ->
      {{error, db_exists}, State};
    error ->
      maybe_create_db_1(DbId, Config, State)
  end;
maybe_create_db(_Pid, _DbId, _Config, State) ->
  {{error, db_exists}, State}.

maybe_create_db_1(DbId, Config0, State =  #{ databases := Dbs }) ->
  DbOpts =  [
    {create_if_missing, true}, {error_if_exists, true} | db_options(Config0, State)
  ],
  case db_path(DbId, State) of
    {ok, Path} ->
      Config1 = Config0#{ <<"_path">> => list_to_binary(Path) },
      case supervisor:start_child(barrel_db_sup, [DbId, Path, DbOpts, Config1]) of
        {ok, _Pid} ->
          Dbs2 = Dbs#{ DbId => Config1 },
          NewState = State#{ databases => Dbs2 },
          case persist_config(NewState) of
            ok ->
              {{ok, Config1}, NewState};
            Error ->
              {Error, State}
          end;
        Error ->
          {Error, State}
      end;
    Error ->
      {Error, State}
  end.


maybe_open_db(undefined, DbId, State = #{ databases := Dbs }) ->
  case maps:find(DbId, Dbs) of
    {ok, #{ <<"_path">> := DbPath } = Config} ->
      DbOpts =  db_options(Config, State),
      case supervisor:start_child(
        barrel_db_sup, [DbId, binary_to_list(DbPath), DbOpts, Config]
      ) of
        {ok, Pid} ->
          barrel_db:get_db(Pid);
        Error ->
          Error
      end;
    error ->
      {error, db_not_found}
  end;
maybe_open_db(_Pid, _DbId, State) ->
  {ok, State}.

maybe_delete_db(undefined, DbId,  State = #{ databases := Dbs }) ->
  case maps:take(DbId, Dbs) of
    {#{ <<"_path">> := DbPath }, Dbs2} ->
      _ = spawn(
        fun() ->
          _ = barrel_db:delete_db_dir(binary_to_list(DbPath))
        end
      ),
      NewState = State#{ databases => Dbs2 },
      case persist_config(NewState) of
        ok ->
          {ok, NewState};
        Error ->
          {Error, State}
      end;
    error ->
      {ok, State}
  end;
maybe_delete_db(DbPid, DbId, State = #{ databases := Dbs }) ->
  DbKey = barrel_db:db_key(DbPid),
  MRef = gproc:monitor(DbKey),
  ok = barrel_db:delete_db(DbPid),
  receive
    {gproc, unreg, MRef, DbKey} ->
      Dbs2 = maps:remove(DbId, Dbs),
      NewState = State#{ databases => Dbs2 },
      case persist_config(NewState) of
        ok ->
          {ok, NewState};
        Error ->
          _ = lager:info(
            "~s: error while persisting the db: ~p~n",
            [?MODULE_STRING,Error]
          ),
          {Error, State}
      end
  end.

maybe_drop_db(undefined, DbId,  State = #{ databases := Dbs }) ->
  case maps:take(DbId, Dbs) of
    {OldConf, Dbs2} ->
      NewState = State#{ databases => Dbs2 },
      case persist_config(NewState) of
        ok ->
          {{ok, OldConf}, NewState};
        Error ->
          _ = lager:info(
            "~s: error while persisting the db configuration: ~p~n",
            [?MODULE_STRING,Error]
          ),
          {Error, State}
      end;
    error ->
      {{ok, undefined}, State}
  end;
maybe_drop_db(DbPid, DbId, State = #{ databases := Dbs }) ->
  DbKey = barrel_db:db_key(DbPid),
  MRef = gproc:monitor(DbKey),
  ok = barrel_db:close_db(DbPid),
  receive
    {gproc, unreg, MRef, DbKey} ->
      {OldConf, Dbs2} = maps:take(DbId, Dbs),
      NewState = State#{ databases => Dbs2 },
      case persist_config(NewState) of
        ok ->
          {{ok, OldConf}, NewState};
        Error ->
          _ = lager:info(
            "~s: error while persisting the db configuration: ~p~n",
            [?MODULE_STRING,Error]
          ),
          {Error, State}
      end
  end.


do_load_db(#{ <<"database_id">>:= DbId} = Config, State = #{ databases := Dbs }) ->
  Dbs2 = Dbs#{ DbId => Config },
  NewState = State#{ databases => Dbs2 },
  case persist_config(NewState) of
    ok ->
      {{ok, Config}, NewState};
    Error ->
      {Error, State}
  end;
do_load_db(_, State) ->
  {{error, invalid_config}, State}.

conf_path() ->
  Path = filename:join(data_dir(), "barrel_config"),
  ok = filelib:ensure_dir(Path),
  Path.

persist_config(State) ->
  Conf = conf_from_state(State),
  file:write_file(conf_path(), jsx:encode(Conf)).

load_config(State) ->
  case filelib:is_regular(conf_path()) of
    true ->
      {ok, ConfBin} = file:read_file(conf_path()),
      Conf = maps:merge(
        empty_conf(),
        jsx:decode(ConfBin, [return_maps])
      ),
      {ok, conf_to_state(Conf, State)};
    false ->
      NewState = conf_to_state(empty_conf(), State),
      ok = persist_config(NewState),
      {ok, NewState}
  end.

empty_conf() ->
  #{
    <<"version">> => ?CONF_VERSION,
    <<"node_id">> => barrel_lib:uniqid(),
    <<"databases">> => []
  }.

conf_from_state(#{ databases := Dbs, node_id := NodeId }) ->
  #{
    <<"version">> => ?CONF_VERSION,
    <<"node_id">> => NodeId,
    <<"databases">> => maps:values(Dbs)
  }.

conf_to_state(#{ <<"databases">> := Dbs, <<"node_id">> := NodeId }, State) ->
  Databases = lists:foldl(
    fun(#{ <<"database_id">> := DbId} = Config, DbMap) ->
      DbMap#{ DbId => Config }
    end,
    #{},
    Dbs
  ),
  State#{ databases => Databases, node_id => NodeId }.

db_dir() ->
  Dir = filename:join(barrel_store:data_dir(), "dbs"),
  ok = filelib:ensure_dir([Dir, "dummy"]),
  Dir.

check_dbid(DbId, #{ db_regexp := RegExp}) ->
  case re:run(DbId, RegExp, [{capture, none}]) of
    nomatch ->
      {error, {invalid_database_id, DbId}};
    match ->
      ok
  end.


db_path(DbId, State) ->
  case check_dbid(DbId, State) of
    ok ->
      DbName = << DbId/binary, "-", (barrel_lib:uniqid())/binary >>,
      Path = binary_to_list(filename:join(db_dir(), DbName)),
      ok = filelib:ensure_dir(Path),
      {ok, Path};
    Error ->
      Error
  end.