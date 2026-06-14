%%%-------------------------------------------------------------------
%%% @doc Path ID Interning for barrel_docdb
%%%
%%% Maps JSON paths to compact 32-bit integer IDs for efficient
%%% posting list keys. Uses ETS for read cache and RocksDB for
%%% persistence.
%%%
%%% Path format: `[&lt;&lt;"field1"&gt;&gt;, &lt;&lt;"field2"&gt;&gt;]' to PathId (32-bit integer)
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_path_dict).
-behaviour(gen_server).

-include("barrel_docdb.hrl").

%% API
-export([start_link/0]).
-export([get_or_create_id/3, get_id/2, get_path/3]).
-export([load_from_store/2, clear_cache/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% For testing
-export([reset/0]).

-define(SERVER, ?MODULE).
-define(ETS_CACHE, barrel_path_dict_cache).
-define(ETS_REVERSE, barrel_path_dict_reverse).

%% RocksDB key prefix for path dict entries
-define(PREFIX_PATH_DICT, 16#11).
-define(PREFIX_PATH_DICT_NEXT_ID, 16#12).

-record(state, {}).

%%====================================================================
%% Types
%%====================================================================

-type path() :: [binary() | term()].
-type path_id() :: non_neg_integer().

-export_type([path/0, path_id/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the path dictionary server
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Get or create a path ID for the given path.
%% If the path doesn't have an ID yet, creates one and persists it.
-spec get_or_create_id(barrel_store_rocksdb:db_ref(), db_name(), path()) -> path_id().
get_or_create_id(StoreRef, DbName, Path) ->
    %% First check ETS cache (fast path)
    case ets:lookup(?ETS_CACHE, {DbName, Path}) of
        [{_, PathId}] ->
            PathId;
        [] ->
            %% Cache miss - go through gen_server for atomic create
            gen_server:call(?SERVER, {get_or_create, StoreRef, DbName, Path})
    end.

%% @doc Get path ID from cache only (no creation).
%% Returns not_found if path is not in cache.
-spec get_id(db_name(), path()) -> {ok, path_id()} | not_found.
get_id(DbName, Path) ->
    case ets:lookup(?ETS_CACHE, {DbName, Path}) of
        [{_, PathId}] -> {ok, PathId};
        [] -> not_found
    end.

%% @doc Get path from cache by ID.
%% Returns undefined if ID is not in cache.
-spec get_path(barrel_store_rocksdb:db_ref(), db_name(), path_id()) -> path() | undefined.
get_path(_StoreRef, DbName, PathId) ->
    case ets:lookup(?ETS_REVERSE, {DbName, PathId}) of
        [{_, Path}] -> Path;
        [] -> undefined
    end.

%% @doc Load all path IDs from store into cache.
%% Called when a database is opened.
-spec load_from_store(barrel_store_rocksdb:db_ref(), db_name()) -> ok.
load_from_store(StoreRef, DbName) ->
    gen_server:call(?SERVER, {load_from_store, StoreRef, DbName}).

%% @doc Clear cache entries for a database.
%% Called when a database is closed or deleted.
-spec clear_cache(db_name()) -> ok.
clear_cache(DbName) ->
    gen_server:call(?SERVER, {clear_cache, DbName}).

%% @doc Reset the path dictionary (for testing only).
-spec reset() -> ok.
reset() ->
    gen_server:call(?SERVER, reset).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS tables for caching
    %% Path -> ID cache (high read concurrency)
    _ = ets:new(?ETS_CACHE, [
        named_table, public, set,
        {read_concurrency, true}
    ]),
    %% ID -> Path reverse cache
    _ = ets:new(?ETS_REVERSE, [
        named_table, public, set,
        {read_concurrency, true}
    ]),
    {ok, #state{}}.

handle_call({get_or_create, StoreRef, DbName, Path}, _From, State) ->
    %% Double-check cache (another process may have created it)
    PathId = case ets:lookup(?ETS_CACHE, {DbName, Path}) of
        [{_, Id}] ->
            Id;
        [] ->
            %% Check RocksDB
            case load_path_from_store(StoreRef, DbName, Path) of
                {ok, Id} ->
                    %% Found in store, add to cache
                    cache_path(DbName, Path, Id),
                    Id;
                not_found ->
                    %% Create new ID
                    NewId = get_next_id(StoreRef, DbName),
                    %% Persist to RocksDB
                    ok = store_path(StoreRef, DbName, Path, NewId),
                    %% Add to cache
                    cache_path(DbName, Path, NewId),
                    NewId
            end
    end,
    {reply, PathId, State};

handle_call({load_from_store, StoreRef, DbName}, _From, State) ->
    %% Load all paths for this database from RocksDB into cache
    load_all_paths(StoreRef, DbName),
    {reply, ok, State};

handle_call(reset, _From, State) ->
    ets:delete_all_objects(?ETS_CACHE),
    ets:delete_all_objects(?ETS_REVERSE),
    {reply, ok, State};

handle_call({clear_cache, DbName}, _From, State) ->
    %% Clear cache entries for this database
    %% Use match_delete for efficiency
    ets:match_delete(?ETS_CACHE, {{DbName, '_'}, '_'}),
    ets:match_delete(?ETS_REVERSE, {{DbName, '_'}, '_'}),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Add path/ID mapping to cache
cache_path(DbName, Path, PathId) ->
    ets:insert(?ETS_CACHE, {{DbName, Path}, PathId}),
    ets:insert(?ETS_REVERSE, {{DbName, PathId}, Path}),
    ok.

%% @private Get the next available path ID for a database
get_next_id(StoreRef, DbName) ->
    Key = next_id_key(DbName),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, <<NextId:32/big-unsigned>>} ->
            %% Increment and store
            ok = barrel_store_rocksdb:put(StoreRef, Key, <<(NextId + 1):32/big-unsigned>>),
            NextId;
        not_found ->
            %% First path ID for this database
            ok = barrel_store_rocksdb:put(StoreRef, Key, <<2:32/big-unsigned>>),
            1
    end.

%% @private Load a single path from store
load_path_from_store(StoreRef, DbName, Path) ->
    Key = path_dict_key(DbName, Path),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, <<PathId:32/big-unsigned>>} ->
            {ok, PathId};
        not_found ->
            not_found
    end.

%% @private Store a path mapping in RocksDB
store_path(StoreRef, DbName, Path, PathId) ->
    Key = path_dict_key(DbName, Path),
    Value = <<PathId:32/big-unsigned>>,
    barrel_store_rocksdb:put(StoreRef, Key, Value).

%% @private Load all paths for a database from store
load_all_paths(StoreRef, DbName) ->
    Prefix = path_dict_prefix(DbName),
    PrefixLen = byte_size(Prefix),
    barrel_store_rocksdb:fold(StoreRef, Prefix,
        fun(Key, <<PathId:32/big-unsigned>>, Acc) ->
            %% Extract path from key
            <<_:PrefixLen/binary, EncodedPath/binary>> = Key,
            Path = barrel_store_keys:decode_path(EncodedPath),
            cache_path(DbName, Path, PathId),
            {ok, Acc}
        end,
        ok),
    ok.

%%====================================================================
%% Key Encoding
%%====================================================================

%% @private Key for path dictionary entry
%% Format: PREFIX + db_name + encoded_path
path_dict_key(DbName, Path) ->
    EncodedPath = barrel_store_keys:encode_path(Path),
    <<?PREFIX_PATH_DICT, (encode_name(DbName))/binary, EncodedPath/binary>>.

%% @private Prefix for scanning all path dict entries for a database
path_dict_prefix(DbName) ->
    <<?PREFIX_PATH_DICT, (encode_name(DbName))/binary>>.

%% @private Key for next ID counter
next_id_key(DbName) ->
    <<?PREFIX_PATH_DICT_NEXT_ID, (encode_name(DbName))/binary>>.

%% @private Encode database name with length prefix
encode_name(Name) when is_binary(Name) ->
    Len = byte_size(Name),
    <<Len:16, Name/binary>>.
