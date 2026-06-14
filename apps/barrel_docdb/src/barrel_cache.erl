%%%-------------------------------------------------------------------
%%% @doc Shared RocksDB block cache manager
%%%
%%% Manages a shared block cache used by all RocksDB instances.
%%% This improves memory efficiency and cache hit rates by sharing
%%% a single cache across document stores and attachment stores.
%%%
%%% The cache is configured via application environment:
%%% ```
%%% {barrel_docdb, [
%%%     {block_cache_size, 536870912},  %% 512MB default
%%%     {block_cache_type, clock}       %% clock (default) or lru
%%% ]}
%%% '''
%%%
%%% Clock cache provides better performance under concurrent load.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_cache).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([get_cache/0]).
-export([get_block_opts/0]).
-export([get_block_opts/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(SERVER, ?MODULE).

-record(state, {
    cache :: rocksdb:cache_handle() | undefined
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the cache manager
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Get the shared cache handle
-spec get_cache() -> {ok, rocksdb:cache_handle()} | {error, not_started}.
get_cache() ->
    gen_server:call(?SERVER, get_cache).

%% @doc Get block-based table options with shared cache and bloom filter
-spec get_block_opts() -> list().
get_block_opts() ->
    get_block_opts(#{}).

%% @doc Get block-based table options with custom settings
%%
%% Options:
%% - `bloom_bits': Bloom filter bits per key (default: 10)
%% - `block_size': Block size in bytes (default: 4096)
%% - `cache_index_and_filter': Cache index/filter blocks (default: true)
%%
%% If the cache process is not running (e.g., during standalone tests),
%% returns options without the shared cache.
-spec get_block_opts(map()) -> list().
get_block_opts(Options) ->
    try
        gen_server:call(?SERVER, {get_block_opts, Options})
    catch
        exit:{noproc, _} ->
            %% Cache not started - return options without shared cache
            build_block_opts_no_cache(Options)
    end.

%% Build block options without shared cache (for standalone usage)
build_block_opts_no_cache(Options) ->
    BloomBits = maps:get(bloom_bits, Options, 10),
    BlockSize = maps:get(block_size, Options, 4096),
    CacheIndexAndFilter = maps:get(cache_index_and_filter, Options, true),
    [
        {filter_policy, {bloom_filter, BloomBits}},
        {whole_key_filtering, false},
        {block_size, BlockSize},
        {cache_index_and_filter_blocks, CacheIndexAndFilter}
    ].

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    CacheSize = application:get_env(barrel_docdb, block_cache_size, 512 * 1024 * 1024),
    CacheType = application:get_env(barrel_docdb, block_cache_type, clock),
    {ok, Cache} = rocksdb:new_cache(CacheType, CacheSize),
    logger:info("barrel_cache: initialized ~p MB ~p cache",
                       [CacheSize div (1024 * 1024), CacheType]),
    {ok, #state{cache = Cache}}.

handle_call(get_cache, _From, #state{cache = Cache} = State) ->
    case Cache of
        undefined -> {reply, {error, not_started}, State};
        _ -> {reply, {ok, Cache}, State}
    end;

handle_call({get_block_opts, Options}, _From, #state{cache = Cache} = State) ->
    BloomBits = maps:get(bloom_bits, Options, 10),
    BlockSize = maps:get(block_size, Options, 4096),
    CacheIndexAndFilter = maps:get(cache_index_and_filter, Options, true),

    BaseOpts = [
        {filter_policy, {bloom_filter, BloomBits}},
        {whole_key_filtering, false},
        {block_size, BlockSize},
        {cache_index_and_filter_blocks, CacheIndexAndFilter}
    ],

    BlockOpts = case Cache of
        undefined -> BaseOpts;
        _ -> [{block_cache, Cache} | BaseOpts]
    end,

    {reply, BlockOpts, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% Cache is automatically released when process terminates
    ok.
