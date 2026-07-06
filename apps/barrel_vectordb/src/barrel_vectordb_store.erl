%%%-------------------------------------------------------------------
%%% @doc Vector store gen_server managing RocksDB and HNSW index
%%%
%%% Uses RocksDB column families:
%%% - vectors: chunk_id → binary float32 array
%%% - metadata: chunk_id → term_to_binary(map)
%%% - text: chunk_id → UTF-8 binary
%%% - hnsw_graph: node_id → serialized HNSW node
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_store).
-behaviour(gen_server).

-include("barrel_vectordb.hrl").

%% API
-export([
    start_link/1,
    stop/0,
    add/3,
    add/4,
    get/1,
    delete/1,
    search/2,
    search/3,
    count/0,
    info/0,
    rebuild_index/0,
    %% DiskANN ID mapping API
    get_diskann_db/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

-record(state, {
    db :: rocksdb:db_handle(),
    cf_vectors :: rocksdb:cf_handle(),
    cf_metadata :: rocksdb:cf_handle(),
    cf_text :: rocksdb:cf_handle(),
    cf_hnsw :: rocksdb:cf_handle(),
    %% DiskANN ID mapping column families
    cf_diskann_ids_fwd :: rocksdb:cf_handle(),
    cf_diskann_ids_rev :: rocksdb:cf_handle(),
    hnsw_index :: hnsw_index(),
    dimension :: pos_integer(),
    embed_state :: barrel_embed:embed_state() | undefined,
    config :: map(),
    %% Encryption context (none or key + EncryptedEnv); retained here
    %% because the NIF frees the env when the handle is garbage
    %% collected.
    crypto = none :: barrel_vectordb_crypto:ctx()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the vector store
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

%% @doc Stop the vector store
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%% @doc Add a chunk with auto-embedding
-spec add(binary(), binary(), metadata()) -> ok | {error, term()}.
add(ChunkId, Text, Metadata) ->
    gen_server:call(?SERVER, {add, ChunkId, Text, Metadata, auto}, infinity).

%% @doc Add a chunk with explicit vector
-spec add(binary(), binary(), metadata(), [float()]) -> ok | {error, term()}.
add(ChunkId, Text, Metadata, Vector) ->
    gen_server:call(?SERVER, {add, ChunkId, Text, Metadata, {vector, Vector}}, infinity).

%% @doc Get a chunk by ID
-spec get(binary()) -> {ok, map()} | not_found | {error, term()}.
get(ChunkId) ->
    gen_server:call(?SERVER, {get, ChunkId}).

%% @doc Delete a chunk
-spec delete(binary()) -> ok | {error, term()}.
delete(ChunkId) ->
    gen_server:call(?SERVER, {delete, ChunkId}).

%% @doc Search for similar chunks
-spec search(binary() | [float()], pos_integer()) -> {ok, [search_result()]} | {error, term()}.
search(Query, K) ->
    search(Query, K, #{}).

%% @doc Search for similar chunks with options
-spec search(binary() | [float()], pos_integer(), map()) -> {ok, [search_result()]} | {error, term()}.
search(Query, K, Options) ->
    gen_server:call(?SERVER, {search, Query, K, Options}, infinity).

%% @doc Get chunk count
-spec count() -> non_neg_integer().
count() ->
    gen_server:call(?SERVER, count).

%% @doc Get store info
-spec info() -> map().
info() ->
    gen_server:call(?SERVER, info).

%% @doc Rebuild HNSW index from stored vectors
-spec rebuild_index() -> ok | {error, term()}.
rebuild_index() ->
    gen_server:call(?SERVER, rebuild_index, infinity).

%% @doc Get DiskANN ID mapping database handles
%% Returns {ok, {Db, CfFwd, CfRev}} | {error, not_started}
-spec get_diskann_db() -> {ok, {rocksdb:db_handle(), rocksdb:cf_handle(), rocksdb:cf_handle()}} | {error, term()}.
get_diskann_db() ->
    gen_server:call(?SERVER, get_diskann_db).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    process_flag(trap_exit, true),

    DbPath = maps:get(db_path, Config, "priv/barrel_vectordb_data"),
    Dimension = maps:get(dimension, Config, ?DEFAULT_DIMENSION),
    HnswConfig = maps:get(hnsw, Config, #{}),

    %% Ensure dimensions is in config for embed init
    EmbedConfig = Config#{dimensions => Dimension},
    case barrel_embed:init(EmbedConfig) of
        {ok, EmbedState} ->
            case init_rocksdb(DbPath, Config) of
                {ok, Db, CfHandles, Crypto} ->
                    %% Load or create HNSW index
                    HnswIndex = load_or_create_index(Db, CfHandles, Dimension, HnswConfig),

                    State = #state{
                        db = Db,
                        cf_vectors = maps:get(vectors, CfHandles),
                        cf_metadata = maps:get(metadata, CfHandles),
                        cf_text = maps:get(text, CfHandles),
                        cf_hnsw = maps:get(hnsw, CfHandles),
                        cf_diskann_ids_fwd = maps:get(diskann_ids_fwd, CfHandles),
                        cf_diskann_ids_rev = maps:get(diskann_ids_rev, CfHandles),
                        hnsw_index = HnswIndex,
                        dimension = Dimension,
                        embed_state = EmbedState,
                        config = Config,
                        crypto = Crypto
                    },
                    {ok, State};
                {error, Reason} ->
                    {stop, {db_open_failed, Reason}}
            end;
        {error, Reason} ->
            {stop, {embedder_init_failed, Reason}}
    end.

handle_call({add, ChunkId, Text, Metadata, VectorSpec}, _From, State) ->
    case get_vector_for_add(VectorSpec, Text, State) of
        {ok, Vector} ->
            case do_add(ChunkId, Text, Metadata, Vector, State) of
                {ok, NewState} ->
                    {reply, ok, NewState};
                {error, _} = Error ->
                    {reply, Error, State}
            end;
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({get, ChunkId}, _From, State) ->
    Result = do_get(ChunkId, State),
    {reply, Result, State};

handle_call({delete, ChunkId}, _From, State) ->
    case do_delete(ChunkId, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({search, Query, K, Options}, _From, State) ->
    Result = do_search(Query, K, Options, State),
    {reply, Result, State};

handle_call(count, _From, #state{hnsw_index = Index} = State) ->
    {reply, barrel_vectordb_hnsw:size(Index), State};

handle_call(info, _From, #state{hnsw_index = Index, dimension = Dim, config = Config} = State) ->
    Info = #{
        dimension => Dim,
        count => barrel_vectordb_hnsw:size(Index),
        hnsw => barrel_vectordb_hnsw:info(Index),
        config => Config
    },
    {reply, Info, State};

handle_call(rebuild_index, _From, State) ->
    case do_rebuild_index(State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call(get_diskann_db, _From, #state{db = Db, cf_diskann_ids_fwd = CfFwd,
                                           cf_diskann_ids_rev = CfRev} = State) ->
    {reply, {ok, {Db, CfFwd, CfRev}}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db = Db, hnsw_index = Index, cf_hnsw = CfHnsw}) ->
    %% Persist HNSW index metadata before closing
    _ = persist_hnsw_meta(Db, CfHnsw, Index),
    _ = rocksdb:close(Db),
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Initialize RocksDB with column families
init_rocksdb(DbPath, Config) ->
    %% Ensure directory exists
    ok = filelib:ensure_dir(DbPath ++ "/"),

    case barrel_vectordb_crypto:init(maps:get(crypto, Config, none),
                                     DbPath) of
        {ok, Crypto} ->
            Options0 = [{create_if_missing, true},
                        {create_missing_column_families, true}],
            Options = case Crypto of
                none -> Options0;
                #{env := Env} -> [{env, Env} | Options0]
            end,

            %% Define column families (including DiskANN ID mapping)
            CfDefs = [
                {?CF_DEFAULT, []},
                {?CF_VECTORS, []},
                {?CF_METADATA, []},
                {?CF_TEXT, []},
                {?CF_HNSW, []},
                {?CF_DISKANN_IDS_FWD, []},
                {?CF_DISKANN_IDS_REV, []}
            ],

            case rocksdb:open(DbPath, Options, CfDefs) of
                {ok, Db, [_Default, CfVectors, CfMetadata, CfText, CfHnsw,
                          CfDiskannIdsFwd, CfDiskannIdsRev]} ->
                    {ok, Db, #{
                        vectors => CfVectors,
                        metadata => CfMetadata,
                        text => CfText,
                        hnsw => CfHnsw,
                        diskann_ids_fwd => CfDiskannIdsFwd,
                        diskann_ids_rev => CfDiskannIdsRev
                    }, Crypto};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, _} = Err ->
            Err
    end.

%% Load existing index or create new one
load_or_create_index(Db, CfHandles, Dimension, HnswConfig) ->
    CfHnsw = maps:get(hnsw, CfHandles),
    CfVectors = maps:get(vectors, CfHandles),

    %% Try to load from persisted metadata
    case load_hnsw_meta(Db, CfHnsw) of
        {ok, IndexMeta} ->
            %% Rebuild index from stored data
            rebuild_from_meta(Db, IndexMeta, CfHnsw, CfVectors, Dimension, HnswConfig);
        not_found ->
            %% Create fresh index
            barrel_vectordb_hnsw:new(HnswConfig#{dimension => Dimension})
    end.

%% Load HNSW metadata from storage
load_hnsw_meta(Db, CfHnsw) ->
    case rocksdb:get(Db, CfHnsw, ?HNSW_META_KEY, []) of
        {ok, Binary} ->
            try
                Meta = binary_to_term(Binary),
                {ok, Meta}
            catch
                _:_ -> not_found
            end;
        not_found ->
            not_found;
        {error, _} ->
            not_found
    end.

%% Persist HNSW metadata
persist_hnsw_meta(Db, CfHnsw, Index) ->
    Meta = #{
        entry_point => Index#hnsw_index.entry_point,
        max_layer => Index#hnsw_index.max_layer,
        size => Index#hnsw_index.size,
        dimension => Index#hnsw_index.dimension
    },
    Binary = term_to_binary(Meta),
    rocksdb:put(Db, CfHnsw, ?HNSW_META_KEY, Binary, []).

%% Rebuild index from metadata
rebuild_from_meta(Db, _IndexMeta, CfHnsw, CfVectors, Dimension, HnswConfig) ->
    %% Rebuild by iterating all stored vectors
    Index = barrel_vectordb_hnsw:new(HnswConfig#{dimension => Dimension}),

    %% Iterate through all vectors and rebuild
    case rocksdb:iterator(Db, CfVectors, []) of
        {ok, Iter} ->
            try
                rebuild_loop(Db, Iter, rocksdb:iterator_move(Iter, first), CfHnsw, Index)
            after
                rocksdb:iterator_close(Iter)
            end;
        {error, _} ->
            Index
    end.

rebuild_loop(_Db, _Iter, {error, _}, _CfHnsw, Index) ->
    Index;
rebuild_loop(Db, Iter, {ok, Key, VectorBin}, CfHnsw, Index) ->
    Vector = decode_vector(VectorBin),
    NewIndex = barrel_vectordb_hnsw:insert(Index, Key, Vector),

    %% Persist node to HNSW graph
    _ = case barrel_vectordb_hnsw:get_node(NewIndex, Key) of
        {ok, Node} ->
            NodeBin = barrel_vectordb_hnsw:serialize_node(Node),
            rocksdb:put(Db, CfHnsw, Key, NodeBin, []);
        not_found ->
            ok
    end,

    rebuild_loop(Db, Iter, rocksdb:iterator_move(Iter, next), CfHnsw, NewIndex).

%% Get vector for add operation
get_vector_for_add(auto, Text, #state{dimension = Dim, embed_state = EmbedState}) ->
    %% Call embedding service
    case barrel_embed:embed(Text, EmbedState) of
        {ok, Vector} when length(Vector) =:= Dim ->
            {ok, Vector};
        {ok, Vector} ->
            {error, {dimension_mismatch, Dim, length(Vector)}};
        {error, _} = Error ->
            Error
    end;
get_vector_for_add({vector, Vector}, _Text, #state{dimension = Dim})
    when length(Vector) =:= Dim ->
    {ok, Vector};
get_vector_for_add({vector, Vector}, _Text, #state{dimension = Dim}) ->
    {error, {dimension_mismatch, Dim, length(Vector)}}.

%% Add a chunk
do_add(ChunkId, Text, Metadata, Vector, #state{db = Db, cf_vectors = CfV,
                                                cf_metadata = CfM, cf_text = CfT,
                                                cf_hnsw = CfH, hnsw_index = Index} = State) ->
    %% Encode data
    VectorBin = encode_vector(Vector),
    MetadataBin = term_to_binary(Metadata),

    %% Write to RocksDB using a batch
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, CfV, ChunkId, VectorBin),
    ok = rocksdb:batch_put(Batch, CfM, ChunkId, MetadataBin),
    ok = rocksdb:batch_put(Batch, CfT, ChunkId, Text),

    case rocksdb:write_batch(Db, Batch, []) of
        ok ->
            %% Update HNSW index
            NewIndex = barrel_vectordb_hnsw:insert(Index, ChunkId, Vector),

            %% Persist HNSW node
            _ = case barrel_vectordb_hnsw:get_node(NewIndex, ChunkId) of
                {ok, Node} ->
                    NodeBin = barrel_vectordb_hnsw:serialize_node(Node),
                    rocksdb:put(Db, CfH, ChunkId, NodeBin, []);
                not_found ->
                    ok
            end,

            {ok, State#state{hnsw_index = NewIndex}};
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%% Get a chunk
do_get(ChunkId, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM, cf_text = CfT}) ->
    case rocksdb:get(Db, CfV, ChunkId, []) of
        {ok, VectorBin} ->
            case {rocksdb:get(Db, CfM, ChunkId, []), rocksdb:get(Db, CfT, ChunkId, [])} of
                {{ok, MetadataBin}, {ok, Text}} ->
                    {ok, #{
                        key => ChunkId,
                        vector => decode_vector(VectorBin),
                        metadata => binary_to_term(MetadataBin),
                        text => Text
                    }};
                _ ->
                    {error, incomplete_data}
            end;
        not_found ->
            not_found;
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%% Delete a chunk
do_delete(ChunkId, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM,
                          cf_text = CfT, cf_hnsw = CfH, hnsw_index = Index} = State) ->
    %% Delete from RocksDB
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_delete(Batch, CfV, ChunkId),
    ok = rocksdb:batch_delete(Batch, CfM, ChunkId),
    ok = rocksdb:batch_delete(Batch, CfT, ChunkId),
    ok = rocksdb:batch_delete(Batch, CfH, ChunkId),

    case rocksdb:write_batch(Db, Batch, []) of
        ok ->
            %% Update HNSW index
            NewIndex = barrel_vectordb_hnsw:delete(Index, ChunkId),
            {ok, State#state{hnsw_index = NewIndex}};
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%% Search for similar chunks
do_search(Query, K, Options, #state{dimension = Dim, embed_state = EmbedState} = State)
    when is_binary(Query) ->
    %% Text query - need to embed first
    case barrel_embed:embed(Query, EmbedState) of
        {ok, Vector} when length(Vector) =:= Dim ->
            do_search(Vector, K, Options, State);
        {ok, Vector} ->
            {error, {dimension_mismatch, Dim, length(Vector)}};
        {error, _} = Error ->
            Error
    end;
do_search(QueryVector, K, Options, #state{db = Db, hnsw_index = Index,
                                           cf_metadata = CfM, cf_text = CfT})
    when is_list(QueryVector) ->
    %% Search HNSW index
    HnswResults = barrel_vectordb_hnsw:search(Index, QueryVector, K, Options),

    %% Apply optional filter and fetch metadata/text
    Filter = maps:get(filter, Options, fun(_) -> true end),

    Results = lists:filtermap(
        fun({ChunkId, Distance}) ->
            case {rocksdb:get(Db, CfM, ChunkId, []), rocksdb:get(Db, CfT, ChunkId, [])} of
                {{ok, MetadataBin}, {ok, Text}} ->
                    Metadata = binary_to_term(MetadataBin),
                    case Filter(Metadata) of
                        true ->
                            {true, #{
                                key => ChunkId,
                                text => Text,
                                metadata => Metadata,
                                score => 1.0 - Distance  %% Convert distance to similarity
                            }};
                        false ->
                            false
                    end;
                _ ->
                    false
            end
        end,
        HnswResults
    ),

    {ok, Results}.

%% Rebuild the entire index
do_rebuild_index(#state{db = Db, cf_vectors = CfV, dimension = Dim, config = Config} = State) ->
    HnswConfig = maps:get(hnsw, Config, #{}),
    NewIndex = barrel_vectordb_hnsw:new(HnswConfig#{dimension => Dim}),

    case rocksdb:iterator(Db, CfV, []) of
        {ok, Iter} ->
            try
                FinalIndex = rebuild_index_loop(Iter, rocksdb:iterator_move(Iter, first), NewIndex),
                {ok, State#state{hnsw_index = FinalIndex}}
            after
                rocksdb:iterator_close(Iter)
            end;
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

rebuild_index_loop(_Iter, {error, _}, Index) ->
    Index;
rebuild_index_loop(Iter, {ok, Key, VectorBin}, Index) ->
    Vector = decode_vector(VectorBin),
    NewIndex = barrel_vectordb_hnsw:insert(Index, Key, Vector),
    rebuild_index_loop(Iter, rocksdb:iterator_move(Iter, next), NewIndex).

%% Vector encoding/decoding (64-bit floats for full precision)
encode_vector(Vector) when is_list(Vector) ->
    << <<F:64/float-little>> || F <- Vector >>.

decode_vector(Binary) ->
    [F || <<F:64/float-little>> <= Binary].
