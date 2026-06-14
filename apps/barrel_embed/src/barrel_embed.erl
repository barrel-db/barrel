%%%-------------------------------------------------------------------
%%% @doc Embedding coordinator
%%%
%%% Lightweight embedding generation for Erlang.
%%% Manages a chain of embedding providers with automatic fallback.
%%%
%%% == Providers ==
%%% Local:
%%% - `local': Local Python with sentence-transformers (CPU, no external calls)
%%% - `ollama': Local Ollama server
%%% - `fastembed': FastEmbed ONNX-based embeddings (lighter than sentence-transformers)
%%%
%%% Cloud:
%%% - `openai': OpenAI Embeddings API
%%% - `cohere': Cohere Embed API
%%% - `voyage': Voyage AI Embeddings
%%% - `jina': Jina AI Embeddings
%%% - `mistral': Mistral AI Embeddings (EU data residency)
%%% - `azure': Azure OpenAI Embeddings
%%% - `bedrock': AWS Bedrock Embeddings (Titan, Cohere)
%%% - `vertex': Google Vertex AI Embeddings
%%%
%%% Specialized:
%%% - `splade': SPLADE sparse embeddings for hybrid search
%%% - `colbert': ColBERT multi-vector embeddings for fine-grained matching
%%% - `clip': CLIP image/text cross-modal embeddings
%%%
%%% == Configuration ==
%%% ```
%%% %% Local Python embeddings (requires Python + sentence-transformers)
%%% Embedder = {local, #{
%%%     python => "python3",
%%%     model => "BAAI/bge-base-en-v1.5"
%%% }}.
%%%
%%% %% Ollama server
%%% Embedder = {ollama, #{
%%%     url => <<"http://localhost:11434">>,
%%%     model => <<"nomic-embed-text">>
%%% }}.
%%%
%%% %% OpenAI API (requires API key)
%%% Embedder = {openai, #{
%%%     api_key => <<"sk-...">>,  %% or set OPENAI_API_KEY env var
%%%     model => <<"text-embedding-3-small">>
%%% }}.
%%%
%%% %% Provider chain with fallback
%%% Embedder = [
%%%     {ollama, #{url => <<"http://localhost:11434">>}},
%%%     {local, #{}}  %% fallback to CPU
%%% ].
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed).

-define(DEFAULT_DIMENSION, 768).
-define(DEFAULT_BATCH_SIZE, 32).

%% API
-export([
    init/1,
    embed/2,
    embed_batch/2,
    embed_batch/3,
    dimension/1,
    info/1
]).

%% Venv management
-export([
    refresh_venv/0,
    install_provider/1,
    venv_path/0,
    has_uvloop/0
]).

%% Types
-type provider() :: {atom(), map()}.
-type provider_chain() :: [provider()] | provider().
-type embed_state() :: #{
    providers := [provider()],
    dimension := pos_integer(),
    batch_size := pos_integer()
}.

-export_type([provider/0, provider_chain/0, embed_state/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Initialize embedding state from configuration.
%%
%% If no `embedder' key is present in Config, returns `{ok, undefined}'.
%%
%% @param Config Map with optional embedder configuration
%% @returns `{ok, embed_state()}', `{ok, undefined}', or `{error, term()}'
-spec init(map()) -> {ok, embed_state() | undefined} | {error, term()}.
init(Config) ->
    case maps:get(embedder, Config, undefined) of
        undefined ->
            {ok, undefined};
        EmbedderConfig ->
            init_with_embedder(EmbedderConfig, Config)
    end.

%% @private
init_with_embedder(EmbedderConfig, Config) ->
    Dimension = maps:get(dimensions, Config, ?DEFAULT_DIMENSION),
    BatchSize = maps:get(batch_size, Config, ?DEFAULT_BATCH_SIZE),

    %% Normalize to provider chain
    Providers = normalize_providers(EmbedderConfig),

    %% Initialize each provider
    InitializedProviders = lists:filtermap(
        fun({ProviderName, ProviderConfig}) ->
            Module = provider_module(ProviderName),
            case init_provider(Module, ProviderConfig) of
                {ok, NewConfig} ->
                    {true, {Module, NewConfig}};
                {error, Reason} ->
                    error_logger:warning_msg(
                        "Failed to initialize provider ~p: ~p~n",
                        [ProviderName, Reason]
                    ),
                    false
            end
        end,
        Providers
    ),

    case InitializedProviders of
        [] ->
            {error, no_providers_available};
        _ ->
            {ok, #{
                providers => InitializedProviders,
                dimension => Dimension,
                batch_size => BatchSize
            }}
    end.

%% @doc Generate embedding for a single text.
-spec embed(binary(), embed_state() | undefined) -> {ok, [float()]} | {error, term()}.
embed(_Text, undefined) ->
    {error, embedder_not_configured};
embed(Text, #{providers := Providers}) when is_binary(Text) ->
    try_providers_embed(Providers, Text);
embed(Text, State) when is_list(Text) ->
    embed(list_to_binary(Text), State).

%% @doc Generate embeddings for multiple texts.
-spec embed_batch([binary()], embed_state() | undefined) -> {ok, [[float()]]} | {error, term()}.
embed_batch(_Texts, undefined) ->
    {error, embedder_not_configured};
embed_batch(Texts, State) ->
    embed_batch(Texts, #{}, State).

%% @doc Generate embeddings for multiple texts with options.
-spec embed_batch([binary()], map(), embed_state() | undefined) -> {ok, [[float()]]} | {error, term()}.
embed_batch(_Texts, _Options, undefined) ->
    {error, embedder_not_configured};
embed_batch(Texts, Options, #{providers := Providers, batch_size := DefaultBatchSize}) ->
    BatchSize = maps:get(batch_size, Options, DefaultBatchSize),
    try_providers_embed_batch(Providers, Texts, BatchSize).

%% @doc Get the dimension of embeddings.
-spec dimension(embed_state() | undefined) -> pos_integer() | undefined.
dimension(undefined) ->
    undefined;
dimension(#{dimension := Dimension}) ->
    Dimension.

%% @doc Get information about the current embedding configuration.
-spec info(embed_state() | undefined) -> map().
info(undefined) ->
    #{configured => false};
info(#{providers := Providers, dimension := Dimension}) ->
    ProviderInfo = [
        #{module => Module, name => Module:name()}
        || {Module, _Config} <- Providers
    ],
    #{
        configured => true,
        providers => ProviderInfo,
        dimension => Dimension
    }.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Normalize embedder config to provider chain
normalize_providers({Name, Config}) when is_atom(Name) ->
    [{Name, Config}];
normalize_providers(Providers) when is_list(Providers) ->
    Providers.

%% Map provider name to module
provider_module(local) -> barrel_embed_local;
provider_module(ollama) -> barrel_embed_ollama;
provider_module(openai) -> barrel_embed_openai;
provider_module(fastembed) -> barrel_embed_fastembed;
provider_module(splade) -> barrel_embed_splade;
provider_module(colbert) -> barrel_embed_colbert;
provider_module(clip) -> barrel_embed_clip;
%% Cloud providers
provider_module(cohere) -> barrel_embed_cohere;
provider_module(voyage) -> barrel_embed_voyage;
provider_module(jina) -> barrel_embed_jina;
provider_module(mistral) -> barrel_embed_mistral;
provider_module(azure) -> barrel_embed_azure;
provider_module(bedrock) -> barrel_embed_bedrock;
provider_module(vertex) -> barrel_embed_vertex;
provider_module(Module) when is_atom(Module) -> Module.

%% Initialize a provider
init_provider(Module, Config) ->
    %% Ensure module is loaded before checking exports
    code:ensure_loaded(Module),
    case erlang:function_exported(Module, init, 1) of
        true ->
            try
                Module:init(Config)
            catch
                _:Reason -> {error, Reason}
            end;
        false ->
            {ok, Config}
    end.

%% Try providers in order for single embed
try_providers_embed([], _Text) ->
    {error, all_providers_failed};
try_providers_embed([{Module, Config} | Rest], Text) ->
    case barrel_embed_provider:check_available(Module, Config) of
        true ->
            case barrel_embed_provider:call_embed(Module, Text, Config) of
                {ok, Vector} ->
                    {ok, Vector};
                {error, Reason} ->
                    error_logger:info_msg(
                        "Provider ~p failed: ~p, trying next~n",
                        [Module, Reason]
                    ),
                    try_providers_embed(Rest, Text)
            end;
        false ->
            error_logger:info_msg("Provider ~p not available, skipping~n", [Module]),
            try_providers_embed(Rest, Text)
    end.

%% Try providers in order for batch embed
try_providers_embed_batch([], _Texts, _BatchSize) ->
    {error, all_providers_failed};
try_providers_embed_batch([{Module, Config} | Rest], Texts, BatchSize) ->
    case barrel_embed_provider:check_available(Module, Config) of
        true ->
            case do_batch_embed(Module, Config, Texts, BatchSize) of
                {ok, Vectors} ->
                    {ok, Vectors};
                {error, Reason} ->
                    error_logger:info_msg(
                        "Provider ~p batch failed: ~p, trying next~n",
                        [Module, Reason]
                    ),
                    try_providers_embed_batch(Rest, Texts, BatchSize)
            end;
        false ->
            try_providers_embed_batch(Rest, Texts, BatchSize)
    end.

%% Execute batch embedding with chunking
do_batch_embed(Module, Config, Texts, BatchSize) ->
    Batches = chunk_list(Texts, BatchSize),
    do_batch_embed_loop(Module, Config, Batches, []).

do_batch_embed_loop(_Module, _Config, [], Acc) ->
    {ok, lists:append(lists:reverse(Acc))};
do_batch_embed_loop(Module, Config, [Batch | Rest], Acc) ->
    case barrel_embed_provider:call_embed_batch(Module, Batch, Config) of
        {ok, Vectors} ->
            do_batch_embed_loop(Module, Config, Rest, [Vectors | Acc]);
        {error, _} = Error ->
            Error
    end.

%% Split list into chunks
chunk_list(List, Size) ->
    chunk_list(List, Size, []).

chunk_list([], _Size, Acc) ->
    lists:reverse(Acc);
chunk_list(List, Size, Acc) ->
    {Chunk, Rest} = safe_split(Size, List),
    chunk_list(Rest, Size, [Chunk | Acc]).

safe_split(N, List) when length(List) =< N ->
    {List, []};
safe_split(N, List) ->
    lists:split(N, List).

%%====================================================================
%% Venv Management Functions
%%====================================================================

%% @doc Refresh the managed venv.
%% Deletes and recreates the venv with base dependencies.
-spec refresh_venv() -> {ok, string()} | {error, term()}.
refresh_venv() ->
    barrel_embed_venv:refresh().

%% @doc Install dependencies for a specific provider.
%% @param Provider The provider atom (fastembed, local, splade, colbert, clip)
-spec install_provider(atom()) -> ok | {error, term()}.
install_provider(Provider) ->
    barrel_embed_venv:install_deps(Provider).

%% @doc Get the managed venv path.
-spec venv_path() -> string().
venv_path() ->
    barrel_embed_venv:venv_path().

%% @doc Check if uvloop is installed in the managed venv.
-spec has_uvloop() -> boolean().
has_uvloop() ->
    barrel_embed_venv:has_uvloop().
