%%%-------------------------------------------------------------------
%%% @doc SPLADE sparse embedding provider
%%%
%%% Uses SPLADE (Sparse Lexical and Expansion) models for neural sparse
%%% embeddings. Produces sparse vectors suitable for inverted index search.
%%%
%%% Dependencies (transformers, torch) are installed automatically
%%% in the managed venv on first use.
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     model => "prithivida/Splade_PP_en_v1",   %% Model name (default)
%%%     python => "python3",                     %% Python executable (default)
%%%     timeout => 120000                        %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Sparse Vector Format ==
%%% Unlike dense embeddings, SPLADE produces sparse vectors:
%%% ```
%%% #{indices => [1, 5, 10], values => [0.5, 0.3, 0.8]}
%%% '''
%%% Where indices are vocabulary token IDs and values are weights.
%%%
%%% == Supported Models ==
%%% - `"prithivida/Splade_PP_en_v1"' - Default, SPLADE++ English
%%% - `"naver/splade-cocondenser-ensembledistil"' - NAVER's SPLADE
%%%
%%% == Use Cases ==
%%% - Lexical-semantic hybrid search
%%% - Term expansion (captures synonyms and related terms)
%%% - Efficient inverted index storage
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_splade).
-behaviour(barrel_embed_provider).

%% Behaviour callbacks
-export([
    embed/2,
    embed_batch/2,
    dimension/1,
    name/0,
    init/1,
    available/1
]).

%% Additional exports for sparse vectors
-export([
    embed_sparse/2,
    embed_batch_sparse/2
]).

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "prithivida/Splade_PP_en_v1").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_VOCAB_SIZE, 30522).

%% Sparse vector type
-type sparse_vector() :: #{
    indices := [non_neg_integer()],
    values := [float()]
}.

-export_type([sparse_vector/0]).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> splade.

%% @doc Get dimension (vocab size) for this provider.
%% For sparse vectors, dimension is the vocabulary size.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(vocab_size, Config, ?DEFAULT_VOCAB_SIZE).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    Python = maps:get(python, Config, ?DEFAULT_PYTHON),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Use managed venv, auto-install deps
    Venv = get_managed_venv(splade),

    %% Validate model (warning only)
    validate_model(Model),

    %% Build args for python -m barrel_embed
    Args = ["-m", "barrel_embed",
            "--provider", "splade",
            "--model", Model],

    Opts = [
        {timeout, Timeout},
        {priv_dir, get_priv_dir()},
        {venv, Venv}
    ],

    case barrel_embed_port_server:start_link(Python, Args, Opts) of
        {ok, Server} ->
            case barrel_embed_port_server:info(Server, Timeout) of
                {ok, #{vocab_size := VocabSize}} ->
                    {ok, Config#{
                        server => Server,
                        vocab_size => VocabSize,
                        timeout => Timeout
                    }};
                {ok, _} ->
                    %% No vocab_size in response, use default
                    {ok, Config#{
                        server => Server,
                        vocab_size => ?DEFAULT_VOCAB_SIZE,
                        timeout => Timeout
                    }};
                {error, Reason} ->
                    barrel_embed_port_server:stop(Server),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Check if provider is available.
-spec available(map()) -> boolean().
available(#{server := Server}) ->
    is_process_alive(Server);
available(_Config) ->
    false.

%% @doc Generate dense embedding (converts sparse to dense).
%% Note: This is inefficient for large vocab sizes. Use embed_sparse/2 instead.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    case embed_sparse(Text, Config) of
        {ok, SparseVec} ->
            {ok, sparse_to_dense(SparseVec, dimension(Config))};
        {error, _} = Error ->
            Error
    end.

%% @doc Generate dense embeddings for batch (converts sparse to dense).
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    case embed_batch_sparse(Texts, Config) of
        {ok, SparseVecs} ->
            Dim = dimension(Config),
            DenseVecs = [sparse_to_dense(S, Dim) || S <- SparseVecs],
            {ok, DenseVecs};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Sparse Vector API
%%====================================================================

%% @doc Generate sparse embedding for a single text.
-spec embed_sparse(binary(), map()) -> {ok, sparse_vector()} | {error, term()}.
embed_sparse(Text, Config) ->
    case embed_batch_sparse([Text], Config) of
        {ok, [SparseVec]} -> {ok, SparseVec};
        {error, _} = Error -> Error
    end.

%% @doc Generate sparse embeddings for multiple texts.
-spec embed_batch_sparse([binary()], map()) -> {ok, [sparse_vector()]} | {error, term()}.
embed_batch_sparse(Texts, #{server := Server, timeout := Timeout}) ->
    case barrel_embed_port_server:embed_sparse_batch(Server, Texts, Timeout) of
        {ok, Embeddings} ->
            SparseVecs = [parse_sparse_vec(E) || E <- Embeddings],
            {ok, SparseVecs};
        {error, _} = Error ->
            Error
    end;
embed_batch_sparse(_Texts, _Config) ->
    {error, server_not_initialized}.

%%====================================================================
%% Internal Functions
%%====================================================================

get_priv_dir() ->
    case code:priv_dir(barrel_embed) of
        {error, bad_name} -> "priv";
        Dir -> Dir
    end.

%% @private
validate_model(Model) ->
    ModelBin = to_binary(Model),
    case is_known_model(ModelBin) of
        true -> ok;
        false ->
            error_logger:warning_msg(
                "Model ~s is not in the known list. "
                "It may still work if it's a valid SPLADE model.~n",
                [ModelBin]
            )
    end.

%% @private
is_known_model(<<"prithivida/Splade_PP_en_v1">>) -> true;
is_known_model(<<"naver/splade-cocondenser-ensembledistil">>) -> true;
is_known_model(_) -> false.

%% @private
to_binary(S) when is_binary(S) -> S;
to_binary(S) when is_list(S) -> list_to_binary(S).

%% @private
parse_sparse_vec(#{<<"indices">> := Indices, <<"values">> := Values}) ->
    #{indices => Indices, values => Values};
parse_sparse_vec(#{indices := _, values := _} = Vec) ->
    Vec.

%% @private
%% Convert sparse vector to dense (for compatibility with dense search)
sparse_to_dense(#{indices := Indices, values := Values}, Dim) ->
    %% Initialize zero vector
    Dense = array:new(Dim, {default, 0.0}),
    %% Set non-zero values
    Dense1 = lists:foldl(
        fun({Idx, Val}, Arr) ->
            array:set(Idx, Val, Arr)
        end,
        Dense,
        lists:zip(Indices, Values)
    ),
    array:to_list(Dense1).

%% @private
%% Get managed venv path and install deps for provider
get_managed_venv(Provider) ->
    case application:get_env(barrel_embed, managed_venv_path) of
        {ok, Path} ->
            _ = barrel_embed_venv:install_deps(Provider),
            Path;
        undefined ->
            undefined
    end.
