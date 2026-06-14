%%%-------------------------------------------------------------------
%%% @doc ColBERT late interaction embedding provider
%%%
%%% Uses ColBERT models for multi-vector embeddings. Each document
%%% produces multiple vectors (one per token) for fine-grained matching.
%%%
%%% Dependencies (transformers, torch) are installed automatically
%%% in the managed venv on first use.
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     model => "colbert-ir/colbertv2.0",     %% Model name (default, 128 dims)
%%%     python => "python3",                   %% Python executable (default)
%%%     timeout => 120000                      %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Multi-Vector Format ==
%%% Unlike single-vector embeddings, ColBERT produces a list of vectors:
%%% ```
%%% [[0.1, 0.2, ...], [0.3, 0.4, ...], ...]  %% One vector per token
%%% '''
%%%
%%% == Late Interaction ==
%%% ColBERT scoring uses MaxSim:
%%% ```
%%% Score(Q, D) = sum(max(qi · dj for all dj in D) for all qi in Q)
%%% '''
%%% This enables fine-grained token-level matching.
%%%
%%% == Supported Models ==
%%% - `"colbert-ir/colbertv2.0"' - Default, 128 dimensions
%%% - `"answerdotai/answerai-colbert-small-v1"' - 96 dimensions, smaller
%%% - `"jinaai/jina-colbert-v2"' - 128 dimensions, long context (8192 tokens)
%%%
%%% == Use Cases ==
%%% - Fine-grained semantic matching
%%% - Passage retrieval with token-level scoring
%%% - Question answering
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_colbert).
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

%% Multi-vector API
-export([
    embed_multi/2,
    embed_batch_multi/2,
    maxsim_score/2
]).

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "colbert-ir/colbertv2.0").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_DIMENSION, 128).

%% Multi-vector type: list of token vectors
-type multi_vector() :: [[float()]].

-export_type([multi_vector/0]).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> colbert.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    Python = maps:get(python, Config, ?DEFAULT_PYTHON),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Use managed venv, auto-install deps
    Venv = get_managed_venv(colbert),

    %% Validate model (warning only)
    validate_model(Model),

    %% Build args for python -m barrel_embed
    Args = ["-m", "barrel_embed",
            "--provider", "colbert",
            "--model", Model],

    Opts = [
        {timeout, Timeout},
        {priv_dir, get_priv_dir()},
        {venv, Venv}
    ],

    case barrel_embed_port_server:start_link(Python, Args, Opts) of
        {ok, Server} ->
            case barrel_embed_port_server:info(Server, Timeout) of
                {ok, #{dimensions := Dims}} ->
                    {ok, Config#{
                        server => Server,
                        dimension => Dims,
                        timeout => Timeout
                    }};
                {ok, _} ->
                    %% No dimensions in response, use default
                    {ok, Config#{
                        server => Server,
                        dimension => ?DEFAULT_DIMENSION,
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

%% @doc Generate single-vector embedding (mean pooling of token vectors).
%% Note: For ColBERT, use embed_multi/2 to get full multi-vector output.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    case embed_multi(Text, Config) of
        {ok, MultiVec} ->
            {ok, mean_pool(MultiVec)};
        {error, _} = Error ->
            Error
    end.

%% @doc Generate single-vector embeddings for batch (mean pooling).
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    case embed_batch_multi(Texts, Config) of
        {ok, MultiVecs} ->
            {ok, [mean_pool(MV) || MV <- MultiVecs]};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Multi-Vector API
%%====================================================================

%% @doc Generate multi-vector embedding for a single text.
%% Returns a list of token vectors.
-spec embed_multi(binary(), map()) -> {ok, multi_vector()} | {error, term()}.
embed_multi(Text, Config) ->
    case embed_batch_multi([Text], Config) of
        {ok, [MultiVec]} -> {ok, MultiVec};
        {error, _} = Error -> Error
    end.

%% @doc Generate multi-vector embeddings for multiple texts.
-spec embed_batch_multi([binary()], map()) -> {ok, [multi_vector()]} | {error, term()}.
embed_batch_multi(Texts, #{server := Server, timeout := Timeout}) ->
    barrel_embed_port_server:embed_multi_batch(Server, Texts, Timeout);
embed_batch_multi(_Texts, _Config) ->
    {error, server_not_initialized}.

%% @doc Calculate MaxSim score between query and document multi-vectors.
%% This is the standard ColBERT scoring function.
%% Score = sum(max(qi · dj for all dj in D) for all qi in Q)
-spec maxsim_score(multi_vector(), multi_vector()) -> float().
maxsim_score(QueryVecs, DocVecs) ->
    lists:sum([max_dot_product(QVec, DocVecs) || QVec <- QueryVecs]).

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
                "It may still work if it's a valid ColBERT model.~n",
                [ModelBin]
            )
    end.

%% @private
is_known_model(<<"colbert-ir/colbertv2.0">>) -> true;
is_known_model(<<"answerdotai/answerai-colbert-small-v1">>) -> true;
is_known_model(<<"jinaai/jina-colbert-v2">>) -> true;
is_known_model(_) -> false.

%% @private
to_binary(S) when is_binary(S) -> S;
to_binary(S) when is_list(S) -> list_to_binary(S).

%% @private
%% Mean pooling of token vectors to get single vector
mean_pool([]) -> [];
mean_pool(Vectors) ->
    N = length(Vectors),
    Dim = length(hd(Vectors)),
    %% Sum all vectors element-wise
    Sums = lists:foldl(
        fun(Vec, Acc) ->
            lists:zipwith(fun(A, B) -> A + B end, Vec, Acc)
        end,
        lists:duplicate(Dim, 0.0),
        Vectors
    ),
    %% Divide by N
    [S / N || S <- Sums].

%% @private
%% Find maximum dot product between query vector and all doc vectors
max_dot_product(QueryVec, DocVecs) ->
    DotProducts = [dot_product(QueryVec, DocVec) || DocVec <- DocVecs],
    lists:max(DotProducts).

%% @private
dot_product(V1, V2) ->
    lists:sum(lists:zipwith(fun(A, B) -> A * B end, V1, V2)).

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
