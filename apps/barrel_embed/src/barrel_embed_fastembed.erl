%%%-------------------------------------------------------------------
%%% @doc FastEmbed embedding provider
%%%
%%% Uses FastEmbed (ONNX-based) for lightweight, fast embeddings.
%%% Lighter alternative to sentence-transformers with similar quality.
%%%
%%% Dependencies (fastembed) are installed automatically
%%% in the managed venv on first use.
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     model => "BAAI/bge-small-en-v1.5",       %% Model name (default, 384 dims)
%%%     python => "python3",                     %% Python executable (default)
%%%     timeout => 120000                        %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Advantages over sentence-transformers ==
%%% - Smaller install size (~100MB vs ~2GB+)
%%% - No PyTorch dependency
%%% - Uses ONNX Runtime for optimized inference
%%% - Similar embedding quality
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_fastembed).
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

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "BAAI/bge-small-en-v1.5").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_DIMENSION, 384).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> fastembed.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
%% Starts the Python port server.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    Python = maps:get(python, Config, ?DEFAULT_PYTHON),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Use managed venv, auto-install deps
    Venv = get_managed_venv(fastembed),

    %% Validate model (warning only)
    validate_model(Model),

    %% Build args for python -m barrel_embed
    Args = ["-m", "barrel_embed",
            "--provider", "fastembed",
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

%% @doc Generate embedding for a single text.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    case embed_batch([Text], Config) of
        {ok, [Vector]} -> {ok, Vector};
        {error, _} = Error -> Error
    end.

%% @doc Generate embeddings for multiple texts.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, #{server := Server, timeout := Timeout}) ->
    barrel_embed_port_server:embed_batch(Server, Texts, Timeout);
embed_batch(_Texts, _Config) ->
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
%% Validate model (warning only)
validate_model(Model) ->
    ModelBin = to_binary(Model),
    %% Just log a warning for unknown models
    case is_known_model(ModelBin) of
        true ->
            ok;
        false ->
            error_logger:warning_msg(
                "Model ~s is not in the known list. "
                "It may still work if supported by FastEmbed.~n",
                [ModelBin]
            )
    end.

%% @private
%% Check if model is in known list (basic check)
is_known_model(<<"BAAI/bge-small-en-v1.5">>) -> true;
is_known_model(<<"BAAI/bge-base-en-v1.5">>) -> true;
is_known_model(<<"BAAI/bge-large-en-v1.5">>) -> true;
is_known_model(<<"sentence-transformers/all-MiniLM-L6-v2">>) -> true;
is_known_model(<<"nomic-ai/nomic-embed-text-v1.5">>) -> true;
is_known_model(_) -> false.

%% @private
to_binary(S) when is_binary(S) -> S;
to_binary(S) when is_list(S) -> list_to_binary(S).

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
