%%%-------------------------------------------------------------------
%%% @doc Local Python embedding provider
%%%
%%% Uses a Python port with sentence-transformers for CPU-based embeddings.
%%% No GPU required, runs entirely on CPU.
%%%
%%% Dependencies (sentence-transformers) are installed automatically
%%% in the managed venv on first use.
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     model => "BAAI/bge-base-en-v1.5",        %% Model name (default, 768 dims)
%%%     python => "python3",                     %% Python executable (default)
%%%     timeout => 120000                        %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Supported Models ==
%%% Any model from sentence-transformers or HuggingFace.
%%%
%%% Common models:
%%% - `"BAAI/bge-base-en-v1.5"' - Default, 768 dimensions, good quality/speed
%%% - `"BAAI/bge-small-en-v1.5"' - 384 dimensions, faster
%%% - `"BAAI/bge-large-en-v1.5"' - 1024 dimensions, best quality
%%% - `"sentence-transformers/all-MiniLM-L6-v2"' - 384 dims, fast
%%% - `"sentence-transformers/all-mpnet-base-v2"' - 768 dims, high quality
%%% - `"nomic-ai/nomic-embed-text-v1.5"' - 768 dims, long context
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_local).
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
-define(DEFAULT_MODEL, "BAAI/bge-base-en-v1.5").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_DIMENSION, 768).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> local.

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
    Venv = get_managed_venv(local),

    %% Build args for python -m barrel_embed
    Args = ["-m", "barrel_embed",
            "--provider", "sentence_transformers",
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
        {ok, [Vector]} when is_list(Vector), length(Vector) > 0 ->
            {ok, Vector};
        {ok, [[]]} ->
            {error, {empty_embedding, Text}};
        {ok, []} ->
            {error, {no_embedding, Text}};
        {ok, Other} ->
            {error, {unexpected_embedding, Other}};
        {error, _} = Error ->
            Error
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
%% Get managed venv path and install deps for provider
get_managed_venv(Provider) ->
    case application:get_env(barrel_embed, managed_venv_path) of
        {ok, Path} ->
            _ = barrel_embed_venv:install_deps(Provider),
            Path;
        undefined ->
            undefined
    end.
