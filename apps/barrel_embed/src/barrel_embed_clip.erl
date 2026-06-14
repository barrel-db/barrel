%%%-------------------------------------------------------------------
%%% @doc CLIP image/text embedding provider
%%%
%%% Uses CLIP (Contrastive Language-Image Pre-training) models for
%%% cross-modal embeddings. Both images and text are encoded into the
%%% same vector space, enabling image-text similarity search.
%%%
%%% Dependencies (transformers, torch, pillow) are installed automatically
%%% in the managed venv on first use.
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     model => "openai/clip-vit-base-patch32",   %% Model name (default)
%%%     python => "python3",                       %% Python executable (default)
%%%     timeout => 120000                          %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Cross-Modal Search ==
%%% CLIP enables searching images with text queries and vice versa:
%%% ```
%%% %% Embed an image
%%% {ok, ImgVec} = embed_image(ImageBase64, Config),
%%%
%%% %% Embed a text query (in same space!)
%%% {ok, TextVec} = embed(<<"a photo of a cat">>, Config),
%%%
%%% %% Now you can compare ImgVec and TextVec with cosine similarity
%%% '''
%%%
%%% == Supported Models ==
%%% - `"openai/clip-vit-base-patch32"' - Default, 512 dimensions, fast
%%% - `"openai/clip-vit-base-patch16"' - 512 dimensions, higher quality
%%% - `"openai/clip-vit-large-patch14"' - 768 dimensions, best quality
%%% - `"laion/CLIP-ViT-B-32-laion2B-s34B-b79K"' - 512 dims, LAION trained
%%%
%%% == Use Cases ==
%%% - Image search with text queries
%%% - Finding similar images
%%% - Multi-modal content retrieval
%%% - Zero-shot image classification
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_clip).
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

%% Image embedding API
-export([
    embed_image/2,
    embed_image_batch/2
]).

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "openai/clip-vit-base-patch32").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_DIMENSION, 512).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> clip.

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
    Venv = get_managed_venv(clip),

    %% Validate model (warning only)
    validate_model(Model),

    %% Build args for python -m barrel_embed
    Args = ["-m", "barrel_embed",
            "--provider", "clip",
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

%% @doc Generate text embedding (for cross-modal search).
%% Text embeddings are in the same space as image embeddings.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    case embed_batch([Text], Config) of
        {ok, [Embedding]} -> {ok, Embedding};
        {error, _} = Error -> Error
    end.

%% @doc Generate text embeddings for batch.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, #{server := Server, timeout := Timeout}) ->
    barrel_embed_port_server:embed_batch(Server, Texts, Timeout);
embed_batch(_Texts, _Config) ->
    {error, server_not_initialized}.

%%====================================================================
%% Image Embedding API
%%====================================================================

%% @doc Generate embedding for a single image.
%% Image should be base64-encoded.
-spec embed_image(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed_image(ImageBase64, Config) ->
    case embed_image_batch([ImageBase64], Config) of
        {ok, [Embedding]} -> {ok, Embedding};
        {error, _} = Error -> Error
    end.

%% @doc Generate embeddings for multiple images.
%% Images should be base64-encoded.
-spec embed_image_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_image_batch(Images, #{server := Server, timeout := Timeout}) ->
    barrel_embed_port_server:embed_image_batch(Server, Images, Timeout);
embed_image_batch(_Images, _Config) ->
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
                "It may still work if it's a valid CLIP model.~n",
                [ModelBin]
            )
    end.

%% @private
is_known_model(<<"openai/clip-vit-base-patch32">>) -> true;
is_known_model(<<"openai/clip-vit-base-patch16">>) -> true;
is_known_model(<<"openai/clip-vit-large-patch14">>) -> true;
is_known_model(<<"laion/CLIP-ViT-B-32-laion2B-s34B-b79K">>) -> true;
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
