%%%-------------------------------------------------------------------
%%% @doc Google Vertex AI embedding provider
%%%
%%% Uses Google Vertex AI's embedding API for embedding generation.
%%% Provides access to Google's embedding models including Gecko and newer models.
%%%
%%% == Requirements ==
%%% - A Google Cloud project with Vertex AI API enabled
%%% - Authentication via access token or API key
%%%
%%% === Option 1: Access Token (Recommended for development) ===
%%% Get a token with: `gcloud auth print-access-token'
%%% - Set `GOOGLE_ACCESS_TOKEN' environment variable
%%% - Or pass `access_token => <<"ya29...">>>' in config
%%%
%%% === Option 2: API Key ===
%%% Create an API key in Google Cloud Console
%%% - Set `GOOGLE_API_KEY' environment variable
%%% - Or pass `api_key => <<"...">>>' in config
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     access_token => <<"ya29...">>,                  %% Access token (or use env var)
%%%     project => <<"my-project">>,                    %% GCP project ID (required)
%%%     region => <<"us-central1">>,                    %% GCP region (default)
%%%     model => <<"text-embedding-004">>,              %% Model name (default, 768 dims)
%%%     timeout => 30000,                               %% Timeout in ms (default)
%%%     dimension => 768                                %% Vector dimension (default)
%%% }.
%%% '''
%%%
%%% == Supported Models ==
%%% - `<<"text-embedding-004">>' - Default, 768 dims, latest
%%% - `<<"text-embedding-005">>' - 768 dims, newest
%%% - `<<"textembedding-gecko@001">>' - 768 dims, legacy
%%% - `<<"textembedding-gecko@003">>' - 768 dims
%%% - `<<"textembedding-gecko-multilingual@001">>' - 768 dims, multilingual
%%% - `<<"text-multilingual-embedding-002">>' - 768 dims, multilingual
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_vertex).
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

-define(DEFAULT_REGION, <<"us-central1">>).
-define(DEFAULT_MODEL, <<"text-embedding-004">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 768).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> vertex.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    Project = get_project(Config),
    AuthInfo = get_auth(Config),
    case {Project, AuthInfo} of
        {undefined, _} ->
            {error, project_not_configured};
        {_, {error, _} = E} ->
            E;
        {_, Auth} ->
            Model = maps:get(model, Config, ?DEFAULT_MODEL),
            BaseConfig = maps:merge(Config, #{project => Project}),
            ConfigWithAuth = maps:merge(BaseConfig, Auth),
            NewConfig = maps:merge(#{
                region => ?DEFAULT_REGION,
                model => Model,
                timeout => ?DEFAULT_TIMEOUT,
                dimension => ?DEFAULT_DIMENSION
            }, ConfigWithAuth),
            {ok, NewConfig}
    end.

%% @doc Check if Vertex AI API is available.
-spec available(map()) -> boolean().
available(Config) ->
    case {maps:get(project, Config, undefined),
          has_auth(Config)} of
        {undefined, _} -> false;
        {_, false} -> false;
        _ -> true
    end.

%% @doc Generate embedding for a single text.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    case embed_batch([Text], Config) of
        {ok, [Vector]} ->
            {ok, Vector};
        {error, _} = Error ->
            Error
    end.

%% @doc Generate embeddings for multiple texts.
%% Vertex AI supports batch embedding.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    Project = maps:get(project, Config),
    Region = maps:get(region, Config, ?DEFAULT_REGION),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    ApiUrl = build_url(Project, Region, Model),
    Body = build_request_body(Texts),
    Headers = build_headers(Config),

    case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}, {with_body, true}]) of
        {ok, 200, _RespHeaders, RespBody} ->
            parse_embeddings_response(RespBody);
        {ok, StatusCode, _RespHeaders, RespBody} ->
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

%%====================================================================
%% Internal Functions - Configuration
%%====================================================================

%% @private
get_project(Config) ->
    case maps:get(project, Config, undefined) of
        undefined ->
            case os:getenv("GOOGLE_CLOUD_PROJECT") of
                false ->
                    case os:getenv("GCLOUD_PROJECT") of
                        false -> undefined;
                        P -> list_to_binary(P)
                    end;
                P -> list_to_binary(P)
            end;
        P when is_binary(P) -> P;
        P when is_list(P) -> list_to_binary(P)
    end.

%% @private
get_auth(Config) ->
    %% Try access token first
    case get_access_token(Config) of
        undefined ->
            %% Try API key
            case get_api_key(Config) of
                undefined ->
                    {error, auth_not_configured};
                ApiKey ->
                    #{api_key => ApiKey, auth_type => api_key}
            end;
        Token ->
            #{access_token => Token, auth_type => access_token}
    end.

%% @private
get_access_token(Config) ->
    case maps:get(access_token, Config, undefined) of
        undefined ->
            case os:getenv("GOOGLE_ACCESS_TOKEN") of
                false -> undefined;
                T -> list_to_binary(T)
            end;
        T when is_binary(T) -> T;
        T when is_list(T) -> list_to_binary(T)
    end.

%% @private
get_api_key(Config) ->
    case maps:get(api_key, Config, undefined) of
        undefined ->
            case os:getenv("GOOGLE_API_KEY") of
                false -> undefined;
                K -> list_to_binary(K)
            end;
        K when is_binary(K) -> K;
        K when is_list(K) -> list_to_binary(K)
    end.

%% @private
has_auth(Config) ->
    maps:get(auth_type, Config, undefined) =/= undefined orelse
    get_access_token(Config) =/= undefined orelse
    get_api_key(Config) =/= undefined.

%%====================================================================
%% Internal Functions - Request Building
%%====================================================================

%% @private
build_url(Project, Region, Model) ->
    <<"https://", Region/binary, "-aiplatform.googleapis.com/v1/projects/",
      Project/binary, "/locations/", Region/binary,
      "/publishers/google/models/", Model/binary, ":predict">>.

%% @private
build_request_body(Texts) ->
    Instances = [#{<<"content">> => Text} || Text <- Texts],
    json:encode(#{<<"instances">> => Instances}).

%% @private
build_headers(Config) ->
    AuthType = maps:get(auth_type, Config),
    AuthHeader = case AuthType of
        access_token ->
            Token = maps:get(access_token, Config),
            {<<"Authorization">>, <<"Bearer ", Token/binary>>};
        api_key ->
            ApiKey = maps:get(api_key, Config),
            {<<"x-goog-api-key">>, ApiKey}
    end,
    [
        AuthHeader,
        {<<"Content-Type">>, <<"application/json">>}
    ].

%%====================================================================
%% Internal Functions - Response Parsing
%%====================================================================

%% @private
parse_embeddings_response(Body) ->
    try
        Response = json:decode(Body),
        case maps:find(<<"predictions">>, Response) of
            {ok, Predictions} when is_list(Predictions) ->
                Embeddings = lists:map(
                    fun(Pred) ->
                        case maps:find(<<"embeddings">>, Pred) of
                            {ok, EmbedObj} ->
                                maps:get(<<"values">>, EmbedObj);
                            error ->
                                %% Some models return flat embedding
                                maps:get(<<"embedding">>, Pred, [])
                        end
                    end,
                    Predictions
                ),
                {ok, Embeddings};
            _ ->
                {error, {invalid_response, no_predictions_field}}
        end
    catch
        _:Reason ->
            {error, {json_decode_failed, Reason}}
    end.
