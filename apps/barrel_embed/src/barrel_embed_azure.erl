%%%-------------------------------------------------------------------
%%% @doc Azure OpenAI embedding provider
%%%
%%% Uses Azure OpenAI's Embeddings API for embedding generation.
%%% Provides enterprise-grade embeddings with Azure security and compliance.
%%%
%%% == Requirements ==
%%% - Azure OpenAI resource with an embedding model deployment
%%% - API key from Azure Portal -> Your Resource -> Keys
%%%
%%% Configuration requires either:
%%% - Environment variables: `AZURE_OPENAI_API_KEY', `AZURE_OPENAI_ENDPOINT'
%%% - Or passed in config directly
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     api_key => <<"...">>,                           %% API key (or use env var)
%%%     endpoint => <<"https://your-resource.openai.azure.com">>,  %% Azure endpoint
%%%     deployment => <<"text-embedding-ada-002">>,     %% Deployment name
%%%     api_version => <<"2024-02-01">>,                %% API version (default)
%%%     timeout => 30000,                               %% Timeout in ms (default)
%%%     dimension => 1536                               %% Vector dimension (default)
%%% }.
%%% '''
%%%
%%% == Supported Models ==
%%% Deploy these models in Azure OpenAI Studio:
%%% - `<<"text-embedding-3-small">>' - 1536 dims, fast and cost-effective
%%% - `<<"text-embedding-3-large">>' - 3072 dims, highest quality
%%% - `<<"text-embedding-ada-002">>' - 1536 dims, legacy
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_azure).
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

-define(DEFAULT_API_VERSION, <<"2024-02-01">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 1536).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> azure.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    ApiKey = get_api_key(Config),
    Endpoint = get_endpoint(Config),
    Deployment = maps:get(deployment, Config, undefined),
    case {ApiKey, Endpoint, Deployment} of
        {undefined, _, _} ->
            {error, api_key_not_configured};
        {_, undefined, _} ->
            {error, endpoint_not_configured};
        {_, _, undefined} ->
            {error, deployment_not_configured};
        _ ->
            NewConfig = maps:merge(#{
                api_version => ?DEFAULT_API_VERSION,
                timeout => ?DEFAULT_TIMEOUT,
                dimension => ?DEFAULT_DIMENSION
            }, Config#{
                api_key => ApiKey,
                endpoint => normalize_endpoint(Endpoint),
                deployment => Deployment
            }),
            {ok, NewConfig}
    end.

%% @doc Check if Azure OpenAI API is available.
-spec available(map()) -> boolean().
available(Config) ->
    case {maps:get(api_key, Config, undefined),
          maps:get(endpoint, Config, undefined),
          maps:get(deployment, Config, undefined)} of
        {undefined, _, _} -> false;
        {_, undefined, _} -> false;
        {_, _, undefined} -> false;
        {ApiKey, Endpoint, Deployment} ->
            Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
            ApiVersion = maps:get(api_version, Config, ?DEFAULT_API_VERSION),
            ApiUrl = build_url(Endpoint, Deployment, ApiVersion),
            Headers = [
                {<<"api-key">>, ApiKey},
                {<<"Content-Type">>, <<"application/json">>}
            ],
            Body = json:encode(#{
                <<"input">> => [<<"test">>]
            }),
            case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}, {with_body, true}]) of
                {ok, 200, _, _RespBody} ->
                    true;
                _ ->
                    false
            end
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
%% Azure OpenAI supports native batch embedding.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    Endpoint = maps:get(endpoint, Config),
    Deployment = maps:get(deployment, Config),
    ApiVersion = maps:get(api_version, Config, ?DEFAULT_API_VERSION),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
    ApiKey = maps:get(api_key, Config),

    ApiUrl = build_url(Endpoint, Deployment, ApiVersion),
    Body = json:encode(#{
        <<"input">> => Texts
    }),
    Headers = [
        {<<"api-key">>, ApiKey},
        {<<"Content-Type">>, <<"application/json">>}
    ],

    case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}, {with_body, true}]) of
        {ok, 200, _RespHeaders, RespBody} ->
            parse_embeddings_response(RespBody);
        {ok, StatusCode, _RespHeaders, RespBody} ->
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
get_api_key(Config) ->
    case maps:get(api_key, Config, undefined) of
        undefined ->
            case os:getenv("AZURE_OPENAI_API_KEY") of
                false -> undefined;
                Key -> list_to_binary(Key)
            end;
        Key when is_binary(Key) ->
            Key;
        Key when is_list(Key) ->
            list_to_binary(Key)
    end.

%% @private
get_endpoint(Config) ->
    case maps:get(endpoint, Config, undefined) of
        undefined ->
            case os:getenv("AZURE_OPENAI_ENDPOINT") of
                false -> undefined;
                Endpoint -> list_to_binary(Endpoint)
            end;
        Endpoint when is_binary(Endpoint) ->
            Endpoint;
        Endpoint when is_list(Endpoint) ->
            list_to_binary(Endpoint)
    end.

%% @private
%% Remove trailing slash from endpoint
normalize_endpoint(Endpoint) ->
    case binary:last(Endpoint) of
        $/ -> binary:part(Endpoint, 0, byte_size(Endpoint) - 1);
        _ -> Endpoint
    end.

%% @private
build_url(Endpoint, Deployment, ApiVersion) ->
    <<Endpoint/binary, "/openai/deployments/", Deployment/binary,
      "/embeddings?api-version=", ApiVersion/binary>>.

%% @private
%% Azure uses OpenAI-compatible response format
parse_embeddings_response(Body) ->
    try
        Response = json:decode(Body),
        case maps:find(<<"data">>, Response) of
            {ok, Data} when is_list(Data) ->
                %% Sort by index to ensure correct order
                Sorted = lists:sort(
                    fun(A, B) ->
                        maps:get(<<"index">>, A, 0) < maps:get(<<"index">>, B, 0)
                    end,
                    Data
                ),
                Embeddings = [maps:get(<<"embedding">>, Item) || Item <- Sorted],
                {ok, Embeddings};
            _ ->
                {error, {invalid_response, no_data_field}}
        end
    catch
        _:Reason ->
            {error, {json_decode_failed, Reason}}
    end.
