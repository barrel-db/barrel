%%%-------------------------------------------------------------------
%%% @doc Voyage AI embedding provider
%%%
%%% Uses Voyage AI's Embeddings API for embedding generation.
%%% Voyage AI provides state-of-the-art embeddings optimized for retrieval.
%%%
%%% == Requirements ==
%%% A Voyage AI API key, either:
%%% - Set via `VOYAGE_API_KEY' environment variable
%%% - Passed in config as `api_key => <<"...">>>'
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     api_key => <<"...">>,                           %% API key (or use env var)
%%%     url => <<"https://api.voyageai.com/v1">>,       %% API base URL (default)
%%%     model => <<"voyage-3">>,                        %% Model name (default, 1024 dims)
%%%     timeout => 30000,                               %% Timeout in ms (default)
%%%     dimension => 1024                               %% Vector dimension (default)
%%% }.
%%% '''
%%%
%%% == Supported Models ==
%%% - `<<"voyage-3">>' - Default, 1024 dims, best quality
%%% - `<<"voyage-3-lite">>' - 512 dims, faster/cheaper
%%% - `<<"voyage-code-3">>' - 1024 dims, optimized for code
%%% - `<<"voyage-finance-2">>' - 1024 dims, financial domain
%%% - `<<"voyage-law-2">>' - 1024 dims, legal domain
%%% - `<<"voyage-large-2">>' - 1536 dims, legacy high quality
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_voyage).
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

-define(DEFAULT_URL, <<"https://api.voyageai.com/v1">>).
-define(DEFAULT_MODEL, <<"voyage-3">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 1024).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> voyage.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    case get_api_key(Config) of
        undefined ->
            {error, api_key_not_configured};
        ApiKey ->
            Model = maps:get(model, Config, ?DEFAULT_MODEL),
            Dim = dimension_for_model(Model),
            NewConfig = maps:merge(#{
                url => ?DEFAULT_URL,
                model => Model,
                timeout => ?DEFAULT_TIMEOUT,
                dimension => Dim
            }, Config#{api_key => ApiKey}),
            {ok, NewConfig}
    end.

%% @doc Check if Voyage AI API is available.
-spec available(map()) -> boolean().
available(Config) ->
    case maps:get(api_key, Config, undefined) of
        undefined ->
            false;
        ApiKey ->
            Url = maps:get(url, Config, ?DEFAULT_URL),
            Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
            %% Check API with a minimal request
            ApiUrl = <<Url/binary, "/embeddings">>,
            Headers = [
                {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
                {<<"Content-Type">>, <<"application/json">>}
            ],
            Body = json:encode(#{
                <<"input">> => [<<"test">>],
                <<"model">> => maps:get(model, Config, ?DEFAULT_MODEL)
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
%% Voyage AI supports native batch embedding.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
    ApiKey = maps:get(api_key, Config),

    ApiUrl = <<Url/binary, "/embeddings">>,
    Body = json:encode(#{
        <<"input">> => Texts,
        <<"model">> => Model
    }),
    Headers = [
        {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
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
            case os:getenv("VOYAGE_API_KEY") of
                false -> undefined;
                Key -> list_to_binary(Key)
            end;
        Key when is_binary(Key) ->
            Key;
        Key when is_list(Key) ->
            list_to_binary(Key)
    end.

%% @private
dimension_for_model(<<"voyage-3-lite">>) -> 512;
dimension_for_model(<<"voyage-large-2">>) -> 1536;
dimension_for_model(<<"voyage-2">>) -> 1024;
dimension_for_model(_) -> 1024.

%% @private
%% Voyage uses OpenAI-compatible response format
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
