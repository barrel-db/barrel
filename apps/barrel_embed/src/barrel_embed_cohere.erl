%%%-------------------------------------------------------------------
%%% @doc Cohere embedding provider
%%%
%%% Uses Cohere's Embed API for embedding generation.
%%% Requires a Cohere API key.
%%%
%%% == Requirements ==
%%% A Cohere API key, either:
%%% - Set via `COHERE_API_KEY' environment variable
%%% - Passed in config as `api_key => <<"...">>>'
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     api_key => <<"...">>,                           %% API key (or use env var)
%%%     url => <<"https://api.cohere.com/v1">>,         %% API base URL (default)
%%%     model => <<"embed-english-v3.0">>,              %% Model name (default, 1024 dims)
%%%     input_type => <<"search_document">>,            %% Input type (default)
%%%     timeout => 30000,                               %% Timeout in ms (default)
%%%     dimension => 1024                               %% Vector dimension (default)
%%% }.
%%% '''
%%%
%%% == Input Types ==
%%% - `<<"search_document">>' - For documents to be searched
%%% - `<<"search_query">>' - For search queries
%%% - `<<"classification">>' - For classification tasks
%%% - `<<"clustering">>' - For clustering tasks
%%%
%%% == Supported Models ==
%%% - `<<"embed-english-v3.0">>' - Default, 1024 dims, English
%%% - `<<"embed-multilingual-v3.0">>' - 1024 dims, 100+ languages
%%% - `<<"embed-english-light-v3.0">>' - 384 dims, faster/cheaper
%%% - `<<"embed-multilingual-light-v3.0">>' - 384 dims, multilingual light
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_cohere).
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

-define(DEFAULT_URL, <<"https://api.cohere.com/v1">>).
-define(DEFAULT_MODEL, <<"embed-english-v3.0">>).
-define(DEFAULT_INPUT_TYPE, <<"search_document">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 1024).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> cohere.

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
                input_type => ?DEFAULT_INPUT_TYPE,
                timeout => ?DEFAULT_TIMEOUT,
                dimension => Dim
            }, Config#{api_key => ApiKey}),
            {ok, NewConfig}
    end.

%% @doc Check if Cohere API is available.
-spec available(map()) -> boolean().
available(Config) ->
    case maps:get(api_key, Config, undefined) of
        undefined ->
            false;
        ApiKey ->
            Url = maps:get(url, Config, ?DEFAULT_URL),
            Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
            %% Check API key with a minimal request
            ApiUrl = <<Url/binary, "/embed">>,
            Headers = [
                {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
                {<<"Content-Type">>, <<"application/json">>}
            ],
            Body = json:encode(#{
                <<"texts">> => [<<"test">>],
                <<"model">> => maps:get(model, Config, ?DEFAULT_MODEL),
                <<"input_type">> => <<"search_document">>
            }),
            Options = [{recv_timeout, Timeout}, {with_body, true}],
            case hackney:request(post, ApiUrl, Headers, Body, Options) of
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
%% Cohere supports native batch embedding.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    InputType = maps:get(input_type, Config, ?DEFAULT_INPUT_TYPE),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
    ApiKey = maps:get(api_key, Config),

    ApiUrl = <<Url/binary, "/embed">>,
    Body = json:encode(#{
        <<"texts">> => Texts,
        <<"model">> => Model,
        <<"input_type">> => InputType
    }),
    Headers = [
        {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
        {<<"Content-Type">>, <<"application/json">>}
    ],
    Options = [{recv_timeout, Timeout}, {with_body, true}],

    case hackney:request(post, ApiUrl, Headers, Body, Options) of
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
            case os:getenv("COHERE_API_KEY") of
                false -> undefined;
                Key -> list_to_binary(Key)
            end;
        Key when is_binary(Key) ->
            Key;
        Key when is_list(Key) ->
            list_to_binary(Key)
    end.

%% @private
dimension_for_model(<<"embed-english-light-v3.0">>) -> 384;
dimension_for_model(<<"embed-multilingual-light-v3.0">>) -> 384;
dimension_for_model(_) -> 1024.

%% @private
parse_embeddings_response(Body) ->
    try
        Response = json:decode(Body),
        case maps:find(<<"embeddings">>, Response) of
            {ok, Embeddings} when is_list(Embeddings) ->
                {ok, Embeddings};
            _ ->
                {error, {invalid_response, no_embeddings_field}}
        end
    catch
        _:Reason ->
            {error, {json_decode_failed, Reason}}
    end.
