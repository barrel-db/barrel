%%%-------------------------------------------------------------------
%%% @doc OpenAI embedding provider
%%%
%%% Uses OpenAI's Embeddings API for embedding generation.
%%% Requires an OpenAI API key.
%%%
%%% == Requirements ==
%%% An OpenAI API key, either:
%%% - Set via `OPENAI_API_KEY' environment variable
%%% - Passed in config as `api_key => <<"sk-...">>>'
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     api_key => <<"sk-...">>,                    %% API key (or use env var)
%%%     url => <<"https://api.openai.com/v1">>,     %% API base URL (default)
%%%     model => <<"text-embedding-3-small">>,      %% Model name (default, 1536 dims)
%%%     timeout => 30000,                           %% Timeout in ms (default)
%%%     dimension => 1536                           %% Vector dimension (default)
%%% }.
%%% '''
%%%
%%% == Supported Models ==
%%% - `<<"text-embedding-3-small">>' - Default, 1536 dims, fast and cheap
%%% - `<<"text-embedding-3-large">>' - 3072 dims, higher quality
%%% - `<<"text-embedding-ada-002">>' - 1536 dims, legacy model
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_openai).
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

-define(DEFAULT_URL, <<"https://api.openai.com/v1">>).
-define(DEFAULT_MODEL, <<"text-embedding-3-small">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 1536).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> openai.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    %% Get API key from config or environment
    ApiKey = case maps:get(api_key, Config, undefined) of
        undefined ->
            case os:getenv("OPENAI_API_KEY") of
                false -> undefined;
                Key -> list_to_binary(Key)
            end;
        Key when is_binary(Key) ->
            Key;
        Key when is_list(Key) ->
            list_to_binary(Key)
    end,
    case ApiKey of
        undefined ->
            {error, api_key_not_configured};
        _ ->
            NewConfig = maps:merge(#{
                url => ?DEFAULT_URL,
                model => ?DEFAULT_MODEL,
                timeout => ?DEFAULT_TIMEOUT,
                dimension => ?DEFAULT_DIMENSION
            }, Config#{api_key => ApiKey}),
            {ok, NewConfig}
    end.

%% @doc Check if OpenAI API is available.
-spec available(map()) -> boolean().
available(Config) ->
    case maps:get(api_key, Config, undefined) of
        undefined ->
            false;
        ApiKey ->
            Url = maps:get(url, Config, ?DEFAULT_URL),
            Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
            %% Check models endpoint to verify API key works
            ApiUrl = <<Url/binary, "/models">>,
            Headers = [
                {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
                {<<"Content-Type">>, <<"application/json">>}
            ],
            case hackney:request(get, ApiUrl, Headers, <<>>, [{recv_timeout, Timeout}]) of
                {ok, 200, _, _Body} ->
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
%% OpenAI supports native batch embedding.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
    ApiKey = maps:get(api_key, Config),

    ApiUrl = <<Url/binary, "/embeddings">>,
    Body = json:encode(#{
        <<"model">> => Model,
        <<"input">> => Texts
    }),
    Headers = [
        {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
        {<<"Content-Type">>, <<"application/json">>}
    ],

    case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}]) of
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
