%%%-------------------------------------------------------------------
%%% @doc Ollama embedding provider
%%%
%%% Uses Ollama's local API for embedding generation.
%%% Requires Ollama to be running locally.
%%%
%%% == Requirements ==
%%% ```
%%% # Install Ollama from https://ollama.ai
%%% # Pull an embedding model:
%%% ollama pull nomic-embed-text
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     url => <<"http://localhost:11434">>,   %% Ollama API URL (default)
%%%     model => <<"nomic-embed-text">>,       %% Model name (default, 768 dims)
%%%     timeout => 30000                       %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Supported Models ==
%%% Any Ollama embedding model. Popular choices:
%%%
%%% - `<<"nomic-embed-text">>' - Default, 768 dims, general purpose
%%% - `<<"mxbai-embed-large">>' - 1024 dims, high quality
%%% - `<<"all-minilm">>' - 384 dims, fast
%%% - `<<"snowflake-arctic-embed">>' - 1024 dims, multilingual
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_ollama).
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

-define(DEFAULT_URL, <<"http://localhost:11434">>).
-define(DEFAULT_MODEL, <<"nomic-embed-text">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 768).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> ollama.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    NewConfig = maps:merge(#{
        url => ?DEFAULT_URL,
        model => ?DEFAULT_MODEL,
        timeout => ?DEFAULT_TIMEOUT,
        dimension => ?DEFAULT_DIMENSION
    }, Config),
    {ok, NewConfig}.

%% @doc Check if Ollama is available.
-spec available(map()) -> boolean().
available(Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
    ApiUrl = <<Url/binary, "/api/tags">>,

    case hackney:request(get, ApiUrl, [], <<>>, [{recv_timeout, Timeout}]) of
        {ok, 200, _, _Body} ->
            true;
        _ ->
            false
    end.

%% @doc Generate embedding for a single text.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Try new /api/embed first, fall back to /api/embeddings
    case embed_with_api(Text, Url, Model, Timeout, <<"/api/embed">>) of
        {ok, _} = Ok -> Ok;
        {error, {http_error, 404, _}} ->
            %% Fall back to old API
            embed_with_api(Text, Url, Model, Timeout, <<"/api/embeddings">>);
        Error -> Error
    end.

%% @private
embed_with_api(Text, Url, Model, Timeout, ApiPath) ->
    ApiUrl = <<Url/binary, ApiPath/binary>>,

    %% Different request format for different APIs
    Body = case ApiPath of
        <<"/api/embed">> ->
            json:encode(#{
                <<"model">> => Model,
                <<"input">> => Text
            });
        <<"/api/embeddings">> ->
            json:encode(#{
                <<"model">> => Model,
                <<"prompt">> => Text
            })
    end,

    Headers = [{<<"Content-Type">>, <<"application/json">>}],

    case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}]) of
        {ok, 200, _RespHeaders, RespBody} ->
            parse_embedding_response(RespBody, ApiPath);
        {ok, StatusCode, _RespHeaders, RespBody} ->
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

%% @doc Generate embeddings for multiple texts.
%% Ollama /api/embed supports batch natively.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    ApiUrl = <<Url/binary, "/api/embed">>,
    Body = json:encode(#{
        <<"model">> => Model,
        <<"input">> => Texts
    }),
    Headers = [{<<"Content-Type">>, <<"application/json">>}],

    case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}]) of
        {ok, 200, _RespHeaders, RespBody} ->
            parse_batch_response(RespBody);
        {ok, 404, _, _RespBody} ->
            %% Old API doesn't support batch, fall back to sequential
            embed_batch_sequential(Texts, Config);
        {ok, StatusCode, _RespHeaders, RespBody} ->
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

%% @private Sequential batch for old API
embed_batch_sequential(Texts, Config) ->
    Results = lists:map(fun(Text) -> embed(Text, Config) end, Texts),
    case lists:partition(fun({ok, _}) -> true; (_) -> false end, Results) of
        {Successes, []} ->
            Vectors = [V || {ok, V} <- Successes],
            {ok, Vectors};
        {_, [FirstError | _]} ->
            FirstError
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
parse_embedding_response(Body, ApiPath) ->
    try
        Response = json:decode(Body),
        case ApiPath of
            <<"/api/embed">> ->
                %% New API returns embeddings array
                case maps:find(<<"embeddings">>, Response) of
                    {ok, [Embedding | _]} when is_list(Embedding) ->
                        {ok, Embedding};
                    {ok, Embeddings} when is_list(Embeddings) ->
                        %% Single embedding without nesting
                        case Embeddings of
                            [H|_] when is_number(H) -> {ok, Embeddings};
                            _ -> {error, {invalid_response, no_embedding}}
                        end;
                    _ ->
                        {error, {invalid_response, no_embeddings_field}}
                end;
            <<"/api/embeddings">> ->
                %% Old API returns single embedding
                case maps:find(<<"embedding">>, Response) of
                    {ok, Embedding} when is_list(Embedding) ->
                        {ok, Embedding};
                    _ ->
                        {error, {invalid_response, no_embedding_field}}
                end
        end
    catch
        _:Reason ->
            {error, {json_decode_failed, Reason}}
    end.

%% @private
parse_batch_response(Body) ->
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
