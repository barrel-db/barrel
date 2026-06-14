%%%-------------------------------------------------------------------
%%% @doc AWS Bedrock embedding provider
%%%
%%% Uses AWS Bedrock's embedding API for embedding generation.
%%% Provides access to Titan and Cohere embedding models on AWS.
%%%
%%% == Requirements ==
%%% AWS credentials, either via API key (ABSK format) or IAM credentials.
%%%
%%% === Option 1: API Key (Recommended) ===
%%% Uses AWS Bedrock API Key (ABSK format, available since July 2025):
%%% - Set `BEDROCK_API_KEY' environment variable
%%% - Or pass `api_key => <<"ABSK...">>>' in config
%%%
%%% === Option 2: IAM Credentials ===
%%% - Set `AWS_ACCESS_KEY_ID', `AWS_SECRET_ACCESS_KEY', and optionally
%%%   `AWS_SESSION_TOKEN' environment variables
%%% - Or pass `access_key_id', `secret_access_key' in config
%%%
%%% == Configuration ==
%%% ```
%%% %% Using API key (simpler)
%%% Config = #{
%%%     api_key => <<"ABSK...">>,                       %% API key (or use env var)
%%%     region => <<"us-east-1">>,                      %% AWS region (default)
%%%     model => <<"amazon.titan-embed-text-v2:0">>,    %% Model ID (default)
%%%     timeout => 30000,                               %% Timeout in ms (default)
%%%     dimension => 1024                               %% Vector dimension (default)
%%% }.
%%%
%%% %% Using IAM credentials (requires SigV4 signing)
%%% Config = #{
%%%     access_key_id => <<"AKIA...">>,
%%%     secret_access_key => <<"...">>,
%%%     session_token => <<"...">>,                     %% Optional, for temporary credentials
%%%     region => <<"us-east-1">>,
%%%     model => <<"amazon.titan-embed-text-v2:0">>
%%% }.
%%% '''
%%%
%%% == Supported Models ==
%%% - `<<"amazon.titan-embed-text-v2:0">>' - Default, 1024 dims
%%% - `<<"amazon.titan-embed-text-v1">>' - 1536 dims, legacy
%%% - `<<"cohere.embed-english-v3">>' - 1024 dims, Cohere on Bedrock
%%% - `<<"cohere.embed-multilingual-v3">>' - 1024 dims, multilingual
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embed_bedrock).
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

-define(DEFAULT_REGION, <<"us-east-1">>).
-define(DEFAULT_MODEL, <<"amazon.titan-embed-text-v2:0">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 1024).
-define(SERVICE, <<"bedrock-runtime">>).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> bedrock.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    case get_credentials(Config) of
        {error, _} = Error ->
            Error;
        Creds ->
            Model = maps:get(model, Config, ?DEFAULT_MODEL),
            Dim = dimension_for_model(Model),
            NewConfig = maps:merge(#{
                region => ?DEFAULT_REGION,
                model => Model,
                timeout => ?DEFAULT_TIMEOUT,
                dimension => Dim
            }, maps:merge(Config, Creds)),
            {ok, NewConfig}
    end.

%% @doc Check if Bedrock API is available.
-spec available(map()) -> boolean().
available(Config) ->
    case has_credentials(Config) of
        false -> false;
        true ->
            %% For API key auth, just check if we have a key
            %% A real check would require making an API call
            true
    end.

%% @doc Generate embedding for a single text.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Region = maps:get(region, Config, ?DEFAULT_REGION),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Build the request body based on model type
    ReqBody = build_embed_request(Model, Text),
    ApiUrl = build_invoke_url(Region, Model),

    case make_request(Config, ApiUrl, ReqBody, Timeout) of
        {ok, RespBody} ->
            parse_embedding_response(Model, RespBody);
        {error, _} = Error ->
            Error
    end.

%% @doc Generate embeddings for multiple texts.
%% Bedrock doesn't support native batch, so we process sequentially.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    %% Process each text sequentially
    Results = lists:map(
        fun(Text) -> embed(Text, Config) end,
        Texts
    ),
    %% Check if all succeeded
    case lists:all(fun({ok, _}) -> true; (_) -> false end, Results) of
        true ->
            Embeddings = [Vec || {ok, Vec} <- Results],
            {ok, Embeddings};
        false ->
            %% Return first error
            lists:foldl(
                fun({error, _} = E, _) -> E;
                   (_, Acc) -> Acc
                end,
                {ok, []},
                Results
            )
    end.

%%====================================================================
%% Internal Functions - Credentials
%%====================================================================

%% @private
get_credentials(Config) ->
    %% Try API key first (ABSK format)
    case get_api_key(Config) of
        undefined ->
            %% Fall back to IAM credentials
            case get_iam_credentials(Config) of
                {ok, Creds} -> Creds;
                {error, _} = E -> E
            end;
        ApiKey ->
            #{api_key => ApiKey, auth_type => api_key}
    end.

%% @private
get_api_key(Config) ->
    case maps:get(api_key, Config, undefined) of
        undefined ->
            case os:getenv("BEDROCK_API_KEY") of
                false -> undefined;
                Key -> list_to_binary(Key)
            end;
        Key when is_binary(Key) ->
            Key;
        Key when is_list(Key) ->
            list_to_binary(Key)
    end.

%% @private
get_iam_credentials(Config) ->
    AccessKeyId = case maps:get(access_key_id, Config, undefined) of
        undefined ->
            case os:getenv("AWS_ACCESS_KEY_ID") of
                false -> undefined;
                K -> list_to_binary(K)
            end;
        K when is_binary(K) -> K;
        K when is_list(K) -> list_to_binary(K)
    end,
    SecretAccessKey = case maps:get(secret_access_key, Config, undefined) of
        undefined ->
            case os:getenv("AWS_SECRET_ACCESS_KEY") of
                false -> undefined;
                S -> list_to_binary(S)
            end;
        S when is_binary(S) -> S;
        S when is_list(S) -> list_to_binary(S)
    end,
    SessionToken = case maps:get(session_token, Config, undefined) of
        undefined ->
            case os:getenv("AWS_SESSION_TOKEN") of
                false -> undefined;
                T -> list_to_binary(T)
            end;
        T when is_binary(T) -> T;
        T when is_list(T) -> list_to_binary(T)
    end,
    case {AccessKeyId, SecretAccessKey} of
        {undefined, _} -> {error, credentials_not_configured};
        {_, undefined} -> {error, credentials_not_configured};
        _ ->
            {ok, #{
                access_key_id => AccessKeyId,
                secret_access_key => SecretAccessKey,
                session_token => SessionToken,
                auth_type => sigv4
            }}
    end.

%% @private
has_credentials(Config) ->
    case maps:get(auth_type, Config, undefined) of
        api_key -> maps:get(api_key, Config, undefined) =/= undefined;
        sigv4 -> maps:get(access_key_id, Config, undefined) =/= undefined;
        undefined ->
            get_api_key(Config) =/= undefined orelse
            os:getenv("AWS_ACCESS_KEY_ID") =/= false
    end.

%%====================================================================
%% Internal Functions - Request Building
%%====================================================================

%% @private
build_invoke_url(Region, Model) ->
    %% URL encode the model ID (colons become %3A)
    EncodedModel = uri_encode(Model),
    <<"https://bedrock-runtime.", Region/binary, ".amazonaws.com/model/",
      EncodedModel/binary, "/invoke">>.

%% @private
uri_encode(Bin) ->
    list_to_binary(uri_string:quote(binary_to_list(Bin))).

%% @private
build_embed_request(Model, Text) ->
    case binary:match(Model, <<"titan">>) of
        nomatch ->
            %% Cohere format
            json:encode(#{
                <<"texts">> => [Text],
                <<"input_type">> => <<"search_document">>
            });
        _ ->
            %% Titan format
            json:encode(#{
                <<"inputText">> => Text
            })
    end.

%%====================================================================
%% Internal Functions - HTTP Request
%%====================================================================

%% @private
make_request(Config, ApiUrl, Body, Timeout) ->
    AuthType = maps:get(auth_type, Config, api_key),
    Headers = build_headers(Config, AuthType, ApiUrl, Body),
    case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}, {with_body, true}]) of
        {ok, 200, _RespHeaders, RespBody} ->
            {ok, RespBody};
        {ok, StatusCode, _RespHeaders, RespBody} ->
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

%% @private
build_headers(Config, api_key, _ApiUrl, _Body) ->
    ApiKey = maps:get(api_key, Config),
    [
        {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
        {<<"Content-Type">>, <<"application/json">>}
    ];
build_headers(Config, sigv4, ApiUrl, Body) ->
    Region = maps:get(region, Config, ?DEFAULT_REGION),
    AccessKeyId = maps:get(access_key_id, Config),
    SecretAccessKey = maps:get(secret_access_key, Config),
    SessionToken = maps:get(session_token, Config, undefined),

    %% Parse URL for signing
    {Host, Path} = parse_url(ApiUrl),
    Now = calendar:universal_time(),
    AmzDate = format_amz_date(Now),
    DateStamp = format_date_stamp(Now),

    %% Create canonical request and sign
    Headers0 = [
        {<<"host">>, Host},
        {<<"x-amz-date">>, AmzDate},
        {<<"content-type">>, <<"application/json">>}
    ],
    Headers1 = case SessionToken of
        undefined -> Headers0;
        Token -> [{<<"x-amz-security-token">>, Token} | Headers0]
    end,

    Signature = sign_request(
        <<"POST">>, Path, <<>>, Headers1, Body,
        Region, ?SERVICE, DateStamp, AmzDate,
        AccessKeyId, SecretAccessKey
    ),

    %% Build Authorization header
    SignedHeaders = signed_headers(Headers1),
    Credential = <<AccessKeyId/binary, "/", DateStamp/binary, "/",
                   Region/binary, "/", ?SERVICE/binary, "/aws4_request">>,
    AuthHeader = <<"AWS4-HMAC-SHA256 Credential=", Credential/binary,
                   ", SignedHeaders=", SignedHeaders/binary,
                   ", Signature=", Signature/binary>>,

    [{<<"Authorization">>, AuthHeader} | Headers1].

%% @private
parse_url(Url) ->
    %% Extract host and path from URL
    case binary:split(Url, <<"://">>) of
        [_Scheme, Rest] ->
            case binary:split(Rest, <<"/">>) of
                [Host] -> {Host, <<"/">>};
                [Host, Path] -> {Host, <<"/", Path/binary>>}
            end
    end.

%% @private
format_amz_date({{Y, M, D}, {H, Min, S}}) ->
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BZ",
                                  [Y, M, D, H, Min, S])).

%% @private
format_date_stamp({{Y, M, D}, _}) ->
    list_to_binary(io_lib:format("~4..0B~2..0B~2..0B", [Y, M, D])).

%% @private
sign_request(Method, Path, Query, Headers, Payload,
             Region, Service, DateStamp, AmzDate,
             _AccessKeyId, SecretAccessKey) ->
    %% Create canonical request
    CanonicalHeaders = canonical_headers(Headers),
    SignedHdrs = signed_headers(Headers),
    PayloadHash = hex_sha256(Payload),

    CanonicalRequest = <<Method/binary, "\n",
                         Path/binary, "\n",
                         Query/binary, "\n",
                         CanonicalHeaders/binary, "\n",
                         SignedHdrs/binary, "\n",
                         PayloadHash/binary>>,

    %% Create string to sign
    Algorithm = <<"AWS4-HMAC-SHA256">>,
    CredentialScope = <<DateStamp/binary, "/", Region/binary, "/",
                        Service/binary, "/aws4_request">>,
    HashedCanonical = hex_sha256(CanonicalRequest),
    StringToSign = <<Algorithm/binary, "\n",
                     AmzDate/binary, "\n",
                     CredentialScope/binary, "\n",
                     HashedCanonical/binary>>,

    %% Calculate signature
    KDate = hmac_sha256(<<"AWS4", SecretAccessKey/binary>>, DateStamp),
    KRegion = hmac_sha256(KDate, Region),
    KService = hmac_sha256(KRegion, Service),
    KSigning = hmac_sha256(KService, <<"aws4_request">>),
    hex(hmac_sha256(KSigning, StringToSign)).

%% @private
canonical_headers(Headers) ->
    Sorted = lists:sort(
        fun({A, _}, {B, _}) ->
            string:lowercase(binary_to_list(A)) =<
            string:lowercase(binary_to_list(B))
        end,
        Headers
    ),
    list_to_binary([
        [string:lowercase(binary_to_list(K)), ":", V, "\n"]
        || {K, V} <- Sorted
    ]).

%% @private
signed_headers(Headers) ->
    Sorted = lists:sort([
        string:lowercase(binary_to_list(K))
        || {K, _} <- Headers
    ]),
    list_to_binary(lists:join(";", Sorted)).

%% @private
hmac_sha256(Key, Data) ->
    crypto:mac(hmac, sha256, Key, Data).

%% @private
hex_sha256(Data) ->
    hex(crypto:hash(sha256, Data)).

%% @private
hex(Bin) ->
    list_to_binary(lists:flatten([io_lib:format("~2.16.0b", [B]) || <<B>> <= Bin])).

%%====================================================================
%% Internal Functions - Response Parsing
%%====================================================================

%% @private
dimension_for_model(<<"amazon.titan-embed-text-v1">>) -> 1536;
dimension_for_model(<<"amazon.titan-embed-text-v2:0">>) -> 1024;
dimension_for_model(<<"cohere.embed-", _/binary>>) -> 1024;
dimension_for_model(_) -> 1024.

%% @private
parse_embedding_response(Model, Body) ->
    try
        Response = json:decode(Body),
        case binary:match(Model, <<"titan">>) of
            nomatch ->
                %% Cohere format
                case maps:find(<<"embeddings">>, Response) of
                    {ok, [Embedding | _]} -> {ok, Embedding};
                    _ -> {error, {invalid_response, no_embeddings_field}}
                end;
            _ ->
                %% Titan format
                case maps:find(<<"embedding">>, Response) of
                    {ok, Embedding} -> {ok, Embedding};
                    _ -> {error, {invalid_response, no_embedding_field}}
                end
        end
    catch
        _:Reason ->
            {error, {json_decode_failed, Reason}}
    end.
