%%% @doc Provider request-building tests. Mock hackney so no network is
%%% touched, and assert each provider constructs the right URL, auth header,
%%% and JSON body, then parses the response into ordered vectors.
-module(barrel_embed_request_tests).

-include_lib("eunit/include/eunit.hrl").

request_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      {"openai builds the embeddings request", fun openai_request/0},
      {"openai orders vectors by index", fun openai_order/0},
      {"openai maps an HTTP error", fun openai_http_error/0},
      {"openai reports a bad JSON body", fun openai_bad_json/0},
      {"cohere builds the embed request", fun cohere_request/0}
     ]}.

setup() ->
    meck:new(hackney, [no_link]),
    ok.

cleanup(_) ->
    meck:unload(hackney).

%% capture the single hackney:request/5 call from the last invocation
last_request() ->
    [{_Pid, {hackney, request, Args}, _Ret} | _] =
        lists:reverse(meck:history(hackney)),
    list_to_tuple(Args).

expect_response(Body) ->
    %% hackney delivers the body as a flat binary; json:encode returns an
    %% iolist, so flatten it the way the transport would.
    Bin = iolist_to_binary(Body),
    meck:expect(hackney, request,
                fun(_Method, _Url, _Headers, _ReqBody, _Opts) ->
                    {ok, 200, [], Bin}
                end).

openai_request() ->
    expect_response(json:encode(#{<<"data">> =>
        [#{<<"index">> => 0, <<"embedding">> => [0.1, 0.2, 0.3]}]})),
    Config = #{api_key => <<"sk-test">>, model => <<"m1">>},
    ?assertEqual({ok, [0.1, 0.2, 0.3]},
                 barrel_embed_openai:embed(<<"hello">>, Config)),
    {Method, Url, Headers, ReqBody, _Opts} = last_request(),
    ?assertEqual(post, Method),
    ?assertEqual(<<"https://api.openai.com/v1/embeddings">>, Url),
    ?assertEqual({<<"Authorization">>, <<"Bearer sk-test">>},
                 lists:keyfind(<<"Authorization">>, 1, Headers)),
    Decoded = json:decode(iolist_to_binary(ReqBody)),
    ?assertEqual(<<"m1">>, maps:get(<<"model">>, Decoded)),
    ?assertEqual([<<"hello">>], maps:get(<<"input">>, Decoded)).

openai_order() ->
    %% data returned out of order must come back sorted by index
    expect_response(json:encode(#{<<"data">> => [
        #{<<"index">> => 1, <<"embedding">> => [9.0]},
        #{<<"index">> => 0, <<"embedding">> => [1.0]}
    ]})),
    ?assertEqual({ok, [[1.0], [9.0]]},
                 barrel_embed_openai:embed_batch([<<"a">>, <<"b">>],
                                                 #{api_key => <<"k">>})).

openai_http_error() ->
    meck:expect(hackney, request,
                fun(_, _, _, _, _) -> {ok, 401, [], <<"nope">>} end),
    ?assertMatch({error, {http_error, 401, _}},
                 barrel_embed_openai:embed(<<"x">>, #{api_key => <<"k">>})).

openai_bad_json() ->
    expect_response(<<"not json">>),
    ?assertMatch({error, _},
                 barrel_embed_openai:embed(<<"x">>, #{api_key => <<"k">>})).

cohere_request() ->
    expect_response(json:encode(#{<<"embeddings">> => [[0.4, 0.5]]})),
    Config = #{api_key => <<"co-test">>, model => <<"embed-x">>},
    ?assertEqual({ok, [0.4, 0.5]},
                 barrel_embed_cohere:embed(<<"hi">>, Config)),
    {post, Url, Headers, ReqBody, _} = last_request(),
    ?assertEqual(<<"https://api.cohere.com/v1/embed">>, Url),
    ?assertEqual({<<"Authorization">>, <<"Bearer co-test">>},
                 lists:keyfind(<<"Authorization">>, 1, Headers)),
    Decoded = json:decode(iolist_to_binary(ReqBody)),
    ?assertEqual([<<"hi">>], maps:get(<<"texts">>, Decoded)),
    ?assertEqual(<<"embed-x">>, maps:get(<<"model">>, Decoded)).
