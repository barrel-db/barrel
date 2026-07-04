%%%-------------------------------------------------------------------
%%% @doc Static bearer-token auth for the whole REST API.
%%%
%%% Configured via `{barrel_server, auth, #{tokens => [Bin]}}' (or
%%% `#{token => Bin}'); a token LIST makes rotation possible (accept
%%% old and new during a rollover). Unconfigured = the middleware is
%%% not installed and the server stays open. When installed, every
%%% route except `/health' (probes carry no secrets) requires
%%% `Authorization: Bearer <token>'.
%%%
%%% Comparison is constant time: both sides are SHA-256 hashed (fixed
%%% length, no length oracle) and compared with crypto:hash_equals/2.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_auth).
-behaviour(livery_middleware).

-export([state_from_env/0]).
-export([call/3]).

%% @doc Middleware state from the app env; undefined = no auth.
-spec state_from_env() -> map() | undefined.
state_from_env() ->
    case application:get_env(barrel_server, auth, undefined) of
        undefined ->
            undefined;
        #{tokens := Tokens} when is_list(Tokens), Tokens =/= [] ->
            #{hashes => [crypto:hash(sha256, T) || T <- Tokens]};
        #{token := Token} when is_binary(Token) ->
            #{hashes => [crypto:hash(sha256, Token)]};
        Other ->
            logger:warning("ignoring invalid barrel_server auth config: ~p",
                           [Other]),
            undefined
    end.

call(Req, Next, #{hashes := Hashes}) ->
    case livery_req:path(Req) of
        <<"/health">> ->
            Next(Req);
        _ ->
            case livery_ext:bearer_token(Req) of
                undefined ->
                    unauthorized();
                Token ->
                    Hash = crypto:hash(sha256, Token),
                    case lists:any(
                             fun(Expected) ->
                                 crypto:hash_equals(Hash, Expected)
                             end,
                             Hashes) of
                        true -> Next(Req);
                        false -> unauthorized()
                    end
            end
    end.

unauthorized() ->
    livery_resp:new(
        401,
        [{<<"www-authenticate">>, <<"Bearer">>},
         {<<"content-type">>, <<"application/json">>}],
        {full, <<"{\"error\":\"unauthorized\"}">>}).
