%%%-------------------------------------------------------------------
%%% @doc Bearer-token auth for the whole REST API.
%%%
%%% Two kinds of bearer. Global tokens come from
%%% `{barrel_server, auth, #{tokens => [Bin]}}' (or `#{token => Bin}';
%%% a LIST makes rotation possible) and open every route. Capability
%%% tokens (`bsp_...', issued by barrel_caps for one space) only
%%% authenticate the agent-layer routes (`/spaces', `/handoffs');
%%% per-route rights are enforced by barrel_server_caps, and any other
%%% path answers 401. Unconfigured = the middleware is not installed
%%% and the server stays open. When installed, every route except
%%% `/health' (probes carry no secrets) requires
%%% `Authorization: Bearer <token>'.
%%%
%%% Global-token comparison is constant time: both sides are SHA-256
%%% hashed (fixed length, no length oracle) and compared with
%%% crypto:hash_equals/2.
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
        Path ->
            case livery_ext:bearer_token(Req) of
                undefined ->
                    unauthorized();
                <<"bsp_", _/binary>> = Token ->
                    case capability_path(Path)
                         andalso live_capability(Token) of
                        true -> Next(Req);
                        false -> unauthorized()
                    end;
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

%% Capability tokens authenticate only the agent-layer surface; the
%% database routes stay global-bearer.
capability_path(<<"/spaces", _/binary>>) -> true;
capability_path(<<"/handoffs", _/binary>>) -> true;
capability_path(_) -> false.

live_capability(Token) ->
    case barrel_caps:auth_context(Token) of
        {ok, _} -> true;
        {error, _} -> false
    end.

unauthorized() ->
    livery_resp:new(
        401,
        [{<<"www-authenticate">>, <<"Bearer">>},
         {<<"content-type">>, <<"application/json">>}],
        {full, <<"{\"error\":\"unauthorized\"}">>}).
