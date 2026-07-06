%%%-------------------------------------------------------------------
%%% @doc barrel_mcp_auth provider for the /mcp endpoint, and the
%%% authorization helpers the MCP tools consume.
%%%
%%% Three principals:
%%% - a server bearer (checked against the same SHA-256 hash set as
%%%   the REST middleware, see barrel_server_auth:hashes/0) is subject
%%%   `server' with the `admin' scope everywhere;
%%% - a capability bearer (`bsp_...') resolves through
%%%   barrel_caps:auth_context/1: subject and scopes come from the
%%%   grant and the claims carry the granted space;
%%% - with no auth configured (open server) every caller is an
%%%   anonymous full-rights principal, matching the open REST surface.
%%% A presented capability token is enforced even on an open server.
%%%
%%% allow/3 is the per-tool gate: server/anonymous principals pass,
%%% capability principals need the database to be their space and the
%%% right within their scopes. prov/2 shapes the provenance map
%%% (actor/session/source) every MCP write carries.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mcp_auth).
-behaviour(barrel_mcp_auth).

-export([init/1, authenticate/2, challenge/2, auth_headers/1]).

%% Tool-side helpers
-export([allow/3, prov/2]).

%%====================================================================
%% barrel_mcp_auth callbacks
%%====================================================================

init(Opts) ->
    {ok, Opts}.

authenticate(Request, _State) ->
    Headers = maps:get(headers, Request, #{}),
    case barrel_mcp_auth:extract_bearer_token(Headers) of
        {ok, <<"bsp_", _/binary>> = Token} ->
            capability_info(Token);
        {ok, Token} ->
            server_info_for(Token);
        {error, no_token} ->
            case barrel_server_auth:hashes() of
                undefined -> {ok, anonymous()};
                _ -> {error, unauthorized}
            end
    end.

challenge(_Reason, _State) ->
    {401,
     #{<<"www-authenticate">> => <<"Bearer">>,
       <<"content-type">> => <<"application/json">>},
     <<"{\"error\":\"unauthorized\"}">>}.

auth_headers(_State) ->
    [<<"authorization">>].

%%====================================================================
%% Tool-side helpers
%%====================================================================

%% @doc Gate a tool call touching database `Db' with `Right'. Server
%% and anonymous principals pass; a capability principal needs Db to
%% be its granted space (branch databases of a space are deferred)
%% and the right within its scopes.
-spec allow(map(), binary(), read | write | admin) ->
    ok | {error, forbidden}.
allow(Ctx, Db, Right) ->
    case maps:get(auth_info, Ctx, undefined) of
        undefined ->
            ok;
        AuthInfo ->
            case maps:get(claims, AuthInfo, #{}) of
                #{<<"space">> := Space} ->
                    Scopes = maps:get(scopes, AuthInfo, []),
                    case Db =:= Space andalso covers(Scopes, Right) of
                        true -> ok;
                        false -> {error, forbidden}
                    end;
                _ ->
                    ok
            end
    end.

%% @doc The provenance of an MCP write: the authenticated subject as
%% actor, the caller's session (a `session' tool argument wins over
%% the MCP session id), source `mcp'.
-spec prov(map(), map()) -> map().
prov(Ctx, Args) ->
    Base = #{actor => actor(Ctx), source => <<"mcp">>},
    case session(Ctx, Args) of
        undefined -> Base;
        Session -> Base#{session => Session}
    end.

%%====================================================================
%% Internal
%%====================================================================

capability_info(Token) ->
    case barrel_caps:auth_context(Token) of
        {ok, #{space := Space, subject := Subject, scopes := Scopes}} ->
            {ok, #{subject => subject(Subject),
                   scopes => Scopes,
                   claims => #{<<"space">> => Space}}};
        {error, _} ->
            {error, invalid_token}
    end.

server_info_for(Token) ->
    case barrel_server_auth:hashes() of
        undefined ->
            %% open server: tokens are not checked anywhere else either
            {ok, anonymous()};
        Hashes ->
            Hash = crypto:hash(sha256, Token),
            case lists:any(
                     fun(Expected) -> crypto:hash_equals(Hash, Expected) end,
                     Hashes) of
                true ->
                    {ok, #{subject => <<"server">>,
                           scopes => [<<"admin">>]}};
                false ->
                    {error, invalid_token}
            end
    end.

anonymous() ->
    #{subject => <<"anonymous">>, scopes => [<<"admin">>]}.

subject(<<>>) -> <<"anonymous">>;
subject(Subject) -> Subject.

actor(Ctx) ->
    case maps:get(auth_info, Ctx, undefined) of
        undefined -> <<"anonymous">>;
        AuthInfo -> subject(maps:get(subject, AuthInfo, <<>>))
    end.

session(Ctx, Args) ->
    case maps:get(<<"session">>, Args, undefined) of
        S when is_binary(S), S =/= <<>> ->
            S;
        _ ->
            case maps:get(session_id, Ctx, undefined) of
                S when is_binary(S), S =/= <<>> -> S;
                _ -> undefined
            end
    end.

%% read < write < admin: any granted scope at or above the required
%% right covers it.
covers(Scopes, Right) ->
    Required = rank(Right),
    lists:any(fun(S) -> scope_rank(S) >= Required end, Scopes).

rank(read) -> 1;
rank(write) -> 2;
rank(admin) -> 3.

scope_rank(<<"read">>) -> 1;
scope_rank(<<"write">>) -> 2;
scope_rank(<<"admin">>) -> 3;
scope_rank(_) -> 0.
