%%%-------------------------------------------------------------------
%%% @doc Per-route capability authorization for the agent-layer REST
%%% surface. The auth middleware only authenticates; each handler asks
%%% this module whether the presented bearer covers the space and
%%% right it is about to touch.
%%%
%%% Three principals fall out of the bearer:
%%% - a capability token (`bsp_...') is verified against the space
%%%   with barrel_caps:verify/3 (wrong space and insufficient rights
%%%   are 403s, a token dead since the middleware check is a 401);
%%% - any other bearer is a global token the middleware already
%%%   validated: full access;
%%% - no bearer means the middleware is not installed (open mode):
%%%   full access, matching the rest of the API.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_caps).

-export([require/3, require_global/1]).

%% @doc The bearer must cover `Right' on `SpaceId'.
-spec require(livery_req:req(), binary(), barrel_caps:right()) ->
    ok | {error, term()}.
require(Req, SpaceId, Right) ->
    case livery_ext:bearer_token(Req) of
        undefined ->
            ok;
        <<"bsp_", _/binary>> = Token ->
            case barrel_caps:verify(Token, SpaceId, Right) of
                {ok, _Grant} -> ok;
                {error, _} = Err -> Err
            end;
        _Global ->
            ok
    end.

%% @doc The route is global-only: capability tokens are denied
%% whatever they grant (space creation, listing, dropping).
-spec require_global(livery_req:req()) -> ok | {error, forbidden}.
require_global(Req) ->
    case livery_ext:bearer_token(Req) of
        <<"bsp_", _/binary>> -> {error, forbidden};
        _ -> ok
    end.
