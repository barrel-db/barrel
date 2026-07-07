%%%-------------------------------------------------------------------
%%% @doc Bearer-token auth for the whole REST API.
%%%
%%% Two kinds of bearer. Global tokens come from
%%% `{barrel_server, auth, #{tokens => [Bin]}}' (or `#{token => Bin}';
%%% a LIST makes rotation possible) and open every route. Capability
%%% tokens (`bsp_...', issued by barrel_caps for one space)
%%% authenticate the agent-layer routes (`/spaces', `/handoffs'; the
%%% handlers enforce per-route rights via barrel_server_caps) and,
%%% scoped to their granted space, the `/db/:db' surface: the
%%% classifier below maps method+path to a required right (pull ops
%%% read, writes and push write) and barrel_caps:verify/3 checks the
%%% db IS the granted space. Everything unmapped answers 403
%%% (fail closed: new routes must be classified consciously); dead or
%%% wrong tokens answer 401. Unconfigured = the middleware is not
%%% installed and the server stays open. When installed, every route
%%% except `/health' (probes carry no secrets) requires
%%% `Authorization: Bearer <token>'.
%%%
%%% Global-token comparison is constant time: both sides are SHA-256
%%% hashed (fixed length, no length oracle) and compared with
%%% crypto:hash_equals/2.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_auth).
-behaviour(livery_middleware).

-export([state_from_env/0, hashes/0]).
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

%% @doc The configured token hashes (undefined = open server). The
%% MCP auth provider checks server bearers against the same set.
-spec hashes() -> [binary()] | undefined.
hashes() ->
    case state_from_env() of
        undefined -> undefined;
        #{hashes := Hashes} -> Hashes
    end.

call(Req, Next, #{hashes := Hashes}) ->
    case livery_req:path(Req) of
        <<"/health">> ->
            Next(Req);
        <<"/mcp">> ->
            %% the MCP endpoint authenticates through its own provider
            %% (barrel_server_mcp_auth), which covers the same server
            %% tokens plus capability tokens
            Next(Req);
        Path ->
            case livery_ext:bearer_token(Req) of
                undefined ->
                    unauthorized();
                <<"bsp_", _/binary>> = Token ->
                    capability(Req, Next, Token, Path);
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

%% Capability tokens: agent-layer surface (rights enforced per route
%% in the handlers), or the /db surface scoped to the granted space.
capability(Req, Next, Token, Path) ->
    case capability_scope(Path, livery_req:method(Req)) of
        agent ->
            case live_capability(Token) of
                true -> Next(Req);
                false -> unauthorized()
            end;
        {db, Db, Right} ->
            case barrel_caps:verify(Token, Db, Right) of
                {ok, _} -> Next(Req);
                {error, wrong_space} -> forbidden();
                {error, forbidden} -> forbidden();
                {error, _} -> unauthorized()
            end;
        forbidden ->
            forbidden()
    end.

%% Method+path to required right. FAIL CLOSED: any /db tail not
%% classified here answers 403 for capability principals, so every
%% route added to barrel_server_http:routes/0 needs a conscious entry.
-spec capability_scope(binary(), binary()) ->
    agent | {db, binary(), barrel_caps:right()} | forbidden.
capability_scope(<<"/spaces", _/binary>>, _Method) ->
    agent;
capability_scope(<<"/handoffs", _/binary>>, _Method) ->
    agent;
capability_scope(<<"/db/", Rest/binary>>, Method) ->
    {DbSeg, Tail} =
        case binary:split(Rest, <<"/">>) of
            [D] -> {D, <<>>};
            [D, T] -> {D, T}
        end,
    case db_right(Tail, Method) of
        forbidden -> forbidden;
        Right -> {db, uri_string:percent_decode(DbSeg), Right}
    end;
capability_scope(_Path, _Method) ->
    forbidden.

%% Space db lifecycle (create/drop) and timeline (branches are dbs
%% outside the grant's space) stay off-limits to capability tokens.
db_right(<<>>, <<"GET">>) -> read;
db_right(<<"doc/", DocTail/binary>>, Method) ->
    case has_segment(DocTail, <<"att">>) of
        true -> att_right(Method);
        false -> doc_right(DocTail, Method)
    end;
db_right(<<"_history">>, <<"GET">>) -> read;
db_right(<<"_bulk_get">>, <<"POST">>) -> read;
db_right(<<"find">>, <<"POST">>) -> read;
db_right(<<"query">>, <<"GET">>) -> read;
db_right(<<"query">>, <<"POST">>) -> read;
db_right(<<"changes">>, <<"GET">>) -> read;
db_right(<<"search/", _/binary>>, <<"POST">>) -> read;
db_right(<<"_bulk_docs">>, <<"POST">>) -> write;
db_right(<<"vector">>, <<"POST">>) -> write;
db_right(<<"_sync/", SyncTail/binary>>, Method) ->
    sync_right(SyncTail, Method);
db_right(_Tail, _Method) -> forbidden.

doc_right(DocTail, <<"GET">>) when DocTail =/= <<>> -> read;
doc_right(DocTail, <<"PUT">>) ->
    %% only the plain doc PUT; /_versions is read-only
    case has_segment(DocTail, <<"_versions">>) of
        true -> forbidden;
        false when DocTail =/= <<>> -> write;
        false -> forbidden
    end;
doc_right(DocTail, <<"DELETE">>) ->
    case has_segment(DocTail, <<"_versions">>) of
        true -> forbidden;
        false when DocTail =/= <<>> -> write;
        false -> forbidden
    end;
doc_right(_DocTail, _Method) -> forbidden.

att_right(<<"GET">>) -> read;
att_right(<<"PUT">>) -> write;
att_right(<<"DELETE">>) -> write;
att_right(_Method) -> forbidden.

%% The pull leg (incl. client-side checkpoint reads) needs read; the
%% push leg (put_version, checkpoint writes, attachment writes) write.
sync_right(<<"info">>, <<"GET">>) -> read;
sync_right(<<"hlc">>, <<"POST">>) -> read;
sync_right(<<"changes">>, <<"POST">>) -> read;
sync_right(<<"diff">>, <<"POST">>) -> read;
sync_right(<<"doc/", _/binary>>, <<"GET">>) -> read;
sync_right(<<"doc/", _/binary>>, <<"PUT">>) -> write;
sync_right(<<"local/", _/binary>>, <<"GET">>) -> read;
sync_right(<<"local/", _/binary>>, <<"PUT">>) -> write;
sync_right(<<"local/", _/binary>>, <<"DELETE">>) -> write;
sync_right(<<"att_changes">>, <<"GET">>) -> read;
sync_right(<<"att_diff">>, <<"POST">>) -> read;
sync_right(<<"att/", _/binary>>, <<"GET">>) -> read;
sync_right(<<"att/", _/binary>>, <<"PUT">>) -> write;
sync_right(<<"att/", _/binary>>, <<"DELETE">>) -> write;
sync_right(_Tail, _Method) -> forbidden.

has_segment(Path, Segment) ->
    lists:member(Segment, binary:split(Path, <<"/">>, [global])).

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

forbidden() ->
    livery_resp:new(
        403,
        [{<<"content-type">>, <<"application/json">>}],
        {full, <<"{\"error\":\"forbidden\"}">>}).
