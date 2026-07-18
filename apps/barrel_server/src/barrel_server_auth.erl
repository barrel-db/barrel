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
%%
%% A config WITHOUT an `accept' key is legacy and behaves exactly as
%% before (bearer only; empty/invalid = open). A config WITH `accept'
%% opts into the multi-method system (bearer | signed | mtls) and is
%% always installed (fail-closed on misconfiguration).
-spec state_from_env() -> map() | undefined.
state_from_env() ->
    case application:get_env(barrel_server, auth, undefined) of
        undefined ->
            undefined;
        Cfg when is_map(Cfg) ->
            case maps:is_key(accept, Cfg) of
                false -> legacy_state(Cfg);
                true -> multi_state(Cfg)
            end;
        Other ->
            logger:warning("ignoring invalid barrel_server auth config: ~p",
                           [Other]),
            undefined
    end.

%% Legacy bearer-only config: byte-for-byte the previous behavior.
legacy_state(#{tokens := Tokens}) when is_list(Tokens), Tokens =/= [] ->
    base_state([crypto:hash(sha256, T) || T <- Tokens], #{}, [bearer], 300000);
legacy_state(#{token := Token}) when is_binary(Token) ->
    base_state([crypto:hash(sha256, Token)], #{}, [bearer], 300000);
legacy_state(Other) ->
    logger:warning("ignoring invalid barrel_server auth config: ~p", [Other]),
    undefined.

%% New-style config: accept => [bearer|signed|mtls], optional signers and
%% skew. Unknown accept methods are dropped; an empty accept locks the
%% server (every request 401s) rather than falling open.
multi_state(Cfg) ->
    Accept0 = [M || M <- maps:get(accept, Cfg, [bearer]),
                    lists:member(M, [bearer, signed, mtls])],
    Accept = drop_ungated_mtls(Accept0),
    Hashes = token_hashes(Cfg),
    Signers = maps:get(signers, Cfg, #{}),
    SkewMs = maps:get(skew_ms, Cfg, 300000),
    base_state(Hashes, Signers, Accept, SkewMs).

%% mtls_ok trusts that a TLS connection carries a verified client cert,
%% which only holds when the listener actually gates on it. If mtls is
%% accepted but the tls env does not set `verify => verify_peer', drop mtls
%% from the accepted set (fail closed) so a certless client is never
%% authenticated as a trusted peer by a config gap.
drop_ungated_mtls(Accept) ->
    case lists:member(mtls, Accept) andalso not mtls_gate_configured() of
        true ->
            logger:warning("barrel_server: auth accepts mtls but tls has no "
                           "verify_peer; disabling mtls auth"),
            Accept -- [mtls];
        false ->
            Accept
    end.

mtls_gate_configured() ->
    case application:get_env(barrel_server, tls, undefined) of
        #{verify := verify_peer} -> true;
        _ -> false
    end.

token_hashes(#{tokens := Tokens}) when is_list(Tokens) ->
    [crypto:hash(sha256, T) || T <- Tokens];
token_hashes(#{token := Token}) when is_binary(Token) ->
    [crypto:hash(sha256, Token)];
token_hashes(_) ->
    [].

base_state(Hashes, Signers, Accept, SkewMs) ->
    #{hashes => Hashes, signers => Signers,
      accept => Accept, skew_ms => SkewMs}.

%% @doc The configured token hashes (undefined = open server). The
%% MCP auth provider checks server bearers against the same set.
-spec hashes() -> [binary()] | undefined.
hashes() ->
    case state_from_env() of
        undefined -> undefined;
        #{hashes := []} -> undefined;
        #{hashes := Hashes} -> Hashes
    end.

call(Req, Next, State) ->
    case livery_req:path(Req) of
        <<"/health">> ->
            Next(Req);
        <<"/mcp">> ->
            %% the MCP endpoint authenticates through its own provider
            %% (barrel_server_mcp_auth), which covers the same server
            %% tokens plus capability tokens
            Next(Req);
        Path ->
            %% Capability tokens (bsp_...) are a scoped principal for the
            %% agent layer and are handled as before, independent of the
            %% global `accept' set. Everything else must satisfy one of
            %% the accepted global methods.
            case livery_ext:bearer_token(Req) of
                <<"bsp_", _/binary>> = Token ->
                    capability(Req, Next, Token, Path);
                _ ->
                    case authorized(Req, State) of
                        true -> Next(Req);
                        false -> unauthorized()
                    end
            end
    end.

%% True if any accepted global method authenticates the request.
authorized(Req, #{accept := Accept} = State) ->
    (lists:member(signed, Accept) andalso try_signed(Req, State))
        orelse (lists:member(bearer, Accept) andalso try_bearer(Req, State))
        orelse (lists:member(mtls, Accept) andalso mtls_ok(Req)).

%% Ed25519 signed request: verify the signature, then consume it against
%% the replay cache. Any failure (no header, unknown key, bad signature,
%% stale timestamp, replay, or cache down) is a decline.
try_signed(Req, #{signers := Signers, skew_ms := SkewMs}) ->
    case barrel_sync_sig:parse_auth(
             livery_req:header(<<"authorization">>, Req, undefined)) of
        {ok, #{key_id := KeyId, ts := Ts, sig := Sig} = Parsed} ->
            Method = livery_req:method(Req),
            Path = livery_req:path(Req),
            ContentHash = livery_req:header(<<"x-barrel-content-sha256">>,
                                            Req, <<>>),
            case barrel_sync_sig:verify(Method, Path, ContentHash,
                                          Parsed, Signers, SkewMs) of
                ok ->
                    barrel_server_sig_cache:check_and_insert(
                        {KeyId, Ts, Sig}, 2 * SkewMs) =:= ok;
                {error, _} ->
                    false
            end;
        _ ->
            false
    end.

%% Constant-time global bearer check (bsp_ tokens never reach here).
try_bearer(Req, #{hashes := Hashes}) ->
    case livery_ext:bearer_token(Req) of
        Token when is_binary(Token) ->
            Hash = crypto:hash(sha256, Token),
            lists:any(fun(Expected) -> crypto:hash_equals(Hash, Expected) end,
                      Hashes);
        _ ->
            false
    end.

%% mTLS transport gate: a request that arrived over TLS is authenticated,
%% relying on the listener's `fail_if_no_peer_cert' so arrival implies a
%% verified client cert. H1-TLS surfaces `tls => #{}'; H2/H3 surface no
%% tls today (see the companion livery change), so this is H1-only.
mtls_ok(Req) ->
    livery_req:tls(Req) =/= undefined.

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
