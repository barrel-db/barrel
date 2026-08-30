%%%-------------------------------------------------------------------
%%% @doc Ed25519 signed-request auth over the REST/sync wire, with
%%% bearer dual-accept and replay protection.
%%%
%%% Boots barrel_server with `accept => [bearer, signed]' and exercises
%%% both principals plus the failure modes the retired peer-auth scheme
%%% missed: replay, tampered body, stale timestamp, unknown key. The
%%% bearer-only (legacy) behavior is covered by barrel_server_auth_SUITE.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_signed_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    t_bearer_authenticates/1,
    t_signed_authenticates/1,
    t_no_auth_rejected/1,
    t_unknown_key_rejected/1,
    t_stale_rejected/1,
    t_replay_rejected/1,
    t_tampered_body_rejected/1,
    t_same_ms_distinct_nonce_accepted/1,
    t_query_tamper_rejected/1,
    t_legacy_v1_accepted/1,
    t_legacy_rejected_when_required/1,
    t_replication_over_signed/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TOKEN, <<"secret-bearer">>).
-define(KEYID, <<"node1">>).
-define(DB, "sdb").

all() ->
    [t_bearer_authenticates, t_signed_authenticates, t_no_auth_rejected,
     t_unknown_key_rejected, t_stale_rejected, t_replay_rejected,
     t_tampered_body_rejected, t_same_ms_distinct_nonce_accepted,
     t_query_tamper_rejected, t_legacy_v1_accepted,
     t_legacy_rejected_when_required, t_replication_over_signed].

init_per_suite(Config) ->
    {Pub, Priv} = crypto:generate_key(eddsa, ed25519),
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    application:set_env(barrel_server, auth, auth_env(Pub)),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Base = "http://127.0.0.1:" ++ integer_to_list(discover_port()),
    {201, _} = send(put, Base, <<"/db/", ?DB, "">>, bearer(), <<>>),
    [{base, Base}, {priv, Priv}, {pub, Pub} | Config].

auth_env(Pub) ->
    #{accept => [bearer, signed],
      tokens => [?TOKEN],
      signers => #{?KEYID => Pub},
      skew_ms => 300000}.

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, auth),
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_bearer_authenticates(_Config) ->
    B = current_base(),
    {200, _} = send(get, B, <<"/db/", ?DB, "">>, bearer(), <<>>),
    ok.

t_signed_authenticates(Config) ->
    B = current_base(),
    H = build_signed(get, <<"/db/", ?DB, "">>, <<>>, ?config(priv, Config),
                     now_ms()),
    {200, _} = send(get, B, <<"/db/", ?DB, "">>, H, <<>>),
    ok.

t_no_auth_rejected(_Config) ->
    B = current_base(),
    {401, _} = send(get, B, <<"/db/", ?DB, "">>, [], <<>>),
    ok.

t_unknown_key_rejected(_Config) ->
    B = current_base(),
    {_OtherPub, OtherPriv} = crypto:generate_key(eddsa, ed25519),
    %% a key the server does not know, even though the signature is valid
    H = build_signed_as(<<"ghost">>, get, <<"/db/", ?DB, "">>, <<>>,
                        OtherPriv, now_ms()),
    {401, _} = send(get, B, <<"/db/", ?DB, "">>, H, <<>>),
    ok.

t_stale_rejected(Config) ->
    B = current_base(),
    H = build_signed(get, <<"/db/", ?DB, "">>, <<>>, ?config(priv, Config),
                     now_ms() - 600000),
    {401, _} = send(get, B, <<"/db/", ?DB, "">>, H, <<>>),
    ok.

%% A byte-identical signed request replayed within the window is refused.
t_replay_rejected(Config) ->
    B = current_base(),
    Path = <<"/db/", ?DB, "/_sync/info">>,
    H = build_signed(get, Path, <<>>, ?config(priv, Config), now_ms()),
    {200, _} = send(get, B, Path, H, <<>>),
    {401, _} = send(get, B, Path, H, <<>>),
    ok.

%% Signature is valid (over the declared content hash) but the body does
%% not hash to it: the handler rejects. Tampering the header instead would
%% break the signature (401); tampering the body alone is 400.
t_tampered_body_rejected(Config) ->
    B = current_base(),
    Path = <<"/db/", ?DB, "/_sync/changes">>,
    Signed = <<"{\"since\":\"first\"}">>,
    Sent = <<"{\"since\":\"tampered\"}">>,
    H = build_signed(post, Path, Signed, ?config(priv, Config), now_ms()),
    {400, _} = send(post, B, Path, H, Sent),
    ok.

%% Two v2 requests in the same millisecond carry different nonces, so
%% neither is mistaken for a replay of the other (the v1 bug).
t_same_ms_distinct_nonce_accepted(Config) ->
    B = current_base(),
    Path = <<"/db/", ?DB, "/_sync/info">>,
    Ts = now_ms(),
    H1 = build_signed(get, Path, <<>>, ?config(priv, Config), Ts),
    H2 = build_signed(get, Path, <<>>, ?config(priv, Config), Ts),
    {200, _} = send(get, B, Path, H1, <<>>),
    {200, _} = send(get, B, Path, H2, <<>>),
    ok.

%% The query string is part of the signed target: altering it 401s.
t_query_tamper_rejected(Config) ->
    B = current_base(),
    Path = <<"/db/", ?DB, "/_sync/att_changes">>,
    H = build_signed_q(get, Path, <<"since=first&limit=1">>, <<>>,
                       ?config(priv, Config), now_ms()),
    {401, _} = send(get, B, <<Path/binary, "?since=first&limit=2">>, H,
                    <<>>),
    H2 = build_signed_q(get, Path, <<"since=first&limit=1">>, <<>>,
                        ?config(priv, Config), now_ms()),
    {200, _} = send(get, B, <<Path/binary, "?since=first&limit=1">>, H2,
                    <<>>),
    ok.

%% A pre-v2 client (no nonce, path only) still authenticates by default.
t_legacy_v1_accepted(Config) ->
    B = current_base(),
    Path = <<"/db/", ?DB, "">>,
    CH = barrel_sync_sig:content_sha256(<<>>),
    Auth = barrel_sync_sig:sign(?KEYID, ?config(priv, Config), <<"GET">>,
                                Path, CH, now_ms()),
    H = [{<<"authorization">>, Auth}, {<<"x-barrel-content-sha256">>, CH}],
    {200, _} = send(get, B, Path, H, <<>>),
    ok.

%% With require_nonce the v1 form is refused and v2 keeps working.
t_legacy_rejected_when_required(Config) ->
    Pub = ?config(pub, Config),
    B = restart_with_auth((auth_env(Pub))#{require_nonce => true}),
    try
        Path = <<"/db/", ?DB, "">>,
        CH = barrel_sync_sig:content_sha256(<<>>),
        V1 = barrel_sync_sig:sign(?KEYID, ?config(priv, Config), <<"GET">>,
                                  Path, CH, now_ms()),
        {401, _} = send(get, B, Path,
                        [{<<"authorization">>, V1},
                         {<<"x-barrel-content-sha256">>, CH}], <<>>),
        H = build_signed(get, Path, <<>>, ?config(priv, Config), now_ms()),
        {200, _} = send(get, B, Path, H, <<>>)
    after
        _ = restart_with_auth(auth_env(Pub))
    end,
    ok.

%% The original failure: replication repeats identical requests back to
%% back; over signed auth every run must complete.
t_replication_over_signed(Config) ->
    B = current_base(),
    Local = <<"signed_rep_local">>,
    {ok, _} = barrel_docdb:create_db(Local, #{
        data_dir => filename:join(?config(priv_dir, Config), "signed_local")
    }),
    try
        lists:foreach(
            fun(I) ->
                {ok, _} = barrel_docdb:put_doc(Local, #{
                    <<"id">> => <<"d", (integer_to_binary(I))/binary>>,
                    <<"n">> => I})
            end, lists:seq(1, 5)),
        Endpoint0 = barrel_rep_transport_http:endpoint(
            list_to_binary(B ++ "/db/" ++ ?DB)),
        Endpoint = Endpoint0#{signing => #{key_id => ?KEYID,
                                           priv_key => ?config(priv, Config)}},
        lists:foreach(
            fun(_) ->
                {ok, #{ok := true}} = barrel_rep:replicate(
                    Local, Endpoint,
                    #{target_transport => barrel_rep_transport_http})
            end, lists:seq(1, 5)),
        H = build_signed(get, <<"/db/", ?DB, "/doc/d5">>, <<>>,
                         ?config(priv, Config), now_ms()),
        {200, _} = send(get, B, <<"/db/", ?DB, "/doc/d5">>, H, <<>>)
    after
        try barrel_docdb:delete_db(Local) catch _:_ -> ok end
    end,
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% The live base URL: a case may have restarted the server on another
%% ephemeral port, so never trust the one captured at suite init.
current_base() ->
    "http://127.0.0.1:" ++ integer_to_list(discover_port()).

%% Stop and restart barrel_server with another auth config; returns the
%% new base URL (the port is ephemeral).
restart_with_auth(Auth) ->
    ok = application:stop(barrel_server),
    application:set_env(barrel_server, auth, Auth),
    {ok, _} = application:ensure_all_started(barrel_server),
    "http://127.0.0.1:" ++ integer_to_list(discover_port()).

now_ms() -> erlang:system_time(millisecond).

bearer() -> [{<<"authorization">>, <<"Bearer ", ?TOKEN/binary>>}].

build_signed(Method, Path, SignBody, Priv, Ts) ->
    build_signed_as(?KEYID, Method, Path, SignBody, Priv, Ts).

build_signed_q(Method, Path, Query, SignBody, Priv, Ts) ->
    build_signed_as(?KEYID, Method, Path, Query, SignBody, Priv, Ts).

build_signed_as(KeyId, Method, Path, SignBody, Priv, Ts) ->
    build_signed_as(KeyId, Method, Path, <<>>, SignBody, Priv, Ts).

%% v2 header: nonce, path plus raw query signed
build_signed_as(KeyId, Method, Path, Query, SignBody, Priv, Ts) ->
    CH = barrel_sync_sig:content_sha256(SignBody),
    Auth = barrel_sync_sig:sign(KeyId, Priv, method_bin(Method), Path, Query,
                                CH, Ts),
    [{<<"authorization">>, Auth},
     {<<"x-barrel-content-sha256">>, CH},
     {<<"content-type">>, <<"application/json">>}].

send(Method, Base, Path, Headers, Body) ->
    Url = iolist_to_binary([Base, Path]),
    {ok, S, _H, RB} = hackney:request(Method, Url, Headers, Body,
                                      [with_body]),
    {S, RB}.

method_bin(get) -> <<"GET">>;
method_bin(post) -> <<"POST">>;
method_bin(put) -> <<"PUT">>;
method_bin(delete) -> <<"DELETE">>.

discover_port() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    Port = barrel_server_test:h1_port(Pid),
    Port.
