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
    t_tampered_body_rejected/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TOKEN, <<"secret-bearer">>).
-define(KEYID, <<"node1">>).
-define(DB, "sdb").

all() ->
    [t_bearer_authenticates, t_signed_authenticates, t_no_auth_rejected,
     t_unknown_key_rejected, t_stale_rejected, t_replay_rejected,
     t_tampered_body_rejected].

init_per_suite(Config) ->
    {Pub, Priv} = crypto:generate_key(eddsa, ed25519),
    application:load(barrel_server),
    application:set_env(barrel_server, data_dir, ?config(priv_dir, Config)),
    application:set_env(barrel_server, http_port, 0),
    application:set_env(barrel_server, auth,
                        #{accept => [bearer, signed],
                          tokens => [?TOKEN],
                          signers => #{?KEYID => Pub},
                          skew_ms => 300000}),
    {ok, _} = application:ensure_all_started(barrel_server),
    {ok, _} = application:ensure_all_started(hackney),
    Base = "http://127.0.0.1:" ++ integer_to_list(discover_port()),
    {201, _} = send(put, Base, <<"/db/", ?DB, "">>, bearer(), <<>>),
    [{base, Base}, {priv, Priv} | Config].

end_per_suite(_Config) ->
    application:stop(barrel_server),
    application:unset_env(barrel_server, auth),
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_bearer_authenticates(Config) ->
    B = ?config(base, Config),
    {200, _} = send(get, B, <<"/db/", ?DB, "">>, bearer(), <<>>),
    ok.

t_signed_authenticates(Config) ->
    B = ?config(base, Config),
    H = build_signed(get, <<"/db/", ?DB, "">>, <<>>, ?config(priv, Config),
                     now_ms()),
    {200, _} = send(get, B, <<"/db/", ?DB, "">>, H, <<>>),
    ok.

t_no_auth_rejected(Config) ->
    B = ?config(base, Config),
    {401, _} = send(get, B, <<"/db/", ?DB, "">>, [], <<>>),
    ok.

t_unknown_key_rejected(Config) ->
    B = ?config(base, Config),
    {_OtherPub, OtherPriv} = crypto:generate_key(eddsa, ed25519),
    %% a key the server does not know, even though the signature is valid
    H = build_signed_as(<<"ghost">>, get, <<"/db/", ?DB, "">>, <<>>,
                        OtherPriv, now_ms()),
    {401, _} = send(get, B, <<"/db/", ?DB, "">>, H, <<>>),
    ok.

t_stale_rejected(Config) ->
    B = ?config(base, Config),
    H = build_signed(get, <<"/db/", ?DB, "">>, <<>>, ?config(priv, Config),
                     now_ms() - 600000),
    {401, _} = send(get, B, <<"/db/", ?DB, "">>, H, <<>>),
    ok.

%% A byte-identical signed request replayed within the window is refused.
t_replay_rejected(Config) ->
    B = ?config(base, Config),
    Path = <<"/db/", ?DB, "/_sync/info">>,
    H = build_signed(get, Path, <<>>, ?config(priv, Config), now_ms()),
    {200, _} = send(get, B, Path, H, <<>>),
    {401, _} = send(get, B, Path, H, <<>>),
    ok.

%% Signature is valid (over the declared content hash) but the body does
%% not hash to it: the handler rejects. Tampering the header instead would
%% break the signature (401); tampering the body alone is 400.
t_tampered_body_rejected(Config) ->
    B = ?config(base, Config),
    Path = <<"/db/", ?DB, "/_sync/changes">>,
    Signed = <<"{\"since\":\"first\"}">>,
    Sent = <<"{\"since\":\"tampered\"}">>,
    H = build_signed(post, Path, Signed, ?config(priv, Config), now_ms()),
    {400, _} = send(post, B, Path, H, Sent),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

now_ms() -> erlang:system_time(millisecond).

bearer() -> [{<<"authorization">>, <<"Bearer ", ?TOKEN/binary>>}].

build_signed(Method, Path, SignBody, Priv, Ts) ->
    build_signed_as(?KEYID, Method, Path, SignBody, Priv, Ts).

build_signed_as(KeyId, Method, Path, SignBody, Priv, Ts) ->
    CH = barrel_sync_sig:content_sha256(SignBody),
    Auth = barrel_sync_sig:sign(KeyId, Priv, method_bin(Method), Path, CH, Ts),
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
    #{h1 := Port} = livery:which_listeners(Pid),
    Port.
