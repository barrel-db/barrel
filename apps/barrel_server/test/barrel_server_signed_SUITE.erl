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
    t_replication_over_signed/1,
    t_att_delete_origin_tamper_rejected/1,
    t_att_put_origin_tamper_rejected/1,
    t_att_put_digest_tamper_rejected/1,
    t_att_put_content_type_tamper_rejected/1,
    t_att_touch_tamper_rejected/1,
    t_sync_hlc_and_type_tamper_rejected/1,
    t_signed_v3_writes_accepted/1,
    t_bearer_att_headers_accepted/1,
    t_legacy_semantic_header_rejected/1,
    t_legacy_att_write_rejected/1
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
     t_legacy_rejected_when_required, t_replication_over_signed,
     t_att_delete_origin_tamper_rejected, t_att_put_origin_tamper_rejected,
     t_att_put_digest_tamper_rejected,
     t_att_put_content_type_tamper_rejected, t_att_touch_tamper_rejected,
     t_sync_hlc_and_type_tamper_rejected, t_signed_v3_writes_accepted,
     t_bearer_att_headers_accepted, t_legacy_semantic_header_rejected,
     t_legacy_att_write_rejected].

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
        {ok, _} = barrel_docdb:put_attachment(
            Local, <<"d1">>, <<"r.txt">>, <<"replicated">>,
            #{content_type => <<"text/plain">>}),
        lists:foreach(
            fun(_) ->
                {ok, #{ok := true}} = barrel_rep:replicate(
                    Local, Endpoint,
                    #{target_transport => barrel_rep_transport_http})
            end, lists:seq(1, 5)),
        H = build_signed(get, <<"/db/", ?DB, "/doc/d5">>, <<>>,
                         ?config(priv, Config), now_ms()),
        {200, _} = send(get, B, <<"/db/", ?DB, "/doc/d5">>, H, <<>>),
        %% attachment PUT and DELETE go out as v3 with their headers signed
        {ok, <<"replicated">>} =
            barrel_docdb:get_attachment(<<?DB>>, <<"d1">>, <<"r.txt">>),
        {ok, #{content_type := <<"text/plain">>}} =
            barrel_docdb:get_attachment_info(<<?DB>>, <<"d1">>, <<"r.txt">>),
        ok = barrel_docdb:delete_attachment(Local, <<"d1">>, <<"r.txt">>),
        {ok, #{ok := true}} = barrel_rep:replicate(
            Local, Endpoint, #{target_transport => barrel_rep_transport_http}),
        {error, not_found} =
            barrel_docdb:get_attachment(<<?DB>>, <<"d1">>, <<"r.txt">>)
    after
        try barrel_docdb:delete_db(Local) catch _:_ -> ok end
    end,
    ok.


%% The reviewer's case: a fresh signed DELETE whose origin would lose
%% last-write-wins, sent first with only its origin made newer. It must
%% fail authentication and leave the attachment in place.
t_att_delete_origin_tamper_rejected(Config) ->
    B = current_base(),
    #{old := Old, new := New, path := Path} = seed_att(<<"a1">>),
    Signed = signed_v3(delete, Path, <<>>, [origin(Old)], Config),
    Altered = set_header(<<"x-barrel-att-origin">>, origin_wire(New), Signed),
    {401, _} = send(delete, B, Path, Altered, <<>>),
    {ok, <<"stored">>} = att_body(<<"a1">>),
    %% the original is still honored: an older delete is ignored
    {200, _} = send(delete, B, Path, Signed, <<>>),
    {ok, <<"stored">>} = att_body(<<"a1">>),
    ok.

t_att_put_origin_tamper_rejected(Config) ->
    B = current_base(),
    #{old := Old, new := New, path := Path} = seed_att(<<"a2">>),
    Body = <<"forged">>,
    Signed = signed_put(Path, Body, [origin(Old)], Config),
    Altered = set_header(<<"x-barrel-att-origin">>, origin_wire(New), Signed),
    {401, _} = send(put, B, Path, Altered, Body),
    {ok, <<"stored">>} = att_body(<<"a2">>),
    ok.

t_att_put_digest_tamper_rejected(Config) ->
    B = current_base(),
    #{new := New, path := Path} = seed_att(<<"a3">>),
    Body = <<"newer">>,
    Signed = signed_put(Path, Body, [origin(New)], Config),
    Other = <<"sha256-", (barrel_sync_sig:content_sha256(<<"x">>))/binary>>,
    Altered = set_header(<<"x-barrel-digest">>, Other, Signed),
    {401, _} = send(put, B, Path, Altered, Body),
    {ok, <<"stored">>} = att_body(<<"a3">>),
    ok.

t_att_put_content_type_tamper_rejected(Config) ->
    B = current_base(),
    #{new := New, path := Path} = seed_att(<<"a4">>),
    Body = <<"newer">>,
    Signed = signed_put(Path, Body, [origin(New)], Config),
    Altered = set_header(<<"content-type">>, <<"text/html">>, Signed),
    {401, _} = send(put, B, Path, Altered, Body),
    {ok, <<"stored">>} = att_body(<<"a4">>),
    ok.

%% Removing or adding the touch marker changes the write: both 401.
t_att_touch_tamper_rejected(Config) ->
    B = current_base(),
    #{new := New, path := Path} = seed_att(<<"a5">>),
    Digest = stored_digest(<<"a5">>),
    Touch = signed_v3(put, Path, {hash, Digest},
                      [{<<"content-type">>, <<"application/octet-stream">>},
                       {<<"x-barrel-att-touch">>, <<"1">>},
                       {<<"x-barrel-digest">>, Digest}, origin(New)],
                      Config),
    {401, _} = send(put, B, Path,
                    lists:keydelete(<<"x-barrel-att-touch">>, 1, Touch),
                    <<>>),
    Upload = signed_put(Path, <<"newer">>, [origin(New)], Config),
    {401, _} = send(put, B, Path,
                    [{<<"x-barrel-att-touch">>, <<"1">>} | Upload],
                    <<"newer">>),
    {ok, <<"stored">>} = att_body(<<"a5">>),
    {200, Touched} = send(put, B, Path, Touch, <<>>),
    #{<<"ok">> := <<"touched">>} = json:decode(Touched),
    ok.

%% The clock header and the body type of a sync write are signed too.
t_sync_hlc_and_type_tamper_rejected(Config) ->
    B = current_base(),
    Path = <<"/db/", ?DB, "/_sync/local/l1">>,
    Body = <<"{\"n\":1}">>,
    Signed = signed_v3(put, Path, Body,
                       [{<<"content-type">>, <<"application/json">>},
                        {<<"x-barrel-hlc">>, origin_wire(
                                               barrel_hlc:new_hlc())}],
                       Config),
    Far = barrel_hlc:new_hlc(),
    {401, _} = send(put, B, Path,
                    set_header(<<"x-barrel-hlc">>, origin_wire(Far), Signed),
                    Body),
    {401, _} = send(put, B, Path,
                    set_header(<<"content-type">>, <<"application/cbor">>,
                               Signed), Body),
    {error, not_found} = barrel_docdb:get_local_doc(<<?DB>>, <<"l1">>),
    {200, _} = send(put, B, Path, Signed, Body),
    {ok, #{<<"n">> := 1}} = barrel_docdb:get_local_doc(<<?DB>>, <<"l1">>),
    ok.

t_signed_v3_writes_accepted(Config) ->
    B = current_base(),
    #{new := New, path := Path} = seed_att(<<"a6">>),
    Put = signed_put(Path, <<"newer">>, [origin(New)], Config),
    {200, _} = send(put, B, Path, Put, <<"newer">>),
    {ok, <<"newer">>} = att_body(<<"a6">>),
    Newest = barrel_hlc:new_hlc(),
    Del = signed_v3(delete, Path, <<>>, [origin(Newest)], Config),
    {200, _} = send(delete, B, Path, Del, <<>>),
    {error, not_found} = att_body(<<"a6">>),
    ok.

t_bearer_att_headers_accepted(_Config) ->
    B = current_base(),
    #{new := New, path := Path} = seed_att(<<"a7">>),
    {200, _} = send(delete, B, Path, [origin(New) | bearer()], <<>>),
    {error, not_found} = att_body(<<"a7">>),
    ok.

%% A v2 signature does not cover the origin header: refused by name.
t_legacy_semantic_header_rejected(Config) ->
    B = current_base(),
    #{new := New, path := Path} = seed_att(<<"a8">>),
    H = build_signed(delete, Path, <<>>, ?config(priv, Config), now_ms()),
    {401, Resp} = send(delete, B, Path, [origin(New) | H], <<>>),
    #{<<"error">> := <<"signature_v3_required">>} = json:decode(Resp),
    {ok, <<"stored">>} = att_body(<<"a8">>),
    ok.

%% Even without those headers, a v2 signature cannot write attachments
%% (its content-type would be unauthenticated).
t_legacy_att_write_rejected(Config) ->
    B = current_base(),
    #{path := Path} = seed_att(<<"a9">>),
    Body = <<"v2 body">>,
    H = build_signed(put, Path, Body, ?config(priv, Config), now_ms()),
    {401, Resp} = send(put, B, Path, H, Body),
    #{<<"error">> := <<"signature_v3_required">>} = json:decode(Resp),
    {ok, <<"stored">>} = att_body(<<"a9">>),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% An attachment written at origin `mid', with an older and a newer HLC.
seed_att(DocId) ->
    Old = barrel_hlc:new_hlc(),
    Mid = barrel_hlc:new_hlc(),
    New = barrel_hlc:new_hlc(),
    {ok, _} = barrel_docdb:put_doc(<<?DB>>, #{<<"id">> => DocId}),
    {ok, _} = barrel_docdb:put_attachment(<<?DB>>, DocId, <<"f">>,
                                          <<"stored">>,
                                          #{origin_hlc => Mid}),
    #{old => Old, new => New,
      path => <<"/db/", ?DB, "/_sync/att/", DocId/binary, "/f">>}.

att_body(DocId) ->
    barrel_docdb:get_attachment(<<?DB>>, DocId, <<"f">>).

stored_digest(DocId) ->
    {ok, #{digest := Digest}} =
        barrel_docdb:get_attachment_info(<<?DB>>, DocId, <<"f">>),
    Digest.

origin(Hlc) -> {<<"x-barrel-att-origin">>, origin_wire(Hlc)}.

origin_wire(Hlc) -> base64:encode(barrel_hlc:encode(Hlc)).

set_header(Name, Value, Headers) ->
    lists:keystore(Name, 1, Headers, {Name, Value}).

%% A signed attachment upload: the digest is the signed content hash.
signed_put(Path, Body, Extra, Config) ->
    Digest = <<"sha256-", (barrel_sync_sig:content_sha256(Body))/binary>>,
    signed_v3(put, Path, {hash, Digest},
              [{<<"content-type">>, <<"application/octet-stream">>},
               {<<"x-barrel-digest">>, Digest} | Extra], Config).

%% v3 headers over `Headers', signed over a body or `{hash, Digest}'.
signed_v3(Method, Path, Body, Headers, Config) ->
    CH = case Body of
        {hash, Digest} -> Digest;
        _ -> barrel_sync_sig:content_sha256(Body)
    end,
    {ok, Picked} = barrel_sync_sig:pick_headers(Headers),
    Auth = barrel_sync_sig:sign_v3(?KEYID, ?config(priv, Config),
                                   method_bin(Method), Path, <<>>, CH,
                                   Picked, now_ms()),
    Headers ++ [{<<"authorization">>, Auth},
                {<<"x-barrel-content-sha256">>, CH}].

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
