%%%-------------------------------------------------------------------
%%% @doc Multi-protocol serving (H1/H2/H3) and the mTLS transport gate.
%%%
%%% Boots barrel_server with an H1-over-TLS listener requiring client
%%% certs (`verify_peer' + `fail_if_no_peer_cert'), plus H2 and H3
%%% listeners, and `accept => [mtls]'. Verifies: all three listeners bind;
%%% a client presenting a CA-signed cert authenticates; a client with no
%%% cert is refused at the TLS handshake; a request presenting a signature
%%% is judged by it alone (`accept => [mtls, signed]'). Requires `openssl' (skips
%%% otherwise). The sync client speaks HTTP/1.1, so the gate is exercised
%%% on H1-TLS.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_mtls_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([t_listeners_bind/1, t_mtls_client_authenticates/1,
         t_certless_refused/1, t_bad_signature_rejected/1,
         t_valid_signature_accepted/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(DB, "mdb").
-define(KEYID, <<"node1">>).

all() ->
    [t_listeners_bind, t_mtls_client_authenticates, t_certless_refused,
     t_bad_signature_rejected, t_valid_signature_accepted].

init_per_suite(Config) ->
    case os:find_executable("openssl") of
        false ->
            {skip, "openssl not available"};
        _ ->
            Dir = ?config(priv_dir, Config),
            ok = gen_certs(Dir),
            application:load(barrel_server),
            application:set_env(barrel_server, data_dir, Dir),
            application:set_env(barrel_server, listeners,
                                #{http => #{port => 0, tls => true},
                                  https => #{port => 0},
                                  http3 => #{port => 0}}),
            application:set_env(barrel_server, tls,
                                #{certfile => f(Dir, "server.pem"),
                                  keyfile => f(Dir, "server.key"),
                                  cacertfile => f(Dir, "ca.pem"),
                                  verify => verify_peer}),
            {Pub, Priv} = crypto:generate_key(eddsa, ed25519),
            application:set_env(barrel_server, auth,
                                #{accept => [mtls, signed],
                                  signers => #{?KEYID => Pub}}),
            {ok, _} = application:ensure_all_started(barrel_server),
            {ok, _} = application:ensure_all_started(hackney),
            Listeners = listeners(),
            [{dir, Dir}, {listeners, Listeners}, {priv, Priv} | Config]
    end.

end_per_suite(Config) ->
    case ?config(dir, Config) of
        undefined -> ok;
        _ ->
            application:stop(barrel_server),
            application:unset_env(barrel_server, listeners),
            application:unset_env(barrel_server, tls),
            application:unset_env(barrel_server, auth)
    end,
    ok.

%%====================================================================
%% Cases
%%====================================================================

t_listeners_bind(Config) ->
    L = ?config(listeners, Config),
    ?assertMatch(#{h1 := _, h2 := _, h3 := _}, L),
    ok.

t_mtls_client_authenticates(Config) ->
    Base = tls_base(Config),
    Dir = ?config(dir, Config),
    {201, _} = req(put, Base ++ "/db/" ++ ?DB, client_ssl(Dir)),
    {200, _} = req(get, Base ++ "/db/" ++ ?DB, client_ssl(Dir)),
    ok.

%% No client certificate: the listener's fail_if_no_peer_cert rejects at
%% the TLS handshake, so the request never reaches the app.
t_certless_refused(Config) ->
    Base = tls_base(Config),
    Dir = ?config(dir, Config),
    Res = hackney:request(get, list_to_binary(Base ++ "/db/" ++ ?DB),
                          [], <<>>,
                          [with_body, {ssl_options, no_cert_ssl(Dir)}]),
    ?assertMatch({error, _}, Res),
    ok.

%% A trusted client certificate does not rescue a failing signature.
t_bad_signature_rejected(Config) ->
    Base = tls_base(Config),
    Dir = ?config(dir, Config),
    Path = <<"/db/", ?DB, "/doc/badsig">>,
    {_, OtherPriv} = crypto:generate_key(eddsa, ed25519),
    H = signed(put, Path, <<"{}">>, OtherPriv),
    {401, _} = req(put, Base, Path, H, <<"{}">>, client_ssl(Dir)),
    {404, _} = req(get, Base, Path, [], <<>>, client_ssl(Dir)),
    ok.

t_valid_signature_accepted(Config) ->
    Base = tls_base(Config),
    Dir = ?config(dir, Config),
    Path = <<"/db/", ?DB, "/doc/goodsig">>,
    H = signed(put, Path, <<"{}">>, ?config(priv, Config)),
    {201, _} = req(put, Base, Path, H, <<"{}">>, client_ssl(Dir)),
    {200, _} = req(get, Base, Path, [], <<>>, client_ssl(Dir)),
    ok.

%%====================================================================
%% Helpers
%%====================================================================

listeners() ->
    Children = supervisor:which_children(barrel_server_sup),
    {_, Pid, _, _} = lists:keyfind(barrel_server_http, 1, Children),
    barrel_server_test:listener_ports(Pid).

tls_base(Config) ->
    #{h1 := Port} = ?config(listeners, Config),
    "https://127.0.0.1:" ++ integer_to_list(Port).

%% present the client cert and trust the test CA (which signed the server
%% cert, SAN IP:127.0.0.1) so the server-cert check passes; the point
%% under test is the server verifying us.
client_ssl(Dir) ->
    [{certfile, f(Dir, "client.pem")},
     {keyfile, f(Dir, "client.key")},
     {cacertfile, f(Dir, "ca.pem")},
     {verify, verify_peer}].

%% trust the server but present no client cert: the server rejects.
no_cert_ssl(Dir) ->
    [{cacertfile, f(Dir, "ca.pem")},
     {verify, verify_peer}].

req(Method, Url, SslOpts) ->
    {ok, S, _H, RB} = hackney:request(Method, list_to_binary(Url), [], <<>>,
                                      [with_body, {ssl_options, SslOpts}]),
    {S, RB}.

req(Method, Base, Path, Headers, Body, SslOpts) ->
    Url = iolist_to_binary([Base, Path]),
    {ok, S, _H, RB} = hackney:request(Method, Url, Headers, Body,
                                      [with_body, {ssl_options, SslOpts}]),
    {S, RB}.

%% v3 signed headers over a JSON body.
signed(Method, Path, Body, Priv) ->
    Headers = [{<<"content-type">>, <<"application/json">>}],
    CH = barrel_sync_sig:content_sha256(Body),
    {ok, Picked} = barrel_sync_sig:pick_headers(Headers),
    Auth = barrel_sync_sig:sign_v3(?KEYID, Priv,
                                   string:uppercase(atom_to_binary(Method)),
                                   Path, <<>>, CH, Picked,
                                   erlang:system_time(millisecond)),
    Headers ++ [{<<"authorization">>, Auth},
                {<<"x-barrel-content-sha256">>, CH}].

f(Dir, Name) -> filename:join(Dir, Name).

gen_certs(Dir) ->
    CA = f(Dir, "ca.pem"), CAK = f(Dir, "ca.key"),
    SV = f(Dir, "server.pem"), SVK = f(Dir, "server.key"),
    CL = f(Dir, "client.pem"), CLK = f(Dir, "client.key"),
    SCsr = f(Dir, "server.csr"), CCsr = f(Dir, "client.csr"),
    Ext = f(Dir, "san.ext"),
    ok = file:write_file(Ext, "subjectAltName=IP:127.0.0.1\n"),
    sh("openssl req -x509 -newkey rsa:2048 -nodes -keyout " ++ CAK ++
       " -out " ++ CA ++ " -days 2 -subj /CN=barrel-test-ca"),
    sh("openssl req -newkey rsa:2048 -nodes -keyout " ++ SVK ++
       " -out " ++ SCsr ++ " -subj /CN=127.0.0.1"),
    sh("openssl x509 -req -in " ++ SCsr ++ " -CA " ++ CA ++ " -CAkey " ++ CAK ++
       " -CAcreateserial -out " ++ SV ++ " -days 2 -extfile " ++ Ext),
    sh("openssl req -newkey rsa:2048 -nodes -keyout " ++ CLK ++
       " -out " ++ CCsr ++ " -subj /CN=peer-node1"),
    sh("openssl x509 -req -in " ++ CCsr ++ " -CA " ++ CA ++ " -CAkey " ++ CAK ++
       " -CAcreateserial -out " ++ CL ++ " -days 2"),
    true = filelib:is_regular(CL),
    ok.

sh(Cmd) ->
    _ = os:cmd(Cmd ++ " 2>/dev/null"),
    ok.
