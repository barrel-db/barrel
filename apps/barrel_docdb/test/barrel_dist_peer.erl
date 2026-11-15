%%% @doc Test helper: peer nodes running barrel_docdb over TCP or QUIC
%%% distribution. The test node drives them through the peer control
%%% channel, so it needs no distribution of its own. QUIC peers follow
%%% erlang_quic's own test helper (quic_dist_peer): one UDP port each,
%%% static discovery holding the whole table on every node.
-module(barrel_dist_peer).

-export([start/4, restart/3, stop/1, call/4, node/1, carrier/1]).

-define(HOST, "127.0.0.1").

%% Start Count peers named Prefix_N over Carrier (tcp | quic), each with
%% barrel_docdb running on a data directory of its own under Dir.
start(Carrier, Prefix, Count, Dir) ->
    Certs = certs(Carrier, Dir),
    Unique = integer_to_list(erlang:unique_integer([positive])),
    Specs = [#{carrier => Carrier, certs => Certs, dir => Dir,
               name => Prefix ++ "_" ++ integer_to_list(N) ++ "_" ++ Unique,
               port => free_port()}
             || N <- lists:seq(1, Count)],
    Peers = [boot(S) || S <- Specs],
    [configure(P, Peers) || P <- Peers],
    [start_docdb(P) || P <- Peers],
    Peers.

%% Bring a stopped peer back under its name, port and data directory.
restart(Peer, All, _Dir) ->
    New = boot(Peer),
    Table = [case P of #{name := N} when N =:= map_get(name, Peer) -> New; _ -> P end
             || P <- All],
    configure(New, Table),
    start_docdb(New),
    New.

stop(Peers) when is_list(Peers) ->
    [stop(P) || P <- Peers],
    ok;
stop(#{peer := Pid}) ->
    try peer:stop(Pid) catch _:_ -> ok end,
    ok.

call(#{peer := Pid}, M, F, A) ->
    peer:call(Pid, M, F, A, 60000).

node(#{node := Node}) -> Node.

%% The carrier the peer's distribution really runs on.
carrier(Peer) ->
    case call(Peer, init, get_argument, [proto_dist]) of
        {ok, [["quic"]]} -> quic;
        error -> tcp;
        {ok, Other} -> {other, Other}
    end.

%%====================================================================
%% Internal
%%====================================================================

boot(#{carrier := Carrier, name := Name, port := Port, certs := Certs} = Spec) ->
    Args = dist_args(Carrier, Port, Certs)
        ++ ["-setcookie", "barrel_dist_test"]
        ++ lists:append([["-pa", P] || P <- code:get_path()]),
    {ok, Pid, Node} = peer:start(#{name => list_to_atom(Name), host => ?HOST,
                                   longnames => true, connection => standard_io,
                                   args => Args}),
    Spec#{peer => Pid, node => Node}.

dist_args(tcp, _Port, _Certs) ->
    [];
dist_args(quic, Port, #{cert := Cert, key := Key}) ->
    ["-proto_dist", "quic", "-epmd_module", "quic_epmd", "-start_epmd", "false",
     "-quic_dist_port", integer_to_list(Port),
     "-quic_dist_cert", Cert, "-quic_dist_key", Key].

configure(#{carrier := tcp}, _Peers) ->
    ok;
configure(#{carrier := quic, certs := #{cert := Cert, key := Key}} = P, Peers) ->
    Table = [{N, {?HOST, Port}} || #{node := N, port := Port} <- Peers],
    ok = call(P, application, set_env,
              [quic, dist, [{cert_file, Cert}, {key_file, Key},
                            {verify, verify_none},
                            {discovery_module, quic_discovery_static},
                            {nodes, Table}]]),
    {ok, _} = call(P, application, ensure_all_started, [quic]),
    {ok, _} = call(P, quic_discovery_static, init, [[{nodes, Table}]]),
    ok.

start_docdb(#{name := Name, dir := Dir} = P) ->
    DataDir = filename:join(Dir, Name),
    ok = filelib:ensure_path(DataDir),
    %% load first: loading reapplies the .app env over an earlier set_env
    _ = call(P, application, load, [barrel_docdb]),
    ok = call(P, application, set_env, [barrel_docdb, data_dir, DataDir]),
    {ok, _} = call(P, application, ensure_all_started, [barrel_docdb]),
    ok.

%% A certificate usable to accept and to open connections, generated
%% with public_key so the run needs no openssl binary.
certs(tcp, _Dir) ->
    #{};
certs(quic, Dir) ->
    pk_certs(Dir).

pk_certs(Dir) ->
    %% RSA keys: with the default ECDSA test keys the QUIC peers do not
    %% connect
    Rsa = [{key, {rsa, 2048, 65537}}],
    CertDir = filename:join(Dir, "certs"),
    ok = filelib:ensure_path(CertDir),
    #{server_config := Conf} =
        public_key:pkix_test_data(#{server_chain => #{root => Rsa, intermediates => [], peer => Rsa},
                                    client_chain => #{root => Rsa, intermediates => [], peer => Rsa}}),
    CertDer = proplists:get_value(cert, Conf),
    {KeyType, KeyDer} = proplists:get_value(key, Conf),
    Cert = filename:join(CertDir, "cert.pem"),
    Key = filename:join(CertDir, "key.pem"),
    ok = file:write_file(Cert, public_key:pem_encode([{'Certificate', CertDer, not_encrypted}])),
    ok = file:write_file(Key, public_key:pem_encode([{KeyType, KeyDer, not_encrypted}])),
    #{cert => Cert, key => Key}.

free_port() ->
    {ok, S} = gen_udp:open(0, [{ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(S),
    ok = gen_udp:close(S),
    Port.
