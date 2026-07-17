%%%-------------------------------------------------------------------
%%% @doc Build the livery listener map for barrel_server (H1/H2/H3, TLS,
%%% mTLS) from the app env.
%%%
%%% With no `listeners' key the server behaves exactly as before: a single
%%% cleartext HTTP/1.1 listener on `http_port'. A `listeners' map opts into
%%% multi-protocol serving:
%%% ```
%%%   {barrel_server, listeners, #{http  => #{port => 8080},
%%%                                https => #{port => 8443},
%%%                                http3 => #{port => 8443}}}
%%% '''
%%% `https' (H2) and `http3' (H3) require TLS, from a shared config:
%%% ```
%%%   {barrel_server, tls, #{certfile, keyfile, cacertfile, verify}}
%%% '''
%%% The `http' (H1) listener stays cleartext unless its entry sets
%%% `tls => true' (used for the sync-wire mTLS gate, since the replication
%%% client speaks HTTP/1.1). mTLS is requested with `verify => verify_peer'
%%% + `cacertfile'; because livery/h1/h2 drop a top-level `verify', the
%%% real knobs are packed into `ssl_opts'. H3 is TLS-serving only; its
%%% client-cert gate is not yet complete (see the companion livery change).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_listeners).

-export([service_listeners/0]).

-define(DEFAULT_MAX_BODY, (1024 * 1024 * 1024)).

%% @doc The `#{http => .., https => .., http3 => ..}' portion of the
%% `livery:start_service/1' map (router and middleware are added by the
%% caller).
-spec service_listeners() -> map().
service_listeners() ->
    MaxBody = application:get_env(barrel_server, max_body, ?DEFAULT_MAX_BODY),
    case application:get_env(barrel_server, listeners, undefined) of
        undefined ->
            Port = application:get_env(barrel_server, http_port, 8080),
            #{http => #{port => Port, max_body => MaxBody}};
        Map when is_map(Map) ->
            Tls = application:get_env(barrel_server, tls, undefined),
            maps:map(fun(Proto, Cfg) -> listener(Proto, Cfg, MaxBody, Tls) end,
                     Map)
    end.

%%====================================================================
%% Internal
%%====================================================================

listener(http, Cfg, MaxBody, Tls) ->
    Base = #{port => maps:get(port, Cfg, 8080), max_body => MaxBody},
    case maps:get(tls, Cfg, false) of
        true -> add_tls(Base, require_tls(http, Tls));
        false -> Base
    end;
listener(https, Cfg, MaxBody, Tls) ->
    Base = #{port => maps:get(port, Cfg, 8443), max_body => MaxBody},
    add_tls(Base, require_tls(https, Tls));
listener(http3, Cfg, MaxBody, Tls) ->
    Base = #{port => maps:get(port, Cfg, 8443), max_body => MaxBody},
    add_h3_tls(Base, require_tls(http3, Tls)).

require_tls(Proto, undefined) ->
    error({barrel_server, {tls_config_required, Proto}});
require_tls(_Proto, Tls) when is_map(Tls) ->
    Tls.

%% H1/H2 over OTP ssl. `transport => ssl' turns the H1 listener into TLS
%% (H2 is TLS by default); mTLS bits ride in `ssl_opts'.
add_tls(Base, #{certfile := Cert, keyfile := Key} = Tls) ->
    M = Base#{transport => ssl, cert => Cert, key => Key},
    case ssl_opts(Tls) of
        [] -> M;
        Opts -> M#{ssl_opts => Opts}
    end.

%% H3/QUIC: serve TLS. A real client-cert gate (fail_if_no_peer_cert +
%% chain validation) is deferred to the companion livery/quic change, so
%% we only wire cert/key here.
add_h3_tls(Base, #{certfile := Cert, keyfile := Key}) ->
    Base#{cert => Cert, key => Key}.

%% verify_peer => a mutual-TLS gate: OTP ssl refuses a client that offers
%% no cert (fail_if_no_peer_cert) or one that does not chain to cacertfile.
%% Operator-supplied ssl_opts win.
ssl_opts(#{verify := verify_peer} = Tls) ->
    CA = maps:get(cacertfile, Tls),
    [{verify, verify_peer}, {fail_if_no_peer_cert, true}, {cacertfile, CA}]
        ++ maps:get(ssl_opts, Tls, []);
ssl_opts(Tls) ->
    maps:get(ssl_opts, Tls, []).
