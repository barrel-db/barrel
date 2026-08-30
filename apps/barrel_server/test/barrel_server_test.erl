%%%-------------------------------------------------------------------
%%% @doc Shared helpers for the barrel_server suites.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_server_test).

-export([h1_port/1, listener_ports/1]).

%% @doc The H1 port of a running livery service. livery 0.8+ reports a
%% list of ports per protocol; the suites bind one listener each.
-spec h1_port(pid()) -> inet:port_number().
h1_port(Pid) ->
    maps:get(h1, listener_ports(Pid)).

%% @doc `#{h1 | h2 | h3 => Port}' with one port per protocol.
-spec listener_ports(pid()) -> #{h1 | h2 | h3 => inet:port_number()}.
listener_ports(Pid) ->
    maps:map(fun(_Proto, [Port | _]) -> Port;
                (_Proto, Port) -> Port
             end, livery:which_listeners(Pid)).
