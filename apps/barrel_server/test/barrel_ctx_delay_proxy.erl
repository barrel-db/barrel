%%%-------------------------------------------------------------------
%%% @doc TCP proxy for fault and latency injection in front of a local
%%% server. Modes: `{delay, Ms}' adds Ms each way (RTT = 2 * Ms, no
%%% throughput cap), `stall' accepts and never answers, `{truncate, N}'
%%% forwards N response bytes then cuts both sides.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_delay_proxy).

-export([start/2, stop/1, port/1]).

-spec start(inet:port_number(), {delay, non_neg_integer()} | stall
                                | {truncate, pos_integer()}) ->
    {pid(), inet:port_number()}.
start(UpstreamPort, Mode) ->
    Parent = self(),
    Pid = spawn(fun() ->
        {ok, LSock} = gen_tcp:listen(0, [binary, {active, false},
                                         {reuseaddr, true}, {nodelay, true},
                                         {backlog, 128}]),
        {ok, Port} = inet:port(LSock),
        Parent ! {proxy_port, self(), Port},
        accept_loop(LSock, UpstreamPort, Mode)
    end),
    receive {proxy_port, Pid, Port} -> {Pid, Port} end.

stop({Pid, _Port}) ->
    exit(Pid, kill),
    ok.

port({_Pid, Port}) ->
    Port.

accept_loop(LSock, Upstream, Mode) ->
    case gen_tcp:accept(LSock) of
        {ok, Client} ->
            Pid = spawn(fun() -> session(Client, Upstream, Mode) end),
            ok = gen_tcp:controlling_process(Client, Pid),
            Pid ! go,
            accept_loop(LSock, Upstream, Mode);
        {error, _} ->
            ok
    end.

session(Client, _Upstream, stall) ->
    receive go -> ok end,
    drain(Client);
session(Client, Upstream, Mode) ->
    receive go -> ok end,
    {ok, Server} = gen_tcp:connect({127, 0, 0, 1}, Upstream,
                                   [binary, {active, false}, {nodelay, true}]),
    Delay = delay(Mode),
    Self = self(),
    ToServer = spawn_link(fun() -> writer(Server, Self) end),
    ToClient = spawn_link(fun() -> writer(Client, Self) end),
    spawn_link(fun() -> reader(Client, ToServer, Delay, infinity) end),
    spawn_link(fun() -> reader(Server, ToClient, Delay, limit(Mode)) end),
    receive
        {closed, _} ->
            gen_tcp:close(Client),
            gen_tcp:close(Server)
    end.

delay({delay, Ms}) -> Ms;
delay(_Mode) -> 0.

limit({truncate, N}) -> N;
limit(_Mode) -> infinity.

reader(Sock, Writer, Delay, Left) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Data} ->
            {Send, Left1} = cut(Data, Left),
            Writer ! {data, Send, now_ms() + Delay},
            case Left1 of
                0 -> Writer ! {close, now_ms() + Delay};
                _ -> reader(Sock, Writer, Delay, Left1)
            end;
        {error, _} ->
            Writer ! {close, now_ms() + Delay}
    end.

cut(Data, infinity) ->
    {Data, infinity};
cut(Data, Left) when byte_size(Data) >= Left ->
    {binary:part(Data, 0, Left), 0};
cut(Data, Left) ->
    {Data, Left - byte_size(Data)}.

writer(Sock, Session) ->
    receive
        {data, Data, Due} ->
            wait_until(Due),
            _ = gen_tcp:send(Sock, Data),
            writer(Sock, Session);
        {close, Due} ->
            wait_until(Due),
            Session ! {closed, self()}
    end.

wait_until(Due) ->
    case Due - now_ms() of
        Ms when Ms > 0 -> timer:sleep(Ms);
        _ -> ok
    end.

drain(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, _} -> drain(Sock);
        {error, _} -> ok
    end.

now_ms() ->
    erlang:monotonic_time(millisecond).
