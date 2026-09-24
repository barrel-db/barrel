%%%-------------------------------------------------------------------
%%% @doc Scripted HTTP/1.1 server for the remote client tests: answers
%%% every request with a fixed script (chunked NDJSON, a stall, a status).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ctx_fake_server).

-export([start/1, stop/1, port/1, closed_port/0]).

%% Script: {chunks, Status, [binary() | stall]} sends a chunked body
%% (`stall' blocks it there); stall accepts and never answers. The owner
%% gets {fake_request, Headers, Body}, then {fake_closed, Pid} once the
%% client closes the connection.
start(Script) ->
    Owner = self(),
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false},
                                     {reuseaddr, true}, {packet, raw}]),
    {ok, Port} = inet:port(LSock),
    Pid = spawn(fun() -> accept_loop(LSock, Script, Owner) end),
    ok = gen_tcp:controlling_process(LSock, Pid),
    {Pid, Port}.

stop({Pid, _Port}) ->
    exit(Pid, kill),
    ok.

port({_Pid, Port}) ->
    Port.

%% A port nothing listens on.
closed_port() ->
    {ok, L} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(L),
    ok = gen_tcp:close(L),
    Port.

accept_loop(LSock, Script, Owner) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            %% linked: stop/1 ends every connection with the acceptor
            Pid = spawn_link(fun() -> serve(Sock, Script, Owner) end),
            _ = gen_tcp:controlling_process(Sock, Pid),
            Pid ! go,
            accept_loop(LSock, Script, Owner);
        {error, _} ->
            ok
    end.

serve(Sock, Script, Owner) ->
    receive go -> ok end,
    case read_request(Sock, <<>>) of
        {Headers, Body} ->
            Owner ! {fake_request, Headers, Body},
            run(Script, Sock),
            Owner ! {fake_closed, self()};
        closed ->
            ok
    end,
    gen_tcp:close(Sock).

run(stall, Sock) ->
    wait_closed(Sock);
run({chunks, Status, Parts}, Sock) ->
    _ = gen_tcp:send(Sock, [<<"HTTP/1.1 ">>, integer_to_binary(Status),
                             <<" X\r\ncontent-type: application/x-ndjson\r\n"
                               "transfer-encoding: chunked\r\n\r\n">>]),
    send_parts(Parts, Sock).

send_parts([], Sock) ->
    _ = gen_tcp:send(Sock, <<"0\r\n\r\n">>),
    wait_closed(Sock);
send_parts([stall | _], Sock) ->
    wait_closed(Sock);
send_parts([<<>> | Rest], Sock) ->
    send_parts(Rest, Sock);
send_parts([Part | Rest], Sock) ->
    Data = iolist_to_binary(Part),
    _ = gen_tcp:send(Sock, [integer_to_list(byte_size(Data), 16),
                            <<"\r\n">>, Data, <<"\r\n">>]),
    send_parts(Rest, Sock).

%% Never answer again: return once the client closed its side.
wait_closed(Sock) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, _} -> wait_closed(Sock);
        {error, _} -> ok
    end.

read_request(Sock, Acc) ->
    case binary:split(Acc, <<"\r\n\r\n">>) of
        [Head, Rest] ->
            Headers = parse_headers(Head),
            Len = binary_to_integer(
                    maps:get(<<"content-length">>, Headers, <<"0">>)),
            case read_body(Sock, Rest, Len) of
                closed -> closed;
                Body -> {Headers, Body}
            end;
        [_] ->
            case gen_tcp:recv(Sock, 0) of
                {ok, Data} -> read_request(Sock, <<Acc/binary, Data/binary>>);
                {error, _} -> closed
            end
    end.

read_body(_Sock, Acc, Len) when byte_size(Acc) >= Len ->
    Acc;
read_body(Sock, Acc, Len) ->
    case gen_tcp:recv(Sock, 0) of
        {ok, Data} -> read_body(Sock, <<Acc/binary, Data/binary>>, Len);
        {error, _} -> closed
    end.

parse_headers(Head) ->
    [_RequestLine | Lines] = binary:split(Head, <<"\r\n">>, [global]),
    maps:from_list(
      [begin
           [K, V] = binary:split(L, <<":">>),
           {string:lowercase(K), string:trim(V)}
       end || L <- Lines, L =/= <<>>]).
