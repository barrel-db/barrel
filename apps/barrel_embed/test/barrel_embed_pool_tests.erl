%% Shared model processes: barrel_embed_pool and the providers using it.
-module(barrel_embed_pool_tests).

-include_lib("eunit/include/eunit.hrl").

-export([dummy/0, failing/0]).

pool_test_() ->
    {setup,
     fun() -> {ok, Apps} = application:ensure_all_started(barrel_embed), Apps end,
     fun(Apps) -> [application:stop(A) || A <- lists:reverse(Apps)] end,
     [fun same_key_shares/0,
      fun keys_do_not_share/0,
      fun last_release_stops/0,
      fun crash_starts_fresh/0,
      fun failed_start/0,
      fun unknown_release/0,
      fun local_provider_shares/0]}.

same_key_shares() ->
    {ok, P1} = barrel_embed_pool:acquire(k1, mfa()),
    {ok, P2} = barrel_embed_pool:acquire(k1, mfa()),
    ?assertEqual(P1, P2),
    release_all([P1, P2]).

keys_do_not_share() ->
    {ok, P1} = barrel_embed_pool:acquire(k2a, mfa()),
    {ok, P2} = barrel_embed_pool:acquire(k2b, mfa()),
    ?assertNotEqual(P1, P2),
    release_all([P1, P2]).

last_release_stops() ->
    {ok, P} = barrel_embed_pool:acquire(k3, mfa()),
    {ok, P} = barrel_embed_pool:acquire(k3, mfa()),
    MRef = monitor(process, P),
    ok = barrel_embed_pool:release(P),
    ?assert(is_process_alive(P)),
    ok = barrel_embed_pool:release(P),
    receive {'DOWN', MRef, process, P, _} -> ok after 5000 -> error(still_running) end.

%% A process that dies is forgotten: the next acquire starts a new one.
crash_starts_fresh() ->
    {ok, P1} = barrel_embed_pool:acquire(k4, mfa()),
    MRef = monitor(process, P1),
    exit(P1, kill),
    receive {'DOWN', MRef, process, P1, _} -> ok end,
    {ok, P2} = barrel_embed_pool:acquire(k4, mfa()),
    ?assertNotEqual(P1, P2),
    ok = barrel_embed_pool:release(P2).

failed_start() ->
    ?assertEqual({error, nope},
                 barrel_embed_pool:acquire(k5, {?MODULE, failing, []})),
    {ok, P} = barrel_embed_pool:acquire(k5, mfa()),
    ok = barrel_embed_pool:release(P).

unknown_release() ->
    ?assertEqual(ok, barrel_embed_pool:release(self())).

%% Two local-model inits share one port server; releasing both stops it.
local_provider_shares() ->
    ok = meck:new(barrel_embed_port_server, [passthrough]),
    meck:expect(barrel_embed_port_server, start_link,
                fun(_Python, _Args, _Opts) -> dummy() end),
    meck:expect(barrel_embed_port_server, info,
                fun(_Server, _Timeout) -> {ok, #{dimensions => 3}} end),
    try
        Cfg = #{embedder => {local, #{model => <<"pool-model">>}},
                dimensions => 3},
        {ok, S1} = barrel_embed:init(Cfg),
        {ok, S2} = barrel_embed:init(Cfg),
        Server = server(S1),
        ?assertEqual(Server, server(S2)),
        MRef = monitor(process, Server),
        ok = barrel_embed:release(S1),
        ?assert(is_process_alive(Server)),
        ok = barrel_embed:release(S2),
        receive {'DOWN', MRef, process, Server, _} -> ok
        after 5000 -> error(still_running)
        end
    after
        meck:unload(barrel_embed_port_server)
    end.

server(#{providers := [{_Module, #{server := Server}}]}) -> Server.

mfa() -> {?MODULE, dummy, []}.

dummy() ->
    {ok, spawn_link(fun() -> receive stop -> ok end end)}.

failing() ->
    {error, nope}.

release_all(Pids) ->
    lists:foreach(fun barrel_embed_pool:release/1, Pids).
