%%%-------------------------------------------------------------------
%%% @doc EUnit tests for the vector store name registry (binary names
%%% via {via, ...} registration, no atom minting) and the binary-name
%%% store lifecycle through the public API.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_registry_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Registry mechanics
%%====================================================================

registry_test_() ->
    {foreach,
     fun() -> ok = barrel_vectordb_registry:ensure() end,
     fun(_) -> ok end,
     [
       {"register / whereis / unregister", fun test_register_cycle/0},
       {"duplicate registration refused", fun test_duplicate/0},
       {"dead pid is cleaned up", fun test_down_cleanup/0}
     ]
    }.

test_register_cycle() ->
    Name = {vstore, <<"reg_cycle">>},
    Pid = spawn(fun() -> receive stop -> ok end end),
    ?assertEqual(yes, barrel_vectordb_registry:register_name(Name, Pid)),
    ?assertEqual(Pid, barrel_vectordb_registry:whereis_name(Name)),
    ok = barrel_vectordb_registry:unregister_name(Name),
    ?assertEqual(undefined, barrel_vectordb_registry:whereis_name(Name)),
    Pid ! stop.

test_duplicate() ->
    Name = {vstore, <<"reg_dup">>},
    P1 = spawn(fun() -> receive stop -> ok end end),
    P2 = spawn(fun() -> receive stop -> ok end end),
    yes = barrel_vectordb_registry:register_name(Name, P1),
    ?assertEqual(no, barrel_vectordb_registry:register_name(Name, P2)),
    ?assertEqual(P1, barrel_vectordb_registry:whereis_name(Name)),
    ok = barrel_vectordb_registry:unregister_name(Name),
    P1 ! stop,
    P2 ! stop.

test_down_cleanup() ->
    Name = {vstore, <<"reg_down">>},
    P = spawn(fun() -> receive stop -> ok end end),
    yes = barrel_vectordb_registry:register_name(Name, P),
    Ref = erlang:monitor(process, P),
    P ! stop,
    receive {'DOWN', Ref, process, P, _} -> ok end,
    %% whereis filters dead pids immediately; the owner clears the row
    ?assertEqual(undefined, barrel_vectordb_registry:whereis_name(Name)),
    %% a new registration under the same name succeeds
    P2 = spawn(fun() -> receive stop -> ok end end),
    ?assertEqual(yes, barrel_vectordb_registry:register_name(Name, P2)),
    ok = barrel_vectordb_registry:unregister_name(Name),
    P2 ! stop.

%%====================================================================
%% Binary-name store lifecycle
%%====================================================================

store_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
       {"binary-name store full cycle + no atom leak", fun test_binary_store/0},
       {"atom names keep working", fun test_atom_compat/0}
     ]
    }.

setup() ->
    application:ensure_all_started(rocksdb),
    mock_embed(),
    ok.

cleanup(_) ->
    unmock_embed(),
    ok.

test_binary_store() ->
    Name = <<"regstore_", (integer_to_binary(
        erlang:unique_integer([positive])))/binary>>,
    Dir = "/tmp/barrel_vectordb_reg_" ++ binary_to_list(Name),
    try
        {ok, Pid} = barrel_vectordb:start_link(#{
            name => Name,
            path => Dir,
            dimension => 3,
            bm25_backend => memory,
            hnsw => #{m => 4, ef_construction => 20}
        }),
        ?assert(is_pid(Pid)),
        %% the whole cycle addressed by binary name
        ok = barrel_vectordb:add_vector(Name, <<"a">>, <<"hello">>, #{},
                                        [1.0, 0.0, 0.0]),
        {ok, [#{key := <<"a">>} | _]} =
            barrel_vectordb:search_vector(Name, [1.0, 0.0, 0.0],
                                          #{k => 1}),
        {ok, _} = barrel_vectordb:get(Name, <<"a">>),
        %% deterministic no-atom-leak proof: the name never became an
        %% atom anywhere in the open path
        ?assertError(badarg, binary_to_existing_atom(Name, utf8)),
        %% destroy by binary name removes the store and its files
        ok = barrel_vectordb:destroy(Name),
        ?assertNot(filelib:is_dir(Dir))
    after
        os:cmd("rm -rf " ++ Dir)
    end.

test_atom_compat() ->
    Dir = "/tmp/barrel_vectordb_reg_atomcompat_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        {ok, _} = barrel_vectordb:start_link(#{
            name => reg_atom_compat_store,
            path => Dir,
            dimension => 3,
            bm25_backend => memory,
            hnsw => #{m => 4, ef_construction => 20}
        }),
        ok = barrel_vectordb:add_vector(reg_atom_compat_store, <<"a">>,
                                        <<"t">>, #{}, [1.0, 0.0, 0.0]),
        %% atom and binary refer to the same store
        {ok, _} = barrel_vectordb:get(<<"reg_atom_compat_store">>,
                                      <<"a">>),
        ok = barrel_vectordb:stop(reg_atom_compat_store)
    after
        os:cmd("rm -rf " ++ Dir)
    end.

%%====================================================================
%% Embed mock (mirrors the other vectordb suites)
%%====================================================================

mock_embed() ->
    (catch meck:unload(barrel_embed)),
    timer:sleep(10),
    meck:new(barrel_embed, [non_strict, no_link]),
    meck:expect(barrel_embed, init, fun(_Config) ->
        {ok, #{providers => [], dimension => 3, batch_size => 32}}
    end),
    meck:expect(barrel_embed, embed, fun(_Text, _State) ->
        {error, no_embedder_in_registry_tests}
    end),
    meck:expect(barrel_embed, embed_batch, fun(_Texts, _State) ->
        {error, no_embedder_in_registry_tests}
    end),
    meck:expect(barrel_embed, info, fun(_State) ->
        #{providers => [], dimension => 3}
    end).

unmock_embed() ->
    (catch meck:unload(barrel_embed)),
    ok.
