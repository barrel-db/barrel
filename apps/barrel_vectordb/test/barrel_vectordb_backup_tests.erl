%%% @doc barrel_vectordb:backup/2: a snapshot of a running store opens
%%% as a store of its own, with every backend that keeps files.
-module(barrel_vectordb_backup_tests).

-include_lib("eunit/include/eunit.hrl").

-define(STORE, backup_test_store).
-define(COPY, backup_test_copy).
-define(KEY, binary:copy(<<16#33>>, 32)).

backup_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [case_(Name, F) || {Name, F} <-
          [{"hnsw store: the copy holds the snapshot, not later writes", fun hnsw_snapshot/1},
           {"disk BM25 with an uncompacted hot layer", fun bm25_disk/1},
           {"diskann store", fun diskann/1},
           {"encrypted store: the copy needs the key", fun encrypted/1},
           {"an existing target is refused", fun existing_dir/1},
           {"writes during a backup wait, the copy opens", fun concurrent_writes/1}]]}.

%% each case gets the directory its setup made
case_(Name, F) -> fun(Dir) -> {Name, fun() -> F(Dir) end} end.

setup() ->
    application:ensure_all_started(rocksdb),
    mock_embed(),
    "/tmp/barrel_vectordb_backup_" ++ integer_to_list(erlang:unique_integer([positive])).

cleanup(Dir) ->
    catch barrel_vectordb:stop(?STORE),
    catch barrel_vectordb:stop(?COPY),
    timer:sleep(50),
    (catch meck:unload(barrel_embed)),
    os:cmd("rm -rf " ++ Dir),
    ok.

src(Dir) -> filename:join(Dir, "src").
dst(Dir) -> filename:join(Dir, "copy").

start(Name, Path, Extra) ->
    barrel_vectordb:start_link(maps:merge(#{name => Name, path => Path, dimension => 3,
                                            bm25_backend => memory,
                                            hnsw => #{m => 4, ef_construction => 20}},
                                          Extra)).

add(Store, Id, Text, Vec) ->
    ok = barrel_vectordb:add_vector(Store, Id, Text, #{}, Vec).

top(Store, Vec) ->
    {ok, [#{key := K} | _]} = barrel_vectordb:search_vector(Store, Vec, #{k => 1}),
    K.

hnsw_snapshot(D) ->
    {ok, _} = start(?STORE, src(D), #{}),
    add(?STORE, <<"a">>, <<"alpha">>, [1.0, 0.0, 0.0]),
    add(?STORE, <<"b">>, <<"beta">>, [0.0, 1.0, 0.0]),
    ok = barrel_vectordb:backup(?STORE, dst(D)),
    add(?STORE, <<"c">>, <<"gamma">>, [0.0, 0.0, 1.0]),
    {ok, _} = start(?COPY, dst(D), #{}),
    ?assertEqual(2, barrel_vectordb:count(?COPY)),
    ?assertEqual(<<"b">>, top(?COPY, [0.0, 1.0, 0.0])),
    ?assertMatch({ok, #{index_origin := loaded}}, barrel_vectordb:stats(?COPY)),
    ?assertEqual(3, barrel_vectordb:count(?STORE)).

bm25_disk(D) ->
    {ok, _} = start(?STORE, src(D), #{bm25_backend => disk}),
    add(?STORE, <<"a">>, <<"swordfish">>, [1.0, 0.0, 0.0]),
    add(?STORE, <<"b">>, <<"marlin">>, [0.0, 1.0, 0.0]),
    ok = barrel_vectordb:backup(?STORE, dst(D)),
    {ok, _} = start(?COPY, dst(D), #{bm25_backend => disk}),
    ?assertMatch({ok, [{<<"a">>, _} | _]},
                 barrel_vectordb:search_bm25(?COPY, <<"swordfish">>, #{k => 1})),
    ?assertMatch({ok, [{<<"b">>, _} | _]},
                 barrel_vectordb:search_bm25(?COPY, <<"marlin">>, #{k => 1})).

diskann(D) ->
    Cfg = #{backend => diskann, diskann => #{r => 8, l_build => 20, l_search => 20}},
    {ok, _} = start(?STORE, src(D), Cfg),
    add(?STORE, <<"a">>, <<"alpha">>, [1.0, 0.0, 0.0]),
    add(?STORE, <<"b">>, <<"beta">>, [0.0, 1.0, 0.0]),
    ok = barrel_vectordb:backup(?STORE, dst(D)),
    {ok, _} = start(?COPY, dst(D), Cfg),
    ?assertEqual(<<"a">>, top(?COPY, [1.0, 0.0, 0.0])),
    ?assertEqual(<<"b">>, top(?COPY, [0.0, 1.0, 0.0])).

encrypted(D) ->
    Crypto = #{crypto => #{key => ?KEY}},
    {ok, _} = start(?STORE, src(D), Crypto),
    add(?STORE, <<"a">>, <<"alpha">>, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:backup(?STORE, dst(D)),
    ?assert(filelib:is_regular(filename:join(dst(D), "CRYPTO"))),
    process_flag(trap_exit, true),
    ?assertMatch({error, _}, start(?COPY, dst(D), #{})),
    {ok, _} = start(?COPY, dst(D), Crypto),
    ?assertEqual(<<"a">>, top(?COPY, [1.0, 0.0, 0.0])).

existing_dir(D) ->
    {ok, _} = start(?STORE, src(D), #{}),
    ok = filelib:ensure_path(dst(D)),
    ?assertMatch({error, {backup_dir_exists, _}}, barrel_vectordb:backup(?STORE, dst(D))).

concurrent_writes(D) ->
    {ok, _} = start(?STORE, src(D), #{}),
    add(?STORE, <<"seed">>, <<"seed">>, [1.0, 0.0, 0.0]),
    Self = self(),
    Writer = spawn_link(fun() -> write_loop(Self, 0) end),
    timer:sleep(20),
    ok = barrel_vectordb:backup(?STORE, dst(D)),
    Writer ! stop,
    Written = receive {written, N} -> N end,
    {ok, _} = start(?COPY, dst(D), #{}),
    Copied = barrel_vectordb:count(?COPY),
    ?assert(Copied >= 1),
    ?assert(Copied =< Written + 1),
    ?assertEqual(Written + 1, barrel_vectordb:count(?STORE)).

write_loop(Parent, N) ->
    receive stop -> Parent ! {written, N}
    after 0 ->
        Id = <<"w", (integer_to_binary(N))/binary>>,
        add(?STORE, Id, Id, [rand:uniform(), rand:uniform(), rand:uniform()]),
        write_loop(Parent, N + 1)
    end.

mock_embed() ->
    (catch meck:unload(barrel_embed)),
    meck:new(barrel_embed, [non_strict, no_link]),
    meck:expect(barrel_embed, release, fun(_) -> ok end),
    meck:expect(barrel_embed, init,
                fun(_) -> {ok, #{providers => [], dimension => 3, batch_size => 32}} end),
    meck:expect(barrel_embed, embed, fun(_, _) -> {error, no_embedder} end),
    meck:expect(barrel_embed, embed_batch, fun(_, _) -> {error, no_embedder} end),
    meck:expect(barrel_embed, info, fun(_) -> #{providers => [], dimension => 3} end).
