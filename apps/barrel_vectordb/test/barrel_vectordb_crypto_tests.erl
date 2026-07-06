%%%-------------------------------------------------------------------
%%% @doc EUnit tests for vectordb encryption at rest: EncryptedEnv on
%%% the store's RocksDB, the CRYPTO key-check marker, the fail-closed
%%% open matrix, and the disk-index backend rejections.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_crypto_tests).

-include_lib("eunit/include/eunit.hrl").

-define(STORE, enc_test_store).
-define(KEY1, binary:copy(<<16#11>>, 32)).
-define(KEY2, binary:copy(<<16#22>>, 32)).
-define(SENTINEL, <<"s3ntinel-9f8a7b6c5d4e3f2a1b0c-vectordb-enc-d1e2f3a4b5c6">>).

crypto_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
       {"encrypted store roundtrip, reopen, GC pin", fun test_roundtrip/0},
       {"sentinel text absent on disk", fun test_sentinel/0},
       {"wrong key fails closed", fun test_wrong_key/0},
       {"plaintext open of an encrypted store fails", fun test_plain_of_enc/0},
       {"encrypting an existing plaintext store fails", fun test_enc_of_plain/0},
       {"encrypted disk BM25 in a store", fun test_bm25_disk_store/0},
       {"bm25 disk index crypto roundtrip + compaction", fun test_bm25_disk_direct/0},
       {"bm25 disk index open matrix", fun test_bm25_disk_matrix/0},
       {"diskann crypto roundtrip + node rewrite churn", fun test_diskann_direct/0},
       {"diskann open matrix", fun test_diskann_matrix/0},
       {"encrypted diskann in a store", fun test_diskann_store/0}
     ]
    }.

setup() ->
    TestDir = "/tmp/barrel_vectordb_crypto_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    mock_embed(),
    TestDir.

cleanup(TestDir) ->
    catch barrel_vectordb:stop(?STORE),
    timer:sleep(50),
    unmock_embed(),
    os:cmd("rm -rf " ++ TestDir),
    ok.

start_store(TestDir, Extra) ->
    application:ensure_all_started(rocksdb),
    barrel_vectordb:start_link(maps:merge(#{
        name => ?STORE,
        path => TestDir,
        dimension => 3,
        bm25_backend => memory,
        hnsw => #{m => 4, ef_construction => 20}
    }, Extra)).

stop_store() ->
    ok = barrel_vectordb:stop(?STORE),
    timer:sleep(50).

found_on_disk(Dir, Bin) ->
    filelib:fold_files(Dir, ".*", true,
        fun(_File, true) -> true;
           (File, false) ->
                case file:read_file(File) of
                    {ok, Data} -> binary:match(Data, Bin) =/= nomatch;
                    {error, _} -> false
                end
        end, false).

mock_embed() ->
    (catch meck:unload(barrel_embed)),
    timer:sleep(10),
    meck:new(barrel_embed, [non_strict, no_link]),
    meck:expect(barrel_embed, init, fun(_Config) ->
        {ok, #{providers => [], dimension => 3, batch_size => 32}}
    end),
    meck:expect(barrel_embed, embed, fun(_Text, _State) ->
        {error, no_embedder_in_crypto_tests}
    end),
    meck:expect(barrel_embed, embed_batch, fun(_Texts, _State) ->
        {error, no_embedder_in_crypto_tests}
    end),
    meck:expect(barrel_embed, info, fun(_State) ->
        #{providers => [], dimension => 3}
    end).

unmock_embed() ->
    (catch meck:unload(barrel_embed)),
    ok.

%%====================================================================
%% Cases
%%====================================================================

test_roundtrip() ->
    TestDir = "/tmp/barrel_vectordb_crypto_rt_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        {ok, _} = start_store(TestDir, #{crypto => #{key => ?KEY1}}),
        ?assert(filelib:is_regular(filename:join(TestDir, "CRYPTO"))),
        ok = barrel_vectordb:add_vector(?STORE, <<"a">>, <<"hello">>,
                                        #{}, [1.0, 0.0, 0.0]),
        {ok, [#{key := <<"a">>}]} =
            barrel_vectordb:search_vector(?STORE, [1.0, 0.0, 0.0],
                                          #{k => 1}),
        %% a full-node GC must not free the env the NIF holds
        lists:foreach(fun(P) -> erlang:garbage_collect(P) end,
                      processes()),
        ok = barrel_vectordb:add_vector(?STORE, <<"b">>, <<"world">>,
                                        #{}, [0.0, 1.0, 0.0]),
        stop_store(),
        {ok, _} = start_store(TestDir, #{crypto => #{key => ?KEY1}}),
        {ok, [#{key := <<"a">>}]} =
            barrel_vectordb:search_vector(?STORE, [1.0, 0.0, 0.0],
                                          #{k => 1})
    after
        os:cmd("rm -rf " ++ TestDir)
    end.

test_sentinel() ->
    EncDir = "/tmp/barrel_vectordb_crypto_se_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    PlainDir = EncDir ++ "_plain",
    try
        {ok, _} = start_store(EncDir, #{crypto => #{key => ?KEY1}}),
        ok = barrel_vectordb:add_vector(?STORE, <<"a">>, ?SENTINEL,
                                        #{}, [1.0, 0.0, 0.0]),
        stop_store(),
        ?assertNot(found_on_disk(EncDir, ?SENTINEL)),
        %% control: a plaintext store leaves the text readable on disk
        {ok, _} = start_store(PlainDir, #{}),
        ok = barrel_vectordb:add_vector(?STORE, <<"a">>, ?SENTINEL,
                                        #{}, [1.0, 0.0, 0.0]),
        stop_store(),
        ?assert(found_on_disk(PlainDir, ?SENTINEL))
    after
        os:cmd("rm -rf " ++ EncDir),
        os:cmd("rm -rf " ++ PlainDir)
    end.

test_wrong_key() ->
    TestDir = "/tmp/barrel_vectordb_crypto_wk_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        {ok, _} = start_store(TestDir, #{crypto => #{key => ?KEY1}}),
        ok = barrel_vectordb:add_vector(?STORE, <<"a">>, <<"t">>,
                                        #{}, [1.0, 0.0, 0.0]),
        stop_store(),
        ?assertEqual({error, wrong_encryption_key},
                     start_store(TestDir, #{crypto => #{key => ?KEY2}})),
        {ok, _} = start_store(TestDir, #{crypto => #{key => ?KEY1}}),
        {ok, [#{key := <<"a">>}]} =
            barrel_vectordb:search_vector(?STORE, [1.0, 0.0, 0.0],
                                          #{k => 1})
    after
        os:cmd("rm -rf " ++ TestDir)
    end.

test_plain_of_enc() ->
    TestDir = "/tmp/barrel_vectordb_crypto_pe_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        {ok, _} = start_store(TestDir, #{crypto => #{key => ?KEY1}}),
        stop_store(),
        ?assertEqual({error, db_is_encrypted},
                     start_store(TestDir, #{}))
    after
        os:cmd("rm -rf " ++ TestDir)
    end.

test_enc_of_plain() ->
    TestDir = "/tmp/barrel_vectordb_crypto_ep_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        {ok, _} = start_store(TestDir, #{}),
        stop_store(),
        ?assertEqual({error, cannot_encrypt_existing_db},
                     start_store(TestDir, #{crypto => #{key => ?KEY1}}))
    after
        os:cmd("rm -rf " ++ TestDir)
    end.


test_bm25_disk_store() ->
    TestDir = "/tmp/barrel_vectordb_crypto_bd_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        {ok, _} = start_store(TestDir, #{crypto => #{key => ?KEY1},
                                         bm25_backend => disk}),
        ok = barrel_vectordb:add_vector(?STORE, <<"a">>, ?SENTINEL,
                                        #{}, [1.0, 0.0, 0.0]),
        {ok, [{<<"a">>, _} | _]} =
            barrel_vectordb:search_bm25(?STORE, ?SENTINEL, #{k => 1}),
        %% flush the hot layer so the postings survive the restart
        %% (encrypted under a rotated nonce)
        ok = barrel_vectordb_server:bm25_compact(?STORE),
        stop_store(),
        %% the sentinel term reaches bm25.ids (EncryptedEnv) and the
        %% text CF; nothing on disk carries it in clear
        ?assertNot(found_on_disk(TestDir, ?SENTINEL)),
        {ok, _} = start_store(TestDir, #{crypto => #{key => ?KEY1},
                                         bm25_backend => disk}),
        {ok, [{<<"a">>, _} | _]} =
            barrel_vectordb:search_bm25(?STORE, ?SENTINEL, #{k => 1})
    after
        catch barrel_vectordb:stop(?STORE),
        os:cmd("rm -rf " ++ TestDir)
    end.

test_bm25_disk_direct() ->
    Path = "/tmp/barrel_vectordb_crypto_bm_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    Crypto = #{key => ?KEY1},
    try
        {ok, I0} = barrel_vectordb_bm25_disk:new(#{base_path => Path,
                                                   crypto => Crypto}),
        {ok, I1} = barrel_vectordb_bm25_disk:add(I0, <<"d1">>,
                                                 <<"hello world">>),
        {ok, I2} = barrel_vectordb_bm25_disk:add(I1, <<"d2">>,
                                                 <<"world peace">>),
        [{<<"d1">>, _} | _] = barrel_vectordb_bm25_disk:search(
                                  I2, <<"hello">>, 2),
        %% first compaction writes postings under a rotated nonce
        {ok, I3} = barrel_vectordb_bm25_disk:compact(I2),
        [{<<"d1">>, _} | _] = barrel_vectordb_bm25_disk:search(
                                  I3, <<"hello">>, 2),
        %% second compaction rewrites the same offsets: readable only
        %% because the nonce rotated again
        {ok, I4} = barrel_vectordb_bm25_disk:add(I3, <<"d3">>,
                                                 <<"hello again">>),
        {ok, I5} = barrel_vectordb_bm25_disk:compact(I4),
        Hits = barrel_vectordb_bm25_disk:search(I5, <<"hello">>, 3),
        ?assert(lists:keymember(<<"d1">>, 1, Hits) orelse
                lists:keymember(<<"d3">>, 1, Hits)),
        ok = barrel_vectordb_bm25_disk:close(I5),
        %% reopen with the key: superblock + rotated nonce persisted
        {ok, I6} = barrel_vectordb_bm25_disk:open(Path,
                                                  #{crypto => Crypto}),
        Hits2 = barrel_vectordb_bm25_disk:search(I6, <<"hello">>, 3),
        ?assert(length(Hits2) >= 1),
        ok = barrel_vectordb_bm25_disk:close(I6)
    after
        os:cmd("rm -rf " ++ Path)
    end.

random_vector(Dim) ->
    [rand:uniform() || _ <- lists:seq(1, Dim)].

diskann_config(Path, Extra) ->
    maps:merge(#{
        dimension => 16,
        r => 8,
        l_build => 20,
        l_search => 20,
        storage_mode => disk,
        base_path => Path,
        use_pq => true,
        pq_m => 2,
        pq_k => 16
    }, Extra).

test_diskann_direct() ->
    Path = "/tmp/barrel_vectordb_crypto_da_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    Crypto = #{key => ?KEY1},
    try
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 50)],
        {ok, I0} = barrel_vectordb_diskann:build(
            diskann_config(Path, #{crypto => Crypto}), Vectors),
        Results = barrel_vectordb_diskann:search(I0, random_vector(16), 5),
        ?assertEqual(5, length(Results)),
        %% incremental inserts rewrite existing graph nodes in place
        %% (fresh embedded nonce per rewrite)
        I3 = lists:foldl(
            fun(I, Acc) ->
                {ok, Acc2} = barrel_vectordb_diskann:insert(
                    Acc, <<"x", (integer_to_binary(I))/binary>>,
                    random_vector(16)),
                Acc2
            end, I0, lists:seq(1, 20)),
        ?assertEqual(70, barrel_vectordb_diskann:size(I3)),
        ok = barrel_vectordb_diskann:close(I3),
        %% reopen with the key: meta superblock + graph + vectors + pq
        {ok, I4} = barrel_vectordb_diskann:open(Path,
                                                #{crypto => Crypto}),
        ?assertEqual(70, barrel_vectordb_diskann:size(I4)),
        Results2 = barrel_vectordb_diskann:search(I4, random_vector(16), 5),
        ?assertEqual(5, length(Results2)),
        ok = barrel_vectordb_diskann:close(I4)
    after
        os:cmd("rm -rf " ++ Path)
    end.

test_diskann_matrix() ->
    Path = "/tmp/barrel_vectordb_crypto_dx_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        Vectors = [{integer_to_binary(I), random_vector(16)}
                   || I <- lists:seq(1, 10)],
        {ok, I0} = barrel_vectordb_diskann:build(
            diskann_config(Path, #{crypto => #{key => ?KEY1}}), Vectors),
        ok = barrel_vectordb_diskann:close(I0),
        ?assertEqual({error, index_is_encrypted},
                     barrel_vectordb_diskann:open(Path, #{})),
        ?assertEqual({error, wrong_encryption_key},
                     barrel_vectordb_diskann:open(
                         Path, #{crypto => #{key => ?KEY2}})),
        os:cmd("rm -rf " ++ Path),
        {ok, P0} = barrel_vectordb_diskann:build(
            diskann_config(Path, #{}), Vectors),
        ok = barrel_vectordb_diskann:close(P0),
        ?assertEqual({error, cannot_encrypt_legacy_index},
                     barrel_vectordb_diskann:open(
                         Path, #{crypto => #{key => ?KEY1}}))
    after
        os:cmd("rm -rf " ++ Path)
    end.

test_diskann_store() ->
    TestDir = "/tmp/barrel_vectordb_crypto_ds_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        {ok, _} = start_store(TestDir, #{crypto => #{key => ?KEY1},
                                         backend => diskann,
                                         diskann => #{r => 8,
                                                      l_build => 20,
                                                      l_search => 20}}),
        ok = barrel_vectordb:add_vector(?STORE, <<"a">>, ?SENTINEL,
                                        #{}, [1.0, 0.0, 0.0]),
        {ok, [#{key := <<"a">>} | _]} =
            barrel_vectordb:search_vector(?STORE, [1.0, 0.0, 0.0],
                                          #{k => 1}),
        stop_store(),
        ?assertNot(found_on_disk(TestDir, ?SENTINEL)),
        {ok, _} = start_store(TestDir, #{crypto => #{key => ?KEY1},
                                         backend => diskann,
                                         diskann => #{r => 8,
                                                      l_build => 20,
                                                      l_search => 20}}),
        {ok, [#{key := <<"a">>} | _]} =
            barrel_vectordb:search_vector(?STORE, [1.0, 0.0, 0.0],
                                          #{k => 1})
    after
        catch barrel_vectordb:stop(?STORE),
        os:cmd("rm -rf " ++ TestDir)
    end.

test_bm25_disk_matrix() ->
    Path = "/tmp/barrel_vectordb_crypto_bx_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        {ok, I0} = barrel_vectordb_bm25_disk:new(#{base_path => Path,
                                                   crypto => #{key => ?KEY1}}),
        ok = barrel_vectordb_bm25_disk:close(I0),
        ?assertEqual({error, index_is_encrypted},
                     barrel_vectordb_bm25_disk:open(Path, #{})),
        ?assertEqual({error, wrong_encryption_key},
                     barrel_vectordb_bm25_disk:open(
                         Path, #{crypto => #{key => ?KEY2}})),
        os:cmd("rm -rf " ++ Path),
        {ok, P0} = barrel_vectordb_bm25_disk:new(#{base_path => Path}),
        ok = barrel_vectordb_bm25_disk:close(P0),
        ?assertEqual({error, cannot_encrypt_legacy_index},
                     barrel_vectordb_bm25_disk:open(
                         Path, #{crypto => #{key => ?KEY1}}))
    after
        os:cmd("rm -rf " ++ Path)
    end.
