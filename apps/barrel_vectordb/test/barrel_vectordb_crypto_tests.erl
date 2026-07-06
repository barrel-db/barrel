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
       {"disk-index backends rejected with crypto", fun test_unsupported/0}
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

test_unsupported() ->
    TestDir = "/tmp/barrel_vectordb_crypto_un_"
        ++ integer_to_list(erlang:unique_integer([positive])),
    try
        %% disk-file index backends keep plaintext flat files until
        %% their sector-cipher layers land: rejected with crypto on
        ?assertEqual({error, {encryption_unsupported, bm25_disk}},
                     start_store(TestDir, #{crypto => #{key => ?KEY1},
                                            bm25_backend => disk})),
        ?assertEqual({error, {encryption_unsupported, diskann}},
                     start_store(TestDir, #{crypto => #{key => ?KEY1},
                                            backend => diskann}))
    after
        os:cmd("rm -rf " ++ TestDir)
    end.
