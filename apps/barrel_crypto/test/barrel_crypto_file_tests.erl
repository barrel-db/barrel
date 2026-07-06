-module(barrel_crypto_file_tests).

-include_lib("eunit/include/eunit.hrl").

-define(KEY, binary:copy(<<16#3C>>, 32)).
-define(KEY2, binary:copy(<<16#C3>>, 32)).
-define(SECTOR, 4096).
-define(PAYLOAD, 4084).

%%====================================================================
%% Static mode
%%====================================================================

static_roundtrip_test() ->
    N = barrel_crypto:new_nonce(8),
    Data = crypto:strong_rand_bytes(10000),
    Ct = barrel_crypto_file:crypt_static(?KEY, N, 0, Data),
    ?assertEqual(byte_size(Data), byte_size(Ct)),
    lists:foreach(
        fun({Off, Len}) ->
            ?assertEqual(binary:part(Data, Off, Len),
                         barrel_crypto_file:crypt_static(
                             ?KEY, N, Off, binary:part(Ct, Off, Len)))
        end,
        [{0, 100}, {13, 37}, {4095, 2}, {4096, 4096}, {9999, 1},
         {17, 8000}]).

%%====================================================================
%% Embedded sectors
%%====================================================================

sector_roundtrip_test() ->
    Payload = crypto:strong_rand_bytes(1000),
    Sector = barrel_crypto_file:seal_sector(?KEY, Payload),
    ?assertEqual(?SECTOR, byte_size(Sector)),
    Opened = barrel_crypto_file:open_sector(?KEY, Sector),
    ?assertEqual(?PAYLOAD, byte_size(Opened)),
    ?assertEqual(Payload, binary:part(Opened, 0, byte_size(Payload))),
    %% padding decrypts back to zeros
    ?assertEqual(binary:copy(<<0>>, ?PAYLOAD - 1000),
                 binary:part(Opened, 1000, ?PAYLOAD - 1000)).

sector_full_payload_test() ->
    Payload = crypto:strong_rand_bytes(?PAYLOAD),
    Sector = barrel_crypto_file:seal_sector(?KEY, Payload),
    ?assertEqual(Payload, barrel_crypto_file:open_sector(?KEY, Sector)).

sector_fresh_nonce_test() ->
    %% same payload seals to different bytes every time (fresh nonce:
    %% in-place rewrites never reuse keystream)
    Payload = <<"same payload">>,
    S1 = barrel_crypto_file:seal_sector(?KEY, Payload),
    S2 = barrel_crypto_file:seal_sector(?KEY, Payload),
    ?assertNotEqual(S1, S2).

sector_payload_cap_test() ->
    Too = crypto:strong_rand_bytes(?PAYLOAD + 1),
    ?assertError({payload_too_large, 4085},
                 barrel_crypto_file:seal_sector(?KEY, Too)).

logical_roundtrip_test() ->
    lists:foreach(
        fun(Size) ->
            Bin = crypto:strong_rand_bytes(Size),
            Sealed = barrel_crypto_file:seal_logical(?KEY, Bin),
            ?assertEqual(0, byte_size(Sealed) rem ?SECTOR),
            Opened = barrel_crypto_file:open_logical(?KEY, Sealed),
            ?assertEqual(Bin, binary:part(Opened, 0, Size))
        end,
        [0, 1, 1000, ?PAYLOAD, ?PAYLOAD + 1, 3 * ?PAYLOAD + 17]).

%%====================================================================
%% Read fun wrappers
%%====================================================================

phys_readfun(Bin) ->
    fun(Off, Size) ->
        case Off + Size =< byte_size(Bin) of
            true -> {ok, binary:part(Bin, Off, Size)};
            false when Off >= byte_size(Bin) -> eof;
            false -> {ok, binary:part(Bin, Off, byte_size(Bin) - Off)}
        end
    end.

static_readfun_test() ->
    N = barrel_crypto:new_nonce(8),
    Data = crypto:strong_rand_bytes(20000),
    Ct = barrel_crypto_file:crypt_static(?KEY, N, 0, Data),
    Read = barrel_crypto_file:decrypting_readfun({static, N}, ?KEY,
                                                 phys_readfun(Ct)),
    lists:foreach(
        fun({Off, Size}) ->
            ?assertEqual({ok, binary:part(Data, Off, Size)},
                         Read(Off, Size))
        end,
        [{0, 6}, {4, 24}, {4090, 100}, {12288, 4096}, {19999, 1}]),
    ?assertEqual(eof, Read(20000, 4)).

embedded_readfun_test() ->
    Data = crypto:strong_rand_bytes(3 * ?PAYLOAD + 500),
    Sealed = barrel_crypto_file:seal_logical(?KEY, Data),
    Read = barrel_crypto_file:decrypting_readfun(embedded, ?KEY,
                                                 phys_readfun(Sealed)),
    lists:foreach(
        fun({Off, Size}) ->
            ?assertEqual({ok, binary:part(Data, Off, Size)},
                         Read(Off, Size))
        end,
        [{0, 6}, {0, ?PAYLOAD}, {?PAYLOAD - 3, 10},        %% straddles
         {?PAYLOAD, ?PAYLOAD}, {2 * ?PAYLOAD + 7, ?PAYLOAD + 100},
         {3 * ?PAYLOAD, 500}, {100, 3 * ?PAYLOAD}]),
    ?assertEqual({ok, <<>>}, Read(0, 0)),
    %% a truncated last sector fails closed
    Trunc = binary:part(Sealed, 0, byte_size(Sealed) - 100),
    ReadT = barrel_crypto_file:decrypting_readfun(embedded, ?KEY,
                                                  phys_readfun(Trunc)),
    ?assertEqual({error, truncated_sector},
                 ReadT(3 * ?PAYLOAD, 500)).

embedded_wrong_key_garbage_test() ->
    %% CTR does not authenticate: a wrong key yields garbage, never a
    %% crash. The key-check token in the superblock is the gate.
    Data = crypto:strong_rand_bytes(1000),
    Sealed = barrel_crypto_file:seal_logical(?KEY, Data),
    Read = barrel_crypto_file:decrypting_readfun(embedded, ?KEY2,
                                                 phys_readfun(Sealed)),
    {ok, Garbage} = Read(0, 1000),
    ?assertNotEqual(Data, Garbage).

%%====================================================================
%% Superblock
%%====================================================================

superblock_roundtrip_test() ->
    Token = barrel_crypto:key_check_new(?KEY),
    Sb = barrel_crypto_file:superblock_encode(
        #{key_check => Token,
          nonces => #{postings => barrel_crypto:new_nonce(8)}}),
    {ok, Map} = barrel_crypto_file:superblock_decode(Sb),
    ?assertEqual(Token, maps:get(key_check, Map)),
    ?assert(barrel_crypto:key_check_verify(?KEY,
                                           maps:get(key_check, Map))),
    ?assertNot(barrel_crypto:key_check_verify(?KEY2,
                                              maps:get(key_check, Map))),
    %% decoding tolerates trailing padding (callers pad to a sector)
    Padded = <<Sb/binary, 0:((?SECTOR - byte_size(Sb)) * 8)>>,
    ?assertEqual({ok, Map}, barrel_crypto_file:superblock_decode(Padded)).

superblock_plaintext_test() ->
    ?assertEqual(plaintext,
                 barrel_crypto_file:superblock_decode(<<"BM25DSK1rest">>)),
    ?assertEqual(plaintext, barrel_crypto_file:superblock_decode(<<>>)).

superblock_corrupt_test() ->
    ?assertEqual({error, corrupt_superblock},
                 barrel_crypto_file:superblock_decode(
                     <<16#42, 16#43, 1, 100:32/big, "short">>)),
    ?assertEqual({error, corrupt_superblock},
                 barrel_crypto_file:superblock_decode(
                     <<16#42, 16#43, 99, 0:32/big>>)).
