%% @doc Sector encryption for mmap'd flat files (BM25 disk, DiskANN).
%%
%% These files are read at arbitrary byte offsets through `iommap:pread'
%% or `file:pread', so encryption must be offset-addressable
%% (decrypt-after-read), not a stream. Two modes, chosen by how a file
%% is rewritten:
%%
%% <ul>
%% <li>STATIC: one 8-byte file nonce, keystream positioned by byte
%%     offset (`<<Nonce:8/binary, Block:64>>' counter blocks).
%%     Ciphertext length equals plaintext length and any slice decrypts
%%     independently. ONLY for files never rewritten in place: immutable
%%     or append-only files, with the nonce rotated on every full
%%     rewrite (compaction). Rewriting an offset under the same nonce
%%     reuses CTR keystream and breaks confidentiality.</li>
%% <li>EMBEDDED: fixed 4096-byte physical sectors, each carrying a
%%     fresh 12-byte nonce and a 4084-byte CTR payload. Safe for
%%     in-place rewrites (every write draws a new nonce). Logical bytes
%%     are the concatenation of payloads; readers translate logical
%%     offsets to sectors.</li>
%% </ul>
%%
%% Neither mode authenticates sector contents (CTR is malleable), which
%% matches the guarantee of RocksDB's EncryptedEnv: the threat model is
%% data at rest. Wrong-key detection comes from the key-check token in
%% the cleartext "BC" superblock a caller stores in its metadata file.
%% @end
-module(barrel_crypto_file).

-export([
    crypt_static/4,
    seal_sector/2,
    open_sector/2,
    seal_logical/2,
    open_logical/2,
    decrypting_readfun/3,
    superblock_encode/1,
    superblock_decode/1,
    sector_size/0,
    payload_size/0
]).

-define(SECTOR, 4096).
-define(EMB_NONCE, 12).
-define(EMB_PAYLOAD, (?SECTOR - ?EMB_NONCE)).  %% 4084

-define(SB_MAGIC, <<16#42, 16#43>>).  %% "BC"
-define(SB_VERSION, 1).

-type readfun() :: fun((non_neg_integer(), non_neg_integer()) ->
    {ok, binary()} | eof | {error, term()}).

%%====================================================================
%% Static mode
%%====================================================================

%% @doc En/decrypt Bin at ByteOffset within the file's CTR stream. The
%% call is symmetric. The caller owns nonce discipline: never rewrite a
%% byte range in place under the same nonce.
-spec crypt_static(binary(), binary(), non_neg_integer(), binary()) ->
    binary().
crypt_static(Key, FileNonce, ByteOffset, Bin) ->
    barrel_crypto:ctr_crypt(Key, FileNonce, ByteOffset, Bin).

%%====================================================================
%% Embedded mode
%%====================================================================

%% @doc Seal one payload into a full 4096-byte sector: fresh 12-byte
%% nonce + CTR payload, zero-padded to the payload size. Payloads over
%% 4084 bytes do not fit and raise `{payload_too_large, Size}'.
-spec seal_sector(binary(), binary()) -> binary().
seal_sector(Key, Payload) when byte_size(Payload) =< ?EMB_PAYLOAD ->
    Nonce = barrel_crypto:new_nonce(?EMB_NONCE),
    Padded = pad_payload(Payload),
    IV = <<Nonce/binary, 0:32>>,
    Ct = barrel_crypto:ctr_crypt(Key, IV, Padded),
    <<Nonce/binary, Ct/binary>>;
seal_sector(_Key, Payload) ->
    error({payload_too_large, byte_size(Payload)}).

%% @doc Open one 4096-byte sector back to its 4084-byte payload (the
%% caller's format carries its own lengths inside the payload).
-spec open_sector(binary(), binary()) -> binary().
open_sector(Key, <<Nonce:?EMB_NONCE/binary, Ct/binary>>)
  when byte_size(Ct) =:= ?EMB_PAYLOAD ->
    IV = <<Nonce/binary, 0:32>>,
    barrel_crypto:ctr_crypt(Key, IV, Ct).

%% @doc Seal a whole logical binary into consecutive sectors.
-spec seal_logical(binary(), binary()) -> binary().
seal_logical(Key, Bin) ->
    iolist_to_binary(seal_chunks(Key, Bin)).

seal_chunks(_Key, <<>>) ->
    [];
seal_chunks(Key, <<Chunk:?EMB_PAYLOAD/binary, Rest/binary>>) ->
    [seal_sector(Key, Chunk) | seal_chunks(Key, Rest)];
seal_chunks(Key, Last) ->
    [seal_sector(Key, Last)].

%% @doc Open consecutive sectors back to the logical bytes (including
%% the last sector's zero padding; callers track their own lengths).
-spec open_logical(binary(), binary()) -> binary().
open_logical(Key, Sectors) ->
    iolist_to_binary(open_chunks(Key, Sectors)).

open_chunks(_Key, <<>>) ->
    [];
open_chunks(Key, <<Sector:?SECTOR/binary, Rest/binary>>) ->
    [open_sector(Key, Sector) | open_chunks(Key, Rest)].

%%====================================================================
%% Read fun wrappers
%%====================================================================

%% @doc Wrap a physical pread-style fun into one that reads decrypted
%% bytes, so file readers change only where they build their read fun.
%%
%% `{static, Nonce}': physical and logical offsets coincide; each slice
%% is decrypted at its own offset.
%%
%% `embedded': logical offsets address the payload stream; the wrapper
%% reads the covering sectors, decrypts each, and slices. A short
%% physical read means a truncated sector and fails closed.
-spec decrypting_readfun({static, binary()} | embedded, binary(),
                         readfun()) -> readfun().
decrypting_readfun({static, Nonce}, Key, PhysRead) ->
    fun(Offset, Size) ->
        case PhysRead(Offset, Size) of
            {ok, Bin} -> {ok, crypt_static(Key, Nonce, Offset, Bin)};
            Other -> Other
        end
    end;
decrypting_readfun(embedded, Key, PhysRead) ->
    fun(Offset, Size) ->
        read_embedded(Key, PhysRead, Offset, Size)
    end.

read_embedded(_Key, _PhysRead, _Offset, 0) ->
    {ok, <<>>};
read_embedded(Key, PhysRead, Offset, Size) ->
    First = Offset div ?EMB_PAYLOAD,
    Last = (Offset + Size - 1) div ?EMB_PAYLOAD,
    PhysOff = First * ?SECTOR,
    PhysSize = (Last - First + 1) * ?SECTOR,
    case PhysRead(PhysOff, PhysSize) of
        {ok, Sectors} when byte_size(Sectors) =:= PhysSize ->
            Logical = open_logical(Key, Sectors),
            InSector = Offset rem ?EMB_PAYLOAD,
            {ok, binary:part(Logical, InSector, Size)};
        {ok, _Short} ->
            {error, truncated_sector};
        Other ->
            Other
    end.

%%====================================================================
%% Cleartext superblock
%%====================================================================

%% @doc Encode the cleartext crypto superblock a caller stores in its
%% metadata file: "BC" magic, version, and a small map that must carry
%% `key_check' (a {@link barrel_crypto:key_check_new/1} token) and may
%% carry per-file nonces or anything else the caller needs.
-spec superblock_encode(#{key_check := binary(), _ => _}) -> binary().
superblock_encode(#{key_check := Token} = Map) when is_binary(Token) ->
    Body = term_to_binary(Map),
    <<?SB_MAGIC/binary, ?SB_VERSION:8, (byte_size(Body)):32/big,
      Body/binary>>.

%% @doc Decode a superblock. `plaintext' means the bytes do not start
%% with the magic (the file predates encryption or is not encrypted);
%% anything magic-prefixed that fails to decode is corrupt.
-spec superblock_decode(binary()) ->
    {ok, map()} | plaintext | {error, corrupt_superblock}.
superblock_decode(<<16#42, 16#43, ?SB_VERSION:8, Len:32/big,
                    Rest/binary>>) when byte_size(Rest) >= Len ->
    <<Body:Len/binary, _/binary>> = Rest,
    try binary_to_term(Body, [safe]) of
        #{key_check := Token} = Map when is_binary(Token) ->
            {ok, Map};
        _ ->
            {error, corrupt_superblock}
    catch
        _:_ -> {error, corrupt_superblock}
    end;
superblock_decode(<<16#42, 16#43, _/binary>>) ->
    {error, corrupt_superblock};
superblock_decode(_) ->
    plaintext.

%%====================================================================
%% Sizes
%%====================================================================

-spec sector_size() -> pos_integer().
sector_size() -> ?SECTOR.

-spec payload_size() -> pos_integer().
payload_size() -> ?EMB_PAYLOAD.

%%====================================================================
%% Internal
%%====================================================================

pad_payload(Payload) when byte_size(Payload) =:= ?EMB_PAYLOAD ->
    Payload;
pad_payload(Payload) ->
    Pad = ?EMB_PAYLOAD - byte_size(Payload),
    <<Payload/binary, 0:(Pad * 8)>>.
