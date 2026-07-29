%%%-------------------------------------------------------------------
%%% @doc Immutable trigram segment file: write and read.
%%%
%%% One segment is a self-contained, immutable index over a set of
%%% documents. Layout (little-endian, header sector-aligned):
%%%
%%% ```
%%% [0, 4096)                      header (magic, offsets, doc_count, watermark)
%%% [4096, +offset_table_len)      offset table: u32 per gram, direct-addressed
%%% [postings_off, +postings_len)  postings region (byte 0 is a sentinel)
%%% [sidecar_off, +sidecar_len)    ordinal -> {key, hlc, deleted} sidecar
%%% '''
%%%
%%% The offset table is direct-addressed: `table[Gram]' is the byte
%%% offset of that gram's posting block within the postings region, or 0
%%% when the gram is absent. Byte 0 of the postings region is a reserved
%%% sentinel so a 0 entry is unambiguously "empty". The table spans only
%%% up to the highest gram present; a query for any higher gram reads
%%% past the table and is treated as empty. The table also serves as the
%%% gram directory for {@link all_postings/1}.
%%%
%%% Each posting block is stored length-prefixed: `[Len:32][block]' where
%%% the block is a delta+varint list of ordinals (see
%%% {@link barrel_ngram_postings}).
%%%
%%% The sidecar maps a local ordinal to its document key, the change HLC
%%% that produced it (the recency sequence number used when merging), and
%%% a deleted flag (a tombstone: a deleted key carries no grams). The
%%% index is `[KeyOff:32][KeyLen:32][Deleted:8][Hlc:12]' per ordinal,
%%% followed by the concatenated key bytes. It is read once at open;
%%% key bytes are read on demand with `file:pread'.
%%%
%%% Reads use `file:pread' and leave caching to the OS/ZFS ARC. Handles
%%% carry a raw read fd owned by the opening process, so a query opens its
%%% own handle rather than sharing the shard's.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_segment).

-export([write/2, open/1, close/1]).
-export([lookup_postings/2, lookup_block/2, keys/2, entries/1, all_postings/1,
         doc_count/1, watermark/1, codec/1]).

-define(MAGIC, <<"NGRAMSEG">>).
-define(VERSION, 3).
-define(SECTOR, 4096).
-define(GRAM_COUNT, (1 bsl 24)).
-define(ENTRY_BYTES, 4).
-define(SIDECAR_ENTRY, 21).   %% KeyOff:32 + KeyLen:32 + Deleted:8 + Hlc:96

-type codec() :: varint | roaring.
-export_type([codec/0]).

-record(segment, {
    fd :: file:fd(),
    codec :: codec(),
    doc_count :: non_neg_integer(),
    offset_table_off :: non_neg_integer(),
    offset_table_len :: non_neg_integer(),
    postings_off :: non_neg_integer(),
    sidecar_off :: non_neg_integer(),
    key_data_start :: non_neg_integer(),
    key_index :: tuple(),        %% ordinal -> {KeyOff, KeyLen, Deleted, Hlc}
    watermark :: binary()
}).

-opaque handle() :: #segment{}.
-export_type([handle/0]).

%% A per-ordinal entry given to write/2 (ordinal order).
-type entry() :: #{key := binary(), hlc := binary(), deleted := boolean()}.

%% Input to write/2.
-type spec() :: #{
    doc_count := non_neg_integer(),
    watermark := binary(),
    postings := [{barrel_ngram_selector:gram(), [barrel_ngram_postings:ordinal()]}],
    entries := [entry()],    %% entry for ordinal 0, 1, ... in order
    codec => codec()         %% posting-block codec (default varint)
}.
-export_type([spec/0, entry/0]).

%%====================================================================
%% Write
%%====================================================================

%% @doc Write an immutable segment to `Path'. Writes to a temp file and
%% renames into place so a reader never sees a partial segment.
-spec write(file:name_all(), spec()) -> ok | {error, term()}.
write(Path, #{doc_count := DocCount, watermark := Wm,
              postings := Postings, entries := Entries} = Spec) ->
    12 = byte_size(Wm),
    DocCount = length(Entries),
    Codec = maps:get(codec, Spec, varint),

    SortedGrams = lists:keysort(1, Postings),
    {PostingsRegion, GramOffsets} = build_postings(SortedGrams, Codec),
    PostingsLen = byte_size(PostingsRegion),

    OffsetTableLen = case GramOffsets of
        [] -> 0;
        _ -> (element(1, lists:last(GramOffsets)) + 1) * ?ENTRY_BYTES
    end,
    OffsetTable = iolist_to_binary(build_table(GramOffsets, 0, [])),
    OffsetTableLen = byte_size(OffsetTable),

    {SidecarIndex, KeyData} = build_sidecar(Entries),
    Sidecar = <<SidecarIndex/binary, KeyData/binary>>,
    SidecarLen = byte_size(Sidecar),

    OffsetTableOff = ?SECTOR,
    PostingsOff = OffsetTableOff + OffsetTableLen,
    SidecarOff = PostingsOff + PostingsLen,

    Header = encode_header(#{
        doc_count => DocCount,
        offset_table_off => OffsetTableOff,
        offset_table_len => OffsetTableLen,
        postings_off => PostingsOff,
        postings_len => PostingsLen,
        sidecar_off => SidecarOff,
        sidecar_len => SidecarLen,
        watermark => Wm,
        codec => Codec
    }),
    PaddedHeader = pad_to_sector(Header),

    ok = filelib:ensure_dir(Path),
    Tmp = iolist_to_binary([to_binary(Path), <<".tmp">>]),
    case file:open(Tmp, [write, binary, raw]) of
        {ok, Fd} ->
            Res = write_all(Fd, [PaddedHeader, OffsetTable, PostingsRegion, Sidecar]),
            _ = file:close(Fd),
            case Res of
                ok -> file:rename(Tmp, Path);
                {error, _} = Err ->
                    _ = file:delete(Tmp),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

write_all(_Fd, []) ->
    ok;
write_all(Fd, [Bin | Rest]) ->
    case file:write(Fd, Bin) of
        ok -> write_all(Fd, Rest);
        {error, _} = Err -> Err
    end.

%% @private Lay out the postings region and collect gram -> region-offset.
build_postings(SortedGrams, Codec) ->
    %% Region byte 0 is the empty sentinel; blocks start at offset 1.
    build_postings(SortedGrams, Codec, [<<0>>], 1, []).

build_postings([], _Codec, RegionAcc, _NextOff, GramOffAcc) ->
    {iolist_to_binary(lists:reverse(RegionAcc)), lists:reverse(GramOffAcc)};
build_postings([{Gram, Ords} | Rest], Codec, RegionAcc, NextOff, GramOffAcc) ->
    Block = encode_block(Codec, Ords),
    Entry = <<(byte_size(Block)):32/little, Block/binary>>,
    build_postings(Rest, Codec, [Entry | RegionAcc], NextOff + byte_size(Entry),
                   [{Gram, NextOff} | GramOffAcc]).

encode_block(varint, Ords) -> barrel_ngram_postings:encode(Ords);
encode_block(roaring, Ords) -> barrel_ngram_roaring:encode(Ords).

decode_block(varint, Block) -> barrel_ngram_postings:decode(Block);
decode_block(roaring, Block) -> barrel_ngram_roaring:decode(Block).

%% @private Build the direct-addressed offset table as an iolist of
%% zero-runs and 4-byte entries. GramOffsets is ascending by gram.
build_table([], _Next, Acc) ->
    lists:reverse(Acc);
build_table([{Gram, Off} | Rest], Next, Acc) ->
    Gap = Gram - Next,
    ZeroRun = <<0:(Gap * ?ENTRY_BYTES * 8)>>,
    Entry = <<Off:32/little>>,
    build_table(Rest, Gram + 1, [Entry, ZeroRun | Acc]).

%% @private Build the sidecar index and key-data blob from ordinal-ordered
%% entries.
build_sidecar(Entries) ->
    {IdxRev, DataRev, _End} =
        lists:foldl(
            fun(#{key := K, hlc := Hlc, deleted := Del}, {IdxAcc, DataAcc, Off}) ->
                12 = byte_size(Hlc),
                Len = byte_size(K),
                DelByte = case Del of true -> 1; false -> 0 end,
                E = <<Off:32/little, Len:32/little, DelByte:8, Hlc:12/binary>>,
                {[E | IdxAcc], [K | DataAcc], Off + Len}
            end, {[], [], 0}, Entries),
    {iolist_to_binary(lists:reverse(IdxRev)),
     iolist_to_binary(lists:reverse(DataRev))}.

%%====================================================================
%% Open / read
%%====================================================================

%% @doc Open a segment for reading. The returned handle owns a raw read
%% fd; close it with {@link close/1}.
-spec open(file:name_all()) -> {ok, handle()} | {error, term()}.
open(Path) ->
    case file:open(Path, [read, binary, raw]) of
        {ok, Fd} ->
            case read_header(Fd) of
                {ok, H} ->
                    case read_key_index(Fd, H) of
                        {ok, KeyIndex} ->
                            {ok, #segment{
                                fd = Fd,
                                codec = maps:get(codec, H),
                                doc_count = maps:get(doc_count, H),
                                offset_table_off = maps:get(offset_table_off, H),
                                offset_table_len = maps:get(offset_table_len, H),
                                postings_off = maps:get(postings_off, H),
                                sidecar_off = maps:get(sidecar_off, H),
                                key_data_start =
                                    maps:get(sidecar_off, H)
                                    + maps:get(doc_count, H) * ?SIDECAR_ENTRY,
                                key_index = KeyIndex,
                                watermark = maps:get(watermark, H)
                            }};
                        {error, _} = Err ->
                            _ = file:close(Fd),
                            Err
                    end;
                {error, _} = Err ->
                    _ = file:close(Fd),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Close a segment handle.
-spec close(handle()) -> ok.
close(#segment{fd = Fd}) ->
    _ = file:close(Fd),
    ok.

%% @doc Document count in the segment (includes tombstones).
-spec doc_count(handle()) -> non_neg_integer().
doc_count(#segment{doc_count = N}) -> N.

%% @doc The high-watermark HLC (12-byte encoded) this segment covers.
-spec watermark(handle()) -> binary().
watermark(#segment{watermark = Wm}) -> Wm.

%% @doc The posting-block codec of this segment.
-spec codec(handle()) -> codec().
codec(#segment{codec = Codec}) -> Codec.

%% @doc Posting list (ascending ordinals) for a gram, or `empty'.
-spec lookup_postings(handle(), barrel_ngram_selector:gram()) ->
    {ok, [barrel_ngram_postings:ordinal()]} | empty | {error, term()}.
lookup_postings(#segment{codec = Codec} = H, Gram) ->
    case lookup_block(H, Gram) of
        {ok, Block} -> {ok, decode_block(Codec, Block)};
        Other -> Other
    end.

%% @doc The raw (undecoded) posting block for a gram, or `empty'. Lets the
%% query combine blocks natively (roaring) without decoding each to a list.
-spec lookup_block(handle(), barrel_ngram_selector:gram()) ->
    {ok, binary()} | empty | {error, term()}.
lookup_block(#segment{offset_table_len = TableLen}, Gram)
        when Gram * ?ENTRY_BYTES + ?ENTRY_BYTES > TableLen ->
    empty;
lookup_block(#segment{fd = Fd, offset_table_off = TableOff,
                      postings_off = PostingsOff}, Gram) ->
    case file:pread(Fd, TableOff + Gram * ?ENTRY_BYTES, ?ENTRY_BYTES) of
        {ok, <<0:32/little>>} ->
            empty;
        {ok, <<RegionOff:32/little>>} ->
            read_raw_block(Fd, PostingsOff + RegionOff);
        eof ->
            empty;
        {error, _} = Err ->
            Err
    end.

read_raw_block(Fd, At) ->
    case file:pread(Fd, At, 4) of
        {ok, <<Len:32/little>>} ->
            case file:pread(Fd, At + 4, Len) of
                {ok, Block} -> {ok, Block};
                eof -> {error, truncated_postings};
                {error, _} = Err -> Err
            end;
        eof -> {error, truncated_postings};
        {error, _} = Err -> Err
    end.

%% @doc Resolve ordinals to `{Ordinal, Key}' pairs (one batched pread).
%% Out-of-range ordinals are dropped.
-spec keys(handle(), [barrel_ngram_postings:ordinal()]) ->
    [{barrel_ngram_postings:ordinal(), binary()}].
keys(_Handle, []) ->
    [];
keys(#segment{fd = Fd, doc_count = DocCount, key_index = KeyIndex,
              key_data_start = DataStart}, Ordinals) ->
    Valid = [O || O <- Ordinals, O >= 0, O < DocCount],
    Pairs = [begin
                 {Off, Len, _Del, _Hlc} = element(O + 1, KeyIndex),
                 {DataStart + Off, Len}
             end || O <- Valid],
    case file:pread(Fd, Pairs) of
        {ok, Bins} ->
            lists:zipwith(fun(O, Key) -> {O, Key} end, Valid, Bins);
        eof ->
            [];
        {error, _} ->
            []
    end.

%% @doc Every ordinal as `{Ordinal, Key, Hlc, Deleted}'. Used by the merger.
-spec entries(handle()) ->
    [{barrel_ngram_postings:ordinal(), binary(), binary(), boolean()}].
entries(#segment{doc_count = 0}) ->
    [];
entries(#segment{doc_count = N, key_index = KI} = H) ->
    Ords = lists:seq(0, N - 1),
    KeyMap = maps:from_list(keys(H, Ords)),
    [begin
         {_Off, _Len, Del, Hlc} = element(O + 1, KI),
         {O, maps:get(O, KeyMap), Hlc, Del}
     end || O <- Ords].

%% @doc Every present `{Gram, [Ordinal]}' in the segment. Reads the offset
%% table (the gram directory) sequentially, then each posting block. Used
%% by the merger to rebuild per-ordinal grams.
-spec all_postings(handle()) ->
    [{barrel_ngram_selector:gram(), [barrel_ngram_postings:ordinal()]}].
all_postings(#segment{offset_table_len = 0}) ->
    [];
all_postings(#segment{fd = Fd, codec = Codec, offset_table_off = TOff,
                      offset_table_len = TLen, postings_off = POff}) ->
    {ok, Table} = file:pread(Fd, TOff, TLen),
    scan_table(Table, 0, Fd, Codec, POff, []).

scan_table(<<>>, _Gram, _Fd, _Codec, _POff, Acc) ->
    lists:reverse(Acc);
scan_table(<<0:32/little, Rest/binary>>, Gram, Fd, Codec, POff, Acc) ->
    scan_table(Rest, Gram + 1, Fd, Codec, POff, Acc);
scan_table(<<RegionOff:32/little, Rest/binary>>, Gram, Fd, Codec, POff, Acc) ->
    {ok, Block} = read_raw_block(Fd, POff + RegionOff),
    scan_table(Rest, Gram + 1, Fd, Codec, POff,
               [{Gram, decode_block(Codec, Block)} | Acc]).

%%====================================================================
%% Header + sidecar-index parsing
%%====================================================================

encode_header(#{doc_count := DocCount, offset_table_off := OTOff,
                offset_table_len := OTLen, postings_off := POff,
                postings_len := PLen, sidecar_off := SOff,
                sidecar_len := SLen, watermark := Wm, codec := Codec}) ->
    <<?MAGIC/binary,
      ?VERSION:32/little,
      DocCount:32/little,
      OTOff:64/little,
      OTLen:64/little,
      POff:64/little,
      PLen:64/little,
      SOff:64/little,
      SLen:64/little,
      Wm:12/binary,
      (codec_byte(Codec)):8>>.

codec_byte(varint) -> 0;
codec_byte(roaring) -> 1.

byte_codec(0) -> varint;
byte_codec(1) -> roaring.

read_header(Fd) ->
    case file:pread(Fd, 0, ?SECTOR) of
        {ok, Bin} -> parse_header(Bin);
        eof -> {error, empty_segment};
        {error, _} = Err -> Err
    end.

parse_header(<<Magic:8/binary, Version:32/little, DocCount:32/little,
               OTOff:64/little, OTLen:64/little, POff:64/little,
               PLen:64/little, SOff:64/little, SLen:64/little,
               Wm:12/binary, CodecByte:8, _/binary>>)
        when Magic =:= ?MAGIC, Version =:= ?VERSION ->
    {ok, #{
        doc_count => DocCount,
        offset_table_off => OTOff,
        offset_table_len => OTLen,
        postings_off => POff,
        postings_len => PLen,
        sidecar_off => SOff,
        sidecar_len => SLen,
        watermark => Wm,
        codec => byte_codec(CodecByte)
    }};
parse_header(<<Magic:8/binary, _/binary>>) when Magic =/= ?MAGIC ->
    {error, invalid_magic};
parse_header(_) ->
    {error, invalid_header}.

read_key_index(_Fd, #{doc_count := 0}) ->
    {ok, {}};
read_key_index(Fd, #{sidecar_off := SOff, doc_count := DocCount}) ->
    Bytes = DocCount * ?SIDECAR_ENTRY,
    case file:pread(Fd, SOff, Bytes) of
        {ok, Bin} when byte_size(Bin) =:= Bytes ->
            {ok, list_to_tuple(parse_index(Bin, []))};
        {ok, _} ->
            {error, truncated_sidecar};
        eof ->
            {error, truncated_sidecar};
        {error, _} = Err ->
            Err
    end.

parse_index(<<>>, Acc) ->
    lists:reverse(Acc);
parse_index(<<Off:32/little, Len:32/little, Del:8, Hlc:12/binary, Rest/binary>>, Acc) ->
    Deleted = Del =/= 0,
    parse_index(Rest, [{Off, Len, Deleted, Hlc} | Acc]).

%%====================================================================
%% Helpers
%%====================================================================

pad_to_sector(Bin) ->
    Size = byte_size(Bin),
    Padded = ((Size + ?SECTOR - 1) div ?SECTOR) * ?SECTOR,
    <<Bin/binary, 0:((Padded - Size) * 8)>>.

to_binary(P) when is_binary(P) -> P;
to_binary(P) when is_list(P) -> list_to_binary(P).
