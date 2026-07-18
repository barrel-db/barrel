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
%%% [sidecar_off, +sidecar_len)    ordinal -> key sidecar
%%% '''
%%%
%%% The offset table is direct-addressed: `table[Gram]' is the byte
%%% offset of that gram's posting block within the postings region, or 0
%%% when the gram is absent. Byte 0 of the postings region is a reserved
%%% sentinel so a 0 entry is unambiguously "empty". The table spans only
%%% up to the highest gram present; a query for any higher gram reads
%%% past the table and is treated as empty.
%%%
%%% Each posting block is stored length-prefixed: `[Len:32][block]' where
%%% the block is a delta+varint list of ordinals (see
%%% {@link barrel_ngram_postings}).
%%%
%%% The sidecar maps a local ordinal back to its barrel document key: a
%%% fixed `[KeyOff:32][KeyLen:32]' index (one entry per ordinal) followed
%%% by the concatenated key bytes. The index is read once at open and
%%% cached; keys are read on demand with `file:pread'.
%%%
%%% Reads use `file:pread' and leave caching to the OS/ZFS ARC. Handles
%%% carry a raw read fd owned by the opening process, so a query opens its
%%% own handle rather than sharing the shard's.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_segment).

-export([write/2, open/1, close/1]).
-export([lookup_postings/2, keys/2, doc_count/1, watermark/1]).

-define(MAGIC, <<"NGRAMSEG">>).
-define(VERSION, 1).
-define(SECTOR, 4096).
-define(GRAM_COUNT, (1 bsl 24)).
-define(ENTRY_BYTES, 4).

-record(segment, {
    fd :: file:fd(),
    doc_count :: non_neg_integer(),
    offset_table_off :: non_neg_integer(),
    offset_table_len :: non_neg_integer(),
    postings_off :: non_neg_integer(),
    sidecar_off :: non_neg_integer(),
    key_data_start :: non_neg_integer(),
    key_index :: tuple(),        %% ordinal -> {KeyOff, KeyLen}
    watermark :: binary()
}).

-opaque handle() :: #segment{}.
-export_type([handle/0]).

%% Input to write/2.
-type spec() :: #{
    doc_count := non_neg_integer(),
    watermark := binary(),
    postings := [{barrel_ngram_selector:gram(), [barrel_ngram_postings:ordinal()]}],
    keys := [binary()]     %% key for ordinal 0, 1, ... in order
}.
-export_type([spec/0]).

%%====================================================================
%% Write
%%====================================================================

%% @doc Write an immutable segment to `Path'. Writes to a temp file and
%% renames into place so a reader never sees a partial segment.
-spec write(file:name_all(), spec()) -> ok | {error, term()}.
write(Path, #{doc_count := DocCount, watermark := Wm,
              postings := Postings, keys := Keys}) ->
    12 = byte_size(Wm),
    DocCount = length(Keys),

    SortedGrams = lists:keysort(1, Postings),
    {PostingsRegion, GramOffsets} = build_postings(SortedGrams),
    PostingsLen = byte_size(PostingsRegion),

    OffsetTableLen = case GramOffsets of
        [] -> 0;
        _ -> (element(1, lists:last(GramOffsets)) + 1) * ?ENTRY_BYTES
    end,
    OffsetTable = iolist_to_binary(build_table(GramOffsets, 0, [])),
    OffsetTableLen = byte_size(OffsetTable),

    {SidecarIndex, KeyData} = build_sidecar(Keys),
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
        watermark => Wm
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
build_postings(SortedGrams) ->
    %% Region byte 0 is the empty sentinel; blocks start at offset 1.
    build_postings(SortedGrams, [<<0>>], 1, []).

build_postings([], RegionAcc, _NextOff, GramOffAcc) ->
    {iolist_to_binary(lists:reverse(RegionAcc)), lists:reverse(GramOffAcc)};
build_postings([{Gram, Ords} | Rest], RegionAcc, NextOff, GramOffAcc) ->
    Block = barrel_ngram_postings:encode(Ords),
    Entry = <<(byte_size(Block)):32/little, Block/binary>>,
    build_postings(Rest, [Entry | RegionAcc], NextOff + byte_size(Entry),
                   [{Gram, NextOff} | GramOffAcc]).

%% @private Build the direct-addressed offset table as an iolist of
%% zero-runs and 4-byte entries. GramOffsets is ascending by gram.
build_table([], _Next, Acc) ->
    lists:reverse(Acc);
build_table([{Gram, Off} | Rest], Next, Acc) ->
    Gap = Gram - Next,
    ZeroRun = <<0:(Gap * ?ENTRY_BYTES * 8)>>,
    Entry = <<Off:32/little>>,
    build_table(Rest, Gram + 1, [Entry, ZeroRun | Acc]).

%% @private Build the sidecar index and key-data blob.
build_sidecar(Keys) ->
    {IdxRev, DataRev, _End} =
        lists:foldl(
            fun(K, {IdxAcc, DataAcc, Off}) ->
                Len = byte_size(K),
                Entry = <<Off:32/little, Len:32/little>>,
                {[Entry | IdxAcc], [K | DataAcc], Off + Len}
            end, {[], [], 0}, Keys),
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
                                doc_count = maps:get(doc_count, H),
                                offset_table_off = maps:get(offset_table_off, H),
                                offset_table_len = maps:get(offset_table_len, H),
                                postings_off = maps:get(postings_off, H),
                                sidecar_off = maps:get(sidecar_off, H),
                                key_data_start =
                                    maps:get(sidecar_off, H)
                                    + maps:get(doc_count, H) * 8,
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

%% @doc Document count in the segment.
-spec doc_count(handle()) -> non_neg_integer().
doc_count(#segment{doc_count = N}) -> N.

%% @doc The high-watermark HLC (12-byte encoded) this segment covers.
-spec watermark(handle()) -> binary().
watermark(#segment{watermark = Wm}) -> Wm.

%% @doc Posting list (ascending ordinals) for a gram, or `empty'.
-spec lookup_postings(handle(), barrel_ngram_selector:gram()) ->
    {ok, [barrel_ngram_postings:ordinal()]} | empty | {error, term()}.
lookup_postings(#segment{offset_table_len = TableLen}, Gram)
        when Gram * ?ENTRY_BYTES + ?ENTRY_BYTES > TableLen ->
    empty;
lookup_postings(#segment{fd = Fd, offset_table_off = TableOff,
                         postings_off = PostingsOff}, Gram) ->
    case file:pread(Fd, TableOff + Gram * ?ENTRY_BYTES, ?ENTRY_BYTES) of
        {ok, <<0:32/little>>} ->
            empty;
        {ok, <<RegionOff:32/little>>} ->
            case file:pread(Fd, PostingsOff + RegionOff, 4) of
                {ok, <<Len:32/little>>} ->
                    case file:pread(Fd, PostingsOff + RegionOff + 4, Len) of
                        {ok, Block} -> {ok, barrel_ngram_postings:decode(Block)};
                        eof -> {error, truncated_postings};
                        {error, _} = Err -> Err
                    end;
                eof -> {error, truncated_postings};
                {error, _} = Err -> Err
            end;
        eof ->
            empty;
        {error, _} = Err ->
            Err
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
                 {Off, Len} = element(O + 1, KeyIndex),
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

%%====================================================================
%% Header + sidecar-index parsing
%%====================================================================

encode_header(#{doc_count := DocCount, offset_table_off := OTOff,
                offset_table_len := OTLen, postings_off := POff,
                postings_len := PLen, sidecar_off := SOff,
                sidecar_len := SLen, watermark := Wm}) ->
    <<?MAGIC/binary,
      ?VERSION:32/little,
      DocCount:32/little,
      OTOff:64/little,
      OTLen:64/little,
      POff:64/little,
      PLen:64/little,
      SOff:64/little,
      SLen:64/little,
      Wm:12/binary>>.

read_header(Fd) ->
    case file:pread(Fd, 0, ?SECTOR) of
        {ok, Bin} -> parse_header(Bin);
        eof -> {error, empty_segment};
        {error, _} = Err -> Err
    end.

parse_header(<<Magic:8/binary, Version:32/little, DocCount:32/little,
               OTOff:64/little, OTLen:64/little, POff:64/little,
               PLen:64/little, SOff:64/little, SLen:64/little,
               Wm:12/binary, _/binary>>)
        when Magic =:= ?MAGIC, Version =:= ?VERSION ->
    {ok, #{
        doc_count => DocCount,
        offset_table_off => OTOff,
        offset_table_len => OTLen,
        postings_off => POff,
        postings_len => PLen,
        sidecar_off => SOff,
        sidecar_len => SLen,
        watermark => Wm
    }};
parse_header(<<Magic:8/binary, _/binary>>) when Magic =/= ?MAGIC ->
    {error, invalid_magic};
parse_header(_) ->
    {error, invalid_header}.

read_key_index(_Fd, #{doc_count := 0}) ->
    {ok, {}};
read_key_index(Fd, #{sidecar_off := SOff, doc_count := DocCount}) ->
    case file:pread(Fd, SOff, DocCount * 8) of
        {ok, Bin} when byte_size(Bin) =:= DocCount * 8 ->
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
parse_index(<<Off:32/little, Len:32/little, Rest/binary>>, Acc) ->
    parse_index(Rest, [{Off, Len} | Acc]).

%%====================================================================
%% Helpers
%%====================================================================

pad_to_sector(Bin) ->
    Size = byte_size(Bin),
    Padded = ((Size + ?SECTOR - 1) div ?SECTOR) * ?SECTOR,
    <<Bin/binary, 0:((Padded - Size) * 8)>>.

to_binary(P) when is_binary(P) -> P;
to_binary(P) when is_list(P) -> list_to_binary(P).
