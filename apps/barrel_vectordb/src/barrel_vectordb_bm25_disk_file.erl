%%%-------------------------------------------------------------------
%%% @doc BM25 Disk File I/O
%%%
%%% Handles persistent storage for disk-native BM25 index with:
%%% - Varint encoding for compact posting storage
%%% - 4KB sector-aligned writes for optimal SSD performance
%%% - Block-max index for early termination
%%% - mmap support for zero-copy reads
%%%
%%% File layout:
%%% - bm25.meta: Header + global stats (4KB)
%%% - bm25.postings: Compressed posting blocks (4KB aligned)
%%% - bm25.blockmax: Block-max index (mmap'd)
%%% - bm25.terms/: RocksDB for term <-> int mapping
%%% - bm25.docs/: RocksDB for doc <-> int mapping
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_bm25_disk_file).

%% API - File operations
-export([
    create/2,
    open/1,
    close/1,
    sync/1,
    get_path/1
]).

%% API - Header operations
-export([
    write_header/2,
    read_header/1,
    update_stats/2
]).

%% API - Varint encoding/decoding
-export([
    varint_encode/1,
    varint_decode/1,
    varint_decode_list/2,
    varint_encode_list/1
]).

%% API - Block I/O operations
-export([
    write_block/3,
    read_block/2,
    read_blocks/2,
    pad_to_sector/1
]).

%% API - mmap operations
-export([
    mmap_open/1,
    mmap_close/1,
    mmap_read/3,
    has_mmap/1
]).

%% API - Posting operations
-export([
    encode_posting_block/1,
    decode_posting_block/1,
    write_postings/3,
    read_postings/3
]).

%% API - Block-max index operations
-export([
    write_blockmax_index/2,
    read_blockmax_index/1,
    lookup_blocks_for_term/2
]).

-define(SECTOR_SIZE, 4096).
-define(MAGIC, <<"BM25DSK1">>).
-define(VERSION, 1).
-define(BLOCK_DOC_COUNT, 128).  %% Documents per posting block

-record(bm25_file, {
    path :: binary(),
    meta_fd :: file:fd() | undefined,
    postings_fd :: file:fd() | undefined,
    blockmax_fd :: file:fd() | undefined,
    blockmax_mmap :: term() | undefined,
    header :: map()
}).

-type bm25_file() :: #bm25_file{}.
-type posting() :: {DocIntId :: non_neg_integer(), TF :: pos_integer()}.
-type block_max_entry() :: #{
    max_impact := float(),
    doc_start := non_neg_integer(),
    doc_end := non_neg_integer(),
    offset := non_neg_integer(),
    size := non_neg_integer()
}.

-export_type([bm25_file/0, posting/0, block_max_entry/0]).

%%====================================================================
%% File Operations
%%====================================================================

%% @doc Create a new BM25 disk index
-spec create(binary() | string(), map()) -> {ok, bm25_file()} | {error, term()}.
create(Path, Config) ->
    PathBin = to_binary(Path),
    ok = filelib:ensure_dir(filename:join(PathBin, "dummy")),

    MetaPath = filename:join(PathBin, "bm25.meta"),
    PostingsPath = filename:join(PathBin, "bm25.postings"),
    BlockmaxPath = filename:join(PathBin, "bm25.blockmax"),

    case open_files(MetaPath, PostingsPath, BlockmaxPath, [write, read, binary, raw]) of
        {ok, MetaFd, PostingsFd, BlockmaxFd} ->
            Header = #{
                magic => ?MAGIC,
                version => ?VERSION,
                term_count => 0,
                doc_count => 0,
                total_tokens => 0,
                avgdl => 0.0,
                k1 => maps:get(k1, Config, 1.2),
                b => maps:get(b, Config, 0.75),
                block_size => maps:get(block_size, Config, ?BLOCK_DOC_COUNT),
                postings_offset => ?SECTOR_SIZE,  %% Start after header
                blockmax_offset => 0
            },
            File = #bm25_file{
                path = PathBin,
                meta_fd = MetaFd,
                postings_fd = PostingsFd,
                blockmax_fd = BlockmaxFd,
                header = Header
            },
            ok = write_header_internal(File, Header),
            {ok, File};
        {error, _} = Error ->
            Error
    end.

%% @doc Open an existing BM25 disk index
-spec open(binary() | string()) -> {ok, bm25_file()} | {error, term()}.
open(Path) ->
    PathBin = to_binary(Path),

    MetaPath = filename:join(PathBin, "bm25.meta"),
    PostingsPath = filename:join(PathBin, "bm25.postings"),
    BlockmaxPath = filename:join(PathBin, "bm25.blockmax"),

    case open_files(MetaPath, PostingsPath, BlockmaxPath, [read, write, binary, raw]) of
        {ok, MetaFd, PostingsFd, BlockmaxFd} ->
            case read_header_internal(MetaFd) of
                {ok, Header} ->
                    %% Try to open blockmax with mmap
                    BlockmaxMmap = try_open_mmap(BlockmaxPath),
                    {ok, #bm25_file{
                        path = PathBin,
                        meta_fd = MetaFd,
                        postings_fd = PostingsFd,
                        blockmax_fd = BlockmaxFd,
                        blockmax_mmap = BlockmaxMmap,
                        header = Header
                    }};
                {error, _} = Error ->
                    _ = file:close(MetaFd),
                    _ = file:close(PostingsFd),
                    _ = file:close(BlockmaxFd),
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Close file handles
-spec close(bm25_file()) -> ok.
close(#bm25_file{meta_fd = MetaFd, postings_fd = PostingsFd,
                  blockmax_fd = BlockmaxFd, blockmax_mmap = BlockmaxMmap,
                  header = Header} = File) ->
    %% Write final header
    _ = write_header_internal(File, Header),
    _ = close_mmap_if_open(BlockmaxMmap),
    _ = close_if_open(MetaFd),
    _ = close_if_open(PostingsFd),
    _ = close_if_open(BlockmaxFd),
    ok.

%% @doc Sync all file handles to disk
-spec sync(bm25_file()) -> ok.
sync(#bm25_file{meta_fd = MetaFd, postings_fd = PostingsFd, blockmax_fd = BlockmaxFd}) ->
    _ = sync_if_open(MetaFd),
    _ = sync_if_open(PostingsFd),
    _ = sync_if_open(BlockmaxFd),
    ok.

%% @doc Get the base path
-spec get_path(bm25_file()) -> binary().
get_path(#bm25_file{path = Path}) -> Path.

%%====================================================================
%% Header Operations
%%====================================================================

%% @doc Write index header
-spec write_header(bm25_file(), map()) -> {ok, bm25_file()}.
write_header(File, Header) ->
    ok = write_header_internal(File, Header),
    {ok, File#bm25_file{header = Header}}.

%% @doc Read index header (returns cached header)
-spec read_header(bm25_file()) -> map().
read_header(#bm25_file{header = Header}) ->
    Header.

%% @doc Update statistics in header
-spec update_stats(bm25_file(), map()) -> {ok, bm25_file()}.
update_stats(#bm25_file{header = Header} = File, Stats) ->
    NewHeader = maps:merge(Header, Stats),
    write_header(File, NewHeader).

%%====================================================================
%% Varint Encoding/Decoding
%%====================================================================

%% @doc Encode an unsigned integer as a varint (7-bit encoding)
%% Each byte uses 7 bits for data and 1 bit as continuation flag
-spec varint_encode(non_neg_integer()) -> binary().
varint_encode(N) when N >= 0 ->
    varint_encode_loop(N, <<>>).

varint_encode_loop(N, Acc) when N < 128 ->
    %% Last byte - no continuation bit
    <<Acc/binary, N:8>>;
varint_encode_loop(N, Acc) ->
    %% More bytes to come - set continuation bit (high bit)
    Byte = (N band 16#7F) bor 16#80,
    varint_encode_loop(N bsr 7, <<Acc/binary, Byte:8>>).

%% @doc Decode a varint from the beginning of a binary
%% Returns {Value, Rest} where Rest is the remaining binary
-spec varint_decode(binary()) -> {non_neg_integer(), binary()} | {error, incomplete}.
varint_decode(<<>>) ->
    {error, incomplete};
varint_decode(Bin) ->
    varint_decode_loop(Bin, 0, 0).

varint_decode_loop(<<>>, _Acc, _Shift) ->
    {error, incomplete};
varint_decode_loop(<<Byte:8, Rest/binary>>, Acc, Shift) ->
    Value = (Byte band 16#7F) bsl Shift,
    NewAcc = Acc bor Value,
    case Byte band 16#80 of
        0 ->
            %% No continuation bit - done
            {NewAcc, Rest};
        _ ->
            %% Continuation bit set - more bytes
            varint_decode_loop(Rest, NewAcc, Shift + 7)
    end.

%% @doc Decode multiple varints from a binary
%% Returns list of values
-spec varint_decode_list(binary(), non_neg_integer()) -> {[non_neg_integer()], binary()}.
varint_decode_list(Bin, Count) ->
    varint_decode_list_loop(Bin, Count, []).

varint_decode_list_loop(Bin, 0, Acc) ->
    {lists:reverse(Acc), Bin};
varint_decode_list_loop(Bin, Count, Acc) ->
    case varint_decode(Bin) of
        {Value, Rest} when is_integer(Value) ->
            varint_decode_list_loop(Rest, Count - 1, [Value | Acc]);
        {error, _} = Error ->
            Error
    end.

%% @doc Encode a list of integers as varints
-spec varint_encode_list([non_neg_integer()]) -> binary().
varint_encode_list(Values) ->
    iolist_to_binary([varint_encode(V) || V <- Values]).

%%====================================================================
%% Block I/O Operations
%%====================================================================

%% @doc Write a block at the specified offset (sector-aligned)
-spec write_block(bm25_file(), non_neg_integer(), binary()) -> ok | {error, term()}.
write_block(#bm25_file{postings_fd = Fd}, Offset, Data) ->
    PaddedData = pad_to_sector(Data),
    file:pwrite(Fd, Offset, PaddedData).

%% @doc Read a block from the specified offset
-spec read_block(bm25_file(), non_neg_integer()) -> {ok, binary()} | {error, term()}.
read_block(#bm25_file{postings_fd = Fd}, Offset) ->
    file:pread(Fd, Offset, ?SECTOR_SIZE).

%% @doc Read multiple blocks (batch read)
-spec read_blocks(bm25_file(), [{non_neg_integer(), non_neg_integer()}]) ->
    {ok, [binary()]} | {error, term()}.
read_blocks(#bm25_file{postings_fd = Fd}, OffsetSizePairs) ->
    case file:pread(Fd, OffsetSizePairs) of
        {ok, Datas} -> {ok, Datas};
        {error, _} = Error -> Error
    end.

%% @doc Pad binary to sector alignment
-spec pad_to_sector(binary()) -> binary().
pad_to_sector(Bin) ->
    Size = byte_size(Bin),
    PaddedSize = ((Size + ?SECTOR_SIZE - 1) div ?SECTOR_SIZE) * ?SECTOR_SIZE,
    Padding = PaddedSize - Size,
    <<Bin/binary, 0:(Padding*8)>>.

%%====================================================================
%% mmap Operations
%%====================================================================

%% @doc Open a file with mmap for zero-copy reads
-spec mmap_open(binary() | string()) -> {ok, term()} | {error, term()}.
mmap_open(Path) ->
    PathBin = to_binary(Path),
    case iommap:open(PathBin, read, []) of
        {ok, Mmap} ->
            %% Hint for random access pattern
            _ = iommap:advise(Mmap, 0, 0, random),
            {ok, Mmap};
        {error, _} = Error ->
            Error
    end.

%% @doc Close mmap handle
-spec mmap_close(term()) -> ok.
mmap_close(Mmap) ->
    _ = iommap:close(Mmap),
    ok.

%% @doc Read from mmap
-spec mmap_read(term(), non_neg_integer(), non_neg_integer()) ->
    {ok, binary()} | {error, term()}.
mmap_read(Mmap, Offset, Size) ->
    iommap:pread(Mmap, Offset, Size).

%% @doc Check if file has mmap handle
-spec has_mmap(bm25_file()) -> boolean().
has_mmap(#bm25_file{blockmax_mmap = undefined}) -> false;
has_mmap(#bm25_file{}) -> true.

%%====================================================================
%% Posting Operations
%%====================================================================

%% @doc Encode a block of postings using delta encoding + varint
%% Input: [{DocIntId, TF}, ...] sorted by DocIntId ascending
%% Output: Binary with format: [Count:varint][Delta1:varint][TF1:varint]...
-spec encode_posting_block([posting()]) -> binary().
encode_posting_block([]) ->
    varint_encode(0);
encode_posting_block(Postings) ->
    Count = length(Postings),
    %% Delta encode doc IDs
    {Deltas, TFs} = delta_encode_postings(Postings, 0, [], []),
    %% Interleave deltas and TFs
    Encoded = encode_delta_tf_pairs(Deltas, TFs, []),
    iolist_to_binary([varint_encode(Count) | Encoded]).

%% @doc Decode a posting block
%% Returns list of {DocIntId, TF}
-spec decode_posting_block(binary()) -> {ok, [posting()]} | {error, term()}.
decode_posting_block(Bin) ->
    case varint_decode(Bin) of
        {0, _Rest} ->
            {ok, []};
        {Count, Rest} when is_integer(Count) ->
            decode_delta_tf_pairs(Rest, Count, 0, []);
        {error, _} = Error ->
            Error
    end.

%% @doc Write postings for a term at specified offset
-spec write_postings(bm25_file(), non_neg_integer(), [posting()]) ->
    {ok, non_neg_integer()} | {error, term()}.
write_postings(#bm25_file{postings_fd = Fd} = _File, Offset, Postings) ->
    EncodedBlock = encode_posting_block(Postings),
    PaddedBlock = pad_to_sector(EncodedBlock),
    case file:pwrite(Fd, Offset, PaddedBlock) of
        ok -> {ok, byte_size(PaddedBlock)};
        {error, _} = Error -> Error
    end.

%% @doc Read postings from specified offset and size
-spec read_postings(bm25_file(), non_neg_integer(), non_neg_integer()) ->
    {ok, [posting()]} | {error, term()}.
read_postings(#bm25_file{postings_fd = Fd}, Offset, Size) ->
    case file:pread(Fd, Offset, Size) of
        {ok, Data} ->
            decode_posting_block(Data);
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Block-Max Index Operations
%%====================================================================

%% @doc Write block-max index to file
%% Format: [TermCount:32][Entry1][Entry2]...
%% Entry: [TermIntId:32][BlockCount:16][Block1]...[BlockN]
%% Block: [MaxImpact:32/float][DocStart:32][DocEnd:32][Offset:64][Size:32]
-spec write_blockmax_index(bm25_file(), #{non_neg_integer() => [block_max_entry()]}) ->
    {ok, bm25_file()} | {error, term()}.
write_blockmax_index(#bm25_file{blockmax_fd = Fd, header = Header} = File, TermBlocks) ->
    TermCount = maps:size(TermBlocks),
    %% Build binary
    EntriesBin = maps:fold(
        fun(TermIntId, Blocks, Acc) ->
            BlockCount = length(Blocks),
            BlocksBin = encode_block_max_entries(Blocks),
            <<Acc/binary, TermIntId:32/little, BlockCount:16/little, BlocksBin/binary>>
        end,
        <<>>,
        TermBlocks
    ),
    IndexBin = <<TermCount:32/little, EntriesBin/binary>>,
    PaddedBin = pad_to_sector(IndexBin),

    case file:pwrite(Fd, 0, PaddedBin) of
        ok ->
            NewHeader = Header#{blockmax_size => byte_size(PaddedBin)},
            {ok, File#bm25_file{header = NewHeader}};
        {error, _} = Error ->
            Error
    end.

%% @doc Read block-max index from file
%% Returns map of TermIntId => [BlockMaxEntry]
-spec read_blockmax_index(bm25_file()) -> {ok, #{non_neg_integer() => [block_max_entry()]}} | {error, term()}.
read_blockmax_index(#bm25_file{blockmax_fd = Fd, blockmax_mmap = Mmap}) ->
    %% Read header to get term count
    ReadFun = case Mmap of
        undefined -> fun(Off, Size) -> file:pread(Fd, Off, Size) end;
        _ -> fun(Off, Size) -> iommap:pread(Mmap, Off, Size) end
    end,

    case ReadFun(0, 4) of
        {ok, <<TermCount:32/little>>} ->
            %% Read rest of index
            read_blockmax_entries(ReadFun, 4, TermCount, #{});
        {ok, <<>>} ->
            %% Empty file
            {ok, #{}};
        eof ->
            %% Empty file
            {ok, #{}};
        {error, _} = Error ->
            Error
    end.

%% @doc Lookup blocks for a specific term
-spec lookup_blocks_for_term(bm25_file(), non_neg_integer()) ->
    {ok, [block_max_entry()]} | {error, not_found}.
lookup_blocks_for_term(File, TermIntId) ->
    case read_blockmax_index(File) of
        {ok, Index} ->
            case maps:get(TermIntId, Index, undefined) of
                undefined -> {error, not_found};
                Blocks -> {ok, Blocks}
            end;
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

to_binary(Path) when is_binary(Path) -> Path;
to_binary(Path) when is_list(Path) -> list_to_binary(Path).

open_files(MetaPath, PostingsPath, BlockmaxPath, Modes) ->
    case file:open(MetaPath, Modes) of
        {ok, MetaFd} ->
            case file:open(PostingsPath, Modes) of
                {ok, PostingsFd} ->
                    case file:open(BlockmaxPath, Modes) of
                        {ok, BlockmaxFd} ->
                            {ok, MetaFd, PostingsFd, BlockmaxFd};
                        {error, Reason} ->
                            _ = file:close(MetaFd),
                            _ = file:close(PostingsFd),
                            {error, {blockmax_file, Reason}}
                    end;
                {error, Reason} ->
                    _ = file:close(MetaFd),
                    {error, {postings_file, Reason}}
            end;
        {error, Reason} ->
            {error, {meta_file, Reason}}
    end.

close_if_open(undefined) -> ok;
close_if_open(Fd) -> file:close(Fd).

sync_if_open(undefined) -> ok;
sync_if_open(Fd) -> file:sync(Fd).

close_mmap_if_open(undefined) -> ok;
close_mmap_if_open(Mmap) -> iommap:close(Mmap).

try_open_mmap(Path) ->
    case iommap:open(Path, read, []) of
        {ok, Mmap} ->
            _ = iommap:advise(Mmap, 0, 0, random),
            Mmap;
        {error, _} ->
            undefined
    end.

write_header_internal(#bm25_file{meta_fd = Fd}, Header) ->
    Magic = maps:get(magic, Header, ?MAGIC),
    Version = maps:get(version, Header, ?VERSION),
    TermCount = maps:get(term_count, Header, 0),
    DocCount = maps:get(doc_count, Header, 0),
    TotalTokens = maps:get(total_tokens, Header, 0),
    AvgDL = maps:get(avgdl, Header, 0.0),
    K1 = maps:get(k1, Header, 1.2),
    B = maps:get(b, Header, 0.75),
    BlockSize = maps:get(block_size, Header, ?BLOCK_DOC_COUNT),
    PostingsOffset = maps:get(postings_offset, Header, ?SECTOR_SIZE),
    BlockmaxOffset = maps:get(blockmax_offset, Header, 0),

    HeaderBin = <<
        Magic/binary,
        Version:32/little,
        TermCount:32/little,
        DocCount:32/little,
        TotalTokens:64/little,
        AvgDL:64/float-little,
        K1:64/float-little,
        B:64/float-little,
        BlockSize:32/little,
        PostingsOffset:64/little,
        BlockmaxOffset:64/little
    >>,
    PaddedHeader = pad_to_sector(HeaderBin),
    file:pwrite(Fd, 0, PaddedHeader).

read_header_internal(Fd) ->
    case file:pread(Fd, 0, ?SECTOR_SIZE) of
        {ok, <<Magic:8/binary, Version:32/little, TermCount:32/little,
               DocCount:32/little, TotalTokens:64/little,
               AvgDL:64/float-little, K1:64/float-little, B:64/float-little,
               BlockSize:32/little, PostingsOffset:64/little,
               BlockmaxOffset:64/little, _Rest/binary>>} ->
            case Magic of
                ?MAGIC ->
                    {ok, #{
                        magic => Magic,
                        version => Version,
                        term_count => TermCount,
                        doc_count => DocCount,
                        total_tokens => TotalTokens,
                        avgdl => AvgDL,
                        k1 => K1,
                        b => B,
                        block_size => BlockSize,
                        postings_offset => PostingsOffset,
                        blockmax_offset => BlockmaxOffset
                    }};
                _ ->
                    {error, invalid_magic}
            end;
        {ok, _} ->
            {error, invalid_header};
        {error, _} = Error ->
            Error
    end.

%% Delta encode postings
delta_encode_postings([], _PrevDocId, DeltaAcc, TFAcc) ->
    {lists:reverse(DeltaAcc), lists:reverse(TFAcc)};
delta_encode_postings([{DocId, TF} | Rest], PrevDocId, DeltaAcc, TFAcc) ->
    Delta = DocId - PrevDocId,
    delta_encode_postings(Rest, DocId, [Delta | DeltaAcc], [TF | TFAcc]).

%% Encode interleaved deltas and TFs
encode_delta_tf_pairs([], [], Acc) ->
    lists:reverse(Acc);
encode_delta_tf_pairs([D | Ds], [T | Ts], Acc) ->
    encode_delta_tf_pairs(Ds, Ts, [varint_encode(T), varint_encode(D) | Acc]).

%% Decode interleaved deltas and TFs
decode_delta_tf_pairs(_Bin, 0, _PrevDocId, Acc) ->
    {ok, lists:reverse(Acc)};
decode_delta_tf_pairs(Bin, Count, PrevDocId, Acc) ->
    case varint_decode(Bin) of
        {Delta, Rest1} when is_integer(Delta) ->
            case varint_decode(Rest1) of
                {TF, Rest2} when is_integer(TF) ->
                    DocId = PrevDocId + Delta,
                    decode_delta_tf_pairs(Rest2, Count - 1, DocId, [{DocId, TF} | Acc]);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% Encode block-max entries
encode_block_max_entries(Blocks) ->
    iolist_to_binary([encode_block_max_entry(B) || B <- Blocks]).

encode_block_max_entry(#{max_impact := MaxImpact, doc_start := DocStart,
                          doc_end := DocEnd, offset := Offset, size := Size}) ->
    <<MaxImpact:32/float-little, DocStart:32/little, DocEnd:32/little,
      Offset:64/little, Size:32/little>>.

%% Read block-max entries
read_blockmax_entries(_ReadFun, _Offset, 0, Acc) ->
    {ok, Acc};
read_blockmax_entries(ReadFun, Offset, Count, Acc) ->
    %% Read term header (term_id + block_count)
    case ReadFun(Offset, 6) of
        {ok, <<TermIntId:32/little, BlockCount:16/little>>} ->
            %% Each block entry is 24 bytes
            BlocksSize = BlockCount * 24,
            case ReadFun(Offset + 6, BlocksSize) of
                {ok, BlocksData} ->
                    Blocks = decode_block_max_entries(BlocksData, BlockCount, []),
                    NewAcc = Acc#{TermIntId => Blocks},
                    read_blockmax_entries(ReadFun, Offset + 6 + BlocksSize, Count - 1, NewAcc);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

decode_block_max_entries(_Data, 0, Acc) ->
    lists:reverse(Acc);
decode_block_max_entries(<<MaxImpact:32/float-little, DocStart:32/little,
                           DocEnd:32/little, Offset:64/little, Size:32/little,
                           Rest/binary>>, Count, Acc) ->
    Entry = #{
        max_impact => MaxImpact,
        doc_start => DocStart,
        doc_end => DocEnd,
        offset => Offset,
        size => Size
    },
    decode_block_max_entries(Rest, Count - 1, [Entry | Acc]);
decode_block_max_entries(_, _, Acc) ->
    %% Incomplete data, return what we have
    lists:reverse(Acc).
