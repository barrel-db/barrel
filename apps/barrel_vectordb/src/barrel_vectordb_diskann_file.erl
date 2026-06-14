%%%-------------------------------------------------------------------
%%% @doc DiskANN sector-aligned disk I/O
%%%
%%% Handles persistent storage for DiskANN index with:
%%% - 4KB sector-aligned writes for optimal SSD performance
%%% - Separate files for graph, vectors, and metadata
%%% - Batch read operations for beam search
%%%
%%% File layout:
%%% - diskann.meta: Erlang term metadata
%%% - diskann.graph: Vamana graph (4KB aligned nodes)
%%% - diskann.vectors: Full float32 vectors (4KB aligned)
%%% - diskann.pq: PQ codebooks + codes
%%% - diskann.idmap: ID to offset mapping
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_diskann_file).

%% API
-export([
    create/2,
    open/1,
    close/1,
    write_header/2,
    read_header/1,
    read_header_from_file/1,
    write_node/3,
    read_node/2,
    read_nodes_batch/2,
    write_vector/3,
    read_vector/2,
    read_vector_mmap/2,
    write_pq_codes/3,
    read_pq_codes/2,
    sync/1,
    get_path/1,
    has_mmap/1,
    %% Integer ID functions for lazy graph loading
    write_node_int/4,
    read_node_by_int_id/3,
    read_nodes_batch_int/3
]).

-define(SECTOR_SIZE, 4096).
-define(MAGIC, <<"DISKANN\0">>).
-define(VERSION, 1).
-define(VERSION_V2, 2).  %% Version 2: Integer IDs + lazy loading

-record(diskann_file, {
    path :: binary(),
    graph_fd :: file:fd() | undefined,
    vector_fd :: file:fd() | undefined,
    vector_mmap :: term() | undefined,  %% iommap handle for zero-copy reads
    pq_fd :: file:fd() | undefined,
    header :: map()
}).

-type diskann_file() :: #diskann_file{}.
-export_type([diskann_file/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new DiskANN index file set
-spec create(binary() | string(), map()) -> {ok, diskann_file()} | {error, term()}.
create(Path, Config) ->
    PathBin = to_binary(Path),
    ok = filelib:ensure_dir(filename:join(PathBin, "dummy")),

    GraphPath = filename:join(PathBin, "diskann.graph"),
    VectorPath = filename:join(PathBin, "diskann.vectors"),
    PqPath = filename:join(PathBin, "diskann.pq"),
    MetaPath = filename:join(PathBin, "diskann.meta"),

    case open_files(GraphPath, VectorPath, PqPath, [write, read, binary, raw]) of
        {ok, GraphFd, VectorFd, PqFd} ->
            Header = #{
                magic => ?MAGIC,
                version => ?VERSION,
                dimension => maps:get(dimension, Config, 128),
                r => maps:get(r, Config, 64),
                node_count => 0,
                entry_point => undefined,
                distance_fn => maps:get(distance_fn, Config, cosine)
            },
            %% Write initial header
            ok = write_header_internal(GraphFd, Header),
            %% Write metadata file
            ok = file:write_file(MetaPath, term_to_binary(Header)),
            {ok, #diskann_file{
                path = PathBin,
                graph_fd = GraphFd,
                vector_fd = VectorFd,
                pq_fd = PqFd,
                header = Header
            }};
        {error, _} = Error ->
            Error
    end.

%% @doc Open an existing DiskANN index
-spec open(binary() | string()) -> {ok, diskann_file()} | {error, term()}.
open(Path) ->
    PathBin = to_binary(Path),

    GraphPath = filename:join(PathBin, "diskann.graph"),
    VectorPath = filename:join(PathBin, "diskann.vectors"),
    PqPath = filename:join(PathBin, "diskann.pq"),
    MetaPath = filename:join(PathBin, "diskann.meta"),

    case file:read_file(MetaPath) of
        {ok, MetaBin} ->
            Header = binary_to_term(MetaBin),
            case open_files(GraphPath, VectorPath, PqPath, [read, write, binary, raw]) of
                {ok, GraphFd, VectorFd, PqFd} ->
                    %% Try to open vector file with mmap for zero-copy reads
                    VectorMmap = try_open_mmap(VectorPath),
                    {ok, #diskann_file{
                        path = PathBin,
                        graph_fd = GraphFd,
                        vector_fd = VectorFd,
                        vector_mmap = VectorMmap,
                        pq_fd = PqFd,
                        header = Header
                    }};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Close file handles
-spec close(diskann_file()) -> ok.
close(#diskann_file{graph_fd = GraphFd, vector_fd = VectorFd, vector_mmap = VectorMmap,
                    pq_fd = PqFd, path = Path, header = Header}) ->
    %% Write final metadata
    MetaPath = filename:join(Path, "diskann.meta"),
    ok = file:write_file(MetaPath, term_to_binary(Header)),
    _ = close_mmap_if_open(VectorMmap),
    _ = close_if_open(GraphFd),
    _ = close_if_open(VectorFd),
    _ = close_if_open(PqFd),
    ok.

%% @doc Write index header
-spec write_header(diskann_file(), map()) -> {ok, diskann_file()}.
write_header(#diskann_file{graph_fd = Fd, path = Path} = File, Header) ->
    ok = write_header_internal(Fd, Header),
    %% Also update metadata file
    MetaPath = filename:join(Path, "diskann.meta"),
    ok = file:write_file(MetaPath, term_to_binary(Header)),
    {ok, File#diskann_file{header = Header}}.

%% @doc Read index header (returns cached header)
-spec read_header(diskann_file()) -> map().
read_header(#diskann_file{header = Header}) ->
    Header.

%% @doc Read header directly from graph file
%% Supports both V1 (string entry point) and V2 (integer entry point) formats
-spec read_header_from_file(diskann_file()) -> {ok, map()} | {error, term()}.
read_header_from_file(#diskann_file{graph_fd = Fd}) ->
    case file:pread(Fd, 0, ?SECTOR_SIZE) of
        {ok, Data} ->
            parse_header(Data);
        {error, _} = Error ->
            Error
    end.

%% Parse header binary data
parse_header(<<Magic:8/binary, Version:32/little, NodeCount:32/little,
               Dimension:16/little, R:16/little, DistFn:8, Rest/binary>>) ->
    case Magic of
        ?MAGIC ->
            case Version of
                ?VERSION ->
                    %% V1 format: variable length string entry point
                    EntryPoint = case Rest of
                        <<0:16, _/binary>> -> undefined;
                        <<Len:16/little, EP:Len/binary, _/binary>> -> EP
                    end,
                    {ok, #{
                        magic => Magic,
                        version => Version,
                        node_count => NodeCount,
                        dimension => Dimension,
                        r => R,
                        distance_fn => int_to_distance_fn(DistFn),
                        entry_point => EntryPoint
                    }};
                ?VERSION_V2 ->
                    %% V2 format: 64-bit integer entry point + next_int_id
                    <<EntryPointInt:64/little, NextIntId:64/little, StrRest/binary>> = Rest,
                    EntryPoint = case StrRest of
                        <<0:16, _/binary>> -> undefined;
                        <<Len:16/little, EP:Len/binary, _/binary>> -> EP
                    end,
                    {ok, #{
                        magic => Magic,
                        version => Version,
                        node_count => NodeCount,
                        dimension => Dimension,
                        r => R,
                        distance_fn => int_to_distance_fn(DistFn),
                        entry_point => EntryPoint,
                        entry_point_int => EntryPointInt,
                        next_int_id => NextIntId
                    }};
                _ ->
                    {error, {unsupported_version, Version}}
            end;
        _ ->
            {error, invalid_magic}
    end;
parse_header(_) ->
    {error, invalid_header}.

int_to_distance_fn(0) -> cosine;
int_to_distance_fn(1) -> euclidean;
int_to_distance_fn(_) -> cosine.

%% @doc Write a node to the graph file (sector-aligned)
%% Returns {ok, Offset} where Offset is the byte offset
-spec write_node(diskann_file(), binary(), map()) -> {ok, non_neg_integer(), diskann_file()} | {error, term()}.
write_node(#diskann_file{graph_fd = Fd, header = Header} = File, Id, Node) ->
    NodeCount = maps:get(node_count, Header, 0),
    %% Calculate offset (skip header sector)
    Offset = ?SECTOR_SIZE + (NodeCount * ?SECTOR_SIZE),

    NodeBin = encode_node(Id, Node, maps:get(r, Header, 64)),
    PaddedBin = pad_to_sector(NodeBin),

    case file:pwrite(Fd, Offset, PaddedBin) of
        ok ->
            NewHeader = Header#{node_count => NodeCount + 1},
            {ok, Offset, File#diskann_file{header = NewHeader}};
        {error, _} = Error ->
            Error
    end.

%% @doc Read a node from the graph file by offset
-spec read_node(diskann_file(), non_neg_integer()) -> {ok, {binary(), map()}} | {error, term()}.
read_node(#diskann_file{graph_fd = Fd, header = Header}, Offset) ->
    case file:pread(Fd, Offset, ?SECTOR_SIZE) of
        {ok, Data} ->
            decode_node(Data, maps:get(r, Header, 64));
        eof ->
            {error, eof};
        {error, _} = Error ->
            Error
    end.

%% @doc Batch read multiple nodes (for beam search)
-spec read_nodes_batch(diskann_file(), [non_neg_integer()]) -> [{ok, {binary(), map()}} | {error, term()}].
read_nodes_batch(#diskann_file{graph_fd = Fd, header = Header}, Offsets) ->
    R = maps:get(r, Header, 64),
    %% Parallel read using pread with multiple positions
    Results = lists:map(
        fun(Offset) ->
            case file:pread(Fd, Offset, ?SECTOR_SIZE) of
                {ok, Data} -> decode_node(Data, R);
                eof -> {error, eof};
                {error, _} = Error -> Error
            end
        end,
        Offsets
    ),
    Results.

%% @doc Write a vector to the vector file
-spec write_vector(diskann_file(), non_neg_integer(), [float()]) -> ok | {error, term()}.
write_vector(#diskann_file{vector_fd = Fd}, Index, Vector) ->
    VectorBin = << <<F:32/float-little>> || F <- Vector >>,
    Dim = length(Vector),
    VectorSize = Dim * 4,
    %% Pad to sector alignment
    PaddedSize = ((VectorSize + ?SECTOR_SIZE - 1) div ?SECTOR_SIZE) * ?SECTOR_SIZE,
    Padding = PaddedSize - VectorSize,
    PaddedBin = <<VectorBin/binary, 0:(Padding*8)>>,
    Offset = Index * PaddedSize,
    file:pwrite(Fd, Offset, PaddedBin).

%% @doc Read a vector from the vector file
-spec read_vector(diskann_file(), non_neg_integer()) -> {ok, [float()]} | {error, term()}.
read_vector(#diskann_file{vector_fd = Fd, header = Header}, Index) ->
    Dim = maps:get(dimension, Header, 128),
    VectorSize = Dim * 4,
    PaddedSize = ((VectorSize + ?SECTOR_SIZE - 1) div ?SECTOR_SIZE) * ?SECTOR_SIZE,
    Offset = Index * PaddedSize,
    case file:pread(Fd, Offset, VectorSize) of
        {ok, Data} ->
            Vector = [F || <<F:32/float-little>> <= Data],
            {ok, Vector};
        eof ->
            {error, eof};
        {error, _} = Error ->
            Error
    end.

%% @doc Read a vector using mmap (zero-copy)
%% Falls back to regular pread if mmap is not available
-spec read_vector_mmap(diskann_file(), non_neg_integer()) -> {ok, [float()]} | {error, term()}.
read_vector_mmap(#diskann_file{vector_mmap = undefined} = File, Index) ->
    %% Fallback to regular pread
    read_vector(File, Index);
read_vector_mmap(#diskann_file{vector_mmap = Mmap, header = Header}, Index) ->
    Dim = maps:get(dimension, Header, 128),
    VectorSize = Dim * 4,
    PaddedSize = ((VectorSize + ?SECTOR_SIZE - 1) div ?SECTOR_SIZE) * ?SECTOR_SIZE,
    Offset = Index * PaddedSize,
    case iommap:pread(Mmap, Offset, VectorSize) of
        {ok, Data} ->
            Vector = [F || <<F:32/float-little>> <= Data],
            {ok, Vector};
        {error, _} = Error ->
            Error
    end.

%% @doc Write PQ codes for a batch of vectors
-spec write_pq_codes(diskann_file(), non_neg_integer(), [binary()]) -> ok | {error, term()}.
write_pq_codes(#diskann_file{pq_fd = Fd}, StartIndex, Codes) ->
    M = byte_size(hd(Codes)),
    Offset = StartIndex * M,
    Data = iolist_to_binary(Codes),
    file:pwrite(Fd, Offset, Data).

%% @doc Read PQ codes for a range of vectors
-spec read_pq_codes(diskann_file(), {non_neg_integer(), non_neg_integer()}) ->
    {ok, [binary()]} | {error, term()}.
read_pq_codes(#diskann_file{pq_fd = Fd}, {StartIndex, Count}) ->
    %% Assume M=8 by default, could be stored in header
    M = 8,
    Offset = StartIndex * M,
    Size = Count * M,
    case file:pread(Fd, Offset, Size) of
        {ok, Data} ->
            Codes = [Code || <<Code:M/binary>> <= Data],
            {ok, Codes};
        eof ->
            {error, eof};
        {error, _} = Error ->
            Error
    end.

%% @doc Sync all file handles to disk
-spec sync(diskann_file()) -> ok.
sync(#diskann_file{graph_fd = GraphFd, vector_fd = VectorFd, pq_fd = PqFd}) ->
    _ = sync_if_open(GraphFd),
    _ = sync_if_open(VectorFd),
    _ = sync_if_open(PqFd),
    ok.

%% @doc Get the base path
-spec get_path(diskann_file()) -> binary().
get_path(#diskann_file{path = Path}) -> Path.

%% @doc Check if mmap is available for zero-copy reads
-spec has_mmap(diskann_file()) -> boolean().
has_mmap(#diskann_file{vector_mmap = undefined}) -> false;
has_mmap(#diskann_file{}) -> true.

%%====================================================================
%% Internal Functions
%%====================================================================

to_binary(Path) when is_binary(Path) -> Path;
to_binary(Path) when is_list(Path) -> list_to_binary(Path).

open_files(GraphPath, VectorPath, PqPath, Modes) ->
    case file:open(GraphPath, Modes) of
        {ok, GraphFd} ->
            case file:open(VectorPath, Modes) of
                {ok, VectorFd} ->
                    case file:open(PqPath, Modes) of
                        {ok, PqFd} ->
                            {ok, GraphFd, VectorFd, PqFd};
                        {error, Reason} ->
                            _ = file:close(GraphFd),
                            _ = file:close(VectorFd),
                            {error, {pq_file, Reason}}
                    end;
                {error, Reason} ->
                    _ = file:close(GraphFd),
                    {error, {vector_file, Reason}}
            end;
        {error, Reason} ->
            {error, {graph_file, Reason}}
    end.

close_if_open(undefined) -> ok;
close_if_open(Fd) -> file:close(Fd).

sync_if_open(undefined) -> ok;
sync_if_open(Fd) -> file:sync(Fd).

close_mmap_if_open(undefined) -> ok;
close_mmap_if_open(Mmap) -> iommap:close(Mmap).

%% Try to open vector file with mmap, return undefined on failure
try_open_mmap(VectorPath) ->
    case iommap:open(VectorPath, read, []) of
        {ok, Mmap} ->
            %% Hint for random access pattern
            _ = iommap:advise(Mmap, 0, 0, random),
            Mmap;
        {error, _} ->
            undefined
    end.

write_header_internal(Fd, Header) ->
    %% Determine version based on whether we have entry_point_int
    Version = case maps:get(entry_point_int, Header, undefined) of
        undefined -> ?VERSION;
        _ -> ?VERSION_V2
    end,

    %% Header takes first sector
    Magic = ?MAGIC,
    NodeCount = maps:get(node_count, Header, 0),
    Dimension = maps:get(dimension, Header, 128),
    R = maps:get(r, Header, 64),
    DistFn = distance_fn_to_int(maps:get(distance_fn, Header, cosine)),

    case Version of
        ?VERSION ->
            %% V1: Entry point ID (variable length binary)
            EntryPointBin = case maps:get(entry_point, Header, undefined) of
                undefined -> <<0:16>>;
                EP when is_binary(EP) -> <<(byte_size(EP)):16, EP/binary>>
            end,
            HeaderData = <<
                Magic/binary,
                Version:32/little,
                NodeCount:32/little,
                Dimension:16/little,
                R:16/little,
                DistFn:8,
                EntryPointBin/binary
            >>,
            PaddedHeader = pad_to_sector(HeaderData),
            file:pwrite(Fd, 0, PaddedHeader);

        ?VERSION_V2 ->
            %% V2: Entry point as 64-bit integer ID + next_int_id
            EntryPointInt = maps:get(entry_point_int, Header, 0),
            NextIntId = maps:get(next_int_id, Header, NodeCount),
            %% Also store string entry point for compatibility
            EntryPointStrBin = case maps:get(entry_point, Header, undefined) of
                undefined -> <<0:16>>;
                EP when is_binary(EP) -> <<(byte_size(EP)):16, EP/binary>>
            end,
            HeaderData = <<
                Magic/binary,
                ?VERSION_V2:32/little,
                NodeCount:32/little,
                Dimension:16/little,
                R:16/little,
                DistFn:8,
                EntryPointInt:64/little,
                NextIntId:64/little,
                EntryPointStrBin/binary
            >>,
            PaddedHeader = pad_to_sector(HeaderData),
            file:pwrite(Fd, 0, PaddedHeader)
    end.

pad_to_sector(Bin) ->
    Size = byte_size(Bin),
    PaddedSize = ((Size + ?SECTOR_SIZE - 1) div ?SECTOR_SIZE) * ?SECTOR_SIZE,
    Padding = PaddedSize - Size,
    <<Bin/binary, 0:(Padding*8)>>.

encode_node(Id, Node, MaxR) ->
    Neighbors = maps:get(neighbors, Node, []),
    NeighborCount = min(length(Neighbors), MaxR),

    IdLen = byte_size(Id),

    %% Encode neighbors as offsets (8 bytes each for 64-bit)
    NeighborBin = << <<(neighbor_to_offset(N)):64/little>> ||
                     N <- lists:sublist(Neighbors, MaxR) >>,
    %% Pad to MaxR neighbors
    PaddingCount = MaxR - NeighborCount,
    NeighborPadding = << <<0:64>> || _ <- lists:seq(1, PaddingCount) >>,

    <<IdLen:16/little, Id/binary, NeighborCount:16/little,
      NeighborBin/binary, NeighborPadding/binary>>.

neighbor_to_offset(N) when is_integer(N) -> N;
neighbor_to_offset({_Id, Offset}) -> Offset.

decode_node(Data, MaxR) ->
    try
        <<IdLen:16/little, Rest1/binary>> = Data,
        <<Id:IdLen/binary, NeighborCount:16/little, Rest2/binary>> = Rest1,

        %% Read neighbor offsets
        NeighborBytes = MaxR * 8,
        <<NeighborData:NeighborBytes/binary, _/binary>> = Rest2,

        %% Extract non-zero offsets, take only NeighborCount
        AllOffsets = [Offset || <<Offset:64/little>> <= NeighborData, Offset > 0],
        ValidNeighbors = lists:sublist(AllOffsets, NeighborCount),

        {ok, {Id, #{neighbors => ValidNeighbors}}}
    catch
        _:_ -> {error, invalid_node_format}
    end.

distance_fn_to_int(cosine) -> 0;
distance_fn_to_int(euclidean) -> 1.

%%====================================================================
%% Integer ID Node Functions (for lazy graph loading)
%%====================================================================

%% @doc Write a node to the graph file using integer ID
%% Node format: [IntId:64/little][NeighborCount:16/little][Neighbor1:64/little]...[NeighborR:64/little][padding]
%% Each node is stored at offset: SECTOR_SIZE + (IntId * SECTOR_SIZE)
-spec write_node_int(diskann_file(), non_neg_integer(), [non_neg_integer()], pos_integer()) ->
    ok | {error, term()}.
write_node_int(#diskann_file{graph_fd = Fd}, IntId, Neighbors, MaxR) ->
    %% Offset: header (1 sector) + IntId * sector_size
    Offset = ?SECTOR_SIZE + (IntId * ?SECTOR_SIZE),
    NodeBin = encode_node_int(IntId, Neighbors, MaxR),
    PaddedBin = pad_to_sector(NodeBin),
    file:pwrite(Fd, Offset, PaddedBin).

%% @doc Read a node from the graph file by integer ID
%% Returns {ok, {IntId, [NeighborIntIds]}} or {error, term()}
-spec read_node_by_int_id(diskann_file(), non_neg_integer(), pos_integer()) ->
    {ok, {non_neg_integer(), [non_neg_integer()]}} | {error, term()}.
read_node_by_int_id(#diskann_file{graph_fd = Fd}, IntId, MaxR) ->
    %% Offset: header (1 sector) + IntId * sector_size
    Offset = ?SECTOR_SIZE + (IntId * ?SECTOR_SIZE),
    case file:pread(Fd, Offset, ?SECTOR_SIZE) of
        {ok, Data} ->
            decode_node_int(Data, MaxR);
        eof ->
            {error, eof};
        {error, _} = Error ->
            Error
    end.

%% @doc Batch read multiple nodes by integer IDs
%% Uses batch pread/2 for efficiency (single system call)
-spec read_nodes_batch_int(diskann_file(), [non_neg_integer()], pos_integer()) ->
    [{ok, {non_neg_integer(), [non_neg_integer()]}} | {error, term()}].
read_nodes_batch_int(#diskann_file{graph_fd = _Fd}, [], _MaxR) ->
    [];
read_nodes_batch_int(#diskann_file{graph_fd = Fd}, IntIds, MaxR) ->
    %% Build list of {Offset, Size} for batch read
    OffsetSizePairs = [{?SECTOR_SIZE + (IntId * ?SECTOR_SIZE), ?SECTOR_SIZE}
                       || IntId <- IntIds],
    case file:pread(Fd, OffsetSizePairs) of
        {ok, Datas} ->
            [decode_node_int(Data, MaxR) || Data <- Datas];
        {error, _} = Error ->
            %% Return error for all requested nodes
            [Error || _ <- IntIds]
    end.

%% Encode node with integer ID
%% Format: [IntId:64/little][NeighborCount:16/little][Neighbors:64/little each][padding]
encode_node_int(IntId, Neighbors, MaxR) ->
    NeighborCount = min(length(Neighbors), MaxR),
    %% Encode neighbors as 64-bit integers
    NeighborBin = << <<N:64/little>> || N <- lists:sublist(Neighbors, MaxR) >>,
    %% Pad to MaxR neighbors
    PaddingCount = MaxR - NeighborCount,
    NeighborPadding = << <<0:64>> || _ <- lists:seq(1, PaddingCount) >>,
    <<IntId:64/little, NeighborCount:16/little, NeighborBin/binary, NeighborPadding/binary>>.

%% Decode node with integer ID
%% Returns {ok, {IntId, [NeighborIntIds]}}
decode_node_int(Data, MaxR) ->
    try
        <<IntId:64/little, NeighborCount:16/little, Rest/binary>> = Data,
        %% Read neighbor integer IDs
        NeighborBytes = MaxR * 8,
        <<NeighborData:NeighborBytes/binary, _/binary>> = Rest,
        %% Extract non-zero neighbors (0 indicates empty slot)
        AllNeighbors = [N || <<N:64/little>> <= NeighborData],
        %% Filter out padding zeros and take NeighborCount
        ValidNeighbors = lists:sublist([N || N <- AllNeighbors, N > 0], NeighborCount),
        {ok, {IntId, ValidNeighbors}}
    catch
        _:_ -> {error, invalid_node_format}
    end.
