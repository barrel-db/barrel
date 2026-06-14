%%%-------------------------------------------------------------------
%%% @doc BlobDB storage backend for attachments
%%%
%%% Uses a separate RocksDB instance with BlobDB enabled for storing
%%% attachment binary data. This avoids compaction issues from mixing
%%% small documents with large blobs.
%%%
%%% Large attachments (>= chunk_threshold) are stored as chunks for
%%% streaming support. Small attachments are stored as single values.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_store).

-include("barrel_docdb.hrl").

%% Chunk configuration - can be overridden in att_ref options
-define(DEFAULT_CHUNK_THRESHOLD, 65536).  %% 64 KB
-define(DEFAULT_CHUNK_SIZE, 65536).       %% 64 KB

%% API
-export([open/2, close/1]).
-export([put/5, put/6, get/4, delete/4]).
-export([delete_all/3]).
-export([fold/5]).

%% Streaming API
-export([put_stream/5, put_stream/6]).
-export([write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, read_chunk/1, close_stream/1]).
-export([get_info/4]).

%%====================================================================
%% Types
%%====================================================================

-type att_ref() :: #{
    ref := rocksdb:db_handle(),
    path := string(),
    chunk_threshold => pos_integer(),
    chunk_size => pos_integer()
}.

-type att_stream() :: #{
    att_ref := att_ref(),
    db_name := binary(),
    doc_id := binary(),
    att_name := binary(),
    info := att_info(),
    chunk_index := non_neg_integer(),
    chunk_count := non_neg_integer()
}.

%% Note: att_info/0 is defined in barrel_docdb.hrl
%% Chunked attachments add optional fields: chunked, chunk_size, chunk_count

-export_type([att_ref/0, att_stream/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Open an attachment store with BlobDB enabled
-spec open(string(), map()) -> {ok, att_ref()} | {error, term()}.
open(Path, Options) ->
    ok = filelib:ensure_dir(Path ++ "/"),
    DbOpts = build_blob_options(Options),
    case rocksdb:open(Path, DbOpts) of
        {ok, Ref} ->
            ChunkThreshold = maps:get(chunk_threshold, Options, ?DEFAULT_CHUNK_THRESHOLD),
            ChunkSize = maps:get(chunk_size, Options, ?DEFAULT_CHUNK_SIZE),
            {ok, #{
                ref => Ref,
                path => Path,
                chunk_threshold => ChunkThreshold,
                chunk_size => ChunkSize
            }};
        {error, Reason} ->
            {error, {att_store_open_failed, Reason}}
    end.

%% @doc Close the attachment store
-spec close(att_ref()) -> ok.
close(#{ref := Ref}) ->
    rocksdb:close(Ref).

%% @doc Store an attachment (async by default)
%% Small attachments are stored as single values.
%% Large attachments (>= chunk_threshold) are stored as chunks.
-spec put(att_ref(), db_name(), docid(), binary(), binary()) ->
    {ok, att_info()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data) ->
    put(AttRef, DbName, DocId, AttName, Data, #{}).

%% @doc Store an attachment with options
%% Options:
%%   - sync: boolean() - if true, sync to disk before returning (default: false)
-spec put(att_ref(), db_name(), docid(), binary(), binary(), map()) ->
    {ok, att_info()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data, Opts) when is_binary(Data) ->
    #{chunk_threshold := Threshold, chunk_size := ChunkSize} = AttRef,
    DataSize = byte_size(Data),
    Digest = compute_digest(Data),
    ContentType = mimerl:filename(AttName),
    Sync = maps:get(sync, Opts, false),

    case DataSize >= Threshold of
        true ->
            %% Large attachment - store as chunks
            put_chunked(AttRef, DbName, DocId, AttName, Data, ContentType, Digest, ChunkSize, Sync);
        false ->
            %% Small attachment - store as single value
            put_single(AttRef, DbName, DocId, AttName, Data, ContentType, Digest, Sync)
    end.

%% @private Store as single value (small attachment)
put_single(#{ref := Ref}, DbName, DocId, AttName, Data, ContentType, Digest, Sync) ->
    Key = make_key(DbName, DocId, AttName),
    case rocksdb:put(Ref, Key, Data, [{sync, Sync}]) of
        ok ->
            AttInfo = #{
                name => AttName,
                content_type => ContentType,
                length => byte_size(Data),
                digest => Digest,
                chunked => false
            },
            {ok, AttInfo};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Store as chunks (large attachment)
put_chunked(#{ref := Ref}, DbName, DocId, AttName, Data, ContentType, Digest, ChunkSize, Sync) ->
    DataSize = byte_size(Data),
    ChunkCount = (DataSize + ChunkSize - 1) div ChunkSize,

    %% Store metadata first
    MetaKey = make_key(DbName, DocId, AttName),
    MetaValue = encode_chunk_meta(#{
        chunk_size => ChunkSize,
        chunk_count => ChunkCount,
        length => DataSize,
        content_type => ContentType,
        digest => Digest
    }),

    case rocksdb:put(Ref, MetaKey, MetaValue, [{sync, false}]) of
        ok ->
            %% Store chunks
            case put_chunks(Ref, DbName, DocId, AttName, Data, ChunkSize, 0, Sync) of
                ok ->
                    AttInfo = #{
                        name => AttName,
                        content_type => ContentType,
                        length => DataSize,
                        digest => Digest,
                        chunked => true,
                        chunk_size => ChunkSize,
                        chunk_count => ChunkCount
                    },
                    {ok, AttInfo};
                {error, _} = Error ->
                    %% Cleanup on error
                    delete_chunked(Ref, DbName, DocId, AttName, ChunkCount),
                    Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Store individual chunks
put_chunks(_Ref, _DbName, _DocId, _AttName, <<>>, _ChunkSize, _Index, _Sync) ->
    ok;
put_chunks(Ref, DbName, DocId, AttName, Data, ChunkSize, Index, Sync) ->
    {Chunk, Rest} = case byte_size(Data) > ChunkSize of
        true -> split_binary(Data, ChunkSize);
        false -> {Data, <<>>}
    end,
    ChunkKey = make_chunk_key(DbName, DocId, AttName, Index),
    %% Only sync on last chunk if sync requested
    DoSync = Sync andalso Rest =:= <<>>,
    case rocksdb:put(Ref, ChunkKey, Chunk, [{sync, DoSync}]) of
        ok ->
            put_chunks(Ref, DbName, DocId, AttName, Rest, ChunkSize, Index + 1, Sync);
        {error, _} = Error ->
            Error
    end.

%% @doc Retrieve an attachment
%% Automatically handles both single-value and chunked attachments.
-spec get(att_ref(), db_name(), docid(), binary()) ->
    {ok, binary()} | not_found | {error, term()}.
get(#{ref := Ref} = AttRef, DbName, DocId, AttName) ->
    Key = make_key(DbName, DocId, AttName),
    case rocksdb:get(Ref, Key, []) of
        {ok, Value} ->
            %% Check if this is metadata (chunked) or raw data
            case is_chunked_metadata(Value) of
                {true, Meta} ->
                    %% Chunked - read and assemble chunks
                    get_chunked(AttRef, DbName, DocId, AttName, Meta);
                false ->
                    %% Single value
                    {ok, Value}
            end;
        not_found ->
            not_found;
        {error, _} = Error ->
            Error
    end.

%% Chunked-metadata wire format.
%%
%% Tagged prefix avoids decoding user-controlled attachment bytes as an
%% Erlang term (binary_to_term used to crash and could allocate
%% arbitrary terms / create atoms). New format:
%%
%%   <<"BARREL_CHUNK_V1:", JsonMeta/binary>>
%%
%% where JsonMeta is `json:encode/1` of the metadata map. Old chunked
%% attachments (written by 0.6.3 and earlier as `term_to_binary/1`)
%% are still decoded for one release via `binary_to_term/2` with the
%% `[safe]` flag; the fallback path will be removed in 0.7.0.
-define(CHUNK_META_TAG, <<"BARREL_CHUNK_V1:">>).
-define(CHUNK_META_TAG_SIZE, 16).

encode_chunk_meta(Meta) when is_map(Meta) ->
    %% json keys must be binaries; convert atom keys for the wire.
    JsonMap = #{
        <<"chunked">> => true,
        <<"chunk_size">> => maps:get(chunk_size, Meta),
        <<"chunk_count">> => maps:get(chunk_count, Meta),
        <<"length">> => maps:get(length, Meta),
        <<"content_type">> => maps:get(content_type, Meta),
        <<"digest">> => maps:get(digest, Meta)
    },
    <<?CHUNK_META_TAG/binary, (iolist_to_binary(json:encode(JsonMap)))/binary>>.

%% @private Check if value is chunked metadata. Recognizes the tagged
%% v1 format and (transitionally) the legacy term_to_binary blob.
is_chunked_metadata(Value) when is_binary(Value),
                                byte_size(Value) > ?CHUNK_META_TAG_SIZE ->
    case Value of
        <<Tag:?CHUNK_META_TAG_SIZE/binary, Json/binary>>
          when Tag =:= ?CHUNK_META_TAG ->
            decode_chunk_meta(Json);
        _ ->
            legacy_is_chunked_metadata(Value)
    end;
is_chunked_metadata(Value) when is_binary(Value) ->
    legacy_is_chunked_metadata(Value).

decode_chunk_meta(Json) ->
    try json:decode(Json) of
        #{<<"chunked">> := true} = Decoded -> {true, normalize_meta(Decoded)};
        _ -> false
    catch
        _:_ -> false
    end.

%% @private Legacy term_to_binary path. Restricted with `[safe]' so
%% only existing atoms are accepted. Will be removed in 0.7.0.
legacy_is_chunked_metadata(Value) ->
    try binary_to_term(Value, [safe]) of
        #{chunked := true} = Meta ->
            logger:warning(
              "Reading legacy term_to_binary chunked attachment metadata; "
              "this format is deprecated and will be unsupported in 0.7.0"),
            {true, Meta};
        _ ->
            false
    catch
        _:_ -> false
    end.

%% Convert JSON-decoded string keys back to the atom-keyed shape the
%% rest of this module pattern-matches on.
normalize_meta(#{<<"chunked">> := Chunked} = M) ->
    #{
        chunked => Chunked,
        chunk_size => maps:get(<<"chunk_size">>, M),
        chunk_count => maps:get(<<"chunk_count">>, M),
        length => maps:get(<<"length">>, M),
        content_type => maps:get(<<"content_type">>, M),
        digest => maps:get(<<"digest">>, M)
    }.

%% @private Read and assemble chunked attachment
get_chunked(AttRef, DbName, DocId, AttName, #{chunk_count := ChunkCount}) ->
    Chunks = get_chunks(AttRef, DbName, DocId, AttName, 0, ChunkCount, []),
    case Chunks of
        {ok, ChunkList} ->
            {ok, iolist_to_binary(ChunkList)};
        {error, _} = Error ->
            Error
    end.

%% @private Read all chunks
get_chunks(_AttRef, _DbName, _DocId, _AttName, Index, ChunkCount, Acc) when Index >= ChunkCount ->
    {ok, lists:reverse(Acc)};
get_chunks(#{ref := Ref} = AttRef, DbName, DocId, AttName, Index, ChunkCount, Acc) ->
    ChunkKey = make_chunk_key(DbName, DocId, AttName, Index),
    case rocksdb:get(Ref, ChunkKey, []) of
        {ok, Chunk} ->
            get_chunks(AttRef, DbName, DocId, AttName, Index + 1, ChunkCount, [Chunk | Acc]);
        not_found ->
            {error, {missing_chunk, Index}};
        {error, _} = Error ->
            Error
    end.

%% @doc Delete an attachment
%% Handles both single-value and chunked attachments.
-spec delete(att_ref(), db_name(), docid(), binary()) -> ok | {error, term()}.
delete(#{ref := Ref}, DbName, DocId, AttName) ->
    Key = make_key(DbName, DocId, AttName),
    %% First check if chunked
    case rocksdb:get(Ref, Key, []) of
        {ok, Value} ->
            case is_chunked_metadata(Value) of
                {true, #{chunk_count := ChunkCount}} ->
                    %% Delete chunks first, then metadata
                    delete_chunked(Ref, DbName, DocId, AttName, ChunkCount),
                    rocksdb:delete(Ref, Key, []);
                false ->
                    %% Single value
                    rocksdb:delete(Ref, Key, [])
            end;
        not_found ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% @private Delete chunks
delete_chunked(Ref, DbName, DocId, AttName, ChunkCount) ->
    lists:foreach(
        fun(Index) ->
            ChunkKey = make_chunk_key(DbName, DocId, AttName, Index),
            rocksdb:delete(Ref, ChunkKey, [])
        end,
        lists:seq(0, ChunkCount - 1)
    ).

%% @doc Delete all attachments for a document
-spec delete_all(att_ref(), db_name(), docid()) -> ok | {error, term()}.
delete_all(AttRef, DbName, DocId) ->
    Keys = fold(AttRef, DbName, DocId,
        fun(Name, _Data, Acc) -> {ok, [Name | Acc]} end,
        []),
    lists:foreach(
        fun(AttName) ->
            delete(AttRef, DbName, DocId, AttName)
        end,
        Keys
    ),
    ok.

%% @doc Fold over all attachments for a document
-spec fold(att_ref(), db_name(), docid(), fun(), term()) -> term().
fold(#{ref := Ref}, DbName, DocId, Fun, Acc) ->
    Prefix = make_prefix(DbName, DocId),
    PrefixEnd = prefix_end(Prefix),
    ReadOpts = [
        {iterate_lower_bound, Prefix},
        {iterate_upper_bound, PrefixEnd}
    ],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop(rocksdb:iterator_move(Itr, first), Itr, Prefix, Fun, Acc)
    after
        rocksdb:iterator_close(Itr)
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Build RocksDB options with BlobDB enabled
build_blob_options(Options) ->
    BlobFileSize = maps:get(blob_file_size, Options, 256 * 1024 * 1024),  % 256MB
    Schedulers = erlang:system_info(schedulers),

    %% Block-based table options with shared cache (for metadata lookups)
    BlockOpts = barrel_cache:get_block_opts(#{
        bloom_bits => maps:get(bloom_bits, Options, 10),
        block_size => maps:get(block_size, Options, 4096)
    }),

    [
        {create_if_missing, true},
        {max_open_files, maps:get(max_open_files, Options, 256)},
        {compression, maps:get(compression, Options, snappy)},

        %% BlobDB settings for large attachments
        {enable_blob_files, true},
        {min_blob_size, maps:get(min_blob_size, Options, 4096)},  % 4KB threshold
        {blob_file_size, BlobFileSize},
        {blob_compression_type, maps:get(blob_compression, Options, snappy)},

        %% Blob garbage collection
        {enable_blob_garbage_collection, true},
        {blob_garbage_collection_age_cutoff,
            maps:get(blob_gc_age_cutoff, Options, 0.25)},
        {blob_garbage_collection_force_threshold,
            maps:get(blob_gc_force_threshold, Options, 0.5)},

        %% Background jobs for compaction/GC
        {max_background_jobs, maps:get(max_background_jobs, Options, erlang:max(2, Schedulers div 2))},

        %% Block-based table with bloom filters and shared cache
        {block_based_table_options, BlockOpts}
    ].

%% Create key for attachment: prefix + att_name
make_key(DbName, DocId, AttName) ->
    Prefix = make_prefix(DbName, DocId),
    <<Prefix/binary, AttName/binary>>.

%% Create key for a chunk: prefix + att_name + NUL + chunk_index
make_chunk_key(DbName, DocId, AttName, ChunkIndex) ->
    BaseKey = make_key(DbName, DocId, AttName),
    <<BaseKey/binary, 0, ChunkIndex:32>>.

%% Create prefix for all attachments of a document
make_prefix(DbName, DocId) ->
    DbNameLen = byte_size(DbName),
    DocIdLen = byte_size(DocId),
    <<DbNameLen:16, DbName/binary, DocIdLen:16, DocId/binary, $:>>.

%% Compute the end of a prefix range
prefix_end(Prefix) ->
    Len = byte_size(Prefix),
    LastByte = binary:last(Prefix),
    if
        LastByte < 16#FF ->
            Init = binary:part(Prefix, 0, Len - 1),
            <<Init/binary, (LastByte + 1)>>;
        true ->
            <<Prefix/binary, 16#FF>>
    end.

%% Extract attachment name from key
extract_att_name(Key, Prefix) ->
    PrefixLen = byte_size(Prefix),
    <<_:PrefixLen/binary, AttName/binary>> = Key,
    AttName.

%% Compute SHA-256 digest of data
compute_digest(Data) ->
    Digest = crypto:hash(sha256, Data),
    <<"sha256-", (to_hex(Digest))/binary>>.

%% Convert binary to hex string
to_hex(Bin) ->
    << <<(hex_char(N))>> || <<N:4>> <= Bin >>.

hex_char(N) when N < 10 -> $0 + N;
hex_char(N) -> $a + N - 10.

%% Iterator fold loop
fold_loop({ok, Key, Value}, Itr, Prefix, Fun, Acc) ->
    AttName = extract_att_name(Key, Prefix),
    case Fun(AttName, Value, Acc) of
        {ok, Acc1} ->
            fold_loop(rocksdb:iterator_move(Itr, next), Itr, Prefix, Fun, Acc1);
        {stop, Acc1} ->
            Acc1;
        stop ->
            Acc
    end;
fold_loop({error, invalid_iterator}, _Itr, _Prefix, _Fun, Acc) ->
    Acc;
fold_loop({error, _Reason}, _Itr, _Prefix, _Fun, Acc) ->
    Acc.

%%====================================================================
%% Streaming API
%%====================================================================

%% @doc Get attachment info/metadata without reading the data
-spec get_info(att_ref(), db_name(), docid(), binary()) ->
    {ok, att_info()} | {error, term()}.
get_info(#{ref := Ref}, DbName, DocId, AttName) ->
    Key = make_key(DbName, DocId, AttName),
    case rocksdb:get(Ref, Key, []) of
        {ok, Value} ->
            case is_chunked_metadata(Value) of
                {true, #{content_type := ContentType, length := Length,
                         digest := Digest, chunk_size := ChunkSize,
                         chunk_count := ChunkCount}} ->
                    {ok, #{
                        name => AttName,
                        content_type => ContentType,
                        length => Length,
                        digest => Digest,
                        chunked => true,
                        chunk_size => ChunkSize,
                        chunk_count => ChunkCount
                    }};
                false ->
                    %% Single value - need to compute info from data
                    {ok, #{
                        name => AttName,
                        content_type => mimerl:filename(AttName),
                        length => byte_size(Value),
                        digest => compute_digest(Value),
                        chunked => false
                    }}
            end;
        not_found ->
          {error, not_found};
        {error, _} = Error ->
            Error
    end.

%% @doc Open a stream for writing an attachment
%% Returns a stream handle that can be used with write_chunk/2 and finish_stream/1
-spec put_stream(att_ref(), db_name(), docid(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType) ->
    put_stream(AttRef, DbName, DocId, AttName, ContentType, #{}).

-spec put_stream(att_ref(), db_name(), docid(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType, Opts) ->
    #{chunk_size := ChunkSize} = AttRef,
    Sync = maps:get(sync, Opts, false),
    {ok, #{
        type => write,
        att_ref => AttRef,
        db_name => DbName,
        doc_id => DocId,
        att_name => AttName,
        content_type => ContentType,
        chunk_size => ChunkSize,
        sync => Sync,
        chunk_index => 0,
        total_length => 0,
        hash_ctx => crypto:hash_init(sha256),
        buffer => <<>>
    }}.

%% @doc Write data to a put stream
%% Data is buffered and written in chunks
-spec write_chunk(map(), binary()) -> {ok, map()} | {error, term()}.
write_chunk(#{type := write, buffer := Buffer, chunk_size := ChunkSize} = Stream, Data) ->
    NewBuffer = <<Buffer/binary, Data/binary>>,
    write_buffered_chunks(Stream#{buffer => NewBuffer}, ChunkSize).

%% @private Write complete chunks from buffer
write_buffered_chunks(#{buffer := Buffer, chunk_size := ChunkSize} = Stream, ChunkSize)
  when byte_size(Buffer) >= ChunkSize ->
    <<Chunk:ChunkSize/binary, Rest/binary>> = Buffer,
    case write_single_chunk(Stream, Chunk) of
        {ok, Stream2} ->
            write_buffered_chunks(Stream2#{buffer => Rest}, ChunkSize);
        {error, _} = Error ->
            Error
    end;
write_buffered_chunks(Stream, _ChunkSize) ->
    {ok, Stream}.

%% @private Write a single chunk to storage
write_single_chunk(#{att_ref := #{ref := Ref}, db_name := DbName, doc_id := DocId,
                     att_name := AttName, chunk_index := Index,
                     total_length := TotalLen, hash_ctx := HashCtx} = Stream, Chunk) ->
    ChunkKey = make_chunk_key(DbName, DocId, AttName, Index),
    case rocksdb:put(Ref, ChunkKey, Chunk, [{sync, false}]) of
        ok ->
            {ok, Stream#{
                chunk_index => Index + 1,
                total_length => TotalLen + byte_size(Chunk),
                hash_ctx => crypto:hash_update(HashCtx, Chunk)
            }};
        {error, _} = Error ->
            Error
    end.

%% @doc Finish a put stream and write metadata
-spec finish_stream(map()) -> {ok, att_info()} | {error, term()}.
finish_stream(#{type := write, att_ref := #{ref := Ref}, db_name := DbName,
                doc_id := DocId, att_name := AttName, content_type := ContentType,
                chunk_size := ChunkSize, sync := Sync, chunk_index := ChunkIndex,
                total_length := TotalLen, hash_ctx := HashCtx, buffer := Buffer}) ->
    %% Write any remaining buffered data as final chunk
    {FinalChunkIndex, FinalLen, FinalHashCtx} = case Buffer of
        <<>> ->
            {ChunkIndex, TotalLen, HashCtx};
        _ ->
            ChunkKey = make_chunk_key(DbName, DocId, AttName, ChunkIndex),
            case rocksdb:put(Ref, ChunkKey, Buffer, [{sync, false}]) of
                ok ->
                    {ChunkIndex + 1, TotalLen + byte_size(Buffer),
                     crypto:hash_update(HashCtx, Buffer)};
                {error, Reason} ->
                    throw({error, Reason})
            end
    end,

    %% Compute final digest
    DigestBin = crypto:hash_final(FinalHashCtx),
    Digest = <<"sha256-", (to_hex(DigestBin))/binary>>,

    %% Write metadata
    MetaKey = make_key(DbName, DocId, AttName),
    MetaValue = encode_chunk_meta(#{
        chunk_size => ChunkSize,
        chunk_count => FinalChunkIndex,
        length => FinalLen,
        content_type => ContentType,
        digest => Digest
    }),

    case rocksdb:put(Ref, MetaKey, MetaValue, [{sync, Sync}]) of
        ok ->
            {ok, #{
                name => AttName,
                content_type => ContentType,
                length => FinalLen,
                digest => Digest,
                chunked => true,
                chunk_size => ChunkSize,
                chunk_count => FinalChunkIndex
            }};
        {error, _} = Error ->
            Error
    end.

%% @doc Abort a put stream and clean up any written chunks
-spec abort_stream(map()) -> ok.
abort_stream(#{type := write, att_ref := #{ref := Ref}, db_name := DbName,
               doc_id := DocId, att_name := AttName, chunk_index := ChunkIndex}) ->
    %% Delete any chunks that were written
    lists:foreach(
        fun(Index) ->
            ChunkKey = make_chunk_key(DbName, DocId, AttName, Index),
            rocksdb:delete(Ref, ChunkKey, [])
        end,
        lists:seq(0, ChunkIndex - 1)
    ),
    ok;
abort_stream(_) ->
    ok.

%% @doc Open a stream for reading an attachment
-spec get_stream(att_ref(), db_name(), docid(), binary()) ->
    {ok, att_stream()} | {error, term()}.
get_stream(AttRef, DbName, DocId, AttName) ->
    case get_info(AttRef, DbName, DocId, AttName) of
        {ok, #{chunked := true, chunk_count := ChunkCount} = Info} ->
            {ok, #{
                type => read,
                att_ref => AttRef,
                db_name => DbName,
                doc_id => DocId,
                att_name => AttName,
                info => Info,
                chunk_index => 0,
                chunk_count => ChunkCount
            }};
        {ok, #{chunked := false} = Info} ->
            %% Single value - wrap as single-chunk stream
            {ok, #{
                type => read,
                att_ref => AttRef,
                db_name => DbName,
                doc_id => DocId,
                att_name => AttName,
                info => Info,
                chunk_index => 0,
                chunk_count => 1,
                single_value => true
            }};
        {error, _} = Error ->
            Error
    end.

%% @doc Read the next chunk from a stream
-spec read_chunk(att_stream()) -> {ok, binary(), att_stream()} | eof | {error, term()}.
read_chunk(#{chunk_index := Index, chunk_count := Count}) when Index >= Count ->
    eof;
read_chunk(#{single_value := true, att_ref := #{ref := Ref},
             db_name := DbName, doc_id := DocId, att_name := AttName} = Stream) ->
    Key = make_key(DbName, DocId, AttName),
    case rocksdb:get(Ref, Key, []) of
        {ok, Value} ->
            {ok, Value, Stream#{chunk_index => 1}};
        not_found ->
            {error, not_found};
        {error, _} = Error ->
            Error
    end;
read_chunk(#{att_ref := #{ref := Ref}, db_name := DbName, doc_id := DocId,
             att_name := AttName, chunk_index := Index} = Stream) ->
    ChunkKey = make_chunk_key(DbName, DocId, AttName, Index),
    case rocksdb:get(Ref, ChunkKey, []) of
        {ok, Chunk} ->
            {ok, Chunk, Stream#{chunk_index => Index + 1}};
        not_found ->
            {error, {missing_chunk, Index}};
        {error, _} = Error ->
            Error
    end.

%% @doc Close a stream (currently a no-op, but included for API completeness)
-spec close_stream(att_stream()) -> ok.
close_stream(_Stream) ->
    ok.
