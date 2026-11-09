%%%-------------------------------------------------------------------
%%% @doc BlobDB storage backend for attachments
%%%
%%% Uses a separate RocksDB instance with BlobDB enabled for storing
%%% attachment binary data. This avoids compaction issues from mixing
%%% small documents with large blobs.
%%%
%%% Large attachments (>= chunk_threshold) are stored as chunks for
%%% streaming support. Small attachments are stored as single values.
%%%
%%% Every upload writes its chunks under its own generation; the commit,
%%% run by the database writer, publishes the metadata and deletes only
%%% the previously published generation. Readers use one snapshot.
%%%
%%% This is the default attachment backend, selected by barrel_att_store when
%%% no other backend is configured.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_store_blob).
-behaviour(barrel_att_backend).

-include("barrel_docdb.hrl").

%% Chunk configuration - can be overridden in att_ref options
-define(DEFAULT_CHUNK_THRESHOLD, 65536).  %% 64 KB
-define(DEFAULT_CHUNK_SIZE, 65536).       %% 64 KB

%% API
-export([open/2, close/1, checkpoint/2]).
-export([put/5, put/6, get/4, delete/4, delete/5]).
-export([delete_all/3]).
-export([fold/5]).

%% Attachment change feed (sync support; see barrel_att_feed)
-export([att_changes/4, att_floor/2, att_last_hlc/2, sweep_att_feed/3,
         rebuild_feed/2, rebuild_feed/3]).

%% Staged writes committed by the database writer (see barrel_att_backend)
-export([stage_put/6, stage_finish/1, stage_delete/5, stage_touch/5,
         commit/3, gc_uploads/3]).

%% Replication support
-export([att_state/4, att_list/3, touch/5, flush/1]).

%% Streaming API
-export([put_stream/5, put_stream/6]).
-export([write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, seek_stream/2, read_chunk/1, close_stream/1]).
-export([get_info/4]).

%%====================================================================
%% Types
%%====================================================================

-type att_ref() :: #{
    ref := rocksdb:db_handle(),
    path := string(),
    chunk_threshold => pos_integer(),
    chunk_size => pos_integer(),
    %% EncryptedEnv handle when the store is encrypted; kept in the ref
    %% so the NIF does not free the env while the db is open.
    env => rocksdb:env_handle(),
    %% set by barrel_att_store / barrel_docdb
    backend => module(),
    db => map()
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
    ReadOnly = maps:get(read_only, Options, false),
    ok = ensure_store_dir(ReadOnly, Path),
    DbOpts0 = build_blob_options(Options),
    Env = maps:get(env, Options, undefined),
    DbOpts = case Env of
        undefined -> DbOpts0;
        _ -> [{env, Env} | DbOpts0]
    end,
    case open_store(ReadOnly, Path, DbOpts) of
        {ok, Ref} ->
            ChunkThreshold = maps:get(chunk_threshold, Options, ?DEFAULT_CHUNK_THRESHOLD),
            ChunkSize = maps:get(chunk_size, Options, ?DEFAULT_CHUNK_SIZE),
            AttRef = #{
                ref => Ref,
                path => Path,
                chunk_threshold => ChunkThreshold,
                chunk_size => ChunkSize
            },
            case Env of
                undefined -> {ok, AttRef};
                _ -> {ok, AttRef#{env => Env}}
            end;
        {error, {read_only_store_missing, _} = Reason} ->
            {error, Reason};
        {error, Reason} ->
            {error, {att_store_open_failed, Reason}}
    end.

ensure_store_dir(true, _Path) -> ok;
ensure_store_dir(_, Path) -> filelib:ensure_dir(Path ++ "/").

open_store(true, Path, DbOpts) -> barrel_rocksdb_ro:open(Path, DbOpts);
open_store(_, Path, DbOpts) -> rocksdb:open(Path, DbOpts).

%% @doc Close the attachment store
-spec close(att_ref()) -> ok.
close(#{ref := Ref}) ->
    rocksdb:close(Ref).

%% @doc Hard-link snapshot into Path (timeline forks); the target
%% must not exist.
-spec checkpoint(att_ref(), string()) -> ok | {error, term()}.
checkpoint(#{ref := Ref}, Path) ->
    rocksdb:checkpoint(Ref, Path).

%% @doc Store an attachment (async by default)
%% Small attachments are stored as single values.
%% Large attachments (>= chunk_threshold) are stored as chunks.
-spec put(att_ref(), db_name(), docid(), binary(), binary()) ->
    {ok, att_info()} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data) ->
    put(AttRef, DbName, DocId, AttName, Data, #{}).

%% @doc Store an attachment with options, committing in the caller
%% (a store used without a database writer).
%% Options:
%%   - sync: sync to disk before returning
%%   - content_type: override the name-derived content type
%%     (replication preserves the source's)
%%   - origin_hlc: replicated write; the last-write-wins guard may
%%     answer {ok, ignored}
%%   - expected_digest: verify the data against a digest before
%%     committing anything
-spec put(att_ref(), db_name(), docid(), binary(), binary(), map()) ->
    {ok, att_info()} | {ok, ignored} | {error, term()}.
put(AttRef, DbName, DocId, AttName, Data, Opts) when is_binary(Data) ->
    commit_here(AttRef, stage_put(AttRef, DbName, DocId, AttName, Data, Opts)).

%% @doc Stage a put in the caller: a large value's chunks land under a
%% fresh generation, invisible until commit/3 publishes it.
-spec stage_put(att_ref(), db_name(), docid(), binary(), binary(), map()) ->
    {commit, map()} | {error, term()}.
stage_put(AttRef, DbName, DocId, AttName, Data, Opts) when is_binary(Data) ->
    #{ref := Ref, chunk_threshold := Threshold,
      chunk_size := ChunkSize} = AttRef,
    DataSize = byte_size(Data),
    Digest = compute_digest(Data),
    Base = #{op => put, db => DbName, doc_id => DocId, att_name => AttName,
             digest => Digest, length => DataSize,
             content_type => maps:get(content_type, Opts,
                                      mimerl:filename(AttName)),
             origin => maps:get(origin_hlc, Opts, undefined),
             sync => maps:get(sync, Opts, false)},
    case maps:get(expected_digest, Opts, Digest) of
        Digest when DataSize >= Threshold ->
            Gen = new_gen(),
            Key = make_key(DbName, DocId, AttName),
            ok = rocksdb:put(Ref, upload_key(DbName, Gen, DocId, AttName),
                             <<>>, []),
            case put_chunks(Ref, Key, Gen, Data, ChunkSize, 0) of
                {ok, Count} ->
                    {commit, Base#{value => {chunked, Gen, ChunkSize, Count}}};
                {error, _} = Error ->
                    _ = drop_gen(Ref, DbName, DocId, AttName, Gen),
                    Error
            end;
        Digest ->
            {commit, Base#{value => {inline, Data}}};
        _Other ->
            {error, digest_mismatch}
    end.

%% @private Store the chunks of one generation; returns their count.
put_chunks(_Ref, _Key, _Gen, <<>>, _ChunkSize, Index) ->
    {ok, Index};
put_chunks(Ref, Key, Gen, Data, ChunkSize, Index) ->
    {Chunk, Rest} = case byte_size(Data) > ChunkSize of
        true -> split_binary(Data, ChunkSize);
        false -> {Data, <<>>}
    end,
    case rocksdb:put(Ref, chunk_key(Key, Gen, Index), Chunk, []) of
        ok -> put_chunks(Ref, Key, Gen, Rest, ChunkSize, Index + 1);
        {error, _} = Error -> Error
    end.

%% @doc Stage a delete (nothing to write before the commit).
-spec stage_delete(att_ref(), db_name(), docid(), binary(), map()) ->
    {commit, map()}.
stage_delete(_AttRef, DbName, DocId, AttName, Opts) ->
    {commit, #{op => delete, db => DbName, doc_id => DocId,
               att_name => AttName,
               origin => maps:get(origin_hlc, Opts, undefined),
               sync => maps:get(sync, Opts, false)}}.

%% @doc Stage a touch: a newer origin for the stored bytes.
-spec stage_touch(att_ref(), db_name(), docid(), binary(), map()) ->
    {commit, map()}.
stage_touch(_AttRef, DbName, DocId, AttName, Opts) ->
    {commit, #{op => touch, db => DbName, doc_id => DocId,
               att_name => AttName,
               origin => maps:get(origin_hlc, Opts),
               digest => maps:get(digest, Opts)}}.

%% @doc Touch committed in the caller (a store used without a database).
-spec touch(att_ref(), db_name(), docid(), binary(), map()) ->
    {ok, touched} | {ok, ignored} | {error, term()}.
touch(AttRef, DbName, DocId, AttName, Opts) ->
    commit_here(AttRef, stage_touch(AttRef, DbName, DocId, AttName, Opts)).

%% @doc Sync the write-ahead log: every acknowledged write survives a
%% power loss once this returns.
-spec flush(att_ref()) -> ok | {error, term()}.
flush(#{ref := Ref}) ->
    rocksdb:sync_wal(Ref).

-spec att_state(att_ref(), db_name(), docid(), binary()) ->
    {ok, map()} | not_found.
att_state(#{ref := Ref}, DbName, DocId, AttName) ->
    barrel_att_feed:index_get(Ref, DbName, DocId, AttName).

-spec att_list(att_ref(), db_name(), docid()) -> [map()].
att_list(#{ref := Ref}, DbName, DocId) ->
    barrel_att_feed:index_list(Ref, DbName, DocId).

%% @doc Publish a staged write. Runs in the database writer, which
%% serializes the last-write-wins check, the metadata and feed batch and
%% the cleanup of the previously published generation. Alloc(Floor)
%% returns a feed HLC above Floor and above every HLC the database wrote.
-spec commit(att_ref(), map(), fun((barrel_hlc:timestamp()) ->
                                     barrel_hlc:timestamp())) ->
    {ok, att_info()} | {ok, ignored} | {ok, touched} | ok | {error, term()}.
commit(#{ref := Ref}, #{op := put, db := DbName, doc_id := DocId,
                        att_name := AttName, value := Value} = C, Alloc) ->
    Own = own_ops(DbName, DocId, AttName, Value),
    case staged(Ref, DbName, DocId, AttName, Value) of
        false ->
            {error, upload_expired};
        true ->
            try publish(Ref, C, Alloc) of
                {ok, Info} = Ok when is_map(Info) ->
                    Ok;
                Other ->
                    _ = apply_batch(Ref, Own, false),
                    Other
            catch
                throw:{error, _} = Error ->
                    _ = apply_batch(Ref, Own, false),
                    Error
            end
    end;
commit(#{ref := Ref}, #{op := delete} = C, Alloc) ->
    try unpublish(Ref, C, Alloc)
    catch throw:{error, _} = Error -> Error
    end;
commit(#{ref := Ref} = AttRef, #{op := touch} = C, Alloc) ->
    try retouch(Ref, AttRef, C, Alloc)
    catch throw:{error, _} = Error -> Error
    end.

%% Only the origin and the feed row move; the bytes must already match.
retouch(Ref, AttRef, #{db := DbName, doc_id := DocId, att_name := AttName,
                       origin := Origin, digest := Digest}, Alloc) ->
    case get_info(AttRef, DbName, DocId, AttName) of
        {ok, #{digest := Digest} = Info} ->
            case resolve_origin(Ref, DbName, DocId, AttName, Origin, Digest,
                                Alloc) of
                ignored ->
                    {ok, ignored};
                {apply, OriginHlc, LocalHlc} ->
                    Meta = case barrel_att_feed:index_get(Ref, DbName, DocId,
                                                          AttName) of
                        {ok, #{op := put} = Idx} -> Idx;
                        _ -> Info
                    end,
                    FeedOps = barrel_att_feed:ops(
                        Ref, DbName, DocId, AttName, put, OriginHlc, LocalHlc,
                        maps:with([digest, length, content_type], Meta)),
                    case apply_batch(Ref, FeedOps, false) of
                        ok -> {ok, touched};
                        {error, _} = Error -> Error
                    end
            end;
        _ ->
            {error, bytes_needed}
    end.

publish(Ref, #{db := DbName, doc_id := DocId, att_name := AttName,
               digest := Digest, length := Length,
               content_type := ContentType, origin := OriginOpt,
               sync := Sync, value := Value}, Alloc) ->
    case resolve_origin(Ref, DbName, DocId, AttName, OriginOpt, Digest,
                        Alloc) of
        ignored ->
            {ok, ignored};
        {apply, OriginHlc, LocalHlc} ->
            Key = make_key(DbName, DocId, AttName),
            Info0 = #{name => AttName, content_type => ContentType,
                      length => Length, digest => Digest},
            {Stored, Info} = case Value of
                {inline, Data} ->
                    {Data, Info0#{chunked => false}};
                {chunked, Gen, ChunkSize, Count} ->
                    {encode_chunk_meta(#{chunk_size => ChunkSize,
                                         chunk_count => Count,
                                         length => Length,
                                         content_type => ContentType,
                                         digest => Digest, gen => Gen}),
                     Info0#{chunked => true, chunk_size => ChunkSize,
                            chunk_count => Count}}
            end,
            FeedOps = barrel_att_feed:ops(
                Ref, DbName, DocId, AttName, put, OriginHlc, LocalHlc,
                #{digest => Digest, length => Length,
                  content_type => ContentType}),
            Ops = [{put, Key, Stored}
                   | published_ops(Ref, DbName, DocId, AttName)
                     ++ marker_ops(DbName, DocId, AttName, Value) ++ FeedOps],
            case apply_batch(Ref, Ops, Sync) of
                ok -> {ok, Info};
                {error, _} = Error -> Error
            end
    end.

unpublish(Ref, #{db := DbName, doc_id := DocId, att_name := AttName,
                 origin := OriginOpt, sync := Sync}, Alloc) ->
    case resolve_origin(Ref, DbName, DocId, AttName, OriginOpt, <<>>,
                        Alloc) of
        ignored ->
            ok;
        {apply, OriginHlc, LocalHlc} ->
            Key = make_key(DbName, DocId, AttName),
            BlobExists = case rocksdb:get(Ref, Key, []) of
                {ok, _} -> true;
                _ -> false
            end,
            IndexExists = barrel_att_feed:index_get(
                              Ref, DbName, DocId, AttName) =/= not_found,
            case BlobExists orelse IndexExists
                 orelse OriginOpt =/= undefined of
                false ->
                    %% local delete of something never written: no-op
                    ok;
                true ->
                    %% only the published generation goes; a staged
                    %% upload still publishes when it finishes
                    Ops = [{delete, Key}
                           | published_ops(Ref, DbName, DocId, AttName)
                             ++ barrel_att_feed:ops(Ref, DbName, DocId,
                                                    AttName, delete,
                                                    OriginHlc, LocalHlc,
                                                    #{})],
                    apply_batch(Ref, Ops, Sync)
            end
    end.

%% @private Commit in the calling process (no database writer).
commit_here(#{ref := Ref} = AttRef, {commit, #{db := DbName} = C}) ->
    commit(AttRef, C, fun(Floor) ->
        barrel_att_feed:next_hlc(
          max_hlc(Floor, barrel_att_feed:last_hlc(Ref, DbName)))
    end);
commit_here(_AttRef, Other) ->
    Other.

%% @private A chunked commit needs its upload row: a missing one means
%% the abandoned-upload sweep already removed the chunks.
staged(_Ref, _DbName, _DocId, _AttName, {inline, _}) ->
    true;
staged(Ref, DbName, DocId, AttName, {chunked, Gen, _, _}) ->
    rocksdb:get(Ref, upload_key(DbName, Gen, DocId, AttName), [])
        =/= not_found.

own_ops(_DbName, _DocId, _AttName, {inline, _}) ->
    [];
own_ops(DbName, DocId, AttName, {chunked, Gen, _, _}) ->
    gen_ops(DbName, DocId, AttName, Gen).

marker_ops(_DbName, _DocId, _AttName, {inline, _}) ->
    [];
marker_ops(DbName, DocId, AttName, {chunked, Gen, _, _}) ->
    [{delete, upload_key(DbName, Gen, DocId, AttName)}].

%% @private Delete ops for one generation's chunks and its upload row.
gen_ops(DbName, DocId, AttName, Gen) ->
    Key = make_key(DbName, DocId, AttName),
    {Start, End} = gen_range(Key, Gen),
    [{delete_range, Start, End},
     {delete, upload_key(DbName, Gen, DocId, AttName)}].

drop_gen(Ref, DbName, DocId, AttName, Gen) ->
    apply_batch(Ref, gen_ops(DbName, DocId, AttName, Gen), false).

%% @private Delete ops for the chunks of the published value: its
%% generation, or the old-layout chunks of a meta without one.
published_ops(Ref, DbName, DocId, AttName) ->
    case lookup(Ref, [], DbName, DocId, AttName) of
        {ok, Key, {chunked, #{gen := Gen}}} when is_integer(Gen) ->
            {Start, End} = gen_range(Key, Gen),
            [{delete_range, Start, End}];
        {ok, Key, {chunked, #{chunk_count := Count}}} ->
            [{delete, legacy_chunk_key(Key, I)}
             || I <- lists:seq(0, Count - 1)];
        _ ->
            []
    end.

%% @doc Retrieve an attachment. Metadata and chunks are read from one
%% snapshot, so a concurrent overwrite or delete is never seen halfway.
-spec get(att_ref(), db_name(), docid(), binary()) ->
    {ok, binary()} | not_found | {error, term()}.
get(#{ref := Ref}, DbName, DocId, AttName) ->
    with_snapshot(Ref, fun(ReadOpts) ->
        case lookup(Ref, ReadOpts, DbName, DocId, AttName) of
            {ok, _Key, {inline, Value}} ->
                {ok, Value};
            {ok, Key, {chunked, #{chunk_count := Count} = Meta}} ->
                case get_chunks(Ref, ReadOpts, Key, Meta, 0, Count, []) of
                    {ok, Chunks} -> {ok, iolist_to_binary(Chunks)};
                    {error, _} = Error -> Error
                end;
            Other ->
                Other
        end
    end).

with_snapshot(Ref, Fun) ->
    {ok, Snap} = rocksdb:snapshot(Ref),
    try Fun([{snapshot, Snap}])
    after release(Snap)
    end.

release(undefined) -> ok;
release(Snap) ->
    try rocksdb:release_snapshot(Snap) of
        _ -> ok
    catch error:badarg -> ok   % the store closed meanwhile
    end.

%% @private The published value of an attachment: inline bytes or the
%% chunk metadata. A value whose digest is the one the feed index
%% recorded is user data, even when it looks like metadata.
lookup(Ref, ReadOpts, DbName, DocId, AttName) ->
    Key = make_key(DbName, DocId, AttName),
    case rocksdb:get(Ref, Key, ReadOpts) of
        {ok, Value} ->
            {ok, Key, classify(Ref, ReadOpts, DbName, DocId, AttName, Value)};
        not_found ->
            not_found;
        {error, _} = Error ->
            Error
    end.

classify(Ref, ReadOpts, DbName, DocId, AttName, Value) ->
    case maybe_meta(Value) andalso is_chunked_metadata(Value) of
        {true, Meta} ->
            case barrel_att_feed:index_get(Ref, DbName, DocId, AttName,
                                           ReadOpts) of
                {ok, #{op := put, digest := Digest}} ->
                    case compute_digest(Value) of
                        Digest -> {inline, Value};
                        _ -> {chunked, Meta}
                    end;
                _ ->
                    {chunked, Meta}
            end;
        _ ->
            {inline, Value}
    end.

maybe_meta(<<"BARREL_CHUNK_V1:", _/binary>>) -> true;
maybe_meta(<<131, _/binary>>) -> true;
maybe_meta(_) -> false.

%% Chunked-metadata wire format.
%%
%% Tagged prefix avoids decoding user-controlled attachment bytes as an
%% Erlang term (binary_to_term used to crash and could allocate
%% arbitrary terms / create atoms). New format:
%%
%% ```
%%   <<"BARREL_CHUNK_V1:", JsonMeta/binary>>
%% '''
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
        <<"digest">> => maps:get(digest, Meta),
        <<"gen">> => maps:get(gen, Meta)
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
        digest => maps:get(<<"digest">>, M),
        gen => maps:get(<<"gen">>, M, undefined)
    }.

%% @private Read chunks [Index, Count) under the read options.
get_chunks(_Ref, _ReadOpts, _Key, _Meta, Index, Count, Acc)
  when Index >= Count ->
    {ok, lists:reverse(Acc)};
get_chunks(Ref, ReadOpts, Key, Meta, Index, Count, Acc) ->
    case read_one(Ref, ReadOpts, Key, Meta, Index) of
        {ok, Chunk} ->
            get_chunks(Ref, ReadOpts, Key, Meta, Index + 1, Count,
                       [Chunk | Acc]);
        {error, _} = Error ->
            Error
    end.

read_one(Ref, ReadOpts, Key, Meta, Index) ->
    case rocksdb:get(Ref, meta_chunk_key(Key, Meta, Index), ReadOpts) of
        {ok, Chunk} -> {ok, Chunk};
        not_found -> {error, {missing_chunk, Index}};
        {error, _} = Error -> Error
    end.

meta_chunk_key(Key, #{gen := Gen}, Index) when is_integer(Gen) ->
    chunk_key(Key, Gen, Index);
meta_chunk_key(Key, _Meta, Index) ->
    legacy_chunk_key(Key, Index).

%% @doc Delete an attachment
%% Handles both single-value and chunked attachments.
-spec delete(att_ref(), db_name(), docid(), binary()) -> ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName) ->
    delete(AttRef, DbName, DocId, AttName, #{}).

%% @doc Delete with options (origin_hlc for replicated deletes: the
%% last-write-wins guard applies, and a tombstone lands even when the
%% attachment was never seen locally). Commits in the caller.
-spec delete(att_ref(), db_name(), docid(), binary(), map()) ->
    ok | {error, term()}.
delete(AttRef, DbName, DocId, AttName, Opts) ->
    commit_here(AttRef, stage_delete(AttRef, DbName, DocId, AttName, Opts)).

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
        %% zstd: the one codec rocksdb always builds (bundled when the system has none)
        {compression, maps:get(compression, Options, zstd)},
        {bottommost_compression, maps:get(bottommost_compression, Options, zstd)},

        %% BlobDB settings for large attachments
        {enable_blob_files, true},
        {min_blob_size, maps:get(min_blob_size, Options, 4096)},  % 4KB threshold
        {blob_file_size, BlobFileSize},
        {blob_compression_type, maps:get(blob_compression, Options, zstd)},

        %% Blob garbage collection
        {enable_blob_garbage_collection, true},
        {blob_garbage_collection_age_cutoff,
            maps:get(blob_gc_age_cutoff, Options, 0.25)},
        {blob_garbage_collection_force_threshold,
            maps:get(blob_gc_force_threshold, Options, 0.5)},

        %% Background jobs for compaction/GC
        {max_background_jobs, maps:get(max_background_jobs, Options, erlang:max(2, Schedulers div 2))},
        %% Opt-in: compactions read their input with direct I/O so they
        %% do not push hot data out of the page cache
        {use_direct_io_for_compaction_reads,
            maps:get(use_direct_io_for_compaction_reads, Options, false)},

        %% Block-based table with bloom filters and shared cache
        {block_based_table_options, BlockOpts}
    ].

%% Create key for attachment: prefix + att_name
make_key(DbName, DocId, AttName) ->
    Prefix = make_prefix(DbName, DocId),
    <<Prefix/binary, AttName/binary>>.

%% Chunk key: prefix + att_name + NUL + upload generation + index.
%% Each upload writes its own generation, so a staged or aborted upload
%% never touches the published chunks.
chunk_key(Key, Gen, Index) ->
    <<Key/binary, 0, Gen:64, Index:32>>.

%% Chunk key of the old layout (a meta without a generation).
legacy_chunk_key(Key, Index) ->
    <<Key/binary, 0, Index:32>>.

%% Every chunk key of one generation, and no old-layout key.
gen_range(Key, Gen) ->
    {<<Key/binary, 0, Gen:64>>,
     <<Key/binary, 0, Gen:64, 16#FF, 16#FF, 16#FF, 16#FF, 0>>}.

%% A time-ordered generation: milliseconds and a per-node counter.
new_gen() ->
    (erlang:system_time(millisecond) bsl 20)
        bor (erlang:unique_integer([positive, monotonic]) band 16#FFFFF).

gen_ms(Gen) -> Gen bsr 20.

%% Upload row of a staged generation (see barrel_att_feed key layout).
upload_key(DbName, Gen, DocId, AttName) ->
    <<16#FF, $U, (byte_size(DbName)):16, DbName/binary, Gen:64,
      (byte_size(DocId)):16, DocId/binary, AttName/binary>>.

upload_prefix(DbName) ->
    <<16#FF, $U, (byte_size(DbName)):16, DbName/binary>>.

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

%% @private Apply a list of ops as one WriteBatch.
apply_batch(Ref, Ops, Sync) ->
    {ok, Batch} = rocksdb:batch(),
    try
        lists:foreach(
            fun({put, K, V}) -> ok = rocksdb:batch_put(Batch, K, V);
               ({delete, K}) -> ok = rocksdb:batch_delete(Batch, K);
               ({delete_range, S, E}) ->
                    ok = rocksdb:batch_delete_range(Batch, S, E)
            end,
            Ops),
        rocksdb:write_batch(Ref, Batch, [{sync, Sync}])
    after
        rocksdb:release_batch(Batch)
    end.

%% @private Local writes take an origin above the stored one (Alloc);
%% replicated writes carry theirs through the LWW guard. Returns the
%% origin and the feed HLC.
resolve_origin(Ref, DbName, DocId, AttName, undefined, _Digest, Alloc) ->
    Floor = case barrel_att_feed:index_get(Ref, DbName, DocId, AttName) of
        {ok, #{origin := Stored}} -> Stored;
        not_found -> barrel_hlc:min()
    end,
    Hlc = Alloc(Floor),
    {apply, Hlc, Hlc};
resolve_origin(Ref, DbName, DocId, AttName, Origin, Digest, Alloc) ->
    case barrel_att_feed:check(Ref, DbName, DocId, AttName, Origin,
                               Digest) of
        apply -> {apply, Origin, Alloc(barrel_hlc:min())};
        ignored -> ignored
    end.

max_hlc(A, B) ->
    case barrel_hlc:less(A, B) of
        true -> B;
        false -> A
    end.

%%====================================================================
%% Attachment change feed (sync support)
%%====================================================================

%% @doc Feed entries since an HLC (exclusive). See barrel_att_feed.
-spec att_changes(att_ref(), db_name(), barrel_hlc:timestamp() | first,
                  map()) ->
    {ok, [barrel_att_feed:entry()], barrel_hlc:timestamp() | first}.
att_changes(#{ref := Ref}, DbName, Since, Opts) ->
    barrel_att_feed:att_changes(Ref, DbName, Since, Opts).

-spec att_floor(att_ref(), db_name()) ->
    barrel_hlc:timestamp() | undefined.
att_floor(#{ref := Ref}, DbName) ->
    barrel_att_feed:att_floor(Ref, DbName).

-spec att_last_hlc(att_ref(), db_name()) -> barrel_hlc:timestamp().
att_last_hlc(#{ref := Ref}, DbName) ->
    barrel_att_feed:last_hlc(Ref, DbName).

-spec sweep_att_feed(att_ref(), db_name(), barrel_hlc:timestamp()) ->
    {ok, #{tombstones_swept := non_neg_integer()}}.
sweep_att_feed(#{ref := Ref}, DbName, Cutoff) ->
    barrel_att_feed:sweep(Ref, DbName, Cutoff).

%% @doc Maintenance escape hatch: synthesize feed rows for attachments
%% written before the feed existed (format-break stance: they do not
%% sync otherwise). Rebuilt entries carry the MINIMUM origin so any
%% real write, local or remote, wins the LWW race against them; two
%% rebuilt replicas converge by digest tie-break. Safe to re-run.
-spec rebuild_feed(att_ref(), db_name()) -> {ok, #{rows := non_neg_integer()}}.
rebuild_feed(#{ref := Ref} = AttRef, DbName) ->
    rebuild_feed(AttRef, DbName, fun(Floor) ->
        barrel_att_feed:next_hlc(
          max_hlc(Floor, barrel_att_feed:last_hlc(Ref, DbName)))
    end).

%% @doc rebuild_feed/2 with feed HLCs from Alloc (the database writer).
-spec rebuild_feed(att_ref(), db_name(),
                   fun((barrel_hlc:timestamp()) -> barrel_hlc:timestamp())) ->
    {ok, #{rows := non_neg_integer()}}.
rebuild_feed(#{ref := Ref} = AttRef, DbName, Alloc) ->
    DbPrefix = <<(byte_size(DbName)):16, DbName/binary>>,
    ReadOpts = [{iterate_lower_bound, DbPrefix},
                {iterate_upper_bound, prefix_end(DbPrefix)}],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    Atts = try
        rebuild_scan(rocksdb:iterator_move(Itr, first), Itr, DbName, [])
    after
        rocksdb:iterator_close(Itr)
    end,
    MinOrigin = barrel_hlc:min(),
    N = lists:foldl(
        fun({DocId, AttName}, Count) ->
            case get_info(AttRef, DbName, DocId, AttName) of
                {ok, #{digest := Digest, length := Length,
                       content_type := ContentType}} ->
                    Ops = barrel_att_feed:ops(
                        Ref, DbName, DocId, AttName, put, MinOrigin,
                        Alloc(MinOrigin),
                        #{digest => Digest, length => Length,
                          content_type => ContentType}),
                    ok = apply_batch(Ref, Ops, false),
                    Count + 1;
                _ ->
                    Count
            end
        end,
        0,
        Atts),
    {ok, #{rows => N}}.

rebuild_scan({error, _}, _Itr, _DbName, Acc) ->
    lists:reverse(Acc);
rebuild_scan({ok, Key, _Value}, Itr, DbName, Acc) ->
    DbLen = byte_size(DbName),
    Acc1 = case Key of
        <<DbLen:16, DbName:DbLen/binary, IdLen:16, DocId:IdLen/binary,
          $:, AttName/binary>> ->
            case binary:match(AttName, <<0>>) of
                nomatch -> [{DocId, AttName} | Acc];
                _ -> Acc
            end;
        _ ->
            Acc
    end,
    rebuild_scan(rocksdb:iterator_move(Itr, next), Itr, DbName, Acc1).

%% @doc Remove abandoned uploads: staged generations older than MaxAgeMs
%% (a published generation has no upload row). Runs in the database
%% writer, where commits check the upload row, so a removed generation
%% can never publish.
-spec gc_uploads(att_ref(), db_name(), non_neg_integer()) ->
    {ok, #{uploads_removed := non_neg_integer()}}.
gc_uploads(#{ref := Ref}, DbName, MaxAgeMs) ->
    Prefix = upload_prefix(DbName),
    Cutoff = erlang:system_time(millisecond) - MaxAgeMs,
    ReadOpts = [{iterate_lower_bound, Prefix},
                {iterate_upper_bound, prefix_end(Prefix)}],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    Ops = try
        gc_scan(rocksdb:iterator_move(Itr, first), Itr, DbName, Cutoff, [])
    after
        rocksdb:iterator_close(Itr)
    end,
    ok = case Ops of
        [] -> ok;
        _ -> apply_batch(Ref, lists:append(Ops), false)
    end,
    {ok, #{uploads_removed => length(Ops)}}.

gc_scan({ok, Key, _Value}, Itr, DbName, Cutoff, Acc) ->
    DbLen = byte_size(DbName),
    <<16#FF, $U, DbLen:16, DbName:DbLen/binary, Gen:64, IdLen:16,
      DocId:IdLen/binary, AttName/binary>> = Key,
    case gen_ms(Gen) < Cutoff of
        true ->
            gc_scan(rocksdb:iterator_move(Itr, next), Itr, DbName, Cutoff,
                    [gen_ops(DbName, DocId, AttName, Gen) | Acc]);
        false ->
            %% rows are in generation (time) order
            Acc
    end;
gc_scan({error, _}, _Itr, _DbName, _Cutoff, Acc) ->
    Acc.

%% Compute SHA-256 digest of data
compute_digest(Data) ->
    Digest = crypto:hash(sha256, Data),
    <<"sha256-", (to_hex(Digest))/binary>>.

%% Convert binary to hex string
to_hex(Bin) ->
    << <<(hex_char(N))>> || <<N:4>> <= Bin >>.

hex_char(N) when N < 10 -> $0 + N;
hex_char(N) -> $a + N - 10.

%% Iterator fold loop. Chunk keys share the doc prefix (name + NUL +
%% index); attachment names cannot contain NUL, so skip them here or
%% they leak into list_attachments/delete_all.
fold_loop({ok, Key, Value}, Itr, Prefix, Fun, Acc) ->
    AttName = extract_att_name(Key, Prefix),
    case binary:match(AttName, <<0>>) of
        nomatch ->
            case Fun(AttName, Value, Acc) of
                {ok, Acc1} ->
                    fold_loop(rocksdb:iterator_move(Itr, next), Itr, Prefix,
                              Fun, Acc1);
                {stop, Acc1} ->
                    Acc1;
                stop ->
                    Acc
            end;
        _ ->
            fold_loop(rocksdb:iterator_move(Itr, next), Itr, Prefix, Fun, Acc)
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
    with_snapshot(Ref, fun(ReadOpts) ->
        info_of(lookup(Ref, ReadOpts, DbName, DocId, AttName), AttName)
    end).

info_of({ok, _Key, {chunked, #{content_type := ContentType, length := Length,
                               digest := Digest, chunk_size := ChunkSize,
                               chunk_count := ChunkCount}}}, AttName) ->
    {ok, #{
        name => AttName,
        content_type => ContentType,
        length => Length,
        digest => Digest,
        chunked => true,
        chunk_size => ChunkSize,
        chunk_count => ChunkCount
    }};
info_of({ok, _Key, {inline, Value}}, AttName) ->
    %% Single value - need to compute info from data
    {ok, #{
        name => AttName,
        content_type => mimerl:filename(AttName),
        length => byte_size(Value),
        digest => compute_digest(Value),
        chunked => false
    }};
info_of(not_found, _AttName) ->
    {error, not_found};
info_of({error, _} = Error, _AttName) ->
    Error.

%% @doc Open a stream for writing an attachment
%% Returns a stream handle that can be used with write_chunk/2 and finish_stream/1
-spec put_stream(att_ref(), db_name(), docid(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put_stream(AttRef, DbName, DocId, AttName, ContentType) ->
    put_stream(AttRef, DbName, DocId, AttName, ContentType, #{}).

%% @doc The upload gets its own chunk generation, recorded by an upload
%% row so an abandoned one is swept (gc_uploads/3).
-spec put_stream(att_ref(), db_name(), docid(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
put_stream(#{ref := Ref} = AttRef, DbName, DocId, AttName, ContentType,
           Opts) ->
    #{chunk_size := ChunkSize} = AttRef,
    Gen = new_gen(),
    case rocksdb:put(Ref, upload_key(DbName, Gen, DocId, AttName), <<>>, []) of
        ok ->
            {ok, #{
                type => write,
                att_ref => AttRef,
                db_name => DbName,
                doc_id => DocId,
                att_name => AttName,
                content_type => ContentType,
                chunk_size => ChunkSize,
                sync => maps:get(sync, Opts, false),
                gen => Gen,
                chunk_index => 0,
                total_length => 0,
                hash_ctx => crypto:hash_init(sha256),
                buffer => <<>>,
                origin_hlc => maps:get(origin_hlc, Opts, undefined),
                expected_digest => maps:get(expected_digest, Opts, undefined)
            }};
        {error, _} = Error ->
            Error
    end.

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

%% @private Write a single chunk of the stream's generation
write_single_chunk(#{att_ref := #{ref := Ref}, db_name := DbName, doc_id := DocId,
                     att_name := AttName, gen := Gen, chunk_index := Index,
                     total_length := TotalLen, hash_ctx := HashCtx} = Stream, Chunk) ->
    ChunkKey = chunk_key(make_key(DbName, DocId, AttName), Gen, Index),
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

%% @doc Finish a put stream and commit it in the caller (a store used
%% without a database writer).
-spec finish_stream(map()) ->
    {ok, att_info()} | {ok, ignored} | {error, term()}.
finish_stream(#{type := write, att_ref := AttRef} = Stream) ->
    commit_here(AttRef, stage_finish(Stream)).

%% @doc Write the buffered tail and verify the digest. Nothing is
%% visible until commit/3 publishes the generation; a digest mismatch
%% drops it.
-spec stage_finish(map()) -> {commit, map()} | {error, term()}.
stage_finish(#{type := write, buffer := Buffer} = Stream0) ->
    Tail = case Buffer of
        <<>> -> {ok, Stream0};
        _ -> write_single_chunk(Stream0#{buffer => <<>>}, Buffer)
    end,
    case Tail of
        {ok, Stream} ->
            stage_commit(Stream);
        {error, _} = Error ->
            ok = abort_stream(Stream0),
            Error
    end.

stage_commit(#{db_name := DbName, doc_id := DocId, att_name := AttName,
               content_type := ContentType, chunk_size := ChunkSize,
               sync := Sync, gen := Gen, chunk_index := Count,
               total_length := Length, hash_ctx := HashCtx,
               origin_hlc := OriginOpt,
               expected_digest := ExpectedDigest} = Stream) ->
    Digest = <<"sha256-", (to_hex(crypto:hash_final(HashCtx)))/binary>>,
    case ExpectedDigest =/= undefined andalso ExpectedDigest =/= Digest of
        true ->
            ok = abort_stream(Stream),
            {error, digest_mismatch};
        false ->
            {commit, #{op => put, db => DbName, doc_id => DocId,
                       att_name => AttName, digest => Digest,
                       length => Length, content_type => ContentType,
                       origin => OriginOpt, sync => Sync,
                       value => {chunked, Gen, ChunkSize, Count}}}
    end.

%% @doc Abort a put stream: drop its own generation only.
-spec abort_stream(map()) -> ok.
abort_stream(#{type := write, att_ref := #{ref := Ref}, db_name := DbName,
               doc_id := DocId, att_name := AttName, gen := Gen}) ->
    _ = drop_gen(Ref, DbName, DocId, AttName, Gen),
    ok;
abort_stream(_) ->
    ok.

%% @doc Open a stream for reading an attachment. The stream reads from
%% one snapshot, released at eof, on error and by close_stream/1;
%% callers must close a stream they stop reading early (an unclosed
%% one holds its snapshot until it is garbage collected).
-spec get_stream(att_ref(), db_name(), docid(), binary()) ->
    {ok, att_stream()} | {error, term()}.
get_stream(#{ref := Ref} = AttRef, DbName, DocId, AttName) ->
    {ok, Snap} = rocksdb:snapshot(Ref),
    ReadOpts = [{snapshot, Snap}],
    Found = lookup(Ref, ReadOpts, DbName, DocId, AttName),
    Base = #{type => read, att_ref => AttRef, db_name => DbName,
             doc_id => DocId, att_name => AttName, chunk_index => 0},
    case {Found, info_of(Found, AttName)} of
        {{ok, Key, {chunked, Meta}}, {ok, #{chunk_count := Count} = Info}} ->
            {ok, Base#{info => Info, chunk_count => Count, key => Key,
                       meta => Meta, snapshot => Snap,
                       read_opts => ReadOpts}};
        {{ok, _Key, {inline, Value}}, {ok, Info}} ->
            %% Single value - wrap as single-chunk stream
            release(Snap),
            {ok, Base#{info => Info, chunk_count => 1, single_value => true,
                       value => Value}};
        {_, {error, _} = Error} ->
            release(Snap),
            Error
    end.

%% @doc Move a fresh read stream to the chunk holding byte Offset.
%% Returns the bytes to drop from that chunk.
-spec seek_stream(att_stream(), non_neg_integer()) ->
    {ok, att_stream(), non_neg_integer()}.
seek_stream(#{single_value := true} = Stream, Offset) ->
    {ok, Stream, Offset};
seek_stream(#{info := #{chunk_size := ChunkSize}, chunk_count := Count} = Stream,
            Offset) ->
    Index = min(Offset div ChunkSize, Count),
    {ok, Stream#{chunk_index => Index}, Offset - Index * ChunkSize}.

%% @doc Read the next chunk from a stream
-spec read_chunk(att_stream()) -> {ok, binary(), att_stream()} | eof | {error, term()}.
read_chunk(#{chunk_index := Index, chunk_count := Count} = Stream)
  when Index >= Count ->
    ok = close_stream(Stream),
    eof;
read_chunk(#{single_value := true, value := Value} = Stream) ->
    {ok, Value, Stream#{chunk_index => 1}};
read_chunk(#{att_ref := #{ref := Ref}, key := Key, meta := Meta,
             read_opts := ReadOpts, chunk_index := Index} = Stream) ->
    case read_one(Ref, ReadOpts, Key, Meta, Index) of
        {ok, Chunk} ->
            {ok, Chunk, Stream#{chunk_index => Index + 1}};
        {error, _} = Error ->
            ok = close_stream(Stream),
            Error
    end.

%% @doc Close a stream, releasing its snapshot (safe to repeat).
-spec close_stream(att_stream()) -> ok.
close_stream(#{snapshot := Snap}) ->
    release(Snap);
close_stream(_Stream) ->
    ok.
