%%%-------------------------------------------------------------------
%%% @doc RocksDB storage backend for barrel_docdb
%%%
%%% Implements the barrel_store behaviour using RocksDB 2.4.0.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_store_rocksdb).
-behaviour(barrel_store).

%% barrel_store callbacks
-export([open/2, close/1]).
-export([put/3, put/4, get/2, key_exists/2, multi_key_exists/2, multi_get/2, delete/2]).
-export([merge/3]).
-export([write_batch/2, write_batch/3]).
-export([fold/4, fold_range/5, fold_range/6, fold_range_reverse/5, fold_range_reverse/6]).
-export([fold_range_with_snapshot/6, fold_range_prefix_with_snapshot/6]).
-export([fold_range_long_scan/5]).
-export([fold_range_limit/6]).  %% Auto-selects read profile based on limit

%% Additional utilities
-export([snapshot/1, release_snapshot/1, safe_release_snapshot/1]).
-export([get_with_snapshot/3, multi_get_with_snapshot/3]).
-export([fold_range_posting_with_snapshot/6]).

%% Posting list operations (using erlang_merge_operator)
-export([posting_append/3, posting_remove/3]).
-export([posting_get/2, posting_get_binary/2, posting_multi_get/2]).
-export([fold_range_posting/5, fold_range_posting_reverse/5]).
-export([fold_range_posting_prefix/5, fold_range_posting_prefix_with_snapshot/6]).
-export([fold_range_posting_compare/5, fold_range_posting_compare_with_snapshot/6]).

%% Body column family operations (BlobDB enabled)
-export([body_put/3, body_put/4, body_get/2, body_multi_get/2, body_multi_get/3, body_delete/2]).
-export([body_multi_get_with_snapshot/3, body_multi_get_with_snapshot/4]).

%% Local document column family operations (no revtree, config/state storage)
-export([local_put/3, local_get/2, local_delete/2, local_fold/4]).

%% Wide column (entity) operations
-export([put_entity/3, put_entity/4, get_entity/2, multi_get_entity/2, delete_entity/2]).
-export([batch_put_entity/3]).
-export([decode_entity/1, encode_entity/1]).  %% Decode/encode entity binary

%% Stats and capacity monitoring
-export([get_stats/1, get_db_size/1, get_property/2]).

%% Compaction
-export([compact_default_cf/1]).

%%====================================================================
%% Types
%%====================================================================

-type db_ref() :: #{
    ref := rocksdb:db_handle(),
    path := string(),
    default_cf := rocksdb:cf_handle(),
    bitmap_cf := rocksdb:cf_handle(),
    posting_cf := rocksdb:cf_handle(),
    body_cf := rocksdb:cf_handle(),
    local_cf := rocksdb:cf_handle()
}.

-type snapshot() :: rocksdb:snapshot_handle().

%% Read profiles for different query patterns
%% - point: Small result sets (limit <= 10), keep blocks in cache
%% - short_range: Medium result sets (limit <= 200), rely on auto-readahead
%% - long_scan: Large/unbounded scans, prefetch aggressively, avoid cache pollution
-type read_profile() :: point | short_range | long_scan.

-export_type([db_ref/0, snapshot/0, read_profile/0]).

%% Column family names
-define(BITMAP_CF_NAME, "bitmap").
-define(POSTING_CF_NAME, "posting").
-define(BODY_CF_NAME, "bodies").
-define(LOCAL_CF_NAME, "local").

%%====================================================================
%% barrel_store callbacks
%%====================================================================

%% @doc Open a RocksDB database with column families
%% Uses create_missing_column_families to auto-create any missing CFs on existing DBs.
-spec open(string(), map()) -> {ok, db_ref()} | {error, term()}.
open(Path, Options) ->
    ok = filelib:ensure_dir(Path ++ "/"),
    DbOpts = build_db_options(Options),
    BitmapSize = maps:get(bitmap_size, Options, 1048576),  %% 1M bits default

    %% Column family descriptors with their options
    CFDescriptors = [
        {"default", build_default_cf_options(Options)},
        {?BITMAP_CF_NAME, [{merge_operator, {bitset_merge_operator, BitmapSize}}]},
        {?POSTING_CF_NAME, build_posting_cf_options()},
        {?BODY_CF_NAME, build_body_cf_options(Options)},
        {?LOCAL_CF_NAME, []}  %% Simple KV storage for local docs (no merge operator)
    ],

    case rocksdb:open(Path, DbOpts, CFDescriptors) of
        {ok, Ref, [DefaultCF, BitmapCF, PostingCF, BodyCF, LocalCF]} ->
            {ok, #{ref => Ref, path => Path,
                   default_cf => DefaultCF, bitmap_cf => BitmapCF,
                   posting_cf => PostingCF, body_cf => BodyCF,
                   local_cf => LocalCF}};
        {error, Reason} ->
            {error, {db_open_failed, Reason}}
    end.

%% @doc Close the database
-spec close(db_ref()) -> ok.
close(#{ref := Ref}) ->
    rocksdb:close(Ref).

%% @doc Put a key-value pair
-spec put(db_ref(), binary(), binary()) -> ok | {error, term()}.
put(DbRef, Key, Value) ->
    put(DbRef, Key, Value, []).

%% @doc Put a key-value pair with options
-spec put(db_ref(), binary(), binary(), list()) -> ok | {error, term()}.
put(#{ref := Ref}, Key, Value, Opts) ->
    rocksdb:put(Ref, Key, Value, Opts).

%% @doc Get a value by key
-spec get(db_ref(), binary()) -> {ok, binary()} | not_found | {error, term()}.
get(#{ref := Ref}, Key) ->
    rocksdb:get(Ref, Key, []).

%% @doc Check if a key exists.
%% Uses rocksdb:get which leverages BloomFilter for fast negative lookups.
%% Much faster than iterator seek for single key checks.
-spec key_exists(db_ref(), binary()) -> boolean().
key_exists(#{ref := Ref}, Key) ->
    case rocksdb:get(Ref, Key, []) of
        {ok, _Value} -> true;
        not_found -> false;
        {error, _} -> false
    end.

%% @doc Check if multiple keys exist (batch existence check).
%% Uses multi_get internally and converts to boolean results.
%% Much faster than calling key_exists for each key individually.
-spec multi_key_exists(db_ref(), [binary()]) -> [boolean()].
multi_key_exists(#{ref := Ref}, Keys) ->
    Results = rocksdb:multi_get(Ref, Keys, []),
    [case R of {ok, _} -> true; _ -> false end || R <- Results].

%% @doc Get multiple values by keys (batch read)
-spec multi_get(db_ref(), [binary()]) -> [{ok, binary()} | not_found | {error, term()}].
multi_get(#{ref := Ref}, Keys) ->
    rocksdb:multi_get(Ref, Keys, []).

%% @doc Delete a key
-spec delete(db_ref(), binary()) -> ok | {error, term()}.
delete(#{ref := Ref}, Key) ->
    rocksdb:delete(Ref, Key, []).

%% @doc Merge a value with the counter merge operator
-spec merge(db_ref(), binary(), integer()) -> ok | {error, term()}.
merge(#{ref := Ref}, Key, Delta) ->
    rocksdb:merge(Ref, Key, integer_to_binary(Delta), []).

%% @doc Execute a batch of operations atomically (async by default)
-spec write_batch(db_ref(), list()) -> ok | {error, term()}.
write_batch(DbRef, Operations) ->
    write_batch(DbRef, Operations, #{}).

%% @doc Execute a batch of operations atomically with options
%% Options:
%%   - sync: boolean() - if true, sync to disk before returning (default: false)
%% Operations:
%%   - {put, Key, Value} - put to default CF
%%   - {delete, Key} - delete from default CF
%%   - {merge, Key, Delta} - merge counter in default CF
%%   - {posting_append, Key, DocId} - append DocId to posting list
%%   - {posting_remove, Key, DocId} - remove DocId from posting list
%%   - {body_put, Key, Value} - put to body CF (BlobDB)
%%   - {body_delete, Key} - delete from body CF
%%   - {entity_put, Key, Columns} - put wide-column entity to default CF
%%   - {entity_delete, Key} - delete entity from default CF
%%   - {local_put, Key, Value} - put to local CF (config/state)
%%   - {local_delete, Key} - delete from local CF
-spec write_batch(db_ref(), list(), map()) -> ok | {error, term()}.
write_batch(#{ref := Ref, posting_cf := PostingCF,
              body_cf := BodyCF, local_cf := LocalCF}, Operations, Opts) ->
    Sync = maps:get(sync, Opts, false),
    {ok, Batch} = rocksdb:batch(),
    try
        lists:foreach(
            fun({put, Key, Value}) ->
                ok = rocksdb:batch_put(Batch, Key, Value);
               ({delete, Key}) ->
                ok = rocksdb:batch_delete(Batch, Key);
               ({merge, Key, Delta}) when is_integer(Delta) ->
                ok = rocksdb:batch_merge(Batch, Key, integer_to_binary(Delta));
               ({posting_append, Key, DocId}) ->
                ok = rocksdb:batch_merge(Batch, PostingCF, Key, {posting_add, DocId});
               ({posting_remove, Key, DocId}) ->
                ok = rocksdb:batch_merge(Batch, PostingCF, Key, {posting_delete, DocId});
               ({body_put, Key, Value}) ->
                ok = rocksdb:batch_put(Batch, BodyCF, Key, Value);
               ({body_delete, Key}) ->
                ok = rocksdb:batch_delete(Batch, BodyCF, Key);
               ({entity_put, Key, Columns}) ->
                %% Encode columns to fixed binary format (fast decode)
                ok = rocksdb:batch_put(Batch, Key, encode_entity(Columns));
               ({entity_delete, Key}) ->
                ok = rocksdb:batch_delete(Batch, Key);
               ({local_put, Key, Value}) ->
                ok = rocksdb:batch_put(Batch, LocalCF, Key, Value);
               ({local_delete, Key}) ->
                ok = rocksdb:batch_delete(Batch, LocalCF, Key)
            end,
            Operations
        ),
        Result = rocksdb:write_batch(Ref, Batch, [{sync, Sync}]),
        Result
    after
        rocksdb:release_batch(Batch)
    end.

%% @doc Fold over all keys with a given prefix
-spec fold(db_ref(), binary(), fun(), term()) -> term().
fold(#{ref := Ref}, Prefix, Fun, Acc) ->
    PrefixEnd = prefix_end(Prefix),
    ReadOpts = [
        {iterate_lower_bound, Prefix},
        {iterate_upper_bound, PrefixEnd},
        {total_order_seek, true}
    ],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop(rocksdb:iterator_move(Itr, first), Itr, Fun, Acc)
    after
        rocksdb:iterator_close(Itr)
    end.

%% @doc Fold over a key range with default read options
-spec fold_range(db_ref(), binary(), binary(), fun(), term()) -> term().
fold_range(#{ref := Ref}, StartKey, EndKey, Fun, Acc) ->
    ReadOpts = [
        {iterate_lower_bound, StartKey},
        {iterate_upper_bound, EndKey},
        {total_order_seek, true}
    ],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop(rocksdb:iterator_move(Itr, first), Itr, Fun, Acc)
    after
        rocksdb:iterator_close(Itr)
    end.

%% @doc Fold over a key range with explicit read profile
%% Profile controls RocksDB read options for different access patterns.
%% Currently all profiles use defaults; infrastructure for future tuning.
-spec fold_range(db_ref(), binary(), binary(), fun(), term(), read_profile()) -> term().
fold_range(DbRef, StartKey, EndKey, Fun, Acc, Profile) ->
    ExtraOpts = read_profile_opts(Profile),
    fold_range_with_opts(DbRef, StartKey, EndKey, Fun, Acc, ExtraOpts).

%% @doc Fold over a key range in reverse order with default options
%% Useful for building sorted lists with prepend: [Item | Acc]
-spec fold_range_reverse(db_ref(), binary(), binary(), fun(), term()) -> term().
fold_range_reverse(#{ref := Ref}, StartKey, EndKey, Fun, Acc) ->
    ReadOpts = [
        {iterate_lower_bound, StartKey},
        {iterate_upper_bound, EndKey},
        {total_order_seek, true}
    ],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop_reverse(rocksdb:iterator_move(Itr, last), Itr, Fun, Acc)
    after
        rocksdb:iterator_close(Itr)
    end.

%% @doc Fold over a key range in reverse order with explicit read profile
-spec fold_range_reverse(db_ref(), binary(), binary(), fun(), term(), read_profile()) -> term().
fold_range_reverse(DbRef, StartKey, EndKey, Fun, Acc, Profile) ->
    ExtraOpts = read_profile_opts(Profile),
    fold_range_reverse_with_opts(DbRef, StartKey, EndKey, Fun, Acc, ExtraOpts).

%% @doc Fold over a key range with explicit extra options (internal)
-spec fold_range_with_opts(db_ref(), binary(), binary(), fun(), term(), list()) -> term().
fold_range_with_opts(#{ref := Ref}, StartKey, EndKey, Fun, Acc, ExtraOpts) ->
    BaseOpts = [
        {iterate_lower_bound, StartKey},
        {iterate_upper_bound, EndKey},
        {total_order_seek, true}
    ],
    ReadOpts = BaseOpts ++ ExtraOpts,
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop(rocksdb:iterator_move(Itr, first), Itr, Fun, Acc)
    after
        rocksdb:iterator_close(Itr)
    end.

%% @doc Fold over a key range in reverse with explicit extra options (internal)
-spec fold_range_reverse_with_opts(db_ref(), binary(), binary(), fun(), term(), list()) -> term().
fold_range_reverse_with_opts(#{ref := Ref}, StartKey, EndKey, Fun, Acc, ExtraOpts) ->
    BaseOpts = [
        {iterate_lower_bound, StartKey},
        {iterate_upper_bound, EndKey},
        {total_order_seek, true}
    ],
    ReadOpts = BaseOpts ++ ExtraOpts,
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop_reverse(rocksdb:iterator_move(Itr, last), Itr, Fun, Acc)
    after
        rocksdb:iterator_close(Itr)
    end.

%%====================================================================
%% Snapshot Operations
%%====================================================================

%% @doc Create a snapshot for consistent reads
-spec snapshot(db_ref()) -> {ok, snapshot()} | {error, term()}.
snapshot(#{ref := Ref}) ->
    rocksdb:snapshot(Ref).

%% @doc Release a snapshot
-spec release_snapshot(snapshot()) -> ok.
release_snapshot(Snapshot) ->
    rocksdb:release_snapshot(Snapshot).

%% @doc Release a snapshot, swallowing any error.
%% Use on cleanup paths where the handle may already be gone (process
%% termination, cursor expiry) and crashing is undesirable.
-spec safe_release_snapshot(snapshot() | undefined) -> ok.
safe_release_snapshot(undefined) -> ok;
safe_release_snapshot(Snapshot) ->
    _ = try rocksdb:release_snapshot(Snapshot)
        catch _:_ -> ok
        end,
    ok.

%% @doc Get with a snapshot for consistent reads
-spec get_with_snapshot(db_ref(), binary(), snapshot()) ->
    {ok, binary()} | not_found | {error, term()}.
get_with_snapshot(#{ref := Ref}, Key, Snapshot) ->
    rocksdb:get(Ref, Key, [{snapshot, Snapshot}]).

%% @doc Multi-get with a snapshot for consistent reads
-spec multi_get_with_snapshot(db_ref(), [binary()], snapshot()) ->
    [{ok, binary()} | not_found | {error, term()}].
multi_get_with_snapshot(#{ref := Ref}, Keys, Snapshot) ->
    rocksdb:multi_get(Ref, Keys, [{snapshot, Snapshot}]).

%% @doc Fold over a key range with snapshot for consistent reads
%% The fold function receives (Key, Value, Acc) and should return:
%%   {ok, NewAcc} - continue iteration
%%   {stop, FinalAcc} - stop iteration early
-spec fold_range_with_snapshot(db_ref(), binary(), binary(), fun(), term(), snapshot()) -> term().
fold_range_with_snapshot(#{ref := Ref}, StartKey, EndKey, Fun, Acc0, Snapshot) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey},
            {total_order_seek, true},
            {snapshot, Snapshot}],
    {ok, Iter} = rocksdb:iterator(Ref, Opts),
    try
        fold_loop(rocksdb:iterator_move(Iter, first), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

%% @doc Fold over a key range with prefix bloom optimization and snapshot
%% Uses prefix_same_as_start=true to stop at prefix boundary and
%% total_order_seek=false to enable prefix bloom filter.
%% Use this for prefix queries where all keys share a common prefix (e.g., value_index scans).
-spec fold_range_prefix_with_snapshot(db_ref(), binary(), binary(), fun(), term(), snapshot()) -> term().
fold_range_prefix_with_snapshot(#{ref := Ref}, StartKey, EndKey, Fun, Acc0, Snapshot) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey},
            {prefix_same_as_start, true},
            {total_order_seek, false},
            {snapshot, Snapshot}],
    {ok, Iter} = rocksdb:iterator(Ref, Opts),
    try
        fold_loop(rocksdb:iterator_move(Iter, first), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

%% @doc Fold over a key range optimized for long sequential scans
%% Uses readahead_size for prefetching and fill_cache=false to avoid cache pollution.
%% Best for scanning large datasets like full changes feed.
-spec fold_range_long_scan(db_ref(), binary(), binary(), fun(), term()) -> term().
fold_range_long_scan(#{ref := Ref}, StartKey, EndKey, Fun, Acc0) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey},
            {total_order_seek, true},
            {readahead_size, 2 * 1024 * 1024},  %% 2MB readahead
            {fill_cache, false}],               %% Don't pollute block cache
    {ok, Iter} = rocksdb:iterator(Ref, Opts),
    try
        fold_loop(rocksdb:iterator_move(Iter, first), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

%% @doc Fold over a key range with auto-selected read profile based on limit.
%% Automatically chooses appropriate RocksDB read options:
%% - Limit =&lt; 10: point profile (fill_cache=true)
%% - Limit =&lt; 100: short_range profile (fill_cache=true)
%% - Limit &gt; 100 or infinity: long_scan profile (fill_cache=false, readahead)
%%
%% This is the recommended function for range queries with known limits.
-spec fold_range_limit(db_ref(), binary(), binary(), fun(), term(),
                       non_neg_integer() | infinity) -> term().
fold_range_limit(DbRef, StartKey, EndKey, Fun, Acc, Limit) ->
    Profile = select_profile(Limit),
    fold_range(DbRef, StartKey, EndKey, Fun, Acc, Profile).

%% @doc Select read profile based on expected result size
-spec select_profile(non_neg_integer() | infinity) -> read_profile().
select_profile(L) when is_integer(L), L =< 10 -> point;
select_profile(L) when is_integer(L), L =< 100 -> short_range;
select_profile(_) -> long_scan.

%% @doc Fold over posting list entries with snapshot for consistent reads
-spec fold_range_posting_with_snapshot(db_ref(), binary(), binary(), fun(), term(), snapshot()) -> term().
fold_range_posting_with_snapshot(#{ref := Ref, posting_cf := PostingCF}, StartKey, EndKey, Fun, Acc0, Snapshot) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey},
            {snapshot, Snapshot}],
    {ok, Iter} = rocksdb:iterator(Ref, PostingCF, Opts),
    try
        fold_posting_loop(rocksdb:iterator_move(Iter, first), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Convert read profile to RocksDB read options
%% - point: Small result sets (limit =&lt; 10), keep blocks in cache
%% - short_range: Medium result sets (limit =&lt; 100), fill cache
%% - long_scan: Large/unbounded scans, prefetch aggressively, avoid cache pollution
%%
%% Cache Hygiene Rules:
%% - long_scan MUST use fill_cache=false to prevent cache eviction by exports/replication
%% - short_range/point MAY use fill_cache=true for faster repeated access
-spec read_profile_opts(read_profile()) -> list().
read_profile_opts(point) ->
    [{fill_cache, true}];
read_profile_opts(short_range) ->
    [{fill_cache, true}];  %% Let RocksDB use automatic readahead
read_profile_opts(long_scan) ->
    [{fill_cache, false}, {readahead_size, 2 * 1024 * 1024}].
    %% async_io is behind feature flag - disabled by default
    %% Must benchmark under concurrent writers before enabling

%% Build RocksDB options from config
build_db_options(Options) ->
    WriteBufferSize = maps:get(write_buffer_size, Options, 64 * 1024 * 1024),
    Schedulers = erlang:system_info(schedulers),

    %% Block-based table options with shared cache and bloom filters
    BlockOpts = barrel_cache:get_block_opts(#{
        bloom_bits => maps:get(bloom_bits, Options, 10),
        block_size => maps:get(block_size, Options, 4096)
    }),

    BaseOpts = [
        %% Auto-create missing column families on existing databases
        {create_missing_column_families, true},
        {create_if_missing, true},
        {max_open_files, maps:get(max_open_files, Options, 1000)},

        %% Write buffer configuration
        {write_buffer_size, WriteBufferSize},
        {max_write_buffer_number, maps:get(max_write_buffer_number, Options, 3)},
        {min_write_buffer_number_to_merge, maps:get(min_write_buffer_number_to_merge, Options, 1)},

        %% Compression - snappy for all levels (zstd optional if available)
        {compression, maps:get(compression, Options, snappy)},
        {bottommost_compression, maps:get(bottommost_compression, Options, snappy)},

        %% Concurrency
        {allow_concurrent_memtable_write, true},
        {enable_write_thread_adaptive_yield, true},
        {enable_pipelined_write, true},

        %% Compaction tuning
        {level0_file_num_compaction_trigger, maps:get(l0_compaction_trigger, Options, 4)},
        {level0_slowdown_writes_trigger, maps:get(l0_slowdown_trigger, Options, 20)},
        {level0_stop_writes_trigger, maps:get(l0_stop_trigger, Options, 36)},
        {max_background_jobs, maps:get(max_background_jobs, Options, Schedulers)},
        {max_subcompactions, maps:get(max_subcompactions, Options, 4)},

        %% Prefix extractor for prefix bloom filters
        %% Enables O(1) existence check for key prefixes (up to 64 bytes)
        {prefix_extractor, {capped_prefix_transform, 64}},

        %% Use counter merge operator for atomic counters
        {merge_operator, counter_merge_operator},
        {total_threads, erlang:max(4, Schedulers)},

        %% Block-based table with bloom filters and shared cache
        {block_based_table_options, BlockOpts}
    ],

    %% Optional rate limiter for production workloads
    case maps:get(rate_limit_bytes_per_sec, Options, 0) of
        0 -> BaseOpts;
        RateLimit ->
            case rocksdb:new_rate_limiter(RateLimit, false) of
                {ok, Limiter} -> [{rate_limiter, Limiter} | BaseOpts];
                _ -> BaseOpts
            end
    end.

%% Compute the end of a prefix range (for iteration bounds)
prefix_end(Prefix) ->
    case Prefix of
        <<>> ->
            <<16#FF>>;
        _ ->
            Len = byte_size(Prefix),
            LastByte = binary:last(Prefix),
            if
                LastByte < 16#FF ->
                    Init = binary:part(Prefix, 0, Len - 1),
                    <<Init/binary, (LastByte + 1)>>;
                true ->
                    <<Prefix/binary, 16#FF>>
            end
    end.

%% Iterator fold loop
fold_loop({ok, Key, Value}, Itr, Fun, Acc) ->
    case Fun(Key, Value, Acc) of
        {ok, Acc1} ->
            fold_loop(rocksdb:iterator_move(Itr, next), Itr, Fun, Acc1);
        {stop, Acc1} ->
            Acc1;
        stop ->
            Acc
    end;
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc) ->
    Acc;
fold_loop({error, _Reason}, _Itr, _Fun, Acc) ->
    Acc.

%% Fold loop for reverse iteration (uses prev instead of next)
fold_loop_reverse({ok, Key, Value}, Itr, Fun, Acc) ->
    case Fun(Key, Value, Acc) of
        {ok, Acc1} ->
            fold_loop_reverse(rocksdb:iterator_move(Itr, prev), Itr, Fun, Acc1);
        {stop, Acc1} ->
            Acc1;
        stop ->
            Acc
    end;
fold_loop_reverse({error, invalid_iterator}, _Itr, _Fun, Acc) ->
    Acc;
fold_loop_reverse({error, _Reason}, _Itr, _Fun, Acc) ->
    Acc.

%%====================================================================
%% Posting List Operations
%%====================================================================

%% Uses rocksdb 2.4.0 posting_list_merge_operator for O(1) appends.
%% Tombstones automatically cleaned during compaction.

%% @doc Append a DocId to a posting list
-spec posting_append(db_ref(), binary(), binary()) -> ok | {error, term()}.
posting_append(#{ref := Ref, posting_cf := PostingCF}, Key, DocId) ->
    rocksdb:merge(Ref, PostingCF, Key, {posting_add, DocId}, []).

%% @doc Remove a DocId from a posting list using tombstone marker
-spec posting_remove(db_ref(), binary(), binary()) -> ok | {error, term()}.
posting_remove(#{ref := Ref, posting_cf := PostingCF}, Key, DocId) ->
    rocksdb:merge(Ref, PostingCF, Key, {posting_delete, DocId}, []).

%% @doc Get a posting list from the posting column family
%% Returns list of active DocIds (tombstoned entries are filtered)
-spec posting_get(db_ref(), binary()) -> {ok, [binary()]} | not_found | {error, term()}.
posting_get(#{ref := Ref, posting_cf := PostingCF}, Key) ->
    case rocksdb:get(Ref, PostingCF, Key, []) of
        {ok, Bin} -> {ok, rocksdb:posting_list_keys(Bin)};
        not_found -> not_found;
        {error, _} = Err -> Err
    end.

%% @doc Get raw posting list binary from the posting column family.
%% Returns the binary without decoding, for use with postings_open/1.
-spec posting_get_binary(db_ref(), binary()) -> {ok, binary()} | not_found | {error, term()}.
posting_get_binary(#{ref := Ref, posting_cf := PostingCF}, Key) ->
    rocksdb:get(Ref, PostingCF, Key, []).

%% @doc Get multiple posting lists from the posting column family (batch read)
-spec posting_multi_get(db_ref(), [binary()]) -> [{ok, [binary()]} | not_found | {error, term()}].
posting_multi_get(#{ref := Ref, posting_cf := PostingCF}, Keys) ->
    Results = rocksdb:multi_get(Ref, PostingCF, Keys, []),
    [case R of
        {ok, Bin} -> {ok, rocksdb:posting_list_keys(Bin)};
        Other -> Other
    end || R <- Results].

%% @doc Fold over posting list entries in a range
-spec fold_range_posting(db_ref(), binary(), binary(), fun(), term()) -> term().
fold_range_posting(#{ref := Ref, posting_cf := PostingCF}, StartKey, EndKey, Fun, Acc0) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey}],
    {ok, Iter} = rocksdb:iterator(Ref, PostingCF, Opts),
    try
        fold_posting_loop(rocksdb:iterator_move(Iter, first), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

%% @doc Fold over posting list entries with prefix bloom optimization
%% Uses prefix_same_as_start=true to stop at prefix boundary and
%% total_order_seek=false to enable prefix bloom filter.
%% Use this for prefix queries where all keys share a common prefix.
-spec fold_range_posting_prefix(db_ref(), binary(), binary(), fun(), term()) -> term().
fold_range_posting_prefix(#{ref := Ref, posting_cf := PostingCF}, StartKey, EndKey, Fun, Acc0) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey},
            {prefix_same_as_start, true},
            {total_order_seek, false}],
    {ok, Iter} = rocksdb:iterator(Ref, PostingCF, Opts),
    try
        fold_posting_loop(rocksdb:iterator_move(Iter, first), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

%% @doc Fold over posting list entries with prefix bloom and snapshot
-spec fold_range_posting_prefix_with_snapshot(db_ref(), binary(), binary(), fun(), term(), snapshot()) -> term().
fold_range_posting_prefix_with_snapshot(#{ref := Ref, posting_cf := PostingCF}, StartKey, EndKey, Fun, Acc0, Snapshot) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey},
            {prefix_same_as_start, true},
            {total_order_seek, false},
            {snapshot, Snapshot}],
    {ok, Iter} = rocksdb:iterator(Ref, PostingCF, Opts),
    try
        fold_posting_loop(rocksdb:iterator_move(Iter, first), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

%% @doc Fold over posting list entries for compare/range queries with bloom filter.
%% Uses total_order_seek=false to enable bloom filter optimization.
%% Unlike prefix fold, does NOT use prefix_same_as_start since range queries
%% may span multiple value prefixes (e.g., age > 50 spans 51, 52, 53...).
-spec fold_range_posting_compare(db_ref(), binary(), binary(), fun(), term()) -> term().
fold_range_posting_compare(#{ref := Ref, posting_cf := PostingCF}, StartKey, EndKey, Fun, Acc0) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey},
            {total_order_seek, false},
            {readahead_size, 1048576}],  %% 1MB readahead for sequential range scans
    {ok, Iter} = rocksdb:iterator(Ref, PostingCF, Opts),
    try
        fold_posting_loop(rocksdb:iterator_move(Iter, first), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

%% @doc Fold over posting list entries for compare/range queries with snapshot.
%% Same as fold_range_posting_compare but uses a snapshot for consistent reads.
-spec fold_range_posting_compare_with_snapshot(db_ref(), binary(), binary(), fun(), term(), snapshot()) -> term().
fold_range_posting_compare_with_snapshot(#{ref := Ref, posting_cf := PostingCF}, StartKey, EndKey, Fun, Acc0, Snapshot) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey},
            {snapshot, Snapshot},
            {total_order_seek, false},
            {readahead_size, 1048576}],
    {ok, Iter} = rocksdb:iterator(Ref, PostingCF, Opts),
    try
        fold_posting_loop(rocksdb:iterator_move(Iter, first), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

fold_posting_loop({error, invalid_iterator}, _Iter, _Fun, Acc) ->
    Acc;
fold_posting_loop({ok, Key, Value}, Iter, Fun, Acc) ->
    DocIds = rocksdb:posting_list_keys(Value),
    case Fun(Key, DocIds, Acc) of
        {ok, NewAcc} ->
            fold_posting_loop(rocksdb:iterator_move(Iter, next), Iter, Fun, NewAcc);
        {stop, FinalAcc} ->
            FinalAcc
    end.

%% @doc Fold over posting list entries in a range in reverse order
-spec fold_range_posting_reverse(db_ref(), binary(), binary(), fun(), term()) -> term().
fold_range_posting_reverse(#{ref := Ref, posting_cf := PostingCF}, StartKey, EndKey, Fun, Acc0) ->
    Opts = [{iterate_lower_bound, StartKey},
            {iterate_upper_bound, EndKey}],
    {ok, Iter} = rocksdb:iterator(Ref, PostingCF, Opts),
    try
        fold_posting_loop_reverse(rocksdb:iterator_move(Iter, last), Iter, Fun, Acc0)
    after
        rocksdb:iterator_close(Iter)
    end.

fold_posting_loop_reverse({error, invalid_iterator}, _Iter, _Fun, Acc) ->
    Acc;
fold_posting_loop_reverse({ok, Key, Value}, Iter, Fun, Acc) ->
    DocIds = rocksdb:posting_list_keys(Value),
    case Fun(Key, DocIds, Acc) of
        {ok, NewAcc} ->
            fold_posting_loop_reverse(rocksdb:iterator_move(Iter, prev), Iter, Fun, NewAcc);
        {stop, FinalAcc} ->
            FinalAcc;
        NewAcc ->
            %% Support non-wrapped return values
            fold_posting_loop_reverse(rocksdb:iterator_move(Iter, prev), Iter, Fun, NewAcc)
    end.

%%====================================================================
%% Body Column Family Operations (BlobDB enabled)
%%====================================================================

%% @doc Build column family options for default CF (document entities)
%% Configures compaction filter for revision tree pruning if handler is provided.
-spec build_default_cf_options(map()) -> list().
build_default_cf_options(Options) ->
    BaseOpts = [{merge_operator, counter_merge_operator}],
    case maps:get(compaction_filter_handler, Options, undefined) of
        undefined ->
            BaseOpts;
        Pid when is_pid(Pid) ->
            CompactionFilter = #{
                handler => Pid,
                batch_size => maps:get(compaction_filter_batch_size, Options, 100),
                timeout => maps:get(compaction_filter_timeout, Options, 5000)
            },
            [{compaction_filter, CompactionFilter} | BaseOpts]
    end.

%% @doc Build column family options for document bodies with BlobDB enabled
%% Bodies are stored as blobs for values larger than min_blob_size (4KB default).
%% This keeps the LSM tree lean and enables efficient garbage collection.
-spec build_body_cf_options(map()) -> list().
build_body_cf_options(Options) ->
    [
        {enable_blob_files, true},
        {min_blob_size, maps:get(min_blob_size, Options, 4096)},        % 4KB threshold
        {blob_file_size, maps:get(blob_file_size, Options, 268435456)}, % 256MB
        {blob_compression_type, maps:get(blob_compression, Options, snappy)},
        {enable_blob_garbage_collection, true},
        {blob_garbage_collection_age_cutoff,
            maps:get(blob_gc_age_cutoff, Options, 0.25)},
        {blob_garbage_collection_force_threshold,
            maps:get(blob_gc_force_threshold, Options, 0.5)}
    ].

%% @doc Build column family options for posting lists (path index)
%% Configures bloom filter, prefix extractor, and block cache for efficient range scans.
-spec build_posting_cf_options() -> list().
build_posting_cf_options() ->
    [
        {merge_operator, posting_list_merge_operator},
        %% Prefix extractor for bloom filter efficiency on path-based keys
        %% Key format: 0x14 | db_name_len(16) | db_name | encoded_path
        %% 32 bytes captures db prefix + field name for most queries
        {prefix_extractor, {capped_prefix_transform, 32}},
        %% Block-based table options with bloom filter
        {block_based_table_options, [
            {filter_policy, {bloom, 10}},
            {cache_index_and_filter_blocks, true},
            {pin_l0_filter_and_index_blocks_in_cache, true}
        ]},
        %% Optimize for range scans (not just point lookups)
        {optimize_filters_for_hits, false}
    ].

%% @doc Put a value in the body column family
-spec body_put(db_ref(), binary(), binary()) -> ok | {error, term()}.
body_put(DbRef, Key, Value) ->
    body_put(DbRef, Key, Value, []).

%% @doc Put a value in the body column family with options
-spec body_put(db_ref(), binary(), binary(), list()) -> ok | {error, term()}.
body_put(#{ref := Ref, body_cf := BodyCF}, Key, Value, Opts) ->
    rocksdb:put(Ref, BodyCF, Key, Value, Opts).

%% @doc Get a value from the body column family
-spec body_get(db_ref(), binary()) -> {ok, binary()} | not_found | {error, term()}.
body_get(#{ref := Ref, body_cf := BodyCF}, Key) ->
    rocksdb:get(Ref, BodyCF, Key, []).

%% @doc Get multiple values from the body column family (batch read)
%% Uses short_range profile by default.
-spec body_multi_get(db_ref(), [binary()]) -> [{ok, binary()} | not_found | {error, term()}].
body_multi_get(DbRef, Keys) ->
    body_multi_get(DbRef, Keys, short_range).

%% @doc Get multiple values from body CF with explicit read profile.
%% BlobDB has different I/O characteristics than LSM - use appropriate profile:
%% - point: Small batches (&lt;50 keys), cache friendly
%% - short_range: Medium batches (50-200 keys), auto readahead
%% - long_scan: Large batches (&gt;200 keys), avoid cache pollution, explicit readahead
-spec body_multi_get(db_ref(), [binary()], read_profile()) ->
    [{ok, binary()} | not_found | {error, term()}].
body_multi_get(#{ref := Ref, body_cf := BodyCF}, Keys, Profile) ->
    Opts = body_read_profile_opts(Profile),
    rocksdb:multi_get(Ref, BodyCF, Keys, Opts).

%% @doc Get multiple values from body CF with snapshot for consistent reads.
%% Uses short_range profile by default.
-spec body_multi_get_with_snapshot(db_ref(), [binary()], snapshot()) ->
    [{ok, binary()} | not_found | {error, term()}].
body_multi_get_with_snapshot(DbRef, Keys, Snapshot) ->
    body_multi_get_with_snapshot(DbRef, Keys, Snapshot, short_range).

%% @doc Get multiple values from body CF with snapshot and explicit read profile.
-spec body_multi_get_with_snapshot(db_ref(), [binary()], snapshot(), read_profile()) ->
    [{ok, binary()} | not_found | {error, term()}].
body_multi_get_with_snapshot(#{ref := Ref, body_cf := BodyCF}, Keys, Snapshot, Profile) ->
    BaseOpts = body_read_profile_opts(Profile),
    Opts = [{snapshot, Snapshot} | BaseOpts],
    rocksdb:multi_get(Ref, BodyCF, Keys, Opts).

%% @doc Read profile options for body column family (BlobDB).
%% BlobDB stores values in separate blob files, so prefetching is beneficial
%% for sequential access patterns.
-spec body_read_profile_opts(read_profile()) -> list().
body_read_profile_opts(Profile) ->
    BaseOpts = body_read_profile_opts_base(Profile),
    case application:get_env(barrel_docdb, body_async_io, false) of
        true -> [{async_io, true} | BaseOpts];
        false -> BaseOpts
    end.

body_read_profile_opts_base(point) ->
    %% Small batch: use cache for faster repeated access
    [{fill_cache, true}];
body_read_profile_opts_base(short_range) ->
    %% Medium batch: use cache, let RocksDB auto-tune readahead
    [{fill_cache, true}, {auto_readahead_size, true}];
body_read_profile_opts_base(long_scan) ->
    %% Large batch: avoid cache pollution, explicit 2MB prefetch
    [{fill_cache, false}, {readahead_size, 2 * 1024 * 1024}].

%% @doc Delete a value from the body column family
-spec body_delete(db_ref(), binary()) -> ok | {error, term()}.
body_delete(#{ref := Ref, body_cf := BodyCF}, Key) ->
    rocksdb:delete(Ref, BodyCF, Key, []).

%% @doc Trigger compaction on the default column family
%% This runs the compaction filter which prunes old revisions.
%% Flushes memtable first, then forces full compaction to ensure filter runs.
-spec compact_default_cf(db_ref()) -> ok | {error, term()}.
compact_default_cf(#{ref := Ref, default_cf := DefaultCF}) ->
    %% Flush to ensure data is in SST files
    ok = rocksdb:flush(Ref, DefaultCF, []),
    %% Force compaction to bottommost level to ensure filter processes all data
    rocksdb:compact_range(Ref, DefaultCF, undefined, undefined,
                          [{bottommost_level_compaction, force}]).

%%====================================================================
%% Local Document Column Family Operations
%%====================================================================
%% Local documents are stored without revision trees.
%% Used for configuration, replication state, and other non-replicated data.
%% Key format: DbName + 0 + DocId (per-database) or "_system" + 0 + DocId (global)

%% @doc Put a local document
-spec local_put(db_ref(), binary(), binary()) -> ok | {error, term()}.
local_put(#{ref := Ref, local_cf := LocalCF}, Key, Value) ->
    rocksdb:put(Ref, LocalCF, Key, Value, []).

%% @doc Get a local document
-spec local_get(db_ref(), binary()) -> {ok, binary()} | not_found | {error, term()}.
local_get(#{ref := Ref, local_cf := LocalCF}, Key) ->
    rocksdb:get(Ref, LocalCF, Key, []).

%% @doc Delete a local document
-spec local_delete(db_ref(), binary()) -> ok | {error, term()}.
local_delete(#{ref := Ref, local_cf := LocalCF}, Key) ->
    rocksdb:delete(Ref, LocalCF, Key, []).

%% @doc Fold over local documents with a prefix
%% Useful for listing all local docs for a database or all system docs.
-spec local_fold(db_ref(), binary(), fun((binary(), binary(), term()) -> term()), term()) -> term().
local_fold(#{ref := Ref, local_cf := LocalCF}, Prefix, Fun, Acc0) ->
    {ok, Itr} = rocksdb:iterator(Ref, LocalCF, []),
    try
        do_local_fold(Itr, Prefix, byte_size(Prefix), Fun, Acc0)
    after
        rocksdb:iterator_close(Itr)
    end.

do_local_fold(Itr, Prefix, PrefixLen, Fun, Acc) ->
    case rocksdb:iterator_move(Itr, Prefix) of
        {ok, Key, Value} ->
            case Key of
                <<Prefix:PrefixLen/binary, _/binary>> ->
                    Acc1 = Fun(Key, Value, Acc),
                    do_local_fold_next(Itr, Prefix, PrefixLen, Fun, Acc1);
                _ ->
                    Acc
            end;
        {error, invalid_iterator} ->
            Acc
    end.

do_local_fold_next(Itr, Prefix, PrefixLen, Fun, Acc) ->
    case rocksdb:iterator_move(Itr, next) of
        {ok, Key, Value} ->
            case Key of
                <<Prefix:PrefixLen/binary, _/binary>> ->
                    Acc1 = Fun(Key, Value, Acc),
                    do_local_fold_next(Itr, Prefix, PrefixLen, Fun, Acc1);
                _ ->
                    Acc
            end;
        {error, invalid_iterator} ->
            Acc
    end.

%%====================================================================
%% Wide Column (Entity) Operations
%%====================================================================

%% @doc Put an entity in the default column family
%% Columns is a list of {Name, Value} tuples where Name and Value are binaries.
%% Uses fixed binary format encoding for performance.
-spec put_entity(db_ref(), binary(), [{binary(), binary()}]) -> ok | {error, term()}.
put_entity(DbRef, Key, Columns) ->
    put_entity(DbRef, Key, Columns, []).

%% @doc Put an entity with options
%% Uses fixed binary format encoding for performance.
-spec put_entity(db_ref(), binary(), [{binary(), binary()}], list()) -> ok | {error, term()}.
put_entity(#{ref := Ref}, Key, Columns, Opts) ->
    rocksdb:put(Ref, Key, encode_entity(Columns), Opts).

%% Entity column names (matching barrel_db_server)
-define(COL_REV, <<"rev">>).
-define(COL_DELETED, <<"del">>).
-define(COL_HLC, <<"hlc">>).
-define(COL_REVTREE, <<"revtree">>).
%% TTL/Tier columns (added in v2)
-define(COL_CREATED_AT, <<"created_at">>).
-define(COL_EXPIRES_AT, <<"expires_at">>).
-define(COL_TIER, <<"tier">>).

%% @doc Encode entity columns to fixed binary format
%% Format v2 (with TTL/tier extension):
%%   `&lt;&lt;RevLen:16, Rev/binary, Deleted:8, HlcLen:16, Hlc/binary, TreeLen:32, Tree/binary,
%%     CreatedAtLen:16, CreatedAt/binary, ExpiresAt:64, Tier:8&gt;&gt;'
%% Backward compatible: old data without extension uses defaults.
-spec encode_entity([{binary(), term()}]) -> binary().
encode_entity(Columns) ->
    Rev = proplists:get_value(?COL_REV, Columns, <<>>),
    Del = proplists:get_value(?COL_DELETED, Columns, <<"false">>),
    Hlc = proplists:get_value(?COL_HLC, Columns, <<>>),
    %% Tree is already term_to_binary'd by barrel_db_server
    TreeBin = proplists:get_value(?COL_REVTREE, Columns, <<>>),
    %% TTL/Tier fields (v2)
    CreatedAt = proplists:get_value(?COL_CREATED_AT, Columns, <<>>),
    ExpiresAt = proplists:get_value(?COL_EXPIRES_AT, Columns, 0),
    Tier = proplists:get_value(?COL_TIER, Columns, 0),

    DelByte = case Del of
        true -> 1;
        <<"true">> -> 1;
        _ -> 0
    end,

    TierByte = case Tier of
        hot -> 0;
        warm -> 1;
        cold -> 2;
        N when is_integer(N) -> N;
        _ -> 0
    end,

    <<(byte_size(Rev)):16, Rev/binary,
      DelByte:8,
      (byte_size(Hlc)):16, Hlc/binary,
      (byte_size(TreeBin)):32, TreeBin/binary,
      (byte_size(CreatedAt)):16, CreatedAt/binary,
      ExpiresAt:64,
      TierByte:8>>.

%% @doc Decode entity from fixed binary format
%% Handles both v1 (without TTL/tier) and v2 (with TTL/tier) formats.
-spec decode_entity(binary()) -> [{binary(), term()}].
decode_entity(<<RevLen:16, Rev:RevLen/binary,
                DelByte:8,
                HlcLen:16, Hlc:HlcLen/binary,
                TreeLen:32, TreeBin:TreeLen/binary,
                Rest/binary>>) ->
    Del = case DelByte of 1 -> <<"true">>; 0 -> <<"false">> end,
    BaseColumns = [{?COL_REV, Rev}, {?COL_DELETED, Del}, {?COL_HLC, Hlc}, {?COL_REVTREE, TreeBin}],
    case Rest of
        <<CreatedAtLen:16, CreatedAt:CreatedAtLen/binary, ExpiresAt:64, TierByte:8>> ->
            %% V2 format with TTL/tier extension
            Tier = case TierByte of 0 -> hot; 1 -> warm; 2 -> cold; _ -> hot end,
            BaseColumns ++ [{?COL_CREATED_AT, CreatedAt}, {?COL_EXPIRES_AT, ExpiresAt}, {?COL_TIER, Tier}];
        <<>> ->
            %% V1 format (no extension) - use defaults
            BaseColumns ++ [{?COL_CREATED_AT, <<>>}, {?COL_EXPIRES_AT, 0}, {?COL_TIER, hot}];
        _ ->
            %% Unknown extension, ignore and use defaults
            BaseColumns ++ [{?COL_CREATED_AT, <<>>}, {?COL_EXPIRES_AT, 0}, {?COL_TIER, hot}]
    end.

%% @doc Get a wide-column entity from the default column family
%% Returns {ok, [{Name, Value}]} or not_found
%% Uses fast binary decoding instead of term_to_binary.
-spec get_entity(db_ref(), binary()) -> {ok, [{binary(), binary()}]} | not_found | {error, term()}.
get_entity(#{ref := Ref}, Key) ->
    case rocksdb:get(Ref, Key, []) of
        {ok, Bin} -> {ok, decode_entity(Bin)};
        not_found -> not_found;
        {error, _} = Err -> Err
    end.

%% @doc Get multiple wide-column entities (batch read)
%% Uses fast binary decoding instead of term_to_binary.
-spec multi_get_entity(db_ref(), [binary()]) -> [{ok, [{binary(), binary()}]} | not_found | {error, term()}].
multi_get_entity(#{ref := Ref}, Keys) ->
    Results = rocksdb:multi_get(Ref, Keys, []),
    [case R of
        {ok, Bin} -> {ok, decode_entity(Bin)};
        not_found -> not_found;
        {error, _} = Err -> Err
    end || R <- Results].

%% @doc Delete an entity
-spec delete_entity(db_ref(), binary()) -> ok | {error, term()}.
delete_entity(#{ref := Ref}, Key) ->
    rocksdb:delete(Ref, Key, []).

%% @doc Add entity put to a batch
%% Helper for building batch operations with entities
%% Uses fixed binary format encoding for performance.
-spec batch_put_entity(rocksdb:batch_handle(), binary(), [{binary(), binary()}]) -> ok.
batch_put_entity(Batch, Key, Columns) ->
    rocksdb:batch_put(Batch, Key, encode_entity(Columns)).

%%====================================================================
%% Stats and Capacity Monitoring
%%====================================================================

%% @doc Get RocksDB statistics
%% Returns all available statistics as a proplist.
-spec get_stats(db_ref()) -> {ok, list()} | {error, term()}.
get_stats(#{ref := Ref}) ->
    rocksdb:stats(Ref).

%% @doc Get a specific RocksDB property
%% Common properties:
%%   - "rocksdb.estimate-live-data-size" - estimated size of live data
%%   - "rocksdb.total-sst-files-size" - total size of SST files
%%   - "rocksdb.estimate-num-keys" - estimated number of keys
%%   - "rocksdb.cur-size-all-mem-tables" - current size of all memtables
-spec get_property(db_ref(), binary()) -> {ok, binary()} | {error, term()}.
get_property(#{ref := Ref}, Property) when is_binary(Property) ->
    rocksdb:get_property(Ref, Property).

%% @doc Get estimated database size in bytes
%% Uses RocksDB's estimate-live-data-size property as primary metric,
%% falling back to total-sst-files-size if not available.
-spec get_db_size(db_ref()) -> {ok, non_neg_integer()} | {error, term()}.
get_db_size(DbRef) ->
    %% Try estimate-live-data-size first (more accurate for active data)
    case get_property(DbRef, <<"rocksdb.estimate-live-data-size">>) of
        {ok, SizeBin} ->
            {ok, binary_to_integer(SizeBin)};
        {error, _} ->
            %% Fall back to total SST files size
            case get_property(DbRef, <<"rocksdb.total-sst-files-size">>) of
                {ok, SstSizeBin} ->
                    {ok, binary_to_integer(SstSizeBin)};
                {error, _} = Err ->
                    Err
            end
    end.

