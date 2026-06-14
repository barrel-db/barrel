%%%-------------------------------------------------------------------
%%% @doc Key encoding for barrel_docdb storage
%%%
%%% Provides functions to encode and decode keys for RocksDB storage.
%%% Keys are prefixed to enable efficient range scans.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_store_keys).

-include("barrel_docdb.hrl").

%% Database metadata keys
-export([db_meta/2, db_uid/1, db_docs_count/1, db_del_count/1, db_last_hlc/1]).

%% Document keys (legacy)
-export([doc_info/2, doc_info_prefix/1, doc_info_end/1]).
-export([doc_rev/3, doc_rev_prefix/2]).
-export([doc_hlc/2, doc_hlc_prefix/1, doc_hlc_end/1]).

%% Column-wide document keys (CBOR codec)
-export([doc_current/2, doc_current_prefix/1, doc_current_end/1]).
-export([doc_tree/2]).
-export([doc_body/2, doc_body_rev/3]).

%% Wide column document keys (entity storage)
-export([doc_entity/2, doc_entity_prefix/1, doc_entity_end/1]).

%% Local document keys (legacy - stored in default CF with prefix)
-export([local_doc/2]).

%% Local document keys for local_cf (new - no prefix needed)
-export([local_doc_key/2, local_doc_prefix/1]).
-export([system_doc_key/1, system_doc_prefix/0]).

%% View keys
-export([view_meta/2, view_seq/2, view_index/3, view_index_prefix/2, view_index_end/2]).
-export([view_by_docid/3, view_by_docid_prefix/3, view_by_docid_end/3]).
-export([encode_view_key/1, decode_view_key/1]).

%% Path index keys (old format: DocId in key)
-export([path_index_key/3, path_index_prefix/2, path_index_end/2]).
-export([doc_paths_key/2, doc_paths_prefix/1]).
-export([encode_path/1, decode_path/1]).

%% Posting list keys (new format: DocIds in value as posting list)
-export([path_posting_key/2, path_posting_prefix/2, path_posting_end/2]).

%% Value-first posting keys (for fast equality queries)
-export([value_posting_key/3, value_posting_prefix/2, value_posting_end/2]).
-export([truncate_value/1]).

%% Bucketed posting keys (split by DocId prefix for sorted iteration)
-export([value_posting_bucket_key/4, value_posting_bucket_prefix/3, value_posting_bucket_end/3]).
-export([docid_bucket/1]).

%% Key parsing (for compaction filter)
-export([parse_key/1]).

%% Value-first index keys (for iterable equality queries with early termination)
-export([value_index_key/4, value_index_prefix/3, value_index_end/3]).

%% Change bucket keys (for idle poll optimization)
-export([change_bucket/2, change_bucket_prefix/1, change_bucket_end/1]).

%% Path stats keys (for cardinality counters)
-export([path_stats_key/2]).

%% Path bitmap keys (for bitmap index)
-export([path_bitmap_key/2]).

%% Path-HLC keys (for path-indexed change feeds)
-export([path_hlc/3, path_hlc_prefix/2, path_hlc_end/2]).
-export([path_hlc_wildcard_start/2, path_hlc_wildcard_end/2]).  %% For # wildcard matching
-export([encode_topic/1, decode_path_hlc_key/2]).

%% Prefix changes posting list keys (sharded by time bucket)
%% Each bucket contains a sorted list of << HLC:12, Change/binary >>
-export([prefix_changes_key/3, prefix_changes_start/3, prefix_changes_end/3]).
-export([hlc_to_bucket/1]).

%% Attachment keys
-export([att_data/3, att_data_prefix/2]).


%% HLC encoding/decoding
-export([encode_hlc/1, decode_hlc/1, decode_hlc_key/2]).

%% Key decoding
-export([decode_doc_id/2, decode_doc_info_key/2]).

%%====================================================================
%% Key Prefixes - single byte for efficiency
%%====================================================================

%% Key type prefixes
-define(PREFIX_DB_META, 16#01).
-define(PREFIX_DOC_INFO, 16#02).
-define(PREFIX_DOC_REV, 16#03).
-define(PREFIX_LOCAL_DOC, 16#05).
-define(PREFIX_VIEW_META, 16#06).
-define(PREFIX_VIEW_SEQ, 16#07).
-define(PREFIX_VIEW_INDEX, 16#08).
-define(PREFIX_VIEW_BY_DOCID, 16#09).
-define(PREFIX_ATT, 16#0A).
-define(PREFIX_PATH_INDEX, 16#0B).
-define(PREFIX_DOC_PATHS, 16#0C).
-define(PREFIX_DOC_HLC, 16#0D).
-define(PREFIX_PATH_HLC, 16#0E).
-define(PREFIX_PATH_STATS, 16#0F).
-define(PREFIX_PATH_BITMAP, 16#10).
-define(PREFIX_PATH_POSTING, 16#14).  %% Posting lists: path → [DocId, ...]
-define(PREFIX_VALUE_POSTING, 16#15). %% Value-first posting: [value_prefix, path] → [DocId, ...]
-define(PREFIX_VALUE_POSTING_BUCKET, 16#19). %% Bucketed posting: [value_prefix, path, bucket] → [DocId, ...]
-define(PREFIX_VALUE_INDEX, 16#16).   %% Value-first index: [value_prefix, path, DocId] → marker (for iteration)
-define(PREFIX_CHANGE_BUCKET, 16#17). %% Change bucket: DbName + BucketTs → {min_hlc, max_hlc, count}
-define(PREFIX_DOC_ENTITY, 16#18).    %% Wide-column doc entity: DbName + DocId → entity with columns
-define(PREFIX_CHANGES, 16#1B).       %% Prefix changes posting: prefix + bucket → [HLC:12, change, ...]

%% Value prefix max length for value-first index (128 bytes)
-define(VALUE_PREFIX_MAX_LEN, 128).

%% Column-wide document storage prefixes (for CBOR codec integration)
-define(PREFIX_DOC_CURRENT, 16#11).  %% DbName + DocId → {rev, deleted, hlc}
-define(PREFIX_DOC_TREE, 16#12).     %% DbName + DocId → revtree (term_to_binary)
-define(PREFIX_DOC_BODY, 16#13).     %% DbName + DocId → CBOR body (current only)

%% Path component type tags (for ordered encoding)
-define(PATH_TYPE_NULL, 16#01).
-define(PATH_TYPE_FALSE, 16#02).
-define(PATH_TYPE_TRUE, 16#03).
-define(PATH_TYPE_NEG_INT, 16#10).  %% Negative integers
-define(PATH_TYPE_ZERO, 16#20).     %% Zero
-define(PATH_TYPE_POS_INT, 16#30).  %% Positive integers
-define(PATH_TYPE_FLOAT, 16#40).    %% Floats
-define(PATH_TYPE_BINARY, 16#50).   %% Binary strings

%% Meta key suffixes
-define(META_UID, <<"uid">>).
-define(META_DOCS_COUNT, <<"docs_count">>).
-define(META_DEL_COUNT, <<"del_count">>).
-define(META_LAST_HLC, <<"last_hlc">>).

%%====================================================================
%% Database Metadata Keys
%%====================================================================

%% @doc General database metadata key
-spec db_meta(db_name(), binary()) -> binary().
db_meta(DbName, MetaKey) ->
    <<?PREFIX_DB_META, (encode_name(DbName))/binary, $:, MetaKey/binary>>.

%% @doc Database UID key
-spec db_uid(db_name()) -> binary().
db_uid(DbName) ->
    db_meta(DbName, ?META_UID).

%% @doc Documents count key
-spec db_docs_count(db_name()) -> binary().
db_docs_count(DbName) ->
    db_meta(DbName, ?META_DOCS_COUNT).

%% @doc Deleted documents count key
-spec db_del_count(db_name()) -> binary().
db_del_count(DbName) ->
    db_meta(DbName, ?META_DEL_COUNT).

%% @doc Last HLC timestamp key
-spec db_last_hlc(db_name()) -> binary().
db_last_hlc(DbName) ->
    db_meta(DbName, ?META_LAST_HLC).

%%====================================================================
%% Document Keys
%%====================================================================

%% @doc Document info key (stores doc_info record)
-spec doc_info(db_name(), docid()) -> binary().
doc_info(DbName, DocId) ->
    <<?PREFIX_DOC_INFO, (encode_name(DbName))/binary, DocId/binary>>.

%% @doc Prefix for all doc_info keys in a database
-spec doc_info_prefix(db_name()) -> binary().
doc_info_prefix(DbName) ->
    <<?PREFIX_DOC_INFO, (encode_name(DbName))/binary>>.

%% @doc End marker for doc_info range scan
-spec doc_info_end(db_name()) -> binary().
doc_info_end(DbName) ->
    <<?PREFIX_DOC_INFO, (encode_name(DbName))/binary, 16#FF>>.

%% @doc Document revision key (stores document body)
-spec doc_rev(db_name(), docid(), revid()) -> binary().
doc_rev(DbName, DocId, RevId) ->
    <<?PREFIX_DOC_REV, (encode_name(DbName))/binary, DocId/binary, $:, RevId/binary>>.

%% @doc Prefix for all revisions of a document
-spec doc_rev_prefix(db_name(), docid()) -> binary().
doc_rev_prefix(DbName, DocId) ->
    <<?PREFIX_DOC_REV, (encode_name(DbName))/binary, DocId/binary, $:>>.

%% @doc Document HLC key (for changes feed with HLC ordering)
%% HLC timestamps are 12 bytes (8 wall_time + 4 logical)
-spec doc_hlc(db_name(), barrel_hlc:timestamp()) -> binary().
doc_hlc(DbName, HlcTS) ->
    <<?PREFIX_DOC_HLC, (encode_name(DbName))/binary, (encode_hlc(HlcTS))/binary>>.

%% @doc Prefix for all HLC keys
-spec doc_hlc_prefix(db_name()) -> binary().
doc_hlc_prefix(DbName) ->
    <<?PREFIX_DOC_HLC, (encode_name(DbName))/binary>>.

%% @doc End marker for HLC range scan
-spec doc_hlc_end(db_name()) -> binary().
doc_hlc_end(DbName) ->
    %% 12 bytes of 0xFF for max HLC
    <<?PREFIX_DOC_HLC, (encode_name(DbName))/binary,
      16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF,
      16#FF, 16#FF, 16#FF, 16#FF>>.

%%====================================================================
%% Path-HLC Keys (Path-Indexed Change Feeds)
%%====================================================================

%% @doc Path-HLC key for indexing changes by topic path.
%% Key format: prefix | db_name | topic (null-terminated) | hlc
%% Topic is an MQTT-style path like "users/123/name"
-spec path_hlc(db_name(), binary(), barrel_hlc:timestamp()) -> binary().
path_hlc(DbName, Topic, Hlc) ->
    <<?PREFIX_PATH_HLC, (encode_name(DbName))/binary,
      (encode_topic(Topic))/binary, (encode_hlc(Hlc))/binary>>.

%% @doc Prefix for scanning path_hlc entries for a specific topic.
%% Returns all changes under this topic since the beginning of time.
-spec path_hlc_prefix(db_name(), binary()) -> binary().
path_hlc_prefix(DbName, Topic) ->
    <<?PREFIX_PATH_HLC, (encode_name(DbName))/binary, (encode_topic(Topic))/binary>>.

%% @doc End marker for path_hlc range scan.
%% Use with path_hlc_prefix or path_hlc for bounded range scans.
-spec path_hlc_end(db_name(), binary()) -> binary().
path_hlc_end(DbName, Topic) ->
    %% Topic followed by max HLC (12 bytes of 0xFF)
    <<?PREFIX_PATH_HLC, (encode_name(DbName))/binary, (encode_topic(Topic))/binary,
      16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF,
      16#FF, 16#FF, 16#FF, 16#FF>>.

%% @doc Start key for wildcard topic prefix matching (# patterns).
%% Unlike path_hlc_prefix, this does NOT include the null terminator,
%% allowing the range to capture all topics that START with the prefix.
%% Example: prefix "users" matches topics "users", "users/123", "users/abc/def", etc.
-spec path_hlc_wildcard_start(db_name(), binary()) -> binary().
path_hlc_wildcard_start(DbName, TopicPrefix) ->
    <<?PREFIX_PATH_HLC, (encode_name(DbName))/binary, TopicPrefix/binary>>.

%% @doc End key for wildcard topic prefix matching (# patterns).
%% Use with path_hlc_wildcard_start for bounded range scans matching all
%% topics that start with the given prefix.
-spec path_hlc_wildcard_end(db_name(), binary()) -> binary().
path_hlc_wildcard_end(DbName, TopicPrefix) ->
    <<?PREFIX_PATH_HLC, (encode_name(DbName))/binary, TopicPrefix/binary, 16#FF>>.

%% @doc Encode topic for null-terminated storage.
%% Topics are MQTT-style paths like "users/123/name"
-spec encode_topic(binary()) -> binary().
encode_topic(Topic) when is_binary(Topic) ->
    <<Topic/binary, 0>>.

%% @doc Decode path_hlc key to extract topic and HLC.
%% Returns {Topic, Hlc} tuple.
-spec decode_path_hlc_key(db_name(), binary()) -> {binary(), barrel_hlc:timestamp()}.
decode_path_hlc_key(DbName, Key) ->
    %% Skip prefix and db_name
    NameLen = byte_size(DbName),
    PrefixLen = 1 + 2 + NameLen, %% PREFIX + 16-bit length + name
    <<_:PrefixLen/binary, Rest/binary>> = Key,
    %% Find null terminator to extract topic
    {Topic, HlcBin} = split_on_null(Rest),
    {Topic, decode_hlc(HlcBin)}.

%% @private Split binary on null byte
split_on_null(Bin) ->
    split_on_null(Bin, <<>>).

split_on_null(<<0, Rest/binary>>, Acc) ->
    {Acc, Rest};
split_on_null(<<B, Rest/binary>>, Acc) ->
    split_on_null(Rest, <<Acc/binary, B>>).

%%====================================================================
%% Prefix Changes Posting Lists (Sharded by Time Bucket)
%%====================================================================
%% Key format: PREFIX_CHANGES | db_name | prefix | 0x00 | time_bucket (4 bytes BE)
%% Value: sorted list of << HLC:12/binary, ChangeLen:16, Change/binary, ... >>
%%
%% Each bucket covers 1 hour of changes (3600 seconds).
%% For a document with path "type/user/admin", we merge into buckets for each prefix:
%% - type | 0x00 | bucket -> [hlc, change]
%% - type/user | 0x00 | bucket -> [hlc, change]
%% - type/user/admin | 0x00 | bucket -> [hlc, change]
%%
%% For wildcard queries like "type/#":
%%   1. Calculate start/end buckets from since_hlc
%%   2. Read each bucket's posting list (prefix bloom filter helps!)
%%   3. Filter entries by HLC within each bucket

%% Bucket granularity: 1 hour (3600 seconds)
-define(PREFIX_CHANGES_BUCKET_GRANULARITY, 3600).

%% @doc Convert HLC to time bucket number.
-spec hlc_to_bucket(barrel_hlc:timestamp()) -> non_neg_integer().
hlc_to_bucket(Hlc) ->
    WallTime = barrel_hlc:wall_time(Hlc),
    WallTime div ?PREFIX_CHANGES_BUCKET_GRANULARITY.

%% @doc Create prefix changes key for a specific bucket.
%% Key format: PREFIX_CHANGES | db_name | prefix | 0x00 | bucket (4 bytes BE)
-spec prefix_changes_key(db_name(), binary(), non_neg_integer()) -> binary().
prefix_changes_key(DbName, Prefix, Bucket) ->
    NormalizedPrefix = normalize_prefix(Prefix),
    <<?PREFIX_CHANGES, (encode_name(DbName))/binary,
      NormalizedPrefix/binary, 0, Bucket:32/big>>.

%% @doc Start key for prefix changes range scan.
%% Used to scan from a specific bucket onwards.
-spec prefix_changes_start(db_name(), binary(), non_neg_integer()) -> binary().
prefix_changes_start(DbName, Prefix, StartBucket) ->
    prefix_changes_key(DbName, Prefix, StartBucket).

%% @doc End key for prefix changes range scan.
%% Creates an upper bound key that's lexicographically after all bucket keys for this prefix.
-spec prefix_changes_end(db_name(), binary(), non_neg_integer()) -> binary().
prefix_changes_end(DbName, Prefix, _EndBucket) ->
    %% Use 0xFF after the prefix separator to create upper bound
    %% This is greater than any valid bucket number
    NormalizedPrefix = normalize_prefix(Prefix),
    <<?PREFIX_CHANGES, (encode_name(DbName))/binary,
      NormalizedPrefix/binary, 0, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>.

%% @private Normalize prefix - remove trailing "/" if present
%% The 0x00 separator acts as the boundary, no trailing / needed
normalize_prefix(<<>>) -> <<>>;
normalize_prefix(Prefix) ->
    case binary:last(Prefix) of
        $/ -> binary:part(Prefix, 0, byte_size(Prefix) - 1);
        _ -> Prefix
    end.

%%====================================================================
%% Local Document Keys (Legacy - Default CF)
%%====================================================================

%% @doc Local document key (not replicated) - legacy format in default CF
-spec local_doc(db_name(), docid()) -> binary().
local_doc(DbName, DocId) ->
    <<?PREFIX_LOCAL_DOC, (encode_name(DbName))/binary, DocId/binary>>.

%%====================================================================
%% Local Document Keys (New - Local CF)
%%====================================================================
%% These keys are used with the dedicated local_cf column family.
%% No prefix needed since they're in their own column family.
%% Format: DbName + 0 + DocId (per-database) or "_system" + 0 + DocId (global)

%% @doc Local document key for local_cf (per-database)
%% Key format: DbName + NUL separator + DocId
-spec local_doc_key(db_name(), docid()) -> binary().
local_doc_key(DbName, DocId) ->
    <<DbName/binary, 0, DocId/binary>>.

%% @doc Prefix for all local docs in a database
%% Use with local_fold to enumerate all local docs for a database.
-spec local_doc_prefix(db_name()) -> binary().
local_doc_prefix(DbName) ->
    <<DbName/binary, 0>>.

%% @doc System (global) document key for local_cf
%% Key format: "_system" + NUL separator + DocId
-spec system_doc_key(docid()) -> binary().
system_doc_key(DocId) ->
    <<"_system", 0, DocId/binary>>.

%% @doc Prefix for all system docs
%% Use with local_fold to enumerate all system docs.
-spec system_doc_prefix() -> binary().
system_doc_prefix() ->
    <<"_system", 0>>.

%%====================================================================
%% Column-Wide Document Keys (CBOR Codec Integration)
%%====================================================================

%% @doc Document current state key (stores {rev, deleted, hlc})
-spec doc_current(db_name(), docid()) -> binary().
doc_current(DbName, DocId) ->
    <<?PREFIX_DOC_CURRENT, (encode_name(DbName))/binary, DocId/binary>>.

%% @doc Prefix for all doc_current keys in a database
-spec doc_current_prefix(db_name()) -> binary().
doc_current_prefix(DbName) ->
    <<?PREFIX_DOC_CURRENT, (encode_name(DbName))/binary>>.

%% @doc End marker for doc_current range scan
-spec doc_current_end(db_name()) -> binary().
doc_current_end(DbName) ->
    <<?PREFIX_DOC_CURRENT, (encode_name(DbName))/binary, 16#FF>>.

%% @doc Document revision tree key (stores revtree as term_to_binary)
-spec doc_tree(db_name(), docid()) -> binary().
doc_tree(DbName, DocId) ->
    <<?PREFIX_DOC_TREE, (encode_name(DbName))/binary, DocId/binary>>.

%% @doc Document body key for current revision (no revision in key).
%% This enables direct body fetch without knowing the revision.
-spec doc_body(db_name(), docid()) -> binary().
doc_body(DbName, DocId) ->
    <<?PREFIX_DOC_BODY, (encode_name(DbName))/binary, DocId/binary>>.

%% @doc Document body key for a specific (non-current) revision.
%% Used to store old revision bodies when updating a document.
-spec doc_body_rev(db_name(), docid(), revid()) -> binary().
doc_body_rev(DbName, DocId, RevId) ->
    <<?PREFIX_DOC_BODY, (encode_name(DbName))/binary, DocId/binary, $:, RevId/binary>>.

%%====================================================================
%% Wide Column Document Keys (Entity Storage)
%%====================================================================

%% @doc Document entity key for wide-column storage.
%% Stores all document metadata as named columns:
%%   - rev: current revision ID
%%   - deleted: "true" or "false"
%%   - hlc: 12-byte encoded HLC timestamp
%%   - revtree: term_to_binary encoded revision tree
-spec doc_entity(db_name(), docid()) -> binary().
doc_entity(DbName, DocId) ->
    <<?PREFIX_DOC_ENTITY, (encode_name(DbName))/binary, DocId/binary>>.

%% @doc Prefix for all doc_entity keys in a database
-spec doc_entity_prefix(db_name()) -> binary().
doc_entity_prefix(DbName) ->
    <<?PREFIX_DOC_ENTITY, (encode_name(DbName))/binary>>.

%% @doc End marker for doc_entity range scan
-spec doc_entity_end(db_name()) -> binary().
doc_entity_end(DbName) ->
    <<?PREFIX_DOC_ENTITY, (encode_name(DbName))/binary, 16#FF>>.

%%====================================================================
%% View Keys
%%====================================================================

%% @doc View metadata key
-spec view_meta(db_name(), binary()) -> binary().
view_meta(DbName, ViewId) ->
    <<?PREFIX_VIEW_META, (encode_name(DbName))/binary, ViewId/binary>>.

%% @doc View indexed sequence key
-spec view_seq(db_name(), binary()) -> binary().
view_seq(DbName, ViewId) ->
    <<?PREFIX_VIEW_SEQ, (encode_name(DbName))/binary, ViewId/binary>>.

%% @doc View index entry key
-spec view_index(db_name(), binary(), binary()) -> binary().
view_index(DbName, ViewId, IndexKey) ->
    <<?PREFIX_VIEW_INDEX, (encode_name(DbName))/binary, ViewId/binary, $:, IndexKey/binary>>.

%% @doc Prefix for view index entries
-spec view_index_prefix(db_name(), binary()) -> binary().
view_index_prefix(DbName, ViewId) ->
    <<?PREFIX_VIEW_INDEX, (encode_name(DbName))/binary, ViewId/binary, $:>>.

%% @doc End marker for view index range scan
-spec view_index_end(db_name(), binary()) -> binary().
view_index_end(DbName, ViewId) ->
    <<?PREFIX_VIEW_INDEX, (encode_name(DbName))/binary, ViewId/binary, $:, 16#FF>>.

%% @doc View by docid key (tracks which index entries belong to each doc)
-spec view_by_docid(db_name(), binary(), docid()) -> binary().
view_by_docid(DbName, ViewId, DocId) ->
    <<?PREFIX_VIEW_BY_DOCID, (encode_name(DbName))/binary, ViewId/binary, $:, DocId/binary>>.

%% @doc Prefix for view by docid entries
-spec view_by_docid_prefix(db_name(), binary(), docid()) -> binary().
view_by_docid_prefix(DbName, ViewId, DocId) ->
    <<?PREFIX_VIEW_BY_DOCID, (encode_name(DbName))/binary, ViewId/binary, $:, DocId/binary>>.

%% @doc End marker for view by docid range scan
-spec view_by_docid_end(db_name(), binary(), docid()) -> binary().
view_by_docid_end(DbName, ViewId, DocId) ->
    <<?PREFIX_VIEW_BY_DOCID, (encode_name(DbName))/binary, ViewId/binary, $:, DocId/binary, 16#FF>>.

%% @doc Encode a view key for sorted storage
%% Uses term_to_binary with ordered encoding to preserve Erlang term ordering
-spec encode_view_key(term()) -> binary().
encode_view_key(Key) ->
    term_to_binary(Key, [{minor_version, 2}]).

%% @doc Decode a view key
-spec decode_view_key(binary()) -> term().
decode_view_key(Bin) ->
    binary_to_term(Bin).

%%====================================================================
%% Attachment Keys
%%====================================================================

%% @doc Attachment data key
-spec att_data(db_name(), docid(), binary()) -> binary().
att_data(DbName, DocId, AttName) ->
    <<?PREFIX_ATT, (encode_name(DbName))/binary, DocId/binary, $:, AttName/binary>>.

%% @doc Prefix for document attachments
-spec att_data_prefix(db_name(), docid()) -> binary().
att_data_prefix(DbName, DocId) ->
    <<?PREFIX_ATT, (encode_name(DbName))/binary, DocId/binary, $:>>.

%%====================================================================
%% Encoding/Decoding Helpers
%%====================================================================

%% @doc Encode database name with length prefix
-spec encode_name(db_name()) -> binary().
encode_name(Name) when is_binary(Name) ->
    Len = byte_size(Name),
    <<Len:16, Name/binary>>.

%% @doc Encode HLC timestamp to binary (big-endian for sort order)
%% Uses barrel_hlc:encode/1 which produces 12 bytes
-spec encode_hlc(barrel_hlc:timestamp()) -> binary().
encode_hlc(HlcTS) ->
    barrel_hlc:encode(HlcTS).

%% @doc Decode binary to HLC timestamp
-spec decode_hlc(binary()) -> barrel_hlc:timestamp().
decode_hlc(Bin) ->
    barrel_hlc:decode(Bin).

%% @doc Extract doc_id from a doc_info key
-spec decode_doc_id(db_name(), binary()) -> docid().
decode_doc_id(DbName, Key) ->
    Prefix = doc_info_prefix(DbName),
    PrefixLen = byte_size(Prefix),
    <<Prefix:PrefixLen/binary, DocId/binary>> = Key,
    DocId.

%% @doc Extract HLC from a doc_hlc key
-spec decode_hlc_key(db_name(), binary()) -> barrel_hlc:timestamp().
decode_hlc_key(DbName, Key) ->
    Prefix = doc_hlc_prefix(DbName),
    PrefixLen = byte_size(Prefix),
    <<Prefix:PrefixLen/binary, HlcBin/binary>> = Key,
    decode_hlc(HlcBin).

%% @doc Extract DocId from a doc_info key
-spec decode_doc_info_key(db_name(), binary()) -> docid().
decode_doc_info_key(DbName, Key) ->
    decode_doc_id(DbName, Key).

%%====================================================================
%% Path Index Keys
%%====================================================================

%% @doc Path index key for a document path.
%% Key format: prefix | db_name | encoded_path | docid
%% Path includes the value at the end: [field1, field2, value]
-spec path_index_key(db_name(), [term()], docid()) -> binary().
path_index_key(DbName, Path, DocId) ->
    EncodedPath = encode_path(Path),
    <<?PREFIX_PATH_INDEX, (encode_name(DbName))/binary, EncodedPath/binary, DocId/binary>>.

%% @doc Prefix for scanning path index entries.
%% Can be used with partial paths for prefix scans.
-spec path_index_prefix(db_name(), [term()]) -> binary().
path_index_prefix(DbName, Path) ->
    EncodedPath = encode_path(Path),
    <<?PREFIX_PATH_INDEX, (encode_name(DbName))/binary, EncodedPath/binary>>.

%% @doc End marker for path index range scan.
-spec path_index_end(db_name(), [term()]) -> binary().
path_index_end(DbName, Path) ->
    EncodedPath = encode_path(Path),
    <<?PREFIX_PATH_INDEX, (encode_name(DbName))/binary, EncodedPath/binary, 16#FF>>.

%% @doc Reverse index key: doc_id -> list of indexed paths.
%% Used to remove old paths when updating a document.
-spec doc_paths_key(db_name(), docid()) -> binary().
doc_paths_key(DbName, DocId) ->
    <<?PREFIX_DOC_PATHS, (encode_name(DbName))/binary, DocId/binary>>.

%% @doc Prefix for doc_paths keys.
-spec doc_paths_prefix(db_name()) -> binary().
doc_paths_prefix(DbName) ->
    <<?PREFIX_DOC_PATHS, (encode_name(DbName))/binary>>.

%% @doc Path stats key for cardinality counter.
%% Stores the count of documents matching a specific path+value.
-spec path_stats_key(db_name(), [term()]) -> binary().
path_stats_key(DbName, Path) ->
    EncodedPath = encode_path(Path),
    <<?PREFIX_PATH_STATS, (encode_name(DbName))/binary, EncodedPath/binary>>.

%% @doc Path bitmap key for bitmap index.
%% Stores a bitmap of document positions matching a specific path+value.
-spec path_bitmap_key(db_name(), [term()]) -> binary().
path_bitmap_key(DbName, Path) ->
    EncodedPath = encode_path(Path),
    <<?PREFIX_PATH_BITMAP, (encode_name(DbName))/binary, EncodedPath/binary>>.

%% @doc Posting list key for path index.
%% Key format: prefix | db_name | encoded_path (NO DocId - DocIds are in value)
%% Path includes the value at the end: [field1, field2, value]
-spec path_posting_key(db_name(), [term()]) -> binary().
path_posting_key(DbName, Path) ->
    EncodedPath = encode_path(Path),
    <<?PREFIX_PATH_POSTING, (encode_name(DbName))/binary, EncodedPath/binary>>.

%% @doc Prefix for scanning posting list entries.
%% Can be used with partial paths for prefix scans.
-spec path_posting_prefix(db_name(), [term()]) -> binary().
path_posting_prefix(DbName, PathPrefix) ->
    EncodedPath = encode_path(PathPrefix),
    <<?PREFIX_PATH_POSTING, (encode_name(DbName))/binary, EncodedPath/binary>>.

%% @doc End marker for posting list range scan.
-spec path_posting_end(db_name(), [term()]) -> binary().
path_posting_end(DbName, PathPrefix) ->
    Prefix = path_posting_prefix(DbName, PathPrefix),
    <<Prefix/binary, 16#FF>>.

%% @doc Value-first posting list key for fast equality queries.
%% Key format: prefix | db_name | value_prefix | encoded_path
%% Value is truncated to 128 bytes max for efficient prefix scans.
%% Path is the field path WITHOUT the value (e.g., `[&lt;&lt;"type"&gt;&gt;]' for type=user)
-spec value_posting_key(db_name(), term(), [term()]) -> binary().
value_posting_key(DbName, Value, Path) ->
    TruncatedValue = truncate_value(Value),
    EncodedValue = encode_path_component(TruncatedValue),
    EncodedPath = encode_path(Path),
    <<?PREFIX_VALUE_POSTING, (encode_name(DbName))/binary,
      EncodedValue/binary, EncodedPath/binary>>.

%% @doc Prefix for scanning value-first posting lists by value.
%% Use this to find all paths with a specific value.
-spec value_posting_prefix(db_name(), term()) -> binary().
value_posting_prefix(DbName, Value) ->
    TruncatedValue = truncate_value(Value),
    EncodedValue = encode_path_component(TruncatedValue),
    <<?PREFIX_VALUE_POSTING, (encode_name(DbName))/binary, EncodedValue/binary>>.

%% @doc End marker for value-first posting list range scan.
-spec value_posting_end(db_name(), term()) -> binary().
value_posting_end(DbName, Value) ->
    Prefix = value_posting_prefix(DbName, Value),
    <<Prefix/binary, 16#FF>>.

%% @doc Truncate a value to max 128 bytes for value-first index.
%% Only applies to binary values; other types are unchanged.
-spec truncate_value(term()) -> term().
truncate_value(Bin) when is_binary(Bin), byte_size(Bin) > ?VALUE_PREFIX_MAX_LEN ->
    <<Prefix:?VALUE_PREFIX_MAX_LEN/binary, _/binary>> = Bin,
    Prefix;
truncate_value(Value) ->
    Value.

%%====================================================================
%% Bucketed Posting List Keys
%%====================================================================

%% @doc Bucketed posting list key for sorted iteration.
%% Format: [value_prefix, path, bucket] where bucket is first 2 bytes of DocId.
%% This splits posting lists by DocId prefix, enabling:
%% - Sorted iteration (buckets are in lexicographic order)
%% - Smaller chunks (faster decode per bucket)
%% - Early termination in intersection
-spec value_posting_bucket_key(db_name(), term(), [term()], docid()) -> binary().
value_posting_bucket_key(DbName, Value, Path, DocId) ->
    TruncatedValue = truncate_value(Value),
    EncodedValue = encode_path_component(TruncatedValue),
    EncodedPath = encode_path(Path),
    Bucket = docid_bucket(DocId),
    <<?PREFIX_VALUE_POSTING_BUCKET, (encode_name(DbName))/binary,
      EncodedValue/binary, EncodedPath/binary, Bucket/binary>>.

%% @doc Prefix for scanning bucketed posting lists for a path+value.
%% Use this to iterate all buckets in sorted order.
-spec value_posting_bucket_prefix(db_name(), term(), [term()]) -> binary().
value_posting_bucket_prefix(DbName, Value, Path) ->
    TruncatedValue = truncate_value(Value),
    EncodedValue = encode_path_component(TruncatedValue),
    EncodedPath = encode_path(Path),
    <<?PREFIX_VALUE_POSTING_BUCKET, (encode_name(DbName))/binary,
      EncodedValue/binary, EncodedPath/binary>>.

%% @doc End marker for bucketed posting list range scan.
-spec value_posting_bucket_end(db_name(), term(), [term()]) -> binary().
value_posting_bucket_end(DbName, Value, Path) ->
    Prefix = value_posting_bucket_prefix(DbName, Value, Path),
    <<Prefix/binary, 16#FF>>.

%% @doc Extract bucket (first 2 bytes) from DocId.
%% Short DocIds are padded with zeros.
-spec docid_bucket(docid()) -> binary().
docid_bucket(DocId) when byte_size(DocId) >= 2 ->
    binary:part(DocId, 0, 2);
docid_bucket(DocId) ->
    %% Pad short DocIds with zeros
    Padding = 2 - byte_size(DocId),
    <<DocId/binary, 0:Padding/unit:8>>.

%%====================================================================
%% Value-First Index Keys (Iterable)
%%====================================================================

%% @doc Value-first index key for iterable equality queries.
%% Format: [value_prefix, path, DocId] enables prefix scan with early termination.
%% Unlike value_posting_key which stores DocIds in a posting list, this stores
%% one key per DocId allowing iteration without full deserialization.
-spec value_index_key(db_name(), term(), [term()], docid()) -> binary().
value_index_key(DbName, Value, Path, DocId) ->
    TruncatedValue = truncate_value(Value),
    EncodedValue = encode_path_component(TruncatedValue),
    EncodedPath = encode_path(Path),
    <<?PREFIX_VALUE_INDEX, (encode_name(DbName))/binary,
      EncodedValue/binary, EncodedPath/binary, DocId/binary>>.

%% @doc Prefix for scanning value-first index by value and path.
%% Use this to find all DocIds matching a specific (path, value) pair.
-spec value_index_prefix(db_name(), term(), [term()]) -> binary().
value_index_prefix(DbName, Value, Path) ->
    TruncatedValue = truncate_value(Value),
    EncodedValue = encode_path_component(TruncatedValue),
    EncodedPath = encode_path(Path),
    <<?PREFIX_VALUE_INDEX, (encode_name(DbName))/binary,
      EncodedValue/binary, EncodedPath/binary>>.

%% @doc End marker for value-first index range scan.
-spec value_index_end(db_name(), term(), [term()]) -> binary().
value_index_end(DbName, Value, Path) ->
    Prefix = value_index_prefix(DbName, Value, Path),
    <<Prefix/binary, 16#FF>>.

%% @doc Encode a path for lexicographic ordering.
%% Path components are encoded with length prefix and type tags.
%% This ensures correct sort order across different types.
-spec encode_path([term()]) -> binary().
encode_path(Path) when is_list(Path) ->
    iolist_to_binary([encode_path_component(C) || C <- Path]).

%% @doc Decode a path from binary.
-spec decode_path(binary()) -> [term()].
decode_path(Bin) ->
    decode_path_components(Bin, []).

%%====================================================================
%% Path Component Encoding
%%====================================================================

%% @private Encode a single path component with type tag for ordering
encode_path_component(null) ->
    <<?PATH_TYPE_NULL>>;
encode_path_component(false) ->
    <<?PATH_TYPE_FALSE>>;
encode_path_component(true) ->
    <<?PATH_TYPE_TRUE>>;
encode_path_component(0) ->
    <<?PATH_TYPE_ZERO>>;
encode_path_component(N) when is_integer(N), N > 0 ->
    %% Positive integers: encode with length prefix for proper ordering
    Bin = integer_to_binary(N),
    Len = byte_size(Bin),
    <<?PATH_TYPE_POS_INT, Len:8, Bin/binary>>;
encode_path_component(N) when is_integer(N), N < 0 ->
    %% Negative integers: invert and encode for proper ordering
    %% -1 should sort after -1000, so we use complement
    Abs = abs(N),
    Bin = integer_to_binary(Abs),
    Len = byte_size(Bin),
    %% Invert the length so larger negative numbers sort first
    InvLen = 255 - Len,
    %% Invert each byte so -1 > -2
    InvBin = << <<(255 - B)>> || <<B>> <= Bin >>,
    <<?PATH_TYPE_NEG_INT, InvLen:8, InvBin/binary>>;
encode_path_component(F) when is_float(F) ->
    %% Floats: use IEEE 754 encoding with sign adjustment
    <<?PATH_TYPE_FLOAT, (encode_float(F))/binary>>;
encode_path_component(Bin) when is_binary(Bin) ->
    %% Binary: escape null bytes and terminate for lexicographic order
    %% 0x00 -> 0x00 0xFF (escape), end with 0x00 0x00
    Escaped = escape_binary(Bin),
    <<?PATH_TYPE_BINARY, Escaped/binary, 0, 0>>.

%% @private Encode float for lexicographic ordering
encode_float(F) when F >= 0 ->
    <<Bits:64/big-unsigned>> = <<F:64/float>>,
    <<(Bits bxor 16#8000000000000000):64/big-unsigned>>;
encode_float(F) ->
    <<Bits:64/big-unsigned>> = <<F:64/float>>,
    <<(bnot Bits):64/big-unsigned>>.

%% @private Decode float from lexicographic encoding
decode_float(<<Encoded:64/big-unsigned>>) ->
    %% Check if sign bit is set (was positive)
    Bits = case Encoded band 16#8000000000000000 of
        0 -> bnot Encoded;  %% Was negative
        _ -> Encoded bxor 16#8000000000000000  %% Was positive
    end,
    <<F:64/float>> = <<Bits:64/big-unsigned>>,
    F.

%% @private Escape null bytes in binary for lexicographic encoding
%% 0x00 -> 0x00 0xFF
escape_binary(Bin) ->
    escape_binary(Bin, <<>>).

escape_binary(<<>>, Acc) ->
    Acc;
escape_binary(<<0, Rest/binary>>, Acc) ->
    escape_binary(Rest, <<Acc/binary, 0, 16#FF>>);
escape_binary(<<B, Rest/binary>>, Acc) ->
    escape_binary(Rest, <<Acc/binary, B>>).

%% @private Unescape binary from lexicographic encoding
%% 0x00 0xFF -> 0x00, 0x00 0x00 = end
unescape_binary(Bin) ->
    unescape_binary(Bin, <<>>).

unescape_binary(<<0, 0, Rest/binary>>, Acc) ->
    {Acc, Rest};
unescape_binary(<<0, 16#FF, Rest/binary>>, Acc) ->
    unescape_binary(Rest, <<Acc/binary, 0>>);
unescape_binary(<<B, Rest/binary>>, Acc) ->
    unescape_binary(Rest, <<Acc/binary, B>>);
unescape_binary(<<>>, Acc) ->
    {Acc, <<>>}.

%% @private Decode path components from binary
decode_path_components(<<>>, Acc) ->
    lists:reverse(Acc);
decode_path_components(<<?PATH_TYPE_NULL, Rest/binary>>, Acc) ->
    decode_path_components(Rest, [null | Acc]);
decode_path_components(<<?PATH_TYPE_FALSE, Rest/binary>>, Acc) ->
    decode_path_components(Rest, [false | Acc]);
decode_path_components(<<?PATH_TYPE_TRUE, Rest/binary>>, Acc) ->
    decode_path_components(Rest, [true | Acc]);
decode_path_components(<<?PATH_TYPE_ZERO, Rest/binary>>, Acc) ->
    decode_path_components(Rest, [0 | Acc]);
decode_path_components(<<?PATH_TYPE_POS_INT, Len:8, Bin:Len/binary, Rest/binary>>, Acc) ->
    N = binary_to_integer(Bin),
    decode_path_components(Rest, [N | Acc]);
decode_path_components(<<?PATH_TYPE_NEG_INT, InvLen:8, InvBin/binary>>, Acc) ->
    Len = 255 - InvLen,
    <<InvBytes:Len/binary, Rest/binary>> = InvBin,
    Bin = << <<(255 - B)>> || <<B>> <= InvBytes >>,
    N = -binary_to_integer(Bin),
    decode_path_components(Rest, [N | Acc]);
decode_path_components(<<?PATH_TYPE_FLOAT, Encoded:8/binary, Rest/binary>>, Acc) ->
    F = decode_float(Encoded),
    decode_path_components(Rest, [F | Acc]);
decode_path_components(<<?PATH_TYPE_BINARY, Rest/binary>>, Acc) ->
    {Bin, Rest2} = unescape_binary(Rest),
    decode_path_components(Rest2, [Bin | Acc]).

%%====================================================================
%% Change Bucket Keys (Idle Poll Optimization)
%%====================================================================

%% @doc Change bucket key for time-bucketed change hints.
%% BucketTs is typically erlang:system_time(second) div 60 (minute granularity).
%% Value stores {min_hlc, max_hlc, count} for quick "has changes?" checks.
-spec change_bucket(db_name(), non_neg_integer()) -> binary().
change_bucket(DbName, BucketTs) ->
    <<?PREFIX_CHANGE_BUCKET, (encode_name(DbName))/binary, BucketTs:64/big-unsigned>>.

%% @doc Prefix for all change buckets in a database.
-spec change_bucket_prefix(db_name()) -> binary().
change_bucket_prefix(DbName) ->
    <<?PREFIX_CHANGE_BUCKET, (encode_name(DbName))/binary>>.

%% @doc End marker for change bucket range scan.
-spec change_bucket_end(db_name()) -> binary().
change_bucket_end(DbName) ->
    <<?PREFIX_CHANGE_BUCKET, (encode_name(DbName))/binary,
      16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>.

%%====================================================================
%% Key Parsing (for compaction filter)
%%====================================================================

%% @doc Parse a key to determine its type and extract components.
%% Used by compaction filter to identify doc_entity keys.
-spec parse_key(binary()) ->
    {doc_entity, DbName :: binary(), DocId :: binary()} |
    {doc_body_rev, DbName :: binary(), DocId :: binary(), Rev :: binary()} |
    other.
parse_key(<<?PREFIX_DOC_ENTITY, Rest/binary>>) ->
    case decode_name(Rest) of
        {ok, DbName, DocId} -> {doc_entity, DbName, DocId};
        error -> other
    end;
parse_key(<<?PREFIX_DOC_BODY, Rest/binary>>) ->
    case decode_name(Rest) of
        {ok, DbName, DocIdAndRev} ->
            case binary:split(DocIdAndRev, <<$:>>) of
                [DocId, Rev] -> {doc_body_rev, DbName, DocId, Rev};
                [DocId] -> {doc_body, DbName, DocId}
            end;
        error -> other
    end;
parse_key(_) ->
    other.

%% @private Decode length-prefixed name from binary
decode_name(<<Len:16, Name:Len/binary, Rest/binary>>) ->
    {ok, Name, Rest};
decode_name(_) ->
    error.
