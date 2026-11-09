%% @doc Attachment change feed: the tracking layer that lets
%% attachments replicate. Lives INSIDE the attachments RocksDB and is
%% committed atomically with the blob write (one WriteBatch).
%%
%% Keys are tagged 0xFF, provably disjoint from blob keys (their first
%% byte is the high byte of a 16-bit db-name length, always 0x00):
%%
%% ```
%%   feed   <<16#FF, $C, DbNameLen:16, DbName, LocalHlc:12>>
%%   index  <<16#FF, $A, DbNameLen:16, DbName, DocIdLen:16, DocId, $:, Name>>
%%   meta   <<16#FF, $M, DbNameLen:16, DbName, "att_floor" | "att_hlc_high">>
%%   upload <<16#FF, $U, DbNameLen:16, DbName, Gen:64, DocIdLen:16, DocId, Name>>
%% '''
%%
%% Upload rows mark chunk generations staged by the blob backend and not
%% yet published (see barrel_att_store_blob).
%%
%% Invariant: ONE feed row per (DocId, AttName), moved on every write
%% (doc-feed style, not history style), so a full resync is one bounded
%% feed walk. Put rows are never age-swept; delete tombstones are kept
%% until the database's tombstone_purge_after window (never when 0),
%% recorded by the floor.
%%
%% Convergence: attachments have no version vectors, so bidirectional
%% sync uses last-write-wins on an ORIGIN HLC that travels with the
%% data (fresh local HLC for local writes, preserved from the source
%% feed entry for replicated ones). Feed keys always use fresh local
%% HLCs so the feed stays monotone; the origin rides in the values.
-module(barrel_att_feed).

-include("barrel_docdb.hrl").

-export([
    ops/7,
    ops/8,
    next_hlc/1,
    check/6,
    lww/4,
    index_get/4,
    index_list/3,
    index_get/5,
    att_changes/4,
    att_floor/2,
    last_hlc/2,
    sweep/3,
    att_count/2,
    count_atts/2
]).

%% Codec (exposed for tests)
-export([encode_feed/2, decode_feed/2, encode_index/1, decode_index/1]).

-export_type([entry/0]).

-type op() :: put | delete.
-type entry() :: #{
    seq := barrel_hlc:timestamp(),
    origin := barrel_hlc:timestamp(),
    op := op(),
    id := docid(),
    name := binary(),
    digest := binary(),
    length := non_neg_integer(),
    content_type := binary()
}.

-define(TAG, 16#FF).
-define(FEED, $C).
-define(INDEX, $A).
-define(META, $M).
-define(FLOOR_NAME, <<"att_floor">>).
-define(HIGH_NAME, <<"att_hlc_high">>).
-define(COUNT_NAME, <<"att_count">>).
-define(COUNTED_NAME, <<"att_counted">>).

%%====================================================================
%% Ops (called from the blob store's commit points)
%%====================================================================

%% @doc Batch ops recording one applied write, with a feed HLC above
%% the newest feed row and the stored origin. Callers outside a
%% database writer use this; the writer passes its own HLC (ops/8).
-spec ops(rocksdb:db_handle(), db_name(), docid(), binary(), op(),
          barrel_hlc:timestamp(), map()) -> [term()].
ops(Ref, DbName, DocId, AttName, Op, OriginHlc, Info) ->
    Floor = case index_get(Ref, DbName, DocId, AttName) of
        {ok, #{origin := Stored}} -> max_hlc(Stored, last_hlc(Ref, DbName));
        not_found -> last_hlc(Ref, DbName)
    end,
    ops(Ref, DbName, DocId, AttName, Op, OriginHlc, next_hlc(Floor), Info).

%% @doc Batch ops recording one applied write: move the (DocId, Name)
%% feed row to LocalHlc, rewrite the index row and raise att_hlc_high.
%% Info carries digest/length/content_type (empty for deletes).
-spec ops(rocksdb:db_handle(), db_name(), docid(), binary(), op(),
          barrel_hlc:timestamp(), barrel_hlc:timestamp(), map()) -> [term()].
ops(Ref, DbName, DocId, AttName, Op, OriginHlc, LocalHlc, Info) ->
    IndexKey = index_key(DbName, DocId, AttName),
    {DeleteOld, OldOp} = case rocksdb:get(Ref, IndexKey, []) of
        {ok, OldIdxBin} ->
            #{seq := OldLocal, op := OOp} = decode_index(OldIdxBin),
            {[{delete, feed_key(DbName, OldLocal)}], OOp};
        _ ->
            {[], none}
    end,
    %% the attachment count moves in the same batch as the index row
    CountOps = case is_put(Op) - is_put(OldOp) of
        0 -> [];
        Delta -> [{merge, count_key(DbName), Delta}]
    end,
    Entry = #{
        origin => OriginHlc,
        op => Op,
        id => DocId,
        name => AttName,
        digest => maps:get(digest, Info, <<>>),
        length => maps:get(length, Info, 0),
        content_type => maps:get(content_type, Info, <<>>)
    },
    DeleteOld ++
        [{put, feed_key(DbName, LocalHlc), encode_feed(Op, Entry)},
         {put, IndexKey, encode_index(Entry#{seq => LocalHlc})},
         {put, high_key(DbName), barrel_hlc:encode(LocalHlc)}] ++ CountOps.

is_put(put) -> 1;
is_put(_) -> 0.

%% @doc The number of attachments (index rows that are puts), exact once
%% the feed has been counted; `not_counted' before.
-spec att_count(rocksdb:db_handle(), db_name()) -> {ok, non_neg_integer()} | not_counted.
att_count(Ref, DbName) ->
    case rocksdb:get(Ref, counted_key(DbName), []) of
        {ok, _} ->
            case rocksdb:get(Ref, count_key(DbName), []) of
                {ok, Bin} -> {ok, max(0, binary_to_integer(Bin))};
                _ -> {ok, 0}
            end;
        _ ->
            not_counted
    end.

%% @doc Count the attachments of a feed written before the counter: from
%% a snapshot, adding the difference to the counter as it was there, so
%% writes made meanwhile (merges) are kept.
-spec count_atts(rocksdb:db_handle(), db_name()) -> ok | {error, term()}.
count_atts(Ref, DbName) ->
    {ok, Snap} = rocksdb:snapshot(Ref),
    try
        Prefix = <<?TAG, ?INDEX, (byte_size(DbName)):16, DbName/binary>>,
        {ok, It} = rocksdb:iterator(Ref, [{snapshot, Snap}, {total_order_seek, true}]),
        Live = try count_puts(rocksdb:iterator_move(It, {seek, Prefix}), It, Prefix, 0)
               after rocksdb:iterator_close(It)
               end,
        C0 = case rocksdb:get(Ref, count_key(DbName), [{snapshot, Snap}]) of
            {ok, Bin} -> binary_to_integer(Bin);
            _ -> 0
        end,
        {ok, Batch} = rocksdb:batch(),
        try
            ok = rocksdb:batch_merge(Batch, count_key(DbName), integer_to_binary(Live - C0)),
            ok = rocksdb:batch_put(Batch, counted_key(DbName), <<>>),
            rocksdb:write_batch(Ref, Batch, [{sync, true}])
        after
            rocksdb:release_batch(Batch)
        end
    after
        rocksdb:release_snapshot(Snap)
    end.

count_puts({ok, K, V}, It, Prefix, N) ->
    case binary:longest_common_prefix([K, Prefix]) =:= byte_size(Prefix) of
        true ->
            N1 = case decode_index(V) of
                #{op := put} -> N + 1;
                _ -> N
            end,
            count_puts(rocksdb:iterator_move(It, next), It, Prefix, N1);
        false ->
            N
    end;
count_puts(_, _It, _Prefix, N) ->
    N.

count_key(DbName) ->
    <<?TAG, ?META, (byte_size(DbName)):16, DbName/binary, ?COUNT_NAME/binary>>.

counted_key(DbName) ->
    <<?TAG, ?META, (byte_size(DbName)):16, DbName/binary, ?COUNTED_NAME/binary>>.

%% @doc A fresh HLC above Floor; throws {error, clock_skew} when the
%% clock refuses to move past it.
-spec next_hlc(barrel_hlc:timestamp()) -> barrel_hlc:timestamp().
next_hlc(Floor) ->
    Hlc = barrel_hlc:new_hlc(),
    case barrel_hlc:less(Floor, Hlc) of
        true ->
            Hlc;
        false ->
            _ = barrel_hlc:sync_hlc(Floor),
            Hlc2 = barrel_hlc:new_hlc(),
            case barrel_hlc:less(Floor, Hlc2) of
                true -> Hlc2;
                false -> throw({error, clock_skew})
            end
    end.

max_hlc(A, B) ->
    case barrel_hlc:less(A, B) of
        true -> B;
        false -> A
    end.

%% @doc Last-write-wins guard for writes carrying an origin HLC.
%% Deterministic across replicas: older origin loses; equal origins
%% tie-break on digest byte order (equal digest = redelivery).
-spec check(rocksdb:db_handle(), db_name(), docid(), binary(),
            barrel_hlc:timestamp(), binary()) -> apply | ignored.
check(Ref, DbName, DocId, AttName, IncomingOrigin, IncomingDigest) ->
    case rocksdb:get(Ref, index_key(DbName, DocId, AttName), []) of
        {ok, IdxBin} ->
            #{origin := StoredOrigin, digest := StoredDigest} =
                decode_index(IdxBin),
            lww(StoredOrigin, StoredDigest, IncomingOrigin, IncomingDigest);
        _ ->
            apply
    end.

%% @doc The last-write-wins rule of check/6 against a stored state.
-spec lww(barrel_hlc:timestamp(), binary(), barrel_hlc:timestamp(),
          binary()) -> apply | ignored.
lww(StoredOrigin, StoredDigest, IncomingOrigin, IncomingDigest) ->
    case {barrel_hlc:less(IncomingOrigin, StoredOrigin),
          barrel_hlc:equal(IncomingOrigin, StoredOrigin)} of
        {true, _} -> ignored;
        {_, false} -> apply;
        {_, true} when IncomingDigest > StoredDigest -> apply;
        {_, true} -> ignored
    end.

%% @doc Current index entry for one attachment (delete tombstones are
%% entries with op = delete).
-spec index_get(rocksdb:db_handle(), db_name(), docid(), binary()) ->
    {ok, map()} | not_found.
index_get(Ref, DbName, DocId, AttName) ->
    index_get(Ref, DbName, DocId, AttName, []).

%% @doc index_get/4 with read options (a snapshot).
-spec index_get(rocksdb:db_handle(), db_name(), docid(), binary(), list()) ->
    {ok, map()} | not_found.
index_get(Ref, DbName, DocId, AttName, ReadOpts) ->
    case rocksdb:get(Ref, index_key(DbName, DocId, AttName), ReadOpts) of
        {ok, Bin} -> {ok, decode_index(Bin)};
        _ -> not_found
    end.

%% @doc Current index entries of one document's attachments (deletes
%% included), shaped like feed entries.
-spec index_list(rocksdb:db_handle(), db_name(), docid()) -> [entry()].
index_list(Ref, DbName, DocId) ->
    Head = <<?TAG, ?INDEX, (byte_size(DbName)):16, DbName/binary,
             (byte_size(DocId)):16, DocId/binary>>,
    Prefix = <<Head/binary, $:>>,
    PLen = byte_size(Prefix),
    ReadOpts = [{iterate_lower_bound, Prefix},
                {iterate_upper_bound, <<Head/binary, ($: + 1)>>}],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        index_loop(rocksdb:iterator_move(Itr, first), Itr, Prefix, PLen,
                   DocId, [])
    after
        rocksdb:iterator_close(Itr)
    end.

index_loop({ok, Key, Value}, Itr, Prefix, PLen, DocId, Acc) ->
    case Key of
        <<Prefix:PLen/binary, Name/binary>> ->
            Entry = (decode_index(Value))#{id => DocId, name => Name},
            index_loop(rocksdb:iterator_move(Itr, next), Itr, Prefix, PLen,
                       DocId, [Entry | Acc]);
        _ ->
            lists:reverse(Acc)
    end;
index_loop({error, _}, _Itr, _Prefix, _PLen, _DocId, Acc) ->
    lists:reverse(Acc).

%%====================================================================
%% Feed reads
%%====================================================================

%% @doc Fold the feed since an HLC (exclusive), in write order.
%% Options: limit. Returns the entries and the last visited HLC.
-spec att_changes(rocksdb:db_handle(), db_name(),
                  barrel_hlc:timestamp() | first, map()) ->
    {ok, [entry()], barrel_hlc:timestamp() | first}.
att_changes(Ref, DbName, Since, Opts) ->
    StartKey = case Since of
        first -> feed_key(DbName, barrel_hlc:min());
        SinceHlc -> feed_key(DbName, SinceHlc)
    end,
    Limit = maps:get(limit, Opts, infinity),
    ReadOpts = [{iterate_lower_bound, StartKey},
                {iterate_upper_bound, feed_end(DbName)}],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        changes_loop(rocksdb:iterator_move(Itr, first), Itr, DbName, Since,
                     Limit, Since, [])
    after
        rocksdb:iterator_close(Itr)
    end.

changes_loop({error, _}, _Itr, _DbName, _Since, _Limit, LastSeq, Acc) ->
    {ok, lists:reverse(Acc), LastSeq};
changes_loop({ok, Key, Value}, Itr, DbName, Since, Limit, LastSeq, Acc) ->
    Hlc = feed_key_hlc(DbName, Key),
    case Since =/= first andalso barrel_hlc:equal(Hlc, Since) of
        true ->
            changes_loop(rocksdb:iterator_move(Itr, next), Itr, DbName,
                         Since, Limit, LastSeq, Acc);
        false ->
            Entry = decode_feed(Value, Hlc),
            Acc1 = [Entry | Acc],
            case is_integer(Limit) andalso length(Acc1) >= Limit of
                true ->
                    {ok, lists:reverse(Acc1), Hlc};
                false ->
                    changes_loop(rocksdb:iterator_move(Itr, next), Itr,
                                 DbName, Since, Limit, Hlc, Acc1)
            end
    end.

%% @doc Oldest HLC the feed is complete from (undefined = never swept).
-spec att_floor(rocksdb:db_handle(), db_name()) ->
    barrel_hlc:timestamp() | undefined.
att_floor(Ref, DbName) ->
    case rocksdb:get(Ref, floor_key(DbName), []) of
        {ok, Bin} -> barrel_hlc:decode(Bin);
        _ -> undefined
    end.

%% @doc Highest feed HLC ever written: the newest feed row or the
%% att_hlc_high mark, which a tombstone sweep never lowers (min() when
%% empty).
-spec last_hlc(rocksdb:db_handle(), db_name()) -> barrel_hlc:timestamp().
last_hlc(Ref, DbName) ->
    ReadOpts = [{iterate_lower_bound, feed_key(DbName, barrel_hlc:min())},
                {iterate_upper_bound, feed_end(DbName)}],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    Tail = try rocksdb:iterator_move(Itr, last) of
        {ok, Key, _Value} -> feed_key_hlc(DbName, Key);
        {error, _} -> barrel_hlc:min()
    after
        rocksdb:iterator_close(Itr)
    end,
    case rocksdb:get(Ref, high_key(DbName), []) of
        {ok, Bin} -> max_hlc(Tail, barrel_hlc:decode(Bin));
        _ -> Tail
    end.

%% @doc Sweep delete tombstones older than the cutoff and advance the
%% floor. Put rows are the live feed and are never swept.
-spec sweep(rocksdb:db_handle(), db_name(), barrel_hlc:timestamp()) ->
    {ok, #{tombstones_swept := non_neg_integer()}}.
sweep(Ref, DbName, Cutoff) ->
    ReadOpts = [{iterate_lower_bound, feed_key(DbName, barrel_hlc:min())},
                {iterate_upper_bound, feed_key(DbName, Cutoff)}],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    Ops = try
        sweep_loop(rocksdb:iterator_move(Itr, first), Itr, DbName, [])
    after
        rocksdb:iterator_close(Itr)
    end,
    {ok, Batch} = rocksdb:batch(),
    try
        lists:foreach(
            fun({delete, K}) -> ok = rocksdb:batch_delete(Batch, K) end,
            Ops),
        ok = rocksdb:batch_put(Batch, floor_key(DbName),
                               barrel_hlc:encode(Cutoff)),
        ok = rocksdb:write_batch(Ref, Batch, [])
    after
        rocksdb:release_batch(Batch)
    end,
    {ok, #{tombstones_swept => length(Ops) div 2}}.

sweep_loop({error, _}, _Itr, _DbName, Acc) ->
    Acc;
sweep_loop({ok, Key, Value}, Itr, DbName, Acc) ->
    Hlc = feed_key_hlc(DbName, Key),
    Acc1 = case decode_feed(Value, Hlc) of
        #{op := delete, id := DocId, name := AttName} ->
            [{delete, Key},
             {delete, index_key(DbName, DocId, AttName)} | Acc];
        _ ->
            Acc
    end,
    sweep_loop(rocksdb:iterator_move(Itr, next), Itr, DbName, Acc1).

%%====================================================================
%% Keys
%%====================================================================

feed_key(DbName, Hlc) ->
    <<?TAG, ?FEED, (byte_size(DbName)):16, DbName/binary,
      (barrel_hlc:encode(Hlc))/binary>>.

feed_end(DbName) ->
    <<?TAG, ?FEED, (byte_size(DbName)):16, DbName/binary,
      16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF,
      16#FF, 16#FF, 16#FF, 16#FF, 16#FF>>.

feed_key_hlc(DbName, Key) ->
    HeaderLen = 2 + 2 + byte_size(DbName),
    <<_:HeaderLen/binary, HlcBin:12/binary>> = Key,
    barrel_hlc:decode(HlcBin).

index_key(DbName, DocId, AttName) ->
    <<?TAG, ?INDEX, (byte_size(DbName)):16, DbName/binary,
      (byte_size(DocId)):16, DocId/binary, $:, AttName/binary>>.

floor_key(DbName) ->
    <<?TAG, ?META, (byte_size(DbName)):16, DbName/binary,
      ?FLOOR_NAME/binary>>.

high_key(DbName) ->
    <<?TAG, ?META, (byte_size(DbName)):16, DbName/binary,
      ?HIGH_NAME/binary>>.

%%====================================================================
%% Codecs
%%====================================================================

-spec encode_feed(op(), map()) -> binary().
encode_feed(Op, #{origin := Origin, id := DocId, name := Name,
                  digest := Digest, length := Length,
                  content_type := ContentType}) ->
    <<(op_byte(Op)):8, (barrel_hlc:encode(Origin))/binary,
      (byte_size(DocId)):16, DocId/binary,
      (byte_size(Name)):16, Name/binary,
      (byte_size(Digest)):8, Digest/binary,
      Length:64,
      (byte_size(ContentType)):16, ContentType/binary>>.

-spec decode_feed(binary(), barrel_hlc:timestamp()) -> entry().
decode_feed(<<OpByte:8, OriginBin:12/binary,
              IdLen:16, DocId:IdLen/binary,
              NameLen:16, Name:NameLen/binary,
              DigestLen:8, Digest:DigestLen/binary,
              Length:64,
              CTLen:16, ContentType:CTLen/binary>>, Seq) ->
    #{
        seq => Seq,
        origin => barrel_hlc:decode(OriginBin),
        op => byte_op(OpByte),
        id => DocId,
        name => Name,
        digest => Digest,
        length => Length,
        content_type => ContentType
    }.

-spec encode_index(map()) -> binary().
encode_index(#{origin := Origin, seq := Seq, op := Op, digest := Digest,
               length := Length, content_type := ContentType}) ->
    <<(barrel_hlc:encode(Origin))/binary,
      (barrel_hlc:encode(Seq))/binary,
      (op_byte(Op)):8,
      (byte_size(Digest)):8, Digest/binary,
      Length:64,
      (byte_size(ContentType)):16, ContentType/binary>>.

-spec decode_index(binary()) -> map().
decode_index(<<OriginBin:12/binary, SeqBin:12/binary, OpByte:8,
               DigestLen:8, Digest:DigestLen/binary,
               Length:64,
               CTLen:16, ContentType:CTLen/binary>>) ->
    #{
        origin => barrel_hlc:decode(OriginBin),
        seq => barrel_hlc:decode(SeqBin),
        op => byte_op(OpByte),
        digest => Digest,
        length => Length,
        content_type => ContentType
    }.

op_byte(put) -> 0;
op_byte(delete) -> 1.

byte_op(0) -> put;
byte_op(1) -> delete.
