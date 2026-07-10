%%%-------------------------------------------------------------------
%%% @doc Generic tagged outbox: a durable work queue written atomically
%%% with the document batch.
%%%
%%% A write carrying the put option `outbox => [Tag]' appends one entry
%%% per tag to the same RocksDB WriteBatch as the document, so the entry
%%% and the doc commit or fail together. Entries are keyed
%%% `prefix | db | tag | hlc' (the write's change HLC), giving
%%% time-ordered scans per tag and exact-key acknowledgements.
%%%
%%% Rewrites of a tagged document replace the entry (the old HLC key is
%%% deleted in the same batch, mirroring the changes feed), so the outbox
%%% holds at most one pending entry per document per tag. Consumers fold
%%% a tag (caller-side, no writer round-trip), process the CURRENT state
%%% of each document, and ack by exact HLC key: if the document was
%%% rewritten mid-processing, the new entry lives at a different key and
%%% re-drives the consumer, so acking the processed key never loses work.
%%% Un-acked entries are the consumer's checkpoint; there is nothing else
%%% to persist.
%%%
%%% docdb knows nothing about what tags mean; producers (for example the
%%% barrel's embedding indexer) define the semantics.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_outbox).

-include("barrel_docdb.hrl").

%% Batch op builders (called from the barrel_db_server write paths)
-export([write_ops/7]).
%% Consumer API
-export([fold/5, fold/6]).
%% Entry codec (exposed for tests)
-export([encode_entry/3, decode_entry/1]).

-type tag() :: binary().
-type entry() :: #{
    hlc := barrel_hlc:timestamp(),
    id := docid(),
    rev := revid(),
    deleted := boolean()
}.
-export_type([tag/0, entry/0]).

%%====================================================================
%% Batch op builders
%%====================================================================

%% @doc Batch ops for a tagged write: one put per tag at the new HLC,
%% plus a replace-delete of the previous entry when the doc existed.
%% Deleting a key that was never written is a RocksDB no-op, so a
%% previously untagged write is harmless.
-spec write_ops(db_name(), [tag()], barrel_hlc:timestamp(),
                barrel_hlc:timestamp() | undefined,
                docid(), revid(), boolean()) -> [term()].
write_ops(_DbName, [], _NewHlc, _OldHlc, _DocId, _Rev, _Deleted) ->
    [];
write_ops(DbName0, Tags, NewHlc, OldHlc, DocId, Rev, Deleted) when is_list(Tags) ->
    DbName = barrel_keyspace:resolve(DbName0),
    Entry = encode_entry(DocId, Rev, Deleted),
    lists:flatmap(
        fun(Tag) when is_binary(Tag) ->
            Put = {put, barrel_store_keys:outbox_key(DbName, Tag, NewHlc), Entry},
            case OldHlc of
                undefined ->
                    [Put];
                _ ->
                    [Put, {delete, barrel_store_keys:outbox_key(DbName, Tag, OldHlc)}]
            end
        end,
        Tags).

%%====================================================================
%% Consumer API
%%====================================================================

%% @doc Fold over the pending entries of a tag in HLC order.
%% Runs in the caller's process (pure RocksDB range scan). `Fun' receives
%% a decoded entry() and the accumulator and returns `{ok, Acc}' or
%% `{stop, Acc}'. Options: `limit' (max entries to visit).
-spec fold(barrel_store_rocksdb:db_ref(), db_name(), tag(),
           fun((entry(), term()) -> {ok, term()} | {stop, term()}), term()) -> term().
fold(StoreRef, DbName, Tag, Fun, Acc) ->
    fold(StoreRef, DbName, Tag, Fun, Acc, #{}).

%% @doc Fold over the pending entries of a tag with options.
-spec fold(barrel_store_rocksdb:db_ref(), db_name(), tag(),
           fun((entry(), term()) -> {ok, term()} | {stop, term()}), term(), map()) -> term().
fold(StoreRef, DbName0, Tag, Fun, Acc, Opts) ->
    DbName = barrel_keyspace:resolve(DbName0),
    Start = barrel_store_keys:outbox_prefix(DbName, Tag),
    End = barrel_store_keys:outbox_end(DbName, Tag),
    Limit = maps:get(limit, Opts, infinity),
    WrapFun = fun(Key, Value, {N, AccIn}) ->
        {_Tag, Hlc} = barrel_store_keys:decode_outbox_key(DbName, Key),
        Entry = (decode_entry(Value))#{hlc => Hlc},
        case Fun(Entry, AccIn) of
            {ok, AccOut} when N + 1 >= Limit -> {stop, {N + 1, AccOut}};
            {ok, AccOut} -> {ok, {N + 1, AccOut}};
            {stop, AccOut} -> {stop, {N + 1, AccOut}}
        end
    end,
    {_Count, FinalAcc} =
        barrel_store_rocksdb:fold_range(StoreRef, Start, End, WrapFun, {0, Acc}),
    FinalAcc.

%%====================================================================
%% Entry codec
%%====================================================================

%% @doc Encode an outbox entry value (compact binary, same style as the
%% change entry codec).
-spec encode_entry(docid(), revid(), boolean()) -> binary().
encode_entry(DocId, Rev, Deleted) ->
    DelBit = case Deleted of true -> 1; false -> 0 end,
    <<(byte_size(DocId)):16, DocId/binary,
      (byte_size(Rev)):16, Rev/binary,
      DelBit:8>>.

%% @doc Decode an outbox entry value (without the key-derived hlc).
-spec decode_entry(binary()) -> #{id := docid(), rev := revid(), deleted := boolean()}.
decode_entry(<<IdLen:16, DocId:IdLen/binary,
               RevLen:16, Rev:RevLen/binary,
               DelBit:8>>) ->
    #{id => DocId, rev => Rev, deleted => DelBit =:= 1}.
