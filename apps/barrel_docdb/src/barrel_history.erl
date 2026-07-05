%%%-------------------------------------------------------------------
%%% @doc barrel_history - Retained history log
%%%
%%% An append-only, HLC-ordered log of every write a database applies:
%%% local writes, replicated versions (including losing conflict
%%% siblings), and conflict resolutions. One entry per write, keyed by
%%% the change-sequence HLC and written in the same atomic batch as the
%%% document, so the log never disagrees with the store.
%%%
%%% Entries carry identity only (doc id, version, deleted, cause,
%%% version vector), never bodies: bodies resolve through the current
%%% body or the version-keyed archive, and stay resolvable within the
%%% retention window. The live changes feed is unaffected and keeps its
%%% one-entry-per-doc invariant.
%%%
%%% Reads run in the caller's process against the store ref (pure
%%% RocksDB range scans), like barrel_docdb_reader.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_history).

-include("barrel_docdb.hrl").

%% Writer-side op builder (appended to the document WriteBatch)
-export([write_ops/7]).

%% Caller-side reads
-export([fold/4, fold/5]).
-export([get_doc_versions/3]).
-export([get_version_body/4]).
-export([history_floor/2]).

%% Entry codec (exposed for tests)
-export([encode_entry/5, decode_entry/1]).

-define(HISTORY_FLOOR_META, <<"history_floor">>).

-type cause() :: local | replicated | resolve.

-type entry() :: #{
    hlc := barrel_hlc:timestamp(),
    id := docid(),
    version := binary(),          %% version token
    deleted := boolean(),
    cause := cause(),
    vv := barrel_vv:vv()
}.

-export_type([cause/0, entry/0]).

%%====================================================================
%% Writer-side op builder
%%====================================================================

%% @doc The history put for one applied write. Appended to the same
%% WriteBatch as the entity/feed ops by the database writer. VV is the
%% vector recorded with the write (the state after a winning write, the
%% sibling's own vector for a losing one).
-spec write_ops(db_name(), barrel_hlc:timestamp(), docid(),
                barrel_version:version(), boolean(), cause(),
                barrel_vv:vv()) -> [term()].
write_ops(DbName, ChangeHlc, DocId, Version, Deleted, Cause, VV) ->
    Ks = barrel_keyspace:resolve(DbName),
    [{put, barrel_store_keys:history_key(Ks, ChangeHlc),
      encode_entry(DocId, Version, Deleted, Cause, VV)}].

%%====================================================================
%% Caller-side reads
%%====================================================================

%% @doc Fold over history entries in HLC order.
-spec fold(barrel_store_rocksdb:db_ref(), db_name(),
           fun((entry(), term()) -> {ok, term()} | {stop, term()}), term()) ->
    term().
fold(StoreRef, DbName, Fun, Acc) ->
    fold(StoreRef, DbName, Fun, Acc, #{}).

%% @doc Fold over history entries in HLC order with options:
%%   - `from': first HLC to include (default: everything retained)
%%   - `to': last HLC to include
%%   - `limit': max entries to visit
-spec fold(barrel_store_rocksdb:db_ref(), db_name(),
           fun((entry(), term()) -> {ok, term()} | {stop, term()}), term(),
           map()) -> term().
fold(StoreRef, DbName0, Fun, Acc, Opts) ->
    DbName = barrel_keyspace:resolve(DbName0),
    Start = case maps:get(from, Opts, undefined) of
        undefined -> barrel_store_keys:history_prefix(DbName);
        From -> barrel_store_keys:history_key(DbName, From)
    end,
    End = case maps:get(to, Opts, undefined) of
        undefined -> barrel_store_keys:history_end(DbName);
        %% fold_range's upper bound is exclusive; HLC keys are fixed
        %% width, so appending a zero byte makes `to' inclusive
        To -> <<(barrel_store_keys:history_key(DbName, To))/binary, 0>>
    end,
    Limit = maps:get(limit, Opts, infinity),
    WrapFun = fun(Key, Value, {N, AccIn}) ->
        Hlc = barrel_store_keys:decode_history_key(DbName, Key),
        Entry = (decode_entry(Value))#{hlc => Hlc},
        case Fun(Entry, AccIn) of
            {ok, AccOut} when N + 1 >= Limit -> {stop, {N + 1, AccOut}};
            {ok, AccOut} -> {ok, {N + 1, AccOut}};
            {stop, AccOut} -> {stop, {N + 1, AccOut}}
        end
    end,
    {_, FinalAcc} =
        barrel_store_rocksdb:fold_range(StoreRef, Start, End, WrapFun, {0, Acc}),
    FinalAcc.

%% @doc Every version of a document still resolvable: the current
%% winner plus the version chain (conflict and superseded siblings),
%% newest first by version. Each item:
%% `#{version, status (current | conflict | superseded), deleted, vv}'.
-spec get_doc_versions(barrel_store_rocksdb:db_ref(), db_name(), docid()) ->
    {ok, [map()]} | {error, not_found}.
get_doc_versions(StoreRef, DbName0, DocId) ->
    DbName = barrel_keyspace:resolve(DbName0),
    EntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, EntityKey) of
        {ok, Columns} ->
            Current = #{
                version => barrel_version:to_token(
                    barrel_version:decode(
                        proplists:get_value(?COL_VERSION, Columns))),
                status => current,
                deleted =>
                    proplists:get_value(?COL_DELETED, Columns, <<"false">>)
                        =:= <<"true">>,
                vv => barrel_vv:decode(
                    proplists:get_value(?COL_VV, Columns, <<>>))
            },
            {ok, [Current | chain_versions(StoreRef, DbName, DocId)]};
        not_found ->
            {error, not_found}
    end.

%% @doc The body of one version of a document: the current body when
%% the token is the winner, the version-keyed archive otherwise. Bodies
%% of swept versions are gone (`{error, not_found}').
-spec get_version_body(barrel_store_rocksdb:db_ref(), db_name(), docid(),
                       binary()) -> {ok, map()} | {error, not_found}.
get_version_body(StoreRef, DbName0, DocId, VersionToken) ->
    DbName = barrel_keyspace:resolve(DbName0),
    Version = barrel_version:from_token(VersionToken),
    EntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    BodyKey = case barrel_store_rocksdb:get_entity(StoreRef, EntityKey) of
        {ok, Columns} ->
            case barrel_version:decode(
                     proplists:get_value(?COL_VERSION, Columns)) of
                Version ->
                    barrel_store_keys:doc_body(DbName, DocId);
                _Other ->
                    barrel_store_keys:doc_body_rev(
                        DbName, DocId, barrel_version:encode(Version))
            end;
        not_found ->
            barrel_store_keys:doc_body_rev(
                DbName, DocId, barrel_version:encode(Version))
    end,
    case barrel_store_rocksdb:body_get(StoreRef, BodyKey) of
        {ok, CborBin} ->
            {ok, barrel_docdb_codec_cbor:decode_any(CborBin)};
        not_found ->
            {error, not_found};
        {error, _} = Err ->
            Err
    end.

%% @doc The oldest HLC the history is complete from, advanced by the
%% retention sweeper. `undefined' until a sweep runs (history complete
%% since database creation).
-spec history_floor(barrel_store_rocksdb:db_ref(), db_name()) ->
    barrel_hlc:timestamp() | undefined.
history_floor(StoreRef, DbName0) ->
    DbName = barrel_keyspace:resolve(DbName0),
    Key = barrel_store_keys:db_meta(DbName, ?HISTORY_FLOOR_META),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, HlcBin} -> barrel_hlc:decode(HlcBin);
        not_found -> undefined
    end.

%%====================================================================
%% Entry codec
%%====================================================================

%% Format: <<Cause:8, Deleted:8, DocIdLen:16, DocId/binary,
%%           VerLen:16, VersionEnc/binary, VV/binary>>
%% The VV takes the remainder: its codec is self-delimiting.

%% @doc Encode a history entry value.
-spec encode_entry(docid(), barrel_version:version(), boolean(), cause(),
                   barrel_vv:vv()) -> binary().
encode_entry(DocId, Version, Deleted, Cause, VV) ->
    VersionEnc = barrel_version:encode(Version),
    VVBin = barrel_vv:encode(VV),
    <<(cause_to_byte(Cause)):8,
      (case Deleted of true -> 1; false -> 0 end):8,
      (byte_size(DocId)):16, DocId/binary,
      (byte_size(VersionEnc)):16, VersionEnc/binary,
      VVBin/binary>>.

%% @doc Decode a history entry value (without the key's HLC).
-spec decode_entry(binary()) -> map().
decode_entry(<<CauseByte:8, DelByte:8,
               DocIdLen:16, DocId:DocIdLen/binary,
               VerLen:16, VersionEnc:VerLen/binary,
               VVBin/binary>>) ->
    #{
        id => DocId,
        version => barrel_version:to_token(barrel_version:decode(VersionEnc)),
        deleted => DelByte =:= 1,
        cause => byte_to_cause(CauseByte),
        vv => barrel_vv:decode(VVBin)
    }.

%%====================================================================
%% Internal
%%====================================================================

cause_to_byte(local) -> 0;
cause_to_byte(replicated) -> 1;
cause_to_byte(resolve) -> 2.

byte_to_cause(0) -> local;
byte_to_cause(1) -> replicated;
byte_to_cause(2) -> resolve.

%% Chain scan: `<<Flag:8, Deleted:8, VV/binary>>' per sibling (see
%% barrel_db_server:sibling_entry/3), keyed by version encoding, so the
%% scan is already version-ordered; reverse for newest first.
chain_versions(StoreRef, DbName, DocId) ->
    Start = barrel_store_keys:doc_version_prefix(DbName, DocId),
    End = barrel_store_keys:doc_version_end(DbName, DocId),
    Fold = fun(Key, <<FlagByte:8, DelByte:8, VVBin/binary>>, Acc) ->
        VersionEnc = barrel_store_keys:decode_doc_version_key(
                         DbName, DocId, Key),
        {ok, [#{
            version => barrel_version:to_token(
                           barrel_version:decode(VersionEnc)),
            status => case FlagByte of
                1 -> conflict;
                _ -> superseded
            end,
            deleted => DelByte =:= 1,
            vv => barrel_vv:decode(VVBin)
        } | Acc]}
    end,
    barrel_store_rocksdb:fold_range(StoreRef, Start, End, Fold, []).
