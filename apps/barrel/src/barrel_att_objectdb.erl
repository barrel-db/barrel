%%%-------------------------------------------------------------------
%%% @doc Object-store attachment backend.
%%%
%%% Implements the barrel_docdb attachment backend behaviour
%%% ({@link barrel_att_backend}) on top of {@link barrel_objectdb}, so document
%%% attachments can be stored on local disk, S3, or a remote object store instead
%%% of the default RocksDB BlobDB. Select it per database with
%%% `att_opts => #{backend => barrel_att_objectdb, objectdb_backend => ...,
%%% objectdb_config => ...}'.
%%%
%%% Attachments are stored whole (one object per attachment) with a small
%%% metadata object alongside; streaming is buffered and flushed on finish. This
%%% is the foundation for a future agent filesystem.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_objectdb).
-behaviour(barrel_att_backend).

-export([open/2, close/1]).
-export([put/5, put/6, get/4, delete/4, delete_all/3, fold/5, get_info/4]).
-export([put_stream/5, put_stream/6, write_chunk/2, finish_stream/1, abort_stream/1]).
-export([get_stream/4, read_chunk/1, close_stream/1]).

-define(DEFAULT_CONTENT_TYPE, <<"application/octet-stream">>).

%%====================================================================
%% Lifecycle
%%====================================================================

%% @private
open(Path, Options) ->
    Backend = maps:get(objectdb_backend, Options, barrel_objectdb_fs),
    Config = maps:get(objectdb_config, Options, #{dir => Path}),
    case barrel_objectdb:open(Backend, Config) of
        {ok, Store} -> {ok, #{objstore => Store}};
        {error, _} = Err -> Err
    end.

%% @private
close(_AttRef) ->
    ok.

%%====================================================================
%% Attachments
%%====================================================================

%% @private
put(AttRef, DbName, DocId, AttName, Data) ->
    put(AttRef, DbName, DocId, AttName, Data, #{}).

%% @private
put(AttRef, DbName, DocId, AttName, Data, _Opts) when is_binary(Data) ->
    do_store(AttRef, DbName, DocId, AttName, Data, ?DEFAULT_CONTENT_TYPE).

%% @private
get(#{objstore := Store}, DbName, DocId, AttName) ->
    barrel_objectdb:get(Store, data_key(DbName, DocId, AttName)).

%% @private
delete(#{objstore := Store}, DbName, DocId, AttName) ->
    _ = barrel_objectdb:delete(Store, data_key(DbName, DocId, AttName)),
    _ = barrel_objectdb:delete(Store, meta_key(DbName, DocId, AttName)),
    ok.

%% @private
delete_all(AttRef, DbName, DocId) ->
    lists:foreach(
        fun(AttName) -> delete(AttRef, DbName, DocId, AttName) end,
        att_names(AttRef, DbName, DocId)
    ),
    ok.

%% @private Fold over attachment names. Matches the backend fold protocol:
%% Fun(Name, Value, Acc) returns {ok, Acc} | {stop, Acc} | stop. Value is the
%% attachment info map (callers that only need names, like list_attachments,
%% ignore it).
fold(AttRef, DbName, DocId, Fun, Acc0) ->
    fold_names(att_names(AttRef, DbName, DocId), AttRef, DbName, DocId, Fun, Acc0).

fold_names([], _AttRef, _DbName, _DocId, _Fun, Acc) ->
    Acc;
fold_names([Name | Rest], AttRef, DbName, DocId, Fun, Acc) ->
    Value = case get_info(AttRef, DbName, DocId, Name) of
                {ok, Info} -> Info;
                _ -> undefined
            end,
    case Fun(Name, Value, Acc) of
        {ok, Acc1} -> fold_names(Rest, AttRef, DbName, DocId, Fun, Acc1);
        {stop, Acc1} -> Acc1;
        stop -> Acc
    end.

%% @private
get_info(#{objstore := Store}, DbName, DocId, AttName) ->
    case barrel_objectdb:get(Store, meta_key(DbName, DocId, AttName)) of
        {ok, Bin} -> {ok, binary_to_term(Bin)};
        {error, not_found} -> {error, not_found};
        {error, _} = Err -> Err
    end.

%%====================================================================
%% Streaming (buffered, flushed on finish)
%%====================================================================

%% @private
put_stream(AttRef, DbName, DocId, AttName, ContentType) ->
    put_stream(AttRef, DbName, DocId, AttName, ContentType, #{}).

%% @private
put_stream(AttRef, DbName, DocId, AttName, ContentType, _Opts) ->
    {ok, #{
        type => write,
        att_ref => AttRef,
        db_name => DbName,
        doc_id => DocId,
        att_name => AttName,
        content_type => ContentType,
        buffer => <<>>
    }}.

%% @private
write_chunk(#{type := write, buffer := Buffer} = Stream, Data) ->
    {ok, Stream#{buffer => <<Buffer/binary, Data/binary>>}}.

%% @private
finish_stream(#{type := write, att_ref := AttRef, db_name := DbName, doc_id := DocId,
                att_name := AttName, content_type := ContentType, buffer := Buffer}) ->
    do_store(AttRef, DbName, DocId, AttName, Buffer, ContentType).

%% @private
abort_stream(_Stream) ->
    ok.

%% @private
get_stream(AttRef, DbName, DocId, AttName) ->
    case get_info(AttRef, DbName, DocId, AttName) of
        {ok, Info} ->
            {ok, #{
                type => read,
                att_ref => AttRef,
                db_name => DbName,
                doc_id => DocId,
                att_name => AttName,
                info => Info,
                sent => false
            }};
        {error, _} = Err ->
            Err
    end.

%% @private
read_chunk(#{sent := true}) ->
    eof;
read_chunk(#{type := read, att_ref := AttRef, db_name := DbName, doc_id := DocId,
             att_name := AttName} = Stream) ->
    case get(AttRef, DbName, DocId, AttName) of
        {ok, Data} -> {ok, Data, Stream#{sent => true}};
        {error, _} = Err -> Err
    end.

%% @private
close_stream(_Stream) ->
    ok.

%%====================================================================
%% Internal
%%====================================================================

do_store(#{objstore := Store}, DbName, DocId, AttName, Data, ContentType) ->
    Info = #{
        name => AttName,
        content_type => ContentType,
        length => byte_size(Data),
        digest => digest(Data),
        chunked => false
    },
    case barrel_objectdb:put(Store, data_key(DbName, DocId, AttName), Data) of
        ok ->
            case barrel_objectdb:put(Store, meta_key(DbName, DocId, AttName), term_to_binary(Info)) of
                ok -> {ok, Info};
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

att_names(#{objstore := Store}, DbName, DocId) ->
    Prefix = data_prefix(DbName, DocId),
    case barrel_objectdb:list(Store, Prefix) of
        {ok, Keys} -> [strip_prefix(Key, Prefix) || Key <- Keys];
        {error, _} -> []
    end.

%% barrel_objectdb:list/2 only returns keys carrying Prefix, so this is safe.
strip_prefix(Key, Prefix) ->
    PS = byte_size(Prefix),
    <<_:PS/binary, Name/binary>> = Key,
    Name.

data_prefix(DbName, DocId) ->
    <<"a", 0, DbName/binary, 0, DocId/binary, 0>>.

data_key(DbName, DocId, AttName) ->
    <<(data_prefix(DbName, DocId))/binary, AttName/binary>>.

meta_key(DbName, DocId, AttName) ->
    <<"m", 0, DbName/binary, 0, DocId/binary, 0, AttName/binary>>.

digest(Data) ->
    <<"sha256-", (to_hex(crypto:hash(sha256, Data)))/binary>>.

to_hex(Bin) ->
    << <<(hex_char(N div 16)), (hex_char(N rem 16))>> || <<N>> <= Bin >>.

hex_char(N) when N < 10 -> $0 + N;
hex_char(N) -> $a + N - 10.
