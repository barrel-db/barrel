%%%-------------------------------------------------------------------
%%% @doc Barrel facade: the embeddable edge database.
%%%
%%% Composes the document layer (`barrel_docdb'), the vector layer
%%% (`barrel_vectordb'), and object storage (`barrel_objectdb', via the docdb
%%% attachment backend) behind one API. A barrel database is a docdb database
%%% plus a vectordb store that share a name and a single id space: a document and
%%% its vector are addressed by the same id.
%%%
%%% {@link open/2} returns a handle used by the rest of this module. Each
%%% underlying application stays usable on its own; this facade adds no storage
%%% of its own, it only coordinates the layers.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel).

%% Lifecycle
-export([
    open/1,
    open/2,
    close/1,
    info/1
]).

%% Documents (barrel_docdb)
-export([
    put_doc/2,
    put_doc/3,
    get_doc/2,
    get_doc/3,
    delete_doc/2,
    find/2,
    find/3
]).

%% Attachments / blobs (barrel_docdb, optionally backed by barrel_objectdb)
-export([
    put_attachment/4,
    get_attachment/3,
    delete_attachment/3,
    list_attachments/2,
    attachment_info/3
]).

%% Attachment streaming
-export([
    open_attachment_reader/3,
    read_attachment/1,
    close_attachment_reader/1,
    open_attachment_writer/4,
    write_attachment/2,
    finish_attachment/1,
    abort_attachment/1
]).

%% Changes feed (barrel_docdb)
-export([
    changes/2,
    changes/3,
    subscribe/2,
    subscribe/3,
    hlc_encode/1,
    hlc_decode/1
]).

%% Vectors (barrel_vectordb)
-export([
    vector_add/4,
    vector_add/5,
    vector_get/2,
    vector_delete/2,
    search/3,
    search_vector/3,
    search_bm25/3,
    search_hybrid/3,
    vector_stats/1
]).

-export_type([db/0]).

-type db() :: #{
    name := atom(),
    docdb := binary(),
    vstore := atom()
}.
%% Handle for a composed barrel database.

-type stream() :: term().
%% Opaque attachment read/write stream handle.

%%====================================================================
%% Lifecycle
%%====================================================================

%% @doc Open a composed barrel database with default options.
-spec open(atom()) -> {ok, db()} | {error, term()}.
open(Name) ->
    open(Name, #{}).

%% @doc Open a composed barrel database.
%%
%% `Opts' may carry `docdb => map()' (passed to {@link barrel_docdb:create_db/2},
%% including `att_opts' to choose an attachment backend) and `vectordb => map()'
%% (a {@link barrel_vectordb} store config; its `name' is set from `Name'). Opens
%% the docdb database if it exists, otherwise creates it, then starts the vectordb
%% store.
-spec open(atom(), map()) -> {ok, db()} | {error, term()}.
open(Name, Opts) when is_atom(Name), is_map(Opts) ->
    DbBin = atom_to_binary(Name, utf8),
    DocOpts = maps:get(docdb, Opts, #{}),
    VecConfig = maps:get(vectordb, Opts, #{}),
    case ensure_docdb(DbBin, DocOpts) of
        {ok, _DbPid} ->
            case barrel_vectordb:start_link(VecConfig#{name => Name}) of
                {ok, _StorePid} ->
                    {ok, #{name => Name, docdb => DbBin, vstore => Name}};
                {error, _} = VErr ->
                    _ = barrel_docdb:close_db(DbBin),
                    VErr
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Close a composed barrel database (vector store then document database).
-spec close(db()) -> ok | {error, term()}.
close(#{docdb := DbBin, vstore := Store}) ->
    _ = barrel_vectordb:stop(Store),
    barrel_docdb:close_db(DbBin).

%% @doc Database metadata.
-spec info(db()) -> {ok, map()} | {error, term()}.
info(#{docdb := DbBin}) ->
    barrel_docdb:db_info(DbBin).

%%====================================================================
%% Documents (barrel_docdb)
%%====================================================================

%% @doc Create or update a document.
-spec put_doc(db(), map()) -> {ok, map()} | {error, term()}.
put_doc(#{docdb := DbBin}, Doc) ->
    barrel_docdb:put_doc(DbBin, Doc).

%% @doc Create or update a document with options.
-spec put_doc(db(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(#{docdb := DbBin}, Doc, Opts) ->
    barrel_docdb:put_doc(DbBin, Doc, Opts).

%% @doc Get a document by id.
-spec get_doc(db(), binary()) -> {ok, map()} | {error, term()}.
get_doc(#{docdb := DbBin}, DocId) ->
    barrel_docdb:get_doc(DbBin, DocId).

%% @doc Get a document by id with options.
-spec get_doc(db(), binary(), map()) -> {ok, map()} | {ok, binary(), map()} | {error, term()}.
get_doc(#{docdb := DbBin}, DocId, Opts) ->
    barrel_docdb:get_doc(DbBin, DocId, Opts).

%% @doc Delete a document by id.
-spec delete_doc(db(), binary()) -> {ok, map()} | {error, term()}.
delete_doc(#{docdb := DbBin}, DocId) ->
    barrel_docdb:delete_doc(DbBin, DocId).

%% @doc Run a declarative query.
-spec find(db(), map()) -> {ok, [map()], map()} | {error, term()}.
find(#{docdb := DbBin}, Query) ->
    barrel_docdb:find(DbBin, Query).

%% @doc Run a declarative query with options.
-spec find(db(), map(), map()) -> {ok, [map()], map()} | {error, term()}.
find(#{docdb := DbBin}, Query, Opts) ->
    barrel_docdb:find(DbBin, Query, Opts).

%%====================================================================
%% Attachments / blobs (barrel_docdb)
%%====================================================================

%% @doc Store a document attachment.
-spec put_attachment(db(), binary(), binary(), binary()) -> {ok, map()} | {error, term()}.
put_attachment(#{docdb := DbBin}, DocId, AttName, Data) ->
    barrel_docdb:put_attachment(DbBin, DocId, AttName, Data).

%% @doc Fetch a document attachment.
-spec get_attachment(db(), binary(), binary()) -> {ok, binary()} | {error, term()}.
get_attachment(#{docdb := DbBin}, DocId, AttName) ->
    barrel_docdb:get_attachment(DbBin, DocId, AttName).

%% @doc Delete a document attachment.
-spec delete_attachment(db(), binary(), binary()) -> ok | {error, term()}.
delete_attachment(#{docdb := DbBin}, DocId, AttName) ->
    barrel_docdb:delete_attachment(DbBin, DocId, AttName).

%% @doc List a document's attachment names.
-spec list_attachments(db(), binary()) -> [binary()].
list_attachments(#{docdb := DbBin}, DocId) ->
    barrel_docdb:list_attachments(DbBin, DocId).

%% @doc Get attachment metadata (content type, length, digest).
-spec attachment_info(db(), binary(), binary()) -> {ok, map()} | {error, term()}.
attachment_info(#{docdb := DbBin}, DocId, AttName) ->
    barrel_docdb:get_attachment_info(DbBin, DocId, AttName).

%%====================================================================
%% Attachment streaming (barrel_docdb)
%%====================================================================

%% @doc Open a streaming reader for an attachment.
-spec open_attachment_reader(db(), binary(), binary()) -> {ok, stream()} | {error, term()}.
open_attachment_reader(#{docdb := DbBin}, DocId, AttName) ->
    barrel_docdb:open_attachment_stream(DbBin, DocId, AttName).

%% @doc Read the next chunk from an attachment reader.
-spec read_attachment(stream()) -> {ok, binary(), stream()} | eof | {error, term()}.
read_attachment(Stream) ->
    barrel_docdb:read_attachment_chunk(Stream).

%% @doc Close an attachment reader.
-spec close_attachment_reader(stream()) -> ok.
close_attachment_reader(Stream) ->
    barrel_docdb:close_attachment_stream(Stream).

%% @doc Open a streaming writer for an attachment.
-spec open_attachment_writer(db(), binary(), binary(), binary()) ->
    {ok, stream()} | {error, term()}.
open_attachment_writer(#{docdb := DbBin}, DocId, AttName, ContentType) ->
    barrel_docdb:open_attachment_writer(DbBin, DocId, AttName, ContentType).

%% @doc Write a chunk to an attachment writer.
-spec write_attachment(stream(), binary()) -> {ok, stream()} | {error, term()}.
write_attachment(Writer, Data) ->
    barrel_docdb:write_attachment_chunk(Writer, Data).

%% @doc Finalise an attachment writer.
-spec finish_attachment(stream()) -> {ok, map()} | {error, term()}.
finish_attachment(Writer) ->
    barrel_docdb:finish_attachment_writer(Writer).

%% @doc Abort an attachment writer.
-spec abort_attachment(stream()) -> ok.
abort_attachment(Writer) ->
    barrel_docdb:abort_attachment_writer(Writer).

%%====================================================================
%% Changes feed (barrel_docdb)
%%====================================================================

%% @doc Get changes since an HLC timestamp (`first' for all).
-spec changes(db(), term()) -> {ok, [map()], term()}.
changes(#{docdb := DbBin}, Since) ->
    barrel_docdb:get_changes(DbBin, Since).

%% @doc Get changes since an HLC timestamp, with options.
-spec changes(db(), term(), map()) -> {ok, [map()], term()}.
changes(#{docdb := DbBin}, Since, Opts) ->
    barrel_docdb:get_changes(DbBin, Since, Opts).

%% @doc Subscribe to a changes stream from `Since'. Returns a stream pid.
-spec subscribe(db(), term()) -> {ok, pid()} | {error, term()}.
subscribe(#{docdb := DbBin}, Since) ->
    barrel_docdb:subscribe_changes(DbBin, Since).

%% @doc Subscribe to a changes stream with options.
-spec subscribe(db(), term(), map()) -> {ok, pid()} | {error, term()}.
subscribe(#{docdb := DbBin}, Since, Opts) ->
    barrel_docdb:subscribe_changes(DbBin, Since, Opts).

%% @doc Encode an HLC timestamp (the cursor returned by {@link changes/2}) as a
%% JSON/URL-safe string, for transports that serialise the changes feed.
-spec hlc_encode(term()) -> binary().
hlc_encode(Hlc) ->
    base64:encode(barrel_hlc:encode(Hlc)).

%% @doc Decode a cursor produced by {@link hlc_encode/1} back to an HLC
%% timestamp usable as the `Since' argument of {@link changes/2}.
-spec hlc_decode(binary()) -> term().
hlc_decode(Cursor) ->
    barrel_hlc:decode(base64:decode(Cursor)).

%%====================================================================
%% Vectors (barrel_vectordb)
%%====================================================================

%% @doc Add a document to the vector store (text embedded by the store).
-spec vector_add(db(), binary(), binary(), map()) -> term().
vector_add(#{vstore := Store}, Id, Text, Metadata) ->
    barrel_vectordb:add(Store, Id, Text, Metadata).

%% @doc Add a document to the vector store with an explicit vector.
-spec vector_add(db(), binary(), binary(), map(), [float()]) -> term().
vector_add(#{vstore := Store}, Id, Text, Metadata, Vector) ->
    barrel_vectordb:add(Store, Id, Text, Metadata, Vector).

%% @doc Get a stored vector entry by id.
-spec vector_get(db(), binary()) -> term().
vector_get(#{vstore := Store}, Id) ->
    barrel_vectordb:get(Store, Id).

%% @doc Delete a vector entry by id.
-spec vector_delete(db(), binary()) -> term().
vector_delete(#{vstore := Store}, Id) ->
    barrel_vectordb:delete(Store, Id).

%% @doc Semantic search over the vector store.
-spec search(db(), binary(), map()) -> term().
search(#{vstore := Store}, Query, Opts) ->
    barrel_vectordb:search(Store, Query, Opts).

%% @doc Vector search with an explicit query vector.
-spec search_vector(db(), [float()], map()) -> term().
search_vector(#{vstore := Store}, Vector, Opts) ->
    barrel_vectordb:search_vector(Store, Vector, Opts).

%% @doc BM25 keyword search.
-spec search_bm25(db(), binary(), map()) -> term().
search_bm25(#{vstore := Store}, Query, Opts) ->
    barrel_vectordb:search_bm25(Store, Query, Opts).

%% @doc Hybrid (vector + BM25) search.
-spec search_hybrid(db(), binary(), map()) -> term().
search_hybrid(#{vstore := Store}, Query, Opts) ->
    barrel_vectordb:search_hybrid(Store, Query, Opts).

%% @doc Vector store statistics.
-spec vector_stats(db()) -> term().
vector_stats(#{vstore := Store}) ->
    barrel_vectordb:stats(Store).

%%====================================================================
%% Internal
%%====================================================================

%% @private Open the docdb database, creating it if it does not exist.
-spec ensure_docdb(binary(), map()) -> {ok, pid()} | {error, term()}.
ensure_docdb(DbBin, DocOpts) ->
    case barrel_docdb:open_db(DbBin) of
        {ok, Pid} ->
            {ok, Pid};
        {error, _} ->
            barrel_docdb:create_db(DbBin, DocOpts)
    end.
