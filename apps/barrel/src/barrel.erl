%%%-------------------------------------------------------------------
%%% @doc Barrel facade: the embeddable edge database.
%%%
%%% Composes the document layer (`barrel_docdb') and the vector layer
%%% (`barrel_vectordb') behind one API. A barrel database is a docdb database
%%% plus a vectordb store that share a name and a single id space: a document,
%%% its attachments (blobs), and its vector are all addressed by the same id.
%%% Blobs are docdb attachments; their storage backend is pluggable per database
%%% via the docdb `barrel_att_backend' seam.
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
    delete/1,
    info/1
]).

%% Documents (barrel_docdb)
-export([
    put_doc/2,
    put_doc/3,
    put_docs/2,
    put_docs/3,
    get_doc/2,
    get_doc/3,
    get_docs/2,
    get_docs/3,
    delete_doc/2,
    delete_docs/2,
    find/2,
    find/3
]).

%% BQL (documents + table functions over the vector store)
-export([
    query/2,
    query/3,
    query_fold/5,
    explain_query/2,
    explain_query/3,
    subscribe_query/2,
    subscribe_query/3,
    unsubscribe_query/1
]).

%% Attachments / blobs (barrel_docdb; backend pluggable per database)
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
    vector_add_batch/2,
    vector_get/2,
    vector_delete/2,
    search/3,
    search_vector/3,
    search_bm25/3,
    search_hybrid/3,
    vector_stats/1
]).

-export_type([db/0]).

%% Outbox tag carried by every record-mode write (and, via docdb
%% config, by replication-applied writes); consumed by the record
%% indexer.
-define(EMBED_TAG, <<"embed">>).

-type db() :: #{
    name := atom(),
    docdb := binary(),
    vstore := atom(),
    embedding => barrel_embedding_policy:policy(),
    embed => term(),
    dimensions => pos_integer()
}.
%% Handle for a composed barrel database. The optional `embedding',
%% `embed', and `dimensions' fields are present on record-mode databases
%% (opened with the `embedding' option): the validated policy, the
%% facade's barrel_embed state, and the resolved vector dimension.

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
%%
%% `embedding => PolicyMap' opens the database in RECORD MODE: writes are
%% tagged for asynchronous vector indexing driven by the database's
%% embedding policy (see {@link barrel_embedding_policy}), the vector
%% store keeps only vectors and indexes (text/metadata read through the
%% record's documents), and BM25 defaults to the disk backend so it
%% survives restarts. Without the option, behavior is unchanged.
-spec open(atom(), map()) -> {ok, db()} | {error, term()}.
open(Name, Opts) when is_atom(Name), is_map(Opts) ->
    case maps:get(embedding, Opts, undefined) of
        undefined -> open_plain(Name, Opts);
        PolicyMap -> open_record(Name, Opts, PolicyMap)
    end.

%% @private Today's composed open, unchanged.
open_plain(Name, Opts) ->
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

%% @private Record-mode open: validate the policy, resolve the vector
%% dimension, init the facade's embedder, persist the policy, and start
%% the vector store with the read-through docstore adapter.
open_record(Name, Opts, PolicyMap) ->
    case barrel_embedding_policy:validate(PolicyMap) of
        {ok, Policy} ->
            do_open_record(Name, Opts, Policy);
        {error, _} = Err ->
            Err
    end.

do_open_record(Name, Opts, Policy) ->
    DbBin = atom_to_binary(Name, utf8),
    %% Replicated arrivals must reach the indexer too: docdb tags
    %% replication-applied writes with the embed tag (docdb stays
    %% ignorant of why). Note: a docdb already running as plain in this
    %% VM keeps its config; runtime config update is a follow-up.
    DocOpts0 = maps:get(docdb, Opts, #{}),
    DocOpts = DocOpts0#{outbox_tags_on_replication => [?EMBED_TAG]},
    VecConfig0 = maps:get(vectordb, Opts, #{}),
    case resolve_dimension(Policy, VecConfig0) of
        {ok, Dim} ->
            case ensure_docdb(DbBin, DocOpts) of
                {ok, _DbPid} ->
                    ok = persist_policy(DbBin, Policy),
                    case init_embed(Policy, Dim) of
                        {ok, EmbedState} ->
                            VecConfig = VecConfig0#{
                                name => Name,
                                dimension => Dim,
                                %% Memory BM25 cannot rebuild without a
                                %% text CF; record mode defaults to disk.
                                bm25_backend =>
                                    maps:get(bm25_backend, VecConfig0, disk),
                                docstore => {barrel_record_docstore,
                                             #{db => DbBin, policy => Policy}}
                            },
                            case barrel_vectordb:start_link(VecConfig) of
                                {ok, _StorePid} ->
                                    case start_indexer(Name, DbBin, Policy,
                                                       EmbedState, Dim) of
                                        ok ->
                                            {ok, #{name => Name, docdb => DbBin,
                                                   vstore => Name,
                                                   embedding => Policy,
                                                   embed => EmbedState,
                                                   dimensions => Dim}};
                                        {error, _} = IErr ->
                                            _ = barrel_vectordb:stop(Name),
                                            _ = barrel_docdb:close_db(DbBin),
                                            IErr
                                    end;
                                {error, _} = VErr ->
                                    _ = barrel_docdb:close_db(DbBin),
                                    VErr
                            end;
                        {error, _} = EErr ->
                            _ = barrel_docdb:close_db(DbBin),
                            EErr
                    end;
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Close a composed barrel database (indexer, then vector store,
%% then document database).
-spec close(db()) -> ok | {error, term()}.
close(#{name := Name, docdb := DbBin, vstore := Store} = Db) ->
    _ = case Db of
        #{embedding := _} -> barrel_record_sup:stop_indexer(Name);
        _ -> ok
    end,
    _ = barrel_vectordb:stop(Store),
    barrel_docdb:close_db(DbBin).

%% @doc Delete a composed barrel database: stop the indexer, destroy
%% the vector store (handles closed, directory removed), and delete
%% the document database's files.
-spec delete(db()) -> ok | {error, term()}.
delete(#{name := Name, docdb := DbBin, vstore := Store} = Db) ->
    _ = case Db of
        #{embedding := _} -> barrel_record_sup:stop_indexer(Name);
        _ -> ok
    end,
    _ = barrel_vectordb:destroy(Store),
    barrel_docdb:delete_db(DbBin).

%% @doc Database metadata.
-spec info(db()) -> {ok, map()} | {error, term()}.
info(#{docdb := DbBin} = Db) ->
    case barrel_docdb:db_info(DbBin) of
        {ok, Info} ->
            case Db of
                #{embedding := Policy, dimensions := Dim} ->
                    {ok, Info#{embedding => Policy, dimensions => Dim}};
                _ ->
                    {ok, Info}
            end;
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Documents (barrel_docdb)
%%====================================================================

%% @doc Create or update a document.
%% On record-mode databases the write is tagged for async vector
%% indexing (same signature, policy applied transparently).
-spec put_doc(db(), map()) -> {ok, map()} | {error, term()}.
put_doc(Db, Doc) ->
    put_doc(Db, Doc, #{}).

%% @doc Create or update a document with options.
%% On record-mode databases: `vector => Vector' supplies an explicit
%% embedding (skips the embedder, indexed synchronously); a policy in
%% sync mode embeds before returning (read-your-write search). Both
%% fail the put on embed/dimension errors BEFORE writing; failures after
%% the document committed are healed by the indexer.
-spec put_doc(db(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(#{docdb := DbBin} = Db, Doc, Opts) ->
    case sync_action(Db, Doc, Opts) of
        async ->
            Result = barrel_docdb:put_doc(DbBin, Doc, record_write_opts(Db, Opts)),
            ok = nudge_indexer(Db, Result),
            Result;
        {sync, ExplicitVector} ->
            put_doc_sync(Db, Doc, maps:remove(vector, Opts), ExplicitVector);
        {error, _} = Err ->
            Err
    end.

%% @doc Create or update multiple documents in one batch.
%% Returns a result per input document, in order.
-spec put_docs(db(), [map()]) -> [{ok, map()} | {error, term()}].
put_docs(Db, Docs) ->
    put_docs(Db, Docs, #{}).

%% @doc Create or update multiple documents in one batch, with options.
%% On a sync-mode record database the batch embeds (embed_batch) before
%% writing and indexes before returning; an embed failure fails the
%% whole batch with nothing written.
-spec put_docs(db(), [map()], map()) -> [{ok, map()} | {error, term()}] | {error, term()}.
put_docs(#{docdb := DbBin} = Db, Docs, Opts) ->
    case validate_batch_embeddings(Db, Docs) of
        ok ->
            case batch_mode(Db) of
                sync ->
                    put_docs_sync(Db, Docs, Opts);
                async ->
                    %% Per-doc _embedding fields persist and are picked up
                    %% by the indexer; the vector opt is single-put only.
                    Results = barrel_docdb:put_docs(
                        DbBin, Docs, record_write_opts(Db, Opts)),
                    ok = nudge_indexer(Db, ok),
                    Results
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Get a document by id.
-spec get_doc(db(), binary()) -> {ok, map()} | {error, term()}.
get_doc(#{docdb := DbBin}, DocId) ->
    barrel_docdb:get_doc(DbBin, DocId).

%% @doc Get a document by id with options.
-spec get_doc(db(), binary(), map()) -> {ok, map()} | {ok, binary(), map()} | {error, term()}.
get_doc(#{docdb := DbBin}, DocId, Opts) ->
    barrel_docdb:get_doc(DbBin, DocId, Opts).

%% @doc Get multiple documents by id in one batch.
%% Returns a result per input id, in order.
-spec get_docs(db(), [binary()]) -> [{ok, map()} | {error, term()}].
get_docs(#{docdb := DbBin}, DocIds) ->
    barrel_docdb:get_docs(DbBin, DocIds).

%% @doc Get multiple documents by id in one batch, with options.
-spec get_docs(db(), [binary()], map()) -> [{ok, map()} | {error, term()}].
get_docs(#{docdb := DbBin}, DocIds, Opts) ->
    barrel_docdb:get_docs(DbBin, DocIds, Opts).

%% @doc Delete a document by id.
%% On record-mode databases the delete is tagged so the indexer removes
%% the document's vector; in sync mode the vector is removed before
%% returning.
-spec delete_doc(db(), binary()) -> {ok, map()} | {error, term()}.
delete_doc(#{docdb := DbBin} = Db, DocId) ->
    case sync_action(Db, #{}, #{}) of
        {sync, undefined} ->
            WriteOpts = record_write_opts(Db, #{return_hlc => true}),
            case barrel_docdb:delete_doc(DbBin, DocId, WriteOpts) of
                {ok, Result} ->
                    ok = sync_index(Db, maps:get(<<"id">>, Result), deindex,
                                    maps:get(hlc, Result)),
                    {ok, maps:remove(hlc, Result)};
                {error, _} = Err ->
                    Err
            end;
        _ ->
            Result = barrel_docdb:delete_doc(DbBin, DocId, record_write_opts(Db, #{})),
            ok = nudge_indexer(Db, Result),
            Result
    end.

%% @doc Delete multiple documents by id. docdb has no bulk delete, so this maps
%% over {@link delete_doc/2} and returns a result per id, in order.
-spec delete_docs(db(), [binary()]) -> [{ok, map()} | {error, term()}].
delete_docs(Db, DocIds) ->
    [delete_doc(Db, DocId) || DocId <- DocIds].

%% @doc Run a declarative query.
-spec find(db(), map()) -> {ok, [map()], map()} | {error, term()}.
find(#{docdb := DbBin}, Query) ->
    barrel_docdb:find(DbBin, Query).

%% @doc Run a declarative query with options.
-spec find(db(), map(), map()) -> {ok, [map()], map()} | {error, term()}.
find(#{docdb := DbBin}, Query, Opts) ->
    barrel_docdb:find(DbBin, Query, Opts).

%%====================================================================
%% BQL
%%====================================================================

%% @doc Run a BQL query (see barrel_bql). Unlike
%% barrel_docdb:query/2, table-function sources (vector_top_k,
%% bm25_top_k, hybrid_top_k) execute here, joined back to their
%% documents. SUBSCRIBE queries need subscribe_query (live queries).
-spec 'query'(db(), binary() | string()) ->
    {ok, [map()], map()} | {error, term()}.
'query'(Db, Bql) ->
    'query'(Db, Bql, #{}).

%% @doc Run a BQL query with options: params (a map for $name
%% placeholders), chunk_size / continuation (streamable queries), and
%% overfetch (table functions, default 3).
-spec 'query'(db(), binary() | string(), map()) ->
    {ok, [map()], map()} | {error, term()}.
'query'(Db, Bql, Opts) ->
    case compile_bql(Bql, Opts) of
        {ok, Plan} ->
            barrel_bql_query:run(Db, Plan, maps:remove(params, Opts));
        {error, _} = Error ->
            Error
    end.

%% @doc Fold BQL rows without materializing the full result.
%% Fun(Row, Acc) -> {ok, Acc} | {stop, Acc}.
-spec query_fold(db(), binary() | string(), map(),
                 fun((map(), term()) -> {ok, term()} | {stop, term()}),
                 term()) ->
    {ok, term(), map()} | {error, term()}.
query_fold(Db, Bql, Opts, Fun, Acc) ->
    case compile_bql(Bql, Opts) of
        {ok, Plan} ->
            barrel_bql_query:fold(Db, Plan, maps:remove(params, Opts),
                                  Fun, Acc);
        {error, _} = Error ->
            Error
    end.

%% @doc Explain a BQL query: source, streamability, warnings, and the
%% engine's index strategy for collection queries.
-spec explain_query(db(), binary() | string()) ->
    {ok, map()} | {error, term()}.
explain_query(Db, Bql) ->
    explain_query(Db, Bql, #{}).

-spec explain_query(db(), binary() | string(), map()) ->
    {ok, map()} | {error, term()}.
explain_query(Db, Bql, Opts) ->
    case compile_bql(Bql, Opts) of
        {ok, Plan} -> barrel_bql_query:explain(Db, Plan);
        {error, _} = Error -> Error
    end.

%% @doc Start a live query for a BQL SUBSCRIBE statement. The initial
%% snapshot and add/change/remove deltas are pushed to the owner
%% process (default: the caller); see barrel_bql_live for the message
%% shapes. Returns an opaque subscription for unsubscribe_query/1;
%% monitor its pid for crash signals. Owner death tears the query
%% down.
-spec subscribe_query(db(), binary() | string()) ->
    {ok, #{ref := reference(), pid := pid()}} | {error, term()}.
subscribe_query(Db, Bql) ->
    subscribe_query(Db, Bql, #{}).

%% @doc Like subscribe_query/2 with options: params, owner.
-spec subscribe_query(db(), binary() | string(), map()) ->
    {ok, #{ref := reference(), pid := pid()}} | {error, term()}.
subscribe_query(Db, Bql, Opts) ->
    case compile_bql(Bql, Opts) of
        {ok, Plan} ->
            barrel_bql_query:subscribe_plan(Db, Plan,
                                            maps:with([owner], Opts));
        {error, _} = Error ->
            Error
    end.

%% @doc Stop a live query. Idempotent.
-spec unsubscribe_query(#{ref := reference(), pid := pid()}) -> ok.
unsubscribe_query(#{pid := Pid}) ->
    try gen_server:call(Pid, stop)
    catch exit:_ -> ok
    end.

compile_bql(Bql, Opts) ->
    barrel_bql:compile(Bql, #{params => maps:get(params, Opts, #{})}).

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
vector_add(#{embedding := _}, _Id, _Text, _Metadata) ->
    %% Record mode: the document is the only write path (use put_doc;
    %% explicit vectors via the `vector' put option).
    {error, record_mode};
vector_add(#{vstore := Store}, Id, Text, Metadata) ->
    barrel_vectordb:add(Store, Id, Text, Metadata).

%% @doc Add a document to the vector store with an explicit vector.
-spec vector_add(db(), binary(), binary(), map(), [float()]) -> term().
vector_add(#{embedding := _}, _Id, _Text, _Metadata, _Vector) ->
    {error, record_mode};
vector_add(#{vstore := Store}, Id, Text, Metadata, Vector) ->
    barrel_vectordb:add(Store, Id, Text, Metadata, Vector).

%% @doc Add many documents to the vector store in one atomic batch.
%%
%% Each element is either `{Id, Text, Metadata}' (text embedded by the store) or
%% `{Id, Text, Metadata, Vector}' (explicit vector). All elements must be the
%% same shape; a mixed batch returns `{error, mixed_batch}'.
-spec vector_add_batch(db(), [tuple()]) ->
    {ok, map()} | {error, term()}.
vector_add_batch(#{embedding := _}, _Docs) ->
    {error, record_mode};
vector_add_batch(#{vstore := Store}, Docs) ->
    case batch_kind(Docs) of
        explicit -> barrel_vectordb:add_vector_batch(Store, Docs);
        auto -> barrel_vectordb:add_batch(Store, Docs);
        mixed -> {error, mixed_batch}
    end.

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
search(#{embedding := _, embed := Embed, vstore := Store}, Query, Opts) ->
    %% Record mode: the facade owns embedding (the store has none)
    case embed_one(Query, Embed) of
        {ok, Vector} -> barrel_vectordb:search_vector(Store, Vector, Opts);
        {error, Reason} -> {error, {embed_failed, Reason}}
    end;
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
%% On record-mode databases the facade embeds the query itself and
%% passes it as `query_vector' (the store has no embedder).
-spec search_hybrid(db(), binary(), map()) -> term().
search_hybrid(#{embedding := _, embed := Embed, vstore := Store}, Query, Opts) ->
    case embed_one(Query, Embed) of
        {ok, Vector} ->
            barrel_vectordb:search_hybrid(Store, Query,
                                          Opts#{query_vector => Vector});
        {error, Reason} ->
            {error, {embed_failed, Reason}}
    end;
search_hybrid(#{vstore := Store}, Query, Opts) ->
    barrel_vectordb:search_hybrid(Store, Query, Opts).

%% @doc Vector store statistics.
-spec vector_stats(db()) -> term().
vector_stats(#{vstore := Store}) ->
    barrel_vectordb:stats(Store).

%%====================================================================
%% Internal
%%====================================================================

%% @private Classify a vector batch: all 4-tuples (explicit vectors), all
%% 3-tuples (auto-embed), or mixed. An empty list embeds (a no-op batch).
-spec batch_kind([tuple()]) -> explicit | auto | mixed.
batch_kind([]) ->
    auto;
batch_kind(Docs) ->
    Sizes = lists:usort([tuple_size(D) || D <- Docs, is_tuple(D)]),
    case Sizes of
        [4] -> explicit;
        [3] -> auto;
        _ -> mixed
    end.

%% @private Open the docdb database, creating it if it does not exist.
-spec ensure_docdb(binary(), map()) -> {ok, pid()} | {error, term()}.
ensure_docdb(DbBin, DocOpts) ->
    case barrel_docdb:open_db(DbBin) of
        {ok, Pid} ->
            {ok, Pid};
        {error, _} ->
            barrel_docdb:create_db(DbBin, DocOpts)
    end.

%%====================================================================
%% Internal: record mode
%%====================================================================

%% Local doc holding the persisted embedding policy.
-define(POLICY_DOC, <<"_barrel/embedding">>).

%% @private Tag record-mode writes for the embedding indexer; plain
%% databases pass options through untouched. User-supplied outbox tags
%% are preserved.
-spec record_write_opts(db(), map()) -> map().
record_write_opts(#{embedding := _}, Opts) ->
    Tags = maps:get(outbox, Opts, []),
    Opts#{outbox => lists:usort([?EMBED_TAG | Tags])};
record_write_opts(_Db, Opts) ->
    Opts.

%% @private Decide how a record-mode write indexes. Precedence:
%% `vector' put option, then the document's `<<"_embedding">>' field,
%% then the policy (sync embeds inline, async defers to the indexer).
%% Client-supplied vectors are dimension-checked BEFORE the write; an
%% async-mode `_embedding' stays async (it persists in the body, so the
%% indexer picks it up without embedding).
-spec sync_action(db(), map() | binary(), map()) ->
    async | {sync, [number()] | undefined} | {error, term()}.
sync_action(#{embedding := Policy, dimensions := Dim}, Doc, Opts) ->
    Supplied = case maps:get(vector, Opts, undefined) of
        Vector when is_list(Vector) -> Vector;
        undefined -> doc_embedding(Doc)
    end,
    case Supplied of
        undefined ->
            case barrel_embedding_policy:mode(Policy) of
                sync -> {sync, undefined};
                async -> async
            end;
        Vector2 when length(Vector2) =/= Dim ->
            {error, {dimension_mismatch, Dim, length(Vector2)}};
        Vector2 ->
            case {maps:is_key(vector, Opts),
                  barrel_embedding_policy:mode(Policy)} of
                {false, async} -> async;  %% _embedding persists; indexer uses it
                _ -> {sync, Vector2}
            end
    end;
sync_action(_Db, _Doc, _Opts) ->
    async.

%% @private A client-supplied embedding carried in the document, in
%% either accepted shape (bare vector, or object with client source;
%% computed objects are round-tripped derived data, not an override).
-spec doc_embedding(map() | binary()) -> [number()] | undefined.
doc_embedding(Doc) when is_map(Doc) ->
    case maps:get(<<"_embedding">>, Doc, undefined) of
        Vector when is_list(Vector) ->
            Vector;
        #{<<"vector">> := Vector} = Obj when is_list(Vector) ->
            case maps:get(<<"source">>, Obj, <<"client">>) of
                <<"computed">> -> undefined;
                _ -> Vector
            end;
        _ ->
            undefined
    end;
doc_embedding(_Binary) ->
    undefined.

%% @private Synchronous single put: embed (or validate the explicit
%% vector) BEFORE writing, write with the embed tag, index, ack. A
%% failure after the commit leaves the entry pending and nudges the
%% indexer, which heals it.
put_doc_sync(#{docdb := DbBin, embedding := Policy, embed := Embed,
               dimensions := Dim} = Db, Doc, Opts, ExplicitVector) ->
    DocMap = barrel_doc:to_map(Doc),
    Action = case ExplicitVector of
        undefined ->
            case barrel_embedding_policy:matches(Policy, DocMap) of
                true ->
                    Text = barrel_embedding_policy:text(Policy, DocMap),
                    case embed_one(Text, Embed) of
                        {ok, Vector} -> {index, Text, Vector};
                        {error, Reason} -> {error, {embed_failed, Reason}}
                    end;
                false ->
                    deindex
            end;
        Vector when length(Vector) =:= Dim ->
            {index, barrel_embedding_policy:text(Policy, DocMap), Vector};
        Vector ->
            {error, {dimension_mismatch, Dim, length(Vector)}}
    end,
    case Action of
        {error, _} = Err ->
            Err;
        _ ->
            %% Inject the vector into the doc so the embedding column
            %% commits atomically with the write (_embedding always
            %% holds the document's vector, whoever computed it).
            Doc2 = inject_embedding(DocMap, Action, ExplicitVector),
            WriteOpts = record_write_opts(Db, Opts#{return_hlc => true}),
            case barrel_docdb:put_doc(DbBin, Doc2, WriteOpts) of
                {ok, Result} ->
                    ok = sync_index(Db, maps:get(<<"id">>, Result), Action,
                                    maps:get(hlc, Result)),
                    {ok, maps:remove(hlc, Result)};
                {error, _} = Err ->
                    Err
            end
    end.

%% @private Carry the resolved vector in the doc for atomic column
%% storage: client-supplied vectors (opt or already-carried field) keep
%% client provenance, policy-computed ones are marked computed inside
%% the _embedding object.
inject_embedding(DocMap, {index, _Text, Vector}, ExplicitVector) ->
    case ExplicitVector of
        undefined ->
            case doc_embedding(DocMap) of
                undefined ->
                    %% Policy-computed, including the case where the doc
                    %% round-trips a stale computed object: overwrite it
                    %% with the fresh vector.
                    DocMap#{<<"_embedding">> => #{<<"vector">> => Vector,
                                                  <<"source">> => <<"computed">>}};
                _ClientVector ->
                    DocMap %% client-carried, already in the doc
            end;
        _ ->
            %% explicit vector option: a client-supplied vector
            DocMap#{<<"_embedding">> => Vector}
    end;
inject_embedding(DocMap, deindex, _ExplicitVector) ->
    DocMap.

%% @private Synchronous batch put: embed all matching docs in one batch
%% before writing (any embed failure fails the whole batch, nothing
%% written), then index and ack the successful writes.
put_docs_sync(#{docdb := DbBin, embedding := Policy, embed := Embed} = Db,
              Docs, Opts) ->
    DocMaps = [barrel_doc:to_map(D) || D <- Docs],
    Plans = [plan_for(Policy, M) || M <- DocMaps],
    Texts = [Text || {embed, Text} <- Plans],
    case embed_many(Texts, Embed) of
        {ok, Vectors} ->
            {Docs2, Actions} = inject_batch(DocMaps, Plans, Vectors),
            WriteOpts = record_write_opts(Db, Opts#{return_hlc => true}),
            Results = barrel_docdb:put_docs(DbBin, Docs2, WriteOpts),
            ok = sync_index_batch(Db, lists:zip(Results, Actions)),
            [case R of
                 {ok, Result} -> {ok, maps:remove(hlc, Result)};
                 Other -> Other
             end || R <- Results];
        {error, Reason} ->
            {error, {embed_failed, Reason}}
    end.

%% @private Index plan for one document: a carried _embedding wins, then
%% the policy fields, else the vector is removed.
plan_for(Policy, DocMap) ->
    case doc_embedding(DocMap) of
        undefined ->
            case barrel_embedding_policy:matches(Policy, DocMap) of
                true -> {embed, barrel_embedding_policy:text(Policy, DocMap)};
                false -> deindex
            end;
        Vector ->
            %% dimension validated before the write (validate_batch_embeddings)
            {index, barrel_embedding_policy:text(Policy, DocMap), Vector}
    end.

%% @private Pair each embed plan with its vector and inject computed
%% vectors into their docs (atomic column storage); carried vectors are
%% already in the doc.
inject_batch(DocMaps, Plans, Vectors) ->
    inject_batch(DocMaps, Plans, Vectors, [], []).

inject_batch([], [], [], DocAcc, ActAcc) ->
    {lists:reverse(DocAcc), lists:reverse(ActAcc)};
inject_batch([DocMap | DocMaps], [{embed, Text} | Plans], [Vector | Vectors],
             DocAcc, ActAcc) ->
    Doc2 = DocMap#{<<"_embedding">> => #{<<"vector">> => Vector,
                                         <<"source">> => <<"computed">>}},
    inject_batch(DocMaps, Plans, Vectors,
                 [Doc2 | DocAcc], [{index, Text, Vector} | ActAcc]);
inject_batch([DocMap | DocMaps], [{index, _, _} = Given | Plans], Vectors,
             DocAcc, ActAcc) ->
    inject_batch(DocMaps, Plans, Vectors, [DocMap | DocAcc], [Given | ActAcc]);
inject_batch([DocMap | DocMaps], [deindex | Plans], Vectors, DocAcc, ActAcc) ->
    inject_batch(DocMaps, Plans, Vectors, [DocMap | DocAcc], [deindex | ActAcc]).

%% @private Which pipeline batch writes use on a record database.
batch_mode(#{embedding := Policy}) ->
    barrel_embedding_policy:mode(Policy);
batch_mode(_Db) ->
    async.

%% @private Fail a batch BEFORE writing when any document carries an
%% _embedding of the wrong dimension. Plain databases skip the scan.
-spec validate_batch_embeddings(db(), [map() | binary()]) -> ok | {error, term()}.
validate_batch_embeddings(#{embedding := _, dimensions := Dim}, Docs) ->
    Bad = lists:filtermap(
        fun(Doc) ->
            case doc_embedding(Doc) of
                undefined -> false;
                Vector when length(Vector) =:= Dim -> false;
                Vector -> {true, length(Vector)}
            end
        end,
        Docs),
    case Bad of
        [] -> ok;
        [Len | _] -> {error, {dimension_mismatch, Dim, Len}}
    end;
validate_batch_embeddings(_Db, _Docs) ->
    ok.

%% @private Apply one index action and ack its outbox entry. On a
%% vectordb failure the entry stays pending; the nudged indexer heals.
-spec sync_index(db(), binary(), {index, binary(), [float()]} | deindex,
                 term()) -> ok.
sync_index(#{docdb := DbBin, vstore := VStore} = Db, Id, Action, Hlc) ->
    Applied = case Action of
        {index, Text, Vector} ->
            try barrel_vectordb:add_index_only(VStore, Id, Text, Vector)
            catch Class:Reason -> {error, {Class, Reason}}
            end;
        deindex ->
            try barrel_vectordb:delete(VStore, Id)
            catch Class:Reason -> {error, {Class, Reason}}
            end
    end,
    case Applied of
        ok ->
            _ = barrel_docdb:outbox_ack(DbBin, ?EMBED_TAG, [Hlc]),
            ok;
        {error, not_found} ->
            _ = barrel_docdb:outbox_ack(DbBin, ?EMBED_TAG, [Hlc]),
            ok;
        {error, _} ->
            nudge_indexer(Db, ok)
    end.

%% @private Batch variant: index the successful writes, ack them; failed
%% writes (conflicts) have no outbox entry to handle.
sync_index_batch(_Db, []) ->
    ok;
sync_index_batch(Db, [{Result, Action} | Rest]) ->
    _ = case Result of
        {ok, Ok} ->
            sync_index(Db, maps:get(<<"id">>, Ok), Action, maps:get(hlc, Ok));
        {error, _} ->
            ok
    end,
    sync_index_batch(Db, Rest).

%% @private Embed helpers with crash isolation.
embed_one(Text, Embed) ->
    try barrel_embed:embed(Text, Embed)
    catch Class:Reason -> {error, {Class, Reason}}
    end.

embed_many([], _Embed) ->
    {ok, []};
embed_many(Texts, Embed) ->
    try barrel_embed:embed_batch(Texts, Embed)
    catch Class:Reason -> {error, {Class, Reason}}
    end.

%% @private Wake the record indexer after a successful record-mode
%% write. The indexer may be restarting; that is fine, it polls as a
%% fallback.
-spec nudge_indexer(db(), term()) -> ok.
nudge_indexer(#{name := Name, embedding := _}, Result) ->
    case Result of
        {error, _} -> ok;
        _ -> barrel_record_indexer:nudge(Name)
    end;
nudge_indexer(_Db, _Result) ->
    ok.

%% @private Start the per-database indexer under barrel_record_sup.
%% Record mode needs the barrel application running (the supervisor).
-spec start_indexer(atom(), binary(), barrel_embedding_policy:policy(), term(),
                    pos_integer()) -> ok | {error, term()}.
start_indexer(Name, DbBin, Policy, EmbedState, Dim) ->
    case erlang:whereis(barrel_record_sup) of
        undefined ->
            {error, barrel_app_not_started};
        _Pid ->
            case barrel_record_sup:start_indexer(#{
                     name => Name, db => DbBin, vstore => Name,
                     policy => Policy, embed => EmbedState,
                     dimensions => Dim}) of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok;
                {error, _} = Err -> Err
            end
    end.

%% @private Resolve the vector dimension from policy and vectordb config
%% (they must agree when both are set).
-spec resolve_dimension(barrel_embedding_policy:policy(), map()) ->
    {ok, pos_integer()} | {error, term()}.
resolve_dimension(Policy, VecConfig) ->
    PDim = maps:get(dimensions, Policy, undefined),
    VDim = case maps:get(dimension, VecConfig, undefined) of
        undefined -> maps:get(dimensions, VecConfig, undefined);
        D -> D
    end,
    case {PDim, VDim} of
        {undefined, undefined} -> {ok, 768};
        {undefined, D2} -> {ok, D2};
        {D2, undefined} -> {ok, D2};
        {Same, Same} -> {ok, Same};
        {P, V} -> {error, {dimension_mismatch, P, V}}
    end.

%% @private Initialise the facade's embedder from the policy. A policy
%% without an embedder still yields a state; embedding then fails at
%% write time and is handled by the indexer (or an explicit vector).
-spec init_embed(barrel_embedding_policy:policy(), pos_integer()) ->
    {ok, term()} | {error, term()}.
init_embed(Policy, Dim) ->
    Cfg0 = case maps:get(embedder, Policy, undefined) of
        undefined -> #{};
        Embedder -> #{embedder => Embedder}
    end,
    barrel_embed:init(Cfg0#{dimensions => Dim}).

%% @private Persist the policy as a local doc. On reopen with a changed
%% policy, warn and overwrite: existing documents are NOT reindexed.
-spec persist_policy(binary(), barrel_embedding_policy:policy()) -> ok.
persist_policy(DbBin, Policy) ->
    New = term_to_binary(Policy),
    case barrel_docdb:get_local_doc(DbBin, ?POLICY_DOC) of
        {ok, #{<<"policy">> := Old}} when Old =/= New ->
            logger:warning(
                "barrel: embedding policy changed for ~s; existing documents "
                "are not reindexed", [DbBin]),
            ok = barrel_docdb:put_local_doc(
                DbBin, ?POLICY_DOC, #{<<"policy">> => New});
        {ok, _Unchanged} ->
            ok;
        {error, not_found} ->
            ok = barrel_docdb:put_local_doc(
                DbBin, ?POLICY_DOC, #{<<"policy">> => New})
    end.
