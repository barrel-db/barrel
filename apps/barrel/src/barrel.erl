%%%-------------------------------------------------------------------
%%% @doc Barrel facade: the embeddable edge database.
%%%
%%% Composes the document layer (`barrel_docdb') and the vector layer
%%% (`barrel_vectordb') behind one API. A barrel database is a docdb
%%% database plus a vectordb store that share a name; {@link open/2}
%%% returns a handle used by the rest of this module.
%%%
%%% This is the Phase 0 facade: it delegates to the two underlying apps.
%%% Later phases add a single document/vector id space, attachments backed
%%% by `barrel_objectdb', changes/replication, and optional encryption.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel).

-export([
    open/1,
    open/2,
    close/1
]).

%% Document operations (delegated to barrel_docdb).
-export([
    put_doc/2,
    get_doc/2,
    delete_doc/2,
    find/2
]).

%% Vector operations (delegated to barrel_vectordb).
-export([
    vector_add/4,
    vector_add/5,
    search/3,
    search_vector/3,
    search_bm25/3,
    search_hybrid/3
]).

-export_type([db/0]).

-type db() :: #{
    name := atom(),
    docdb := binary(),
    vstore := atom()
}.
%% Handle for a composed barrel database.

%%====================================================================
%% Lifecycle
%%====================================================================

%% @doc Open a composed barrel database with default options.
-spec open(atom()) -> {ok, db()} | {error, term()}.
open(Name) ->
    open(Name, #{}).

%% @doc Open a composed barrel database.
%%
%% `Opts' may carry `docdb => map()' (passed to {@link barrel_docdb:create_db/2})
%% and `vectordb => map()' (a {@link barrel_vectordb} store config; the `name'
%% key is set from `Name'). Opens the docdb database if it exists, otherwise
%% creates it, then starts the vectordb store.
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

%%====================================================================
%% Documents (barrel_docdb)
%%====================================================================

%% @doc Create or update a document.
-spec put_doc(db(), map()) -> {ok, map()} | {error, term()}.
put_doc(#{docdb := DbBin}, Doc) ->
    barrel_docdb:put_doc(DbBin, Doc).

%% @doc Get a document by id.
-spec get_doc(db(), binary()) -> {ok, map()} | {error, term()}.
get_doc(#{docdb := DbBin}, DocId) ->
    barrel_docdb:get_doc(DbBin, DocId).

%% @doc Delete a document by id.
-spec delete_doc(db(), binary()) -> {ok, map()} | {error, term()}.
delete_doc(#{docdb := DbBin}, DocId) ->
    barrel_docdb:delete_doc(DbBin, DocId).

%% @doc Run a declarative query.
-spec find(db(), map()) -> {ok, [map()], map()} | {error, term()}.
find(#{docdb := DbBin}, Query) ->
    barrel_docdb:find(DbBin, Query).

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
