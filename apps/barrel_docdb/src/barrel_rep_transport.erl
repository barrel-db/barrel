%%%-------------------------------------------------------------------
%%% @author Benoit Chesneau
%%% @copyright (C) 2024, Benoit Chesneau
%%% @doc barrel_rep_transport - Transport behaviour for replication
%%%
%%% This behaviour defines the interface for replication transports.
%%% Transports abstract the communication with databases, allowing
%%% replication to work between local databases or remote ones.
%%%
%%% == Implementing a Transport ==
%%%
%%% To implement a custom transport, create a module that exports all
%%% the callback functions defined in this behaviour:
%%%
%%% ```
%%% -module(my_transport).
%%% -behaviour(barrel_rep_transport).
%%% -export([get_doc/3, put_rev/4, revsdiff/3, get_changes/3,
%%%          get_local_doc/2, put_local_doc/3, delete_local_doc/2,
%%%          db_info/1]).
%%%
%%% get_doc(Endpoint, DocId, Opts) ->
%%%     %% Fetch document from remote
%%%     ...
%%% '''
%%%
%%% == Callback Functions ==
%%%
%%% <ul>
%%%   <li>`get_doc/3' - Retrieve a document with options</li>
%%%   <li>`put_rev/4' - Store a document with explicit revision history</li>
%%%   <li>`revsdiff/3' - Find missing revisions</li>
%%%   <li>`get_changes/3' - Get changes since a sequence</li>
%%%   <li>`get_local_doc/2' - Get a local (non-replicated) document</li>
%%%   <li>`put_local_doc/3' - Store a local document</li>
%%%   <li>`delete_local_doc/2' - Delete a local document</li>
%%%   <li>`db_info/1' - Get database information</li>
%%% </ul>
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_transport).

-include("barrel_docdb.hrl").

%%====================================================================
%% Behaviour callbacks
%%====================================================================

%% Get a document with options
%% Options may include: rev, history
-callback get_doc(Endpoint :: term(), DocId :: docid(), Opts :: map()) ->
    {ok, Doc :: map(), Meta :: map()} | {error, not_found} | {error, term()}.

%% Put a document with explicit revision history (replication)
-callback put_rev(Endpoint :: term(), Doc :: map(), History :: [revid()], Deleted :: boolean()) ->
    {ok, DocId :: docid(), RevId :: revid()} | {error, term()}.

%% Get revision differences
%% Returns {ok, MissingRevs, PossibleAncestors}
-callback revsdiff(Endpoint :: term(), DocId :: docid(), RevIds :: [revid()]) ->
    {ok, Missing :: [revid()], Ancestors :: [revid()]} | {error, term()}.

%% Get changes since a sequence
-callback get_changes(Endpoint :: term(), Since :: seq() | first, Opts :: map()) ->
    {ok, Changes :: [map()], LastSeq :: seq()} | {error, term()}.

%% Get a local document (for checkpoints)
-callback get_local_doc(Endpoint :: term(), DocId :: docid()) ->
    {ok, Doc :: map()} | {error, not_found} | {error, term()}.

%% Put a local document (for checkpoints)
-callback put_local_doc(Endpoint :: term(), DocId :: docid(), Doc :: map()) ->
    ok | {error, term()}.

%% Delete a local document
-callback delete_local_doc(Endpoint :: term(), DocId :: docid()) ->
    ok | {error, not_found} | {error, term()}.

%% Get database info
-callback db_info(Endpoint :: term()) ->
    {ok, Info :: map()} | {error, term()}.

%% Synchronize HLC with remote timestamp (optional - for distributed ordering)
-callback sync_hlc(Endpoint :: term(), Hlc :: barrel_hlc:timestamp()) ->
    {ok, barrel_hlc:timestamp()} | {error, term()}.
