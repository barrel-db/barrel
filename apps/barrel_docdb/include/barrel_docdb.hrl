%%%-------------------------------------------------------------------
%%% @doc barrel_docdb header file
%%%
%%% Contains type definitions and common macros for barrel_docdb.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(BARREL_DOCDB_HRL).
-define(BARREL_DOCDB_HRL, true).

%%====================================================================
%% Type Definitions
%%====================================================================

%% Database types
-type db_name() :: binary().
%% Database name (unique identifier).

-type db_ref() :: pid() | atom().
%% Reference to a database (pid or registered name).

-type db_config() :: #{
    path => string(),
    store => module(),
    atom() => term()
}.
%% Database configuration options.

%% Document types
-type docid() :: binary().
%% Unique document identifier.

-type revid() :: binary().
%% Revision identifier in format "N-HASH" where N is generation.

-type doc() :: #{
    binary() => term()
}.
%% JSON document as an Erlang map with binary keys (<<"id">>, <<"_rev">>, etc.).

-type doc_info() :: #{
    id := docid(),
    rev := revid(),
    deleted := boolean(),
    revtree := revtree()
}.
%% Internal document metadata.

%% Revision tree types
-type revtree() :: #{revid() => rev_info()}.
%% Revision tree mapping revisions to their info.

-type rev_info() :: #{
    id := revid(),
    parent := revid() | undefined,
    deleted := boolean(),
    attachments => #{binary() => att_info()}
}.
%% Information about a single revision. parent is undefined for root revisions.

%% Attachment types
-type att_info() :: #{
    name := binary(),
    content_type := binary(),
    length := non_neg_integer(),
    digest := binary(),
    chunked => boolean(),
    chunk_size => pos_integer(),
    chunk_count => pos_integer()
}.
%% Attachment metadata. Chunked attachments include optional chunked/chunk_size/chunk_count fields.

%% Sequence types
-type seq() :: barrel_hlc:timestamp().
%% Sequence number as HLC timestamp.

-type seq_string() :: binary().
%% Sequence number as a string for external use.

%% Changes types
-type change() :: map().
%% A single change entry.

%% View types
-type view_name() :: binary().
%% View name.

-type view_result() :: #{
    key := term(),
    value := term(),
    id := docid()
}.
%% A single view result row.

%% Replication types
-type endpoint() :: db_name() | {node(), db_name()} | {module(), term()}.
%% Replication endpoint - local db, remote Erlang node, or custom transport.

-type rep_options() :: #{
    continuous => boolean(),
    since => seq_string(),
    filter => fun((doc()) -> boolean()),
    atom() => term()
}.
%% Replication options.

%%====================================================================
%% Export type definitions
%%====================================================================

-export_type([
    db_name/0, db_ref/0, db_config/0,
    docid/0, revid/0, doc/0, doc_info/0,
    revtree/0, rev_info/0,
    att_info/0,
    seq/0, seq_string/0,
    change/0,
    view_name/0, view_result/0,
    endpoint/0, rep_options/0
]).

%%====================================================================
%% Macros
%%====================================================================

%% Default configuration values
-define(DEFAULT_DATA_DIR, "data/barrel_docdb").
-define(DEFAULT_STORE_MODULE, barrel_store_rocksdb).

-endif. %% BARREL_DOCDB_HRL
