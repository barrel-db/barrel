%%%-------------------------------------------------------------------
%%% @doc barrel_rep_transport - Transport behaviour for replication
%%%
%%% This behaviour defines the interface for the version-vector
%%% replication protocol. Transports abstract the communication with
%%% databases, allowing replication to work between local databases or
%%% remote ones.
%%%
%%% The protocol per batch of changes:
%%% <ol>
%%% <li>`get_changes/3' on the source (each change carries the doc's
%%%     current version token as `rev').</li>
%%% <li>`diff_versions/2' on the target with `#{DocId => Token}': the
%%%     target answers `have' when its version vector already covers the
%%%     offered version, `missing' otherwise.</li>
%%% <li>For each missing doc: `get_doc/3' on the source returns the
%%%     current body plus `#{version, vv, deleted}', then
%%%     `put_version/5' applies it on the target, which fast-forwards,
%%%     ignores (already covered), or records a conflict sibling with a
%%%     deterministic last-write-wins winner.</li>
%%% </ol>
%%%
%%% Local documents carry replication checkpoints and are not
%%% replicated themselves.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_transport).

-include("barrel_docdb.hrl").

%%====================================================================
%% Behaviour callbacks
%%====================================================================

%% Get a document for replication. Meta carries the version protocol
%% fields: #{version := Token, vv := EncodedVV, deleted := boolean()}.
-callback get_doc(Endpoint :: term(), DocId :: docid(), Opts :: map()) ->
    {ok, Doc :: map(), Meta :: map()} | {error, not_found} | {error, term()}.

%% Apply a replicated version (token + encoded VV preserved from source)
-callback put_version(Endpoint :: term(), Doc :: map(), VersionToken :: binary(),
                      VVBin :: binary(), Deleted :: boolean()) ->
    {ok, DocId :: docid(), WinnerToken :: binary()} | {error, term()}.

%% Which offered versions is the endpoint missing?
-callback diff_versions(Endpoint :: term(), TokenMap :: #{docid() => binary()}) ->
    {ok, #{docid() => missing | have}} | {error, term()}.

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
