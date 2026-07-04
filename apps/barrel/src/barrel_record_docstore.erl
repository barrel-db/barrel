%%%-------------------------------------------------------------------
%%% @doc Read-through docstore adapter for record-mode databases.
%%%
%%% Implements `barrel_vectordb_docstore' over the record's document
%%% database: search-result hydration reads text and metadata from the
%%% CURRENT document, rendered through the database's embedding policy.
%%% The document is primary; this adapter never writes or deletes it:
%%%
%%% <ul>
%%% <li>`get'/`multi_get': caller-side document reads (they run inside
%%%     the vectordb server during search fetch, so they must not
%%%     round-trip the docdb writer).</li>
%%% <li>`put'/`multi_put': `{error, read_only}'. The record write path
%%%     uses index-only vector writes and never produces doc pairs, so
%%%     reaching this is a misuse of `add'/`add_vector' on a record
%%%     store, and it fails loudly.</li>
%%% <li>`delete': a no-op. Deleting a vector must never delete the
%%%     document.</li>
%%% <li>`terminate': a no-op. The facade owns the database lifecycle.</li>
%%% </ul>
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_record_docstore).
-behaviour(barrel_vectordb_docstore).

-export([init/2, put/4, multi_put/2, get/2, multi_get/2, delete/2, terminate/1]).

%% @private Config: #{db := binary(), policy := barrel_embedding_policy:policy()}.
%% Does not open or own the database; the facade opened it already.
init(_Name, #{db := DbBin, policy := Policy}) when is_binary(DbBin) ->
    {ok, #{db => DbBin, policy => Policy}}.

%% @private The record write path never stores doc data through the seam.
put(_Ctx, _Id, _Text, _Metadata) ->
    {error, read_only}.

%% @private Same as put/4.
multi_put(_Ctx, _Pairs) ->
    {error, read_only}.

%% @private Read the current document; render text and metadata through
%% the policy. Deleted or missing documents are `not_found' (their hits
%% hydrate as empty metadata upstream).
get(#{db := Db, policy := Policy}, Id) ->
    case barrel_docdb:get_doc(Db, Id) of
        {ok, Doc} ->
            {ok, barrel_embedding_policy:text(Policy, Doc),
             barrel_embedding_policy:metadata(Policy, Doc)};
        {error, not_found} ->
            not_found;
        {error, _} = Err ->
            Err
    end.

%% @private Batch read via the caller-side docdb batch path.
multi_get(#{db := Db, policy := Policy}, Ids) ->
    case barrel_docdb:get_docs(Db, Ids) of
        Results when is_list(Results) ->
            [case R of
                 {ok, Doc} ->
                     {ok, barrel_embedding_policy:text(Policy, Doc),
                      barrel_embedding_policy:metadata(Policy, Doc)};
                 {error, not_found} ->
                     not_found;
                 {error, _} = Err ->
                     Err
             end || R <- Results];
        {error, _} = Err ->
            %% Database missing/closed: same error for every id
            [Err || _ <- Ids]
    end.

%% @private The document is primary; vector deletion never touches it.
delete(_Ctx, _Id) ->
    ok.

%% @private The facade owns the database lifecycle.
terminate(_Ctx) ->
    ok.
