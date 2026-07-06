%%%-------------------------------------------------------------------
%%% @doc Timeline: branch a database, work on the branch, merge back.
%%%
%%% A branch is an instant fork: both RocksDB stores (docs and
%%% attachments) are checkpointed into the branch's directory (hard
%%% links, O(1) in data size, copy-on-write at the file level), and
%%% the branch opens as a normal database whose storage keys keep the
%%% parent's name (keyspace indirection, see barrel_keyspace). The
%%% fork instant is minted inside the parent's writer, so the
%%% checkpoint is exactly "everything up to fork_hlc".
%%%
%%% The two stores are checkpointed back to back, not atomically:
%%% attachment writes do not serialize through the writer, so an
%%% in-flight attachment may straddle the fork. Attachments are LWW
%%% state with origin guards and sync out of band, the same tolerance
%%% replication has.
%%%
%%% v1 lineage is linear: branching a branch is rejected, which keeps
%%% barrel_keyspace:resolve/1 single-level.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_timeline).

-include("barrel_docdb.hrl").

-export([branch_db/3, list_branches/1]).

%%====================================================================
%% Branch
%%====================================================================

%% @doc The OPEN branches of a database, from the registry (a parent
%% tracks nothing; on-disk-but-closed branches are not listed).
-spec list_branches(binary()) -> [binary()].
list_branches(Parent) ->
    lists:filter(
        fun(Name) ->
            case barrel_docdb:db_info(Name) of
                {ok, #{parent := Parent}} -> true;
                _ -> false
            end
        end,
        barrel_docdb:list_dbs()).

%% @doc Fork a database. `Opts':
%% - `at => now' (default): the branch starts at the fork instant.
%% - `data_dir => Dir': base directory for the branch (defaults to the
%%   parent's base, i.e. the branch lives next to its parent).
%% Every other option is merged over the inherited parent config for
%% the branch's FIRST open (channels are always inherited: the copied
%% feed rows are keyspace-scoped and a mismatched config would orphan
%% them). Reopening the branch later follows the normal create_db
%% contract: config is runtime-only and must be supplied again.
-spec branch_db(binary(), binary(), map()) ->
    {ok, pid()} | {error, term()}.
branch_db(Parent, BranchName, Opts) when is_binary(Parent),
                                         is_binary(BranchName),
                                         is_map(Opts) ->
    case barrel_docdb:validate_db_name(BranchName) of
        ok ->
            case barrel_docdb:open_db(BranchName) of
                {ok, _} ->
                    {error, already_exists};
                {error, not_found} ->
                    case barrel_docdb:open_db(Parent) of
                        {ok, ParentPid} ->
                            do_branch(Parent, ParentPid, BranchName, Opts);
                        {error, not_found} ->
                            {error, parent_not_found}
                    end
            end;
        {error, _} = Err ->
            Err
    end.

do_branch(Parent, ParentPid, BranchName, Opts) ->
    {ok, PInfo} = barrel_db_server:info(ParentPid),
    case maps:get(keyspace, PInfo) of
        Parent ->
            BaseDir = case maps:get(data_dir, Opts, undefined) of
                undefined ->
                    %% next to the parent: db_path is data_dir/Parent
                    filename:dirname(maps:get(db_path, PInfo));
                Dir ->
                    Dir
            end,
            BranchPath = filename:join(BaseDir,
                                       binary_to_list(BranchName)),
            case filelib:is_file(BranchPath) of
                true ->
                    {error, branch_dir_exists};
                false ->
                    fork(Parent, ParentPid, PInfo, BranchName,
                         BranchPath, Opts)
            end;
        _OtherKeyspace ->
            %% v1 invariant: linear lineage only
            {error, cannot_branch_a_branch}
    end.

fork(Parent, ParentPid, PInfo, BranchName, BranchPath, Opts) ->
    ok = filelib:ensure_path(BranchPath),
    DocsPath = filename:join(BranchPath, "docs"),
    AttPath = filename:join(BranchPath, "attachments"),
    try
        {ok, ForkHlc} =
            barrel_db_server:checkpoint_to(ParentPid, DocsPath, AttPath),
        ok = barrel_keyspace:write_meta(BranchPath, #{
            keyspace => Parent,
            parent => Parent,
            fork_hlc => barrel_hlc:encode(ForkHlc)
        }),
        case maps:get(at, Opts, now) of
            now -> ok;
            T when is_tuple(T) ->
                ok = rewind_branch(DocsPath, PInfo, ForkHlc, T);
            BadAt ->
                throw({error, {bad_at, BadAt}})
        end,
        BranchConfig = branch_config(PInfo, BranchPath, Opts),
        case barrel_docdb:create_db(BranchName, BranchConfig) of
            {ok, Pid} -> {ok, Pid};
            {error, _} = StartErr -> throw(StartErr)
        end
    catch
        throw:{error, _} = Thrown ->
            _ = file:del_dir_r(BranchPath),
            Thrown;
        Class:Reason:Stack ->
            _ = file:del_dir_r(BranchPath),
            erlang:raise(Class, Reason, Stack)
    end.

%% The branch's first open inherits the parent's config; channels are
%% forced from the parent, the data_dir points at the branch's base,
%% and caller options are merged on top of the rest.
branch_config(PInfo, BranchPath, Opts) ->
    ParentConfig = maps:get(config, PInfo, #{}),
    CallerOpts = maps:without([at, data_dir], Opts),
    Merged = maps:merge(ParentConfig, CallerOpts),
    WithChannels = case maps:find(channels, ParentConfig) of
        {ok, Channels} -> Merged#{channels => Channels};
        error -> maps:remove(channels, Merged)
    end,
    WithChannels#{data_dir => filename:dirname(BranchPath)}.

%%====================================================================
%% PITR rewind (at => HlcT)
%%====================================================================
%%
%% Runs on the branch's directly-opened store BEFORE the branch is
%% registered: no persistent_term context exists, so everything takes
%% explicit arguments (keyspace, compiled channels) and the store is
%% opened without the compaction filter (its absence drops nothing).
%% Any error aborts the fork, which deletes the branch directory.
%%
%% Semantics: every doc changed after T is restored to its latest
%% retained state at or before T (linear history; concurrent windows
%% are refined by the conflict-window pass), and docs created after T
%% are forgotten entirely. Restored docs keep their ORIGINAL change
%% HLCs, so a later merge ships only branch edits. Embedding columns
%% are dropped (derived data; archived bodies do not carry them).
%% Attachments keep fork-time state (LWW with a feed, no history).

-define(REWIND_BATCH, 200).

rewind_branch(DocsPath, PInfo, ForkHlc, T) ->
    case barrel_hlc:less(ForkHlc, T) of
        true ->
            throw({error, {bad_at, future}});
        false ->
            ok
    end,
    case maps:get(history_floor, PInfo, undefined) of
        undefined -> ok;
        Floor ->
            case barrel_hlc:less(T, Floor) of
                true -> throw({error, pitr_window_exceeded});
                false -> ok
            end
    end,
    Ks = maps:get(keyspace, PInfo),
    Config = maps:get(config, PInfo, #{}),
    {ok, Compiled} = barrel_channel:compile(
        maps:get(channels, Config, #{})),
    StoreOpts = maps:get(store_opts, Config, #{}),
    {ok, StoreRef} = barrel_store_rocksdb:open(DocsPath, StoreOpts),
    try
        rewind(StoreRef, Ks, Compiled, T)
    after
        barrel_store_rocksdb:close(StoreRef)
    end.

rewind(StoreRef, Ks, Compiled, T) ->
    {PostT, PostTHistDels} = collect_post_t(StoreRef, Ks, T),
    case maps:size(PostT) of
        0 ->
            ok;
        _ ->
            Targets = resolve_targets(StoreRef, Ks, T, PostT),
            rewind_docs(StoreRef, Ks, Compiled, maps:to_list(Targets)),
            GlobalOps = PostTHistDels
                ++ outbox_rewind_ops(StoreRef, Ks, T)
                ++ bucket_drop_ops(StoreRef, Ks),
            ok = barrel_store_rocksdb:write_batch(StoreRef, GlobalOps,
                                                  #{}),
            ok = rewrite_last_hlc(StoreRef, Ks)
    end.

%% Pass A: post-T history entries per doc (ascending fold; `from' is
%% inclusive, so entries AT T are skipped: they belong to the kept
%% state), plus the delete ops for the post-T history rows.
collect_post_t(StoreRef, Ks, T) ->
    barrel_history:fold(
        StoreRef, Ks,
        fun(#{hlc := Hlc} = Entry, {Map, Dels} = Acc) ->
            case barrel_hlc:equal(Hlc, T) of
                true ->
                    {ok, Acc};
                false ->
                    Id = maps:get(id, Entry),
                    Map2 = maps:update_with(
                        Id, fun(Es) -> [Entry | Es] end, [Entry], Map),
                    Del = {delete,
                           barrel_store_keys:history_key(Ks, Hlc)},
                    {ok, {Map2, [Del | Dels]}}
            end
        end,
        {#{}, []},
        #{from => T}).

%% Pass B: one reverse scan of history at or before T collecting
%% every retained entry of each affected doc, then per doc the
%% CONTAINMENT-MAXIMAL entries (no other entry's vector contains
%% their version) reconstruct the state at T: the deterministic LWW
%% winner among the maximals is restored as current and the other
%% maximals as live conflict siblings, exactly the shape put_version
%% would have left. Docs with no retained entry at or before T either
%% never existed at T (their created_at is after T) and are
%% forgotten, or their T-state predates retention and the fork
%% aborts.
resolve_targets(StoreRef, Ks, T, PostT) ->
    Start = barrel_store_keys:history_prefix(Ks),
    End = <<(barrel_store_keys:history_key(Ks, T))/binary, 0>>,
    Collected = barrel_store_rocksdb:fold_range_reverse(
        StoreRef, Start, End,
        fun(Key, Value, Acc) ->
            Hlc = barrel_store_keys:decode_history_key(Ks, Key),
            Entry = (barrel_history:decode_entry(Value))#{hlc => Hlc},
            Id = maps:get(id, Entry),
            case maps:is_key(Id, PostT) of
                true ->
                    {ok, maps:update_with(
                        Id, fun(Es) -> [Entry | Es] end, [Entry], Acc)};
                false ->
                    {ok, Acc}
            end
        end,
        #{}),
    maps:map(
        fun(Id, _) ->
            case maps:get(Id, Collected, []) of
                [] ->
                    %% No retained entry at or before T: either the
                    %% doc was created after T (forget) or its pre-T
                    %% history was swept (abort). The write-invariant
                    %% created_at column disambiguates; a missing one
                    %% aborts conservatively.
                    case entity_created_at(StoreRef, Ks, Id) of
                        undefined ->
                            throw({error, {pitr_window_exceeded, Id}});
                        CreatedAt ->
                            case barrel_hlc:less(T, CreatedAt) of
                                true ->
                                    forget;
                                false ->
                                    throw({error,
                                           {pitr_window_exceeded, Id}})
                            end
                    end;
                Entries ->
                    {Winner, Siblings, Superseded} =
                        split_window(Entries),
                    {restore, Winner, Siblings, Superseded}
            end
        end,
        PostT).

entity_created_at(StoreRef, Ks, Id) ->
    case barrel_store_rocksdb:get_entity(
             StoreRef, barrel_store_keys:doc_entity(Ks, Id)) of
        {ok, Columns} ->
            case proplists:get_value(?COL_CREATED_AT, Columns, <<>>) of
                <<>> -> undefined;
                Bin -> barrel_hlc:decode(Bin)
            end;
        not_found ->
            undefined
    end.

%% Containment-maximal split of a doc's retained entries at or before
%% T: maximals are the concurrent heads (nothing covers them), the
%% LWW-largest version among them is the winner, the rest are live
%% conflict siblings; everything else is superseded. Linear histories
%% degrade to a single maximal.
split_window(Entries) ->
    Maximal = [E || E <- Entries, is_maximal(E, Entries)],
    [Winner | Siblings] = lists:sort(
        fun(A, B) ->
            VA = barrel_version:from_token(maps:get(version, A)),
            VB = barrel_version:from_token(maps:get(version, B)),
            barrel_version:max(VA, VB) =:= VA
        end,
        Maximal),
    {Winner, Siblings, Entries -- Maximal}.

is_maximal(E, Entries) ->
    V = barrel_version:from_token(maps:get(version, E)),
    not lists:any(
        fun(O) ->
            O =/= E andalso barrel_vv:contains(maps:get(vv, O), V)
        end,
        Entries).

rewind_docs(_StoreRef, _Ks, _Compiled, []) ->
    ok;
rewind_docs(StoreRef, Ks, Compiled, Targets) ->
    {Chunk, Rest} = case length(Targets) > ?REWIND_BATCH of
        true -> lists:split(?REWIND_BATCH, Targets);
        false -> {Targets, []}
    end,
    Ops = lists:flatmap(
        fun({Id, Target}) ->
            doc_rewind_ops(StoreRef, Ks, Compiled, Id, Target)
        end,
        Chunk),
    ok = barrel_store_rocksdb:write_batch(StoreRef, Ops, #{}),
    rewind_docs(StoreRef, Ks, Compiled, Rest).

doc_rewind_ops(StoreRef, Ks, Compiled, Id, Target) ->
    Current = read_current_state(StoreRef, Ks, Id),
    CommonDels = current_row_dels(Ks, Compiled, Id, Current),
    case Target of
        forget ->
            CommonDels
            ++ chain_forget_ops(StoreRef, Ks, Id)
            ++ [{entity_delete, barrel_store_keys:doc_entity(Ks, Id)},
                {body_delete, barrel_store_keys:doc_body(Ks, Id)}];
        {restore, Winner, Siblings, Superseded} ->
            {SiblingOps, Kept} =
                sibling_ops(StoreRef, Ks, Id, Siblings),
            %% the entity's vector is the MERGE of the winner and its
            %% live siblings, exactly what put_version leaves behind
            %% (otherwise a redelivered sibling would double up)
            MergedVV = lists:foldl(
                fun(E, Acc) -> barrel_vv:merge(Acc, maps:get(vv, E)) end,
                maps:get(vv, Winner),
                Kept),
            CommonDels
            ++ post_t_version_dels(Ks, Id, Winner, Current)
            ++ SiblingOps
            ++ superseded_flip_ops(Ks, Id, Superseded)
            ++ restore_ops(StoreRef, Ks, Compiled, Id,
                           Winner#{vv := MergedVV}, length(Kept))
    end.

%% Live conflict siblings at T: their chain rows flip back to
%% conflict. A sibling whose archived body was dropped by the
%% conflict bound is dropped here too (the bound is deterministic
%% across replicas), chain row and all.
sibling_ops(StoreRef, Ks, Id, Siblings) ->
    lists:foldr(
        fun(#{version := Token, deleted := Del, vv := VV} = E,
            {Ops, Kept}) ->
            VEnc = barrel_version:encode(
                barrel_version:from_token(Token)),
            BodyKey = barrel_store_keys:doc_body_rev(Ks, Id, VEnc),
            HasBody = case Del of
                true -> true;
                false ->
                    barrel_store_rocksdb:body_get(StoreRef, BodyKey)
                        =/= not_found
            end,
            case HasBody of
                true ->
                    Row = <<1:8,
                            (case Del of true -> 1; false -> 0 end):8,
                            (barrel_vv:encode(VV))/binary>>,
                    Op = {put,
                          barrel_store_keys:doc_version(Ks, Id, VEnc),
                          Row},
                    {[Op | Ops], [E | Kept]};
                false ->
                    Drop = [{delete,
                             barrel_store_keys:doc_version(Ks, Id,
                                                           VEnc)}],
                    {Drop ++ Ops, Kept}
            end
        end,
        {[], []},
        Siblings).

%% Non-maximal entries at or before T are superseded at T; post-T
%% resolutions may have left their chain rows in another state, so
%% rewrite them (overwriting an identical row is a no-op).
superseded_flip_ops(Ks, Id, Superseded) ->
    [{put,
      barrel_store_keys:doc_version(
          Ks, Id,
          barrel_version:encode(
              barrel_version:from_token(maps:get(version, E)))),
      <<0:8,
        (case maps:get(deleted, E) of true -> 1; false -> 0 end):8,
        (barrel_vv:encode(maps:get(vv, E)))/binary>>}
     || E <- Superseded].

%% The doc's CURRENT footprint outside the entity/body: live feed row,
%% path-index rows, channel rows, and the current-body path index.
current_row_dels(Ks, Compiled, Id, Current) ->
    #{hlc := CurHlc, rev := CurRev, deleted := CurDeleted,
      body := CurBody} = Current,
    CurInfo0 = #{id => Id, rev => CurRev, deleted => CurDeleted},
    CurInfo = case CurDeleted of
        true -> CurInfo0;
        false -> CurInfo0#{doc => CurBody}
    end,
    ArsDels = case CurDeleted of
        true ->
            [];
        false ->
            barrel_ars_index:remove_doc_ops(Ks, Id,
                                            barrel_ars:analyze(CurBody))
    end,
    [{delete, barrel_store_keys:doc_hlc(Ks, CurHlc)}]
    ++ barrel_changes:remove_path_index_ops(Ks, CurHlc, CurInfo)
    ++ ArsDels
    ++ [{delete, barrel_store_keys:channel_key(Ks, C, CurHlc)}
        || C <- maps:keys(Compiled)].

%% Chain rows and archived bodies of versions that became current
%% after T (their history entries are post-T): the restored winner's
%% own archive also clears since it becomes current again.
post_t_version_dels(Ks, Id, #{version := RestToken}, _Current) ->
    RestVEnc = barrel_version:encode(
        barrel_version:from_token(RestToken)),
    [{delete, barrel_store_keys:doc_version(Ks, Id, RestVEnc)},
     {body_delete, barrel_store_keys:doc_body_rev(Ks, Id, RestVEnc)}].

chain_forget_ops(StoreRef, Ks, Id) ->
    Start = barrel_store_keys:doc_version_prefix(Ks, Id),
    End = barrel_store_keys:doc_version_end(Ks, Id),
    barrel_store_rocksdb:fold_range(
        StoreRef, Start, End,
        fun(Key, _Value, Acc) ->
            VEnc = barrel_store_keys:decode_doc_version_key(Ks, Id, Key),
            {ok, [{delete, Key},
                  {body_delete,
                   barrel_store_keys:doc_body_rev(Ks, Id, VEnc)}
                  | Acc]}
        end,
        []).

restore_ops(StoreRef, Ks, Compiled, Id, Entry, NConflicts) ->
    #{hlc := RestHlc, version := RestToken, deleted := RestDeleted,
      vv := RestVV} = Entry,
    Current = read_current_state(StoreRef, Ks, Id),
    RestVersion = barrel_version:from_token(RestToken),
    RestVEnc = barrel_version:encode(RestVersion),
    RestBody = restore_body(StoreRef, Ks, Id, RestVEnc, RestDeleted,
                            Current),
    %% Entity: identity columns from the restored entry; created_at /
    %% expires_at / tier carried from the current entity (created_at
    %% is write-invariant, the others best effort); embedding columns
    %% dropped (derived data).
    Cols = [
        {?COL_VERSION, RestVEnc},
        {?COL_DELETED, case RestDeleted of
                           true -> <<"true">>;
                           false -> <<"false">>
                       end},
        {?COL_HLC, barrel_hlc:encode(RestHlc)},
        {?COL_VV, barrel_vv:encode(RestVV)},
        {?COL_NCONFLICTS, NConflicts},
        {?COL_CREATED_AT, maps:get(created_at, Current)},
        {?COL_EXPIRES_AT, maps:get(expires_at, Current)},
        {?COL_TIER, maps:get(tier, Current)}
    ],
    RestInfo0 = #{id => Id, rev => RestToken, deleted => RestDeleted,
                  num_conflicts => NConflicts, hlc => RestHlc},
    RestInfo = case RestDeleted of
        true -> RestInfo0;
        false -> RestInfo0#{doc => RestBody}
    end,
    BodyOps = case RestDeleted of
        true ->
            [{body_delete, barrel_store_keys:doc_body(Ks, Id)}];
        false ->
            [{body_put, barrel_store_keys:doc_body(Ks, Id),
              barrel_docdb_codec_cbor:encode_cbor(RestBody)}]
    end,
    ArsOps = case RestDeleted of
        true ->
            [];
        false ->
            barrel_ars_index:index_doc_ops(Ks, Id, RestBody)
    end,
    ChannelOps = case RestDeleted of
        true ->
            %% restored tombstones get no channel rows (their at-T
            %% membership is not reconstructable); documented
            [];
        false ->
            barrel_channel:write_ops_compiled(
                Compiled, Ks, RestHlc, RestInfo,
                barrel_channel:topics(RestBody), undefined, [])
    end,
    [{entity_put, barrel_store_keys:doc_entity(Ks, Id), Cols},
     {put, barrel_store_keys:doc_hlc(Ks, RestHlc),
      barrel_changes:encode_change(RestInfo)}]
    ++ BodyOps
    ++ barrel_changes:write_path_index_ops(Ks, RestHlc, RestInfo)
    ++ ArsOps
    ++ ChannelOps.

%% The restored version's body: the current body when it is somehow
%% still current, else the version-keyed archive. A missing archive
%% within the window aborts the fork (conflict-bound drops can remove
%% sibling bodies early).
restore_body(_StoreRef, _Ks, _Id, _RestVEnc, true, _Current) ->
    undefined;
restore_body(StoreRef, Ks, Id, RestVEnc, false, Current) ->
    case maps:get(version_enc, Current) of
        RestVEnc ->
            maps:get(body, Current);
        _ ->
            Key = barrel_store_keys:doc_body_rev(Ks, Id, RestVEnc),
            case barrel_store_rocksdb:body_get(StoreRef, Key) of
                {ok, Cbor} ->
                    barrel_docdb_codec_cbor:decode_any(Cbor);
                not_found ->
                    throw({error, {pitr_body_missing, Id}})
            end
    end.

read_current_state(StoreRef, Ks, Id) ->
    EntityKey = barrel_store_keys:doc_entity(Ks, Id),
    {ok, Columns} = barrel_store_rocksdb:get_entity(StoreRef, EntityKey),
    VersionEnc = proplists:get_value(?COL_VERSION, Columns),
    Deleted = proplists:get_value(?COL_DELETED, Columns, <<"false">>)
                  =:= <<"true">>,
    Body = case Deleted of
        true ->
            undefined;
        false ->
            case barrel_store_rocksdb:body_get(
                     StoreRef, barrel_store_keys:doc_body(Ks, Id)) of
                {ok, Cbor} -> barrel_docdb_codec_cbor:decode_any(Cbor);
                not_found -> #{}
            end
    end,
    #{version_enc => VersionEnc,
      rev => barrel_version:to_token(barrel_version:decode(VersionEnc)),
      hlc => barrel_hlc:decode(proplists:get_value(?COL_HLC, Columns)),
      deleted => Deleted,
      created_at => proplists:get_value(?COL_CREATED_AT, Columns, <<>>),
      expires_at => proplists:get_value(?COL_EXPIRES_AT, Columns, 0),
      tier => proplists:get_value(?COL_TIER, Columns, 0),
      body => Body}.

%% Pending outbox entries minted after T are work for post-T state:
%% drop them (the tags themselves are config, not data).
outbox_rewind_ops(StoreRef, Ks, T) ->
    barrel_store_rocksdb:fold_range(
        StoreRef,
        barrel_store_keys:outbox_db_prefix(Ks),
        barrel_store_keys:outbox_db_end(Ks),
        fun(Key, _Value, Acc) ->
            {_Tag, Hlc} = barrel_store_keys:decode_outbox_key(Ks, Key),
            case barrel_hlc:less(T, Hlc) of
                true -> {ok, [{delete, Key} | Acc]};
                false -> {ok, Acc}
            end
        end,
        []).

%% Change buckets are idle-poll hints keyed by wall-clock minute; the
%% rewind invalidates their max bounds, so drop them all (absent
%% buckets just mean the slow path answers).
bucket_drop_ops(StoreRef, Ks) ->
    barrel_store_rocksdb:fold_range(
        StoreRef,
        barrel_store_keys:change_bucket_prefix(Ks),
        barrel_store_keys:change_bucket_end(Ks),
        fun(Key, _Value, Acc) -> {ok, [{delete, Key} | Acc]} end,
        []).

%% db_last_hlc must equal the newest remaining feed row.
rewrite_last_hlc(StoreRef, Ks) ->
    Key = barrel_store_keys:db_last_hlc(Ks),
    Last = barrel_store_rocksdb:fold_range_reverse(
        StoreRef,
        barrel_store_keys:doc_hlc_prefix(Ks),
        barrel_store_keys:doc_hlc_end(Ks),
        fun(K, _V, _Acc) ->
            {stop, barrel_store_keys:decode_hlc_key(Ks, K)}
        end,
        none),
    case Last of
        none ->
            barrel_store_rocksdb:delete(StoreRef, Key);
        Hlc ->
            barrel_store_rocksdb:put(StoreRef, Key,
                                     barrel_hlc:encode(Hlc))
    end.
