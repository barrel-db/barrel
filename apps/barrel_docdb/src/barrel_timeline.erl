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
            _At -> throw({error, pitr_not_implemented})
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
