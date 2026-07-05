%%%-------------------------------------------------------------------
%%% @doc Keyspace indirection for timeline branches.
%%%
%%% Every storage key embeds a database name. A branch created by
%%% checkpointing a parent database keeps the PARENT's name inside the
%%% copied keys, so the branch carries a `keyspace' (the name used for
%%% key building) distinct from its logical (registered) name. Normal
%%% databases have keyspace =:= name and no entry is installed.
%%%
%%% `resolve/1' is idempotent under the v1 invariant: a keyspace value
%%% is always the name of a database whose own keyspace is itself
%%% (branching a branch is rejected, lineage is linear). This lets
%%% internals pass an already-resolved name into helpers that resolve
%%% again.
%%%
%%% Branch identity (keyspace, parent, fork_hlc) persists in a sidecar
%%% file `TIMELINE' inside the database directory, NOT in db_meta: the
%%% compaction filter starts before RocksDB opens, so identity must be
%%% readable without the store. The file is written atomically at fork
%%% (temp + rename) and never rewritten.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_keyspace).

-export([install/2, uninstall/1, resolve/1]).
-export([read_meta/1, write_meta/2]).

-define(SIDECAR, "TIMELINE").

-type meta() :: #{
    keyspace := binary(),
    parent := binary(),
    fork_hlc := binary()    %% barrel_hlc:encode/1 form (12 bytes)
}.
-export_type([meta/0]).

%%====================================================================
%% Registry
%%====================================================================

%% @doc Register the key name used for a logical database name. Only
%% called when they differ (branches).
-spec install(binary(), binary()) -> ok.
install(Logical, KeyName) when Logical =/= KeyName ->
    persistent_term:put({barrel_keyspace, Logical}, KeyName),
    ok.

-spec uninstall(binary()) -> ok.
uninstall(Logical) ->
    _ = persistent_term:erase({barrel_keyspace, Logical}),
    ok.

%% @doc The name to build storage keys with for a logical database
%% name. Identity for normal databases.
-spec resolve(binary()) -> binary().
resolve(Name) ->
    persistent_term:get({barrel_keyspace, Name}, Name).

%%====================================================================
%% Sidecar identity file
%%====================================================================

%% @doc Read the branch identity sidecar of a database directory.
-spec read_meta(string()) ->
    {ok, meta()} | not_found | {error, corrupt_timeline_meta}.
read_meta(DbPath) ->
    File = filename:join(DbPath, ?SIDECAR),
    case file:consult(File) of
        {ok, [#{keyspace := Ks, parent := Parent, fork_hlc := ForkHlc}]}
                when is_binary(Ks), is_binary(Parent),
                     is_binary(ForkHlc), byte_size(ForkHlc) =:= 12 ->
            {ok, #{keyspace => Ks, parent => Parent,
                   fork_hlc => ForkHlc}};
        {error, enoent} ->
            not_found;
        _ ->
            {error, corrupt_timeline_meta}
    end.

%% @doc Write the branch identity sidecar atomically (temp + rename).
-spec write_meta(string(), meta()) -> ok | {error, term()}.
write_meta(DbPath, #{keyspace := Ks, parent := Parent,
                     fork_hlc := ForkHlc} = Meta)
        when is_binary(Ks), is_binary(Parent),
             is_binary(ForkHlc), byte_size(ForkHlc) =:= 12 ->
    File = filename:join(DbPath, ?SIDECAR),
    Tmp = File ++ ".tmp",
    Data = io_lib:format("~p.~n", [maps:with([keyspace, parent,
                                              fork_hlc], Meta)]),
    case file:write_file(Tmp, Data) of
        ok -> file:rename(Tmp, File);
        {error, _} = Error -> Error
    end.
