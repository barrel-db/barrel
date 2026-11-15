%%%-------------------------------------------------------------------
%%% @doc Backups of a database: a consistent copy of its documents and
%%% attachments taken while it stays open, with a manifest, and a
%%% restore that opens the copy as a database of its own.
%%%
%%% A backup is the state at `backup_hlc', an instant the writer mints:
%%% every write it applied before is in the copy, none after. The copy
%%% is built next to the target and renamed into place once complete,
%%% so a directory that exists is a whole backup.
%%%
%%% A restored database is a new author: it gets a new source id and a
%%% new writer id, so it never shares an identity with the database it
%%% came from or with that database's replicas.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_db_backup).

-export([backup/3, restore/3, read_manifest/1, local_dbs/0]).

-define(FORMAT, 1).
-define(MANIFEST, "MANIFEST.json").
-define(RESTORE_MARKER, "RESTORE").

%% @doc Back up a database into `Dir', which must not exist.
-spec backup(binary() | pid(), file:filename_all(), map()) ->
    {ok, #{dir := binary(), backup_hlc := binary(), manifest := map()}} |
    {error, term()}.
backup(Db, Dir0, _Opts) ->
    Dir = to_list(Dir0),
    case filelib:is_file(Dir) of
        true ->
            {error, {backup_dir_exists, list_to_binary(Dir)}};
        false ->
            case open(Db) of
                {ok, Pid} -> backup_open(Pid, Dir);
                {error, _} = Err -> Err
            end
    end.

backup_open(Pid, Dir) ->
    {ok, #{name := Name, db_path := DbPath} = Info} = barrel_db_server:info(Pid),
    {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
    case att_mode(AttRef) of
        {error, _} = Err ->
            Err;
        AttMode ->
            Tmp = Dir ++ ".partial-" ++ rand_suffix(),
            try
                {ok, Hlc} = checkpoint(Pid, DbPath, Tmp, AttMode),
                ok = copy_if_exists(filename:join(DbPath, "CRYPTO"),
                                    filename:join(Tmp, "CRYPTO")),
                Manifest = manifest(Name, Info, AttRef, Hlc, Tmp),
                ok = file:write_file(filename:join(Tmp, ?MANIFEST),
                                     json:encode(Manifest)),
                ok = filelib:ensure_dir(Dir),
                ok = file:rename(Tmp, Dir),
                {ok, #{dir => list_to_binary(Dir),
                       backup_hlc => maps:get(<<"backup_hlc">>, Manifest),
                       manifest => Manifest}}
            catch
                error:{badmatch, {error, _} = Failed} ->
                    _ = file:del_dir_r(Tmp),
                    Failed;
                Class:Reason:Stack ->
                    _ = file:del_dir_r(Tmp),
                    erlang:raise(Class, Reason, Stack)
            end
    end.

%% Attachments are in the copy for the blob store, absent for none; an
%% S3 store keeps them in the bucket, out of reach of a local copy.
att_mode(AttRef) ->
    case maps:get(backend, AttRef, barrel_att_store_blob) of
        barrel_att_store_blob -> blob;
        barrel_att_store_none -> none;
        Other -> {error, {unsupported_att_backend, Other}}
    end.

%% Hard links when the target is on the database's filesystem; else a
%% checkpoint next to the database, copied over.
checkpoint(Pid, DbPath, Tmp, AttMode) ->
    ok = filelib:ensure_dir(filename:join(Tmp, "x")),
    case same_device(DbPath, Tmp) of
        true ->
            checkpoint_into(Pid, Tmp, AttMode);
        false ->
            Stage = filename:join(DbPath, ".backup-" ++ rand_suffix()),
            ok = filelib:ensure_dir(filename:join(Stage, "x")),
            try
                {ok, Hlc} = checkpoint_into(Pid, Stage, AttMode),
                ok = copy_tree(Stage, Tmp),
                {ok, Hlc}
            after
                _ = file:del_dir_r(Stage)
            end
    end.

checkpoint_into(Pid, Base, AttMode) ->
    AttPath = case AttMode of
        blob -> filename:join(Base, "attachments");
        none -> undefined
    end,
    barrel_db_server:checkpoint_to(Pid, filename:join(Base, "docs"), AttPath).

manifest(Name, Info, AttRef, Hlc, Tmp) ->
    Config = maps:get(config, Info, #{}),
    #{<<"format">> => ?FORMAT,
      <<"db">> => Name,
      <<"keyspace">> => maps:get(keyspace, Info, Name),
      <<"source_id">> => barrel_db_server:source_id(Name),
      <<"backup_hlc">> => hex(barrel_hlc:encode(Hlc)),
      <<"created_at">> => erlang:system_time(millisecond),
      <<"retention_period">> => retention(maps:get(retention_period, Info, undefined)),
      <<"history_floor">> => floor_hex(maps:get(history_floor, Info, undefined)),
      <<"att_backend">> => atom_to_binary(maps:get(backend, AttRef, barrel_att_store_blob)),
      <<"encrypted">> => maps:get(encryption, Config, disabled) =/= disabled,
      <<"docdb_version">> => docdb_version(),
      <<"files">> => [#{<<"path">> => list_to_binary(P),
                        <<"size">> => filelib:file_size(filename:join(Tmp, P)),
                        <<"sha256">> => sha256(filename:join(Tmp, P))}
                      || P <- files(Tmp)]}.

%% @doc Restore the backup in `Dir' as database `Name', under the
%% `data_dir' option (else the node's). Opts: `data_dir', `encryption'
%% (required for an encrypted backup), `retention_period' (default the
%% backup's), `read_only', and any other create_db/2 option.
-spec restore(file:filename_all(), binary(), map()) -> {ok, pid()} | {error, term()}.
restore(Dir0, Name, Opts) when is_binary(Name) ->
    Dir = to_list(Dir0),
    maybe
        {ok, Manifest} ?= read_manifest(Dir),
        ok ?= verify(Dir, Manifest),
        ok ?= free_name(Name, Opts),
        DataDir = to_list(maps:get(data_dir, Opts,
                                   application:get_env(barrel_docdb, data_dir,
                                                       "/tmp/barrel_data"))),
        DbPath = filename:join(DataDir, binary_to_list(Name)),
        ok ?= place(Dir, Manifest, DbPath, Name),
        barrel_docdb:create_db(Name, open_opts(Opts, Manifest, DataDir))
    end.

%% @doc Read and check the manifest of a backup directory.
-spec read_manifest(file:filename_all()) -> {ok, map()} | {error, term()}.
read_manifest(Dir) ->
    case file:read_file(filename:join(to_list(Dir), ?MANIFEST)) of
        {ok, Bin} ->
            try json:decode(Bin) of
                #{<<"format">> := ?FORMAT, <<"files">> := Files} = M
                  when is_list(Files) -> {ok, M};
                #{<<"format">> := F} -> {error, {unsupported_backup_format, F}};
                _ -> {error, bad_manifest}
            catch
                _:_ -> {error, bad_manifest}
            end;
        {error, enoent} ->
            {error, not_a_backup};
        {error, _} = Err ->
            Err
    end.

%% @doc The databases with files under the node's data directory.
%% Internal databases (names starting with `_') are left out.
-spec local_dbs() -> [binary()].
local_dbs() ->
    DataDir = to_list(application:get_env(barrel_docdb, data_dir, "/tmp/barrel_data")),
    case file:list_dir(DataDir) of
        {ok, Entries} ->
            lists:sort([list_to_binary(E) || E <- Entries,
                        hd(E) =/= $_, hd(E) =/= $.,
                        filelib:is_dir(filename:join([DataDir, E, "docs"]))]);
        {error, _} ->
            []
    end.

%%====================================================================
%% Internal: restore
%%====================================================================

verify(Dir, #{<<"files">> := Files}) ->
    Bad = [P || #{<<"path">> := P, <<"size">> := Size, <<"sha256">> := Sum} <- Files,
                begin
                    F = filename:join(Dir, binary_to_list(P)),
                    filelib:file_size(F) =/= Size orelse sha256(F) =/= Sum
                end],
    case Bad of
        [] -> ok;
        _ -> {error, {corrupt_backup, Bad}}
    end.

free_name(Name, Opts) ->
    case barrel_docdb:db_exists(Name, maps:with([data_dir], Opts)) of
        true -> {error, {db_exists, Name}};
        false -> ok
    end.

%% Copy the files into the database directory; a new name keeps the
%% source keyspace through the sidecar, and the marker makes the first
%% writable open mint a new source id.
place(Dir, #{<<"files">> := Files, <<"keyspace">> := Ks}, DbPath, Name) ->
    Tmp = DbPath ++ ".restoring-" ++ rand_suffix(),
    try
        lists:foreach(
          fun(#{<<"path">> := P}) ->
                  Dst = filename:join(Tmp, binary_to_list(P)),
                  ok = filelib:ensure_dir(Dst),
                  {ok, _} = file:copy(filename:join(Dir, binary_to_list(P)), Dst)
          end, Files),
        ok = case Ks of
            Name -> ok;
            _ -> barrel_keyspace:write_meta(Tmp, #{
                     keyspace => Ks, parent => Ks, kind => import,
                     fork_hlc => barrel_hlc:encode(barrel_hlc:new_hlc())})
        end,
        ok = file:write_file(filename:join(Tmp, ?RESTORE_MARKER), <<>>),
        ok = filelib:ensure_dir(DbPath),
        ok = file:rename(Tmp, DbPath)
    catch
        error:{badmatch, {error, _} = Failed} ->
            _ = file:del_dir_r(Tmp),
            Failed
    end.

open_opts(Opts, Manifest, DataDir) ->
    Base = maps:without([data_dir], Opts),
    WithRetention = case {maps:is_key(retention_period, Base),
                          maps:get(<<"retention_period">>, Manifest, null)} of
        {false, Secs} when is_integer(Secs) -> Base#{retention_period => Secs};
        {false, <<"infinity">>} -> Base#{retention_period => infinity};
        _ -> Base
    end,
    WithRetention#{data_dir => DataDir}.

%%====================================================================
%% Internal
%%====================================================================

open(Pid) when is_pid(Pid) ->
    {ok, Pid};
open(Name) when is_binary(Name) ->
    case barrel_docdb:open_db(Name) of
        {ok, Pid} ->
            {ok, Pid};
        {error, _} ->
            case barrel_docdb:db_exists(Name, #{}) of
                true -> open_with_opener(Name);
                false -> {error, not_found}
            end
    end.

%% A database on disk but closed opens the way the application opens it
%% (barrel_server sets the opener), so its runtime config applies.
open_with_opener(Name) ->
    case application:get_env(barrel_docdb, task_db_opener) of
        {ok, {M, F}} ->
            _ = M:F(Name),
            barrel_docdb:open_db(Name);
        undefined ->
            barrel_docdb:create_db(Name, #{})
    end.

files(Root) ->
    lists:sort(files(Root, "")).

files(Root, Rel) ->
    {ok, Names} = file:list_dir(filename:join(Root, Rel)),
    lists:flatmap(
      fun(N) ->
              R = case Rel of "" -> N; _ -> filename:join(Rel, N) end,
              case filelib:is_dir(filename:join(Root, R)) of
                  true -> files(Root, R);
                  false -> [R]
              end
      end, Names).

copy_tree(Src, Dst) ->
    lists:foreach(
      fun(R) ->
              D = filename:join(Dst, R),
              ok = filelib:ensure_dir(D),
              {ok, _} = file:copy(filename:join(Src, R), D)
      end, files(Src)).

copy_if_exists(Src, Dst) ->
    case filelib:is_regular(Src) of
        true -> {ok, _} = file:copy(Src, Dst), ok;
        false -> ok
    end.

same_device(A, B) ->
    case {file:read_file_info(A), file:read_file_info(B)} of
        {{ok, IA}, {ok, IB}} -> element(10, IA) =:= element(10, IB);
        _ -> false
    end.

sha256(File) ->
    {ok, Fd} = file:open(File, [read, raw, binary]),
    try hex(hash_loop(Fd, crypto:hash_init(sha256)))
    after file:close(Fd)
    end.

hash_loop(Fd, Ctx) ->
    case file:read(Fd, 1 bsl 20) of
        {ok, Data} -> hash_loop(Fd, crypto:hash_update(Ctx, Data));
        eof -> crypto:hash_final(Ctx)
    end.

hex(Bin) -> binary:encode_hex(Bin, lowercase).

floor_hex(undefined) -> null;
floor_hex(Hlc) -> hex(barrel_hlc:encode(Hlc)).

retention(infinity) -> <<"infinity">>;
retention(N) when is_integer(N) -> N;
retention(_) -> null.

docdb_version() ->
    case application:get_key(barrel_docdb, vsn) of
        {ok, V} -> list_to_binary(V);
        undefined -> null
    end.

rand_suffix() ->
    binary_to_list(binary:encode_hex(crypto:strong_rand_bytes(4), lowercase)).

to_list(B) when is_binary(B) -> unicode:characters_to_list(B);
to_list(L) when is_list(L) -> L.
