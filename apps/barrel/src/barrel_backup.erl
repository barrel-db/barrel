%%%-------------------------------------------------------------------
%%% @doc Backups of a barrel database, taken while it stays open:
%%%
%%%   Dir/docdb/      a barrel_docdb backup (barrel_db_backup)
%%%   Dir/vectordb/   plain mode: a snapshot of the vector store
%%%   Dir/BARREL.json the mode, and the size and sha256 of every file
%%%
%%% Record mode copies no vectors: a restore rebuilds the index from the
%%% embeddings stored in the documents (barrel_record_backfill), so it
%%% always matches the documents restored. Plain mode keeps its vectors
%%% only in the vector store, so the store is copied.
%%%
%%% The copy is built next to `Dir' and renamed once complete. A
%%% restored database is a new author, as a restored docdb is.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_backup).

-export([backup/3, restore/3, read_manifest/1, local_dbs/0]).

-define(MANIFEST, "BARREL.json").
-define(FORMAT, 1).

%% @doc Back up a database (a handle, or a name opened through
%% barrel_dbs with the `open' option) into `Dir', which must not exist.
-spec backup(barrel:db() | barrel:db_name(), file:filename_all(), map()) ->
    {ok, #{dir := binary(), mode := atom(), backup_hlc := binary()}} |
    {error, term()}.
backup(Db, Dir0, _Opts) when is_map(Db) ->
    Dir = to_list(Dir0),
    case filelib:is_file(Dir) of
        true -> {error, {backup_dir_exists, list_to_binary(Dir)}};
        false -> backup_into(Db, Dir)
    end;
backup(Name, Dir, Opts) ->
    case barrel_dbs:ensure(Name, maps:get(open, Opts, #{})) of
        {ok, Db} -> backup(Db, Dir, Opts);
        {error, _} = Err -> Err
    end.

backup_into(#{docdb := DocDb} = Db, Dir) ->
    Tmp = Dir ++ ".partial-" ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_path(Tmp),
    Mode = mode(Db),
    Result =
        case barrel_docdb:backup_db(DocDb, filename:join(Tmp, "docdb")) of
            {ok, #{backup_hlc := Hlc}} ->
                case steps([fun() -> backup_vectors(Mode, Db, filename:join(Tmp, "vectordb")) end,
                            fun() -> write_manifest(Tmp, Db, Mode, Hlc) end,
                            fun() -> file:rename(Tmp, Dir) end]) of
                    ok -> {ok, #{dir => list_to_binary(Dir), mode => Mode, backup_hlc => Hlc}};
                    {error, _} = Err -> Err
                end;
            {error, _} = Err ->
                Err
        end,
    case Result of
        {ok, _} -> Result;
        {error, _} -> _ = file:del_dir_r(Tmp), Result
    end.

steps([]) -> ok;
steps([F | Rest]) ->
    case F() of
        ok -> steps(Rest);
        {error, _} = Err -> Err
    end.

mode(#{embedding := _}) -> record;
mode(#{vstore := _}) -> plain;
mode(_) -> docs_only.

backup_vectors(plain, #{vstore := Store}, Target) ->
    barrel_vectordb:backup(Store, Target);
backup_vectors(_Mode, _Db, _Target) ->
    ok.

write_manifest(Tmp, #{docdb := DocDb} = Db, Mode, Hlc) ->
    Files = [#{<<"path">> => P, <<"size">> => B, <<"sha256">> => S}
             || #{path := P, bytes := B, sha256 := S} <- barrel_ctx_manifest:scan(Tmp)],
    M = #{<<"format">> => ?FORMAT,
          <<"db">> => DocDb,
          <<"mode">> => atom_to_binary(Mode),
          <<"backup_hlc">> => Hlc,
          <<"created_at">> => erlang:system_time(millisecond),
          <<"vectors">> => vector_count(Mode, Db),
          <<"barrel_version">> => vsn(),
          <<"files">> => Files},
    file:write_file(filename:join(Tmp, ?MANIFEST), json:encode(M)).

vector_count(plain, #{vstore := Store}) -> barrel_vectordb:count(Store);
vector_count(_Mode, _Db) -> null.

%% @doc Restore the backup in `Dir' as database `Name' and open it.
%%
%% Opts are those of barrel:open/2 (`docdb', `vectordb', `encryption'),
%% given as the database is opened: a plain database needs its vector
%% config again, an encrypted one its key provider. Also `backfill'
%% (record mode: `sync', the default, or `none'). The vector store goes
%% to `vectordb => #{db_path => P}' if given, else next to the documents.
-spec restore(file:filename_all(), barrel:db_name(), map()) ->
    {ok, barrel:db()} | {error, term()}.
restore(Dir0, Name0, Opts) ->
    Dir = to_list(Dir0),
    Name = to_name(Name0),
    case read_manifest(Dir) of
        {ok, #{<<"mode">> := ModeBin} = M} ->
            case verify(Dir, M) of
                ok -> restore_checked(Dir, Name, binary_to_existing_atom(ModeBin), Opts);
                {error, _} = Err -> Err
            end;
        {error, _} = Err ->
            Err
    end.

restore_checked(Dir, Name, Mode, Opts) ->
    DocOpts = maps:get(docdb, Opts, #{}),
    DbPath = filename:join(data_dir(DocOpts), binary_to_list(Name)),
    VecPath = vec_path(Opts, DbPath),
    DocDir = filename:join(Dir, "docdb"),
    case {barrel_db_backup:read_manifest(DocDir), free_vec_path(Mode, VecPath)} of
        {{ok, DocManifest}, ok} ->
            case barrel_docdb:restore_db(DocDir, Name,
                                         (maps:with([data_dir], DocOpts))#{open => false}) of
                {ok, Placed} ->
                    case place_vectors(Mode, filename:join(Dir, "vectordb"), VecPath) of
                        ok ->
                            open_restored(Mode, Name, Opts, DocManifest, VecPath);
                        {error, _} = Err ->
                            %% no half database left behind
                            _ = file:del_dir_r(Placed),
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end;
        {{error, _} = Err, _} -> Err;
        {_, {error, _} = Err} -> Err
    end.

%% A restored docdb keeps the backup's retention unless told otherwise,
%% as barrel_docdb:restore_db/3 does.
open_restored(Mode, Name, Opts, DocManifest, VecPath) ->
    DocOpts0 = maps:get(docdb, Opts, #{}),
    DocOpts = case DocManifest of
        #{<<"retention_period">> := R} when is_integer(R) ->
            maps:merge(#{retention_period => R}, DocOpts0);
        _ -> DocOpts0
    end,
    Base = (maps:without([backfill, embedding], Opts))#{docdb => DocOpts},
    case Mode of
        docs_only ->
            barrel:open(Name, Base#{vectordb => none});
        plain ->
            barrel:open(Name, Base#{vectordb => (maps:get(vectordb, Opts, #{}))#{db_path => VecPath}});
        record ->
            open_record(Name, Base#{embedding => stored}, maps:get(backfill, Opts, sync))
    end.

%% Record mode: a fresh store, filled from the embeddings stored in the
%% documents (pending outbox entries came with the docdb backup).
open_record(Name, OpenOpts, Backfill) ->
    case barrel:open(Name, OpenOpts) of
        {ok, Db} when Backfill =:= none ->
            {ok, Db};
        {ok, #{docdb := DbBin, vstore := VStore, embedding := Policy, embed := Embed,
               dimensions := Dim} = Db} ->
            case barrel_record_backfill:run(#{db => DbBin, vstore => VStore,
                                              policy => Policy, embed => Embed,
                                              dimensions => Dim}) of
                {ok, _Report} ->
                    {ok, Db};
                {error, Reason} ->
                    _ = barrel:close(Db),
                    {error, {backfill_failed, Reason}}
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Read and check the manifest of a barrel backup directory.
-spec read_manifest(file:filename_all()) -> {ok, map()} | {error, term()}.
read_manifest(Dir) ->
    case file:read_file(filename:join(to_list(Dir), ?MANIFEST)) of
        {ok, Bin} ->
            try json:decode(Bin) of
                #{<<"format">> := ?FORMAT, <<"mode">> := Mode, <<"files">> := Files} = M
                  when is_list(Files), (Mode =:= <<"plain">> orelse Mode =:= <<"record">>
                                        orelse Mode =:= <<"docs_only">>) ->
                    {ok, M};
                #{<<"format">> := F} when F =/= ?FORMAT ->
                    {error, {unsupported_backup_format, F}};
                _ ->
                    {error, bad_manifest}
            catch
                _:_ -> {error, bad_manifest}
            end;
        {error, enoent} ->
            {error, not_a_backup};
        {error, _} = Err ->
            Err
    end.

%% @doc The databases with files under the node's data directory.
-spec local_dbs() -> [binary()].
local_dbs() ->
    barrel_db_backup:local_dbs().

%%====================================================================
%% Internal
%%====================================================================

verify(Dir, #{<<"files">> := Files}) ->
    Bad = [P || #{<<"path">> := P, <<"size">> := Size, <<"sha256">> := Sum} <- Files,
                not barrel_ctx_manifest:verify_file(filename:join(Dir, binary_to_list(P)),
                                                    #{sha256 => Sum, bytes => Size})],
    case Bad of
        [] -> ok;
        _ -> {error, {corrupt_backup, Bad}}
    end.

data_dir(DocOpts) ->
    to_list(maps:get(data_dir, DocOpts,
                     application:get_env(barrel_docdb, data_dir, "/tmp/barrel_data"))).

%% barrel:open's rule: a caller path, else next to the documents.
vec_path(Opts, DbPath) ->
    case maps:get(vectordb, Opts, #{}) of
        #{db_path := P} -> to_list(P);
        #{path := P} -> to_list(P);
        _ -> DbPath ++ ".vectordb"
    end.

free_vec_path(plain, VecPath) ->
    case filelib:is_file(VecPath) of
        true -> {error, {vector_store_exists, list_to_binary(VecPath)}};
        false -> ok
    end;
free_vec_path(_Mode, _VecPath) ->
    ok.

place_vectors(plain, Src, VecPath) ->
    Tmp = VecPath ++ ".restoring-" ++ integer_to_list(erlang:unique_integer([positive])),
    case copy_tree(Src, Tmp) of
        ok -> file:rename(Tmp, VecPath);
        {error, _} = Err -> _ = file:del_dir_r(Tmp), Err
    end;
place_vectors(_Mode, _Src, _VecPath) ->
    ok.

copy_tree(From, To) ->
    case file:list_dir(From) of
        {ok, Names} ->
            ok = filelib:ensure_path(To),
            lists:foldl(fun(N, ok) ->
                                F = filename:join(From, N),
                                T = filename:join(To, N),
                                case filelib:is_dir(F) of
                                    true -> copy_tree(F, T);
                                    false -> copy_file(F, T)
                                end;
                           (_, Err) -> Err
                        end, ok, Names);
        {error, _} = Err ->
            Err
    end.

copy_file(F, T) ->
    case file:copy(F, T) of
        {ok, _} -> ok;
        {error, _} = Err -> Err
    end.

vsn() ->
    case application:get_key(barrel, vsn) of
        {ok, V} -> list_to_binary(V);
        undefined -> <<"unknown">>
    end.

to_name(N) when is_atom(N) -> atom_to_binary(N);
to_name(N) when is_binary(N) -> N.

to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(L) when is_list(L) -> L.
