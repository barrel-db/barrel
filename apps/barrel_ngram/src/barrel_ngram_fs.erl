%%%-------------------------------------------------------------------
%%% @doc Durable file helpers: fsync before rename, fsync the directory
%%% after, and streaming sha256 of a file.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_fs).

-export([write_file/2, commit/2, fsync_dir/1, sha256_file/1, hex/1]).

-on_load(init/0).

-define(APPNAME, barrel_ngram).
-define(LIBNAME, "barrel_ngram_fs_nif").
-define(CHUNK, (1024 * 1024)).

%% @doc Write `Data' to `Path' durably: temp file, fsync, rename, fsync
%% the parent directory.
-spec write_file(file:name_all(), iodata()) -> ok | {error, term()}.
write_file(Path, Data) ->
    Tmp = tmp_path(Path),
    case file:open(Tmp, [write, binary, raw]) of
        {ok, Fd} ->
            Res = write_sync(Fd, Data),
            _ = file:close(Fd),
            case Res of
                ok -> commit(Tmp, Path);
                {error, _} = Err ->
                    _ = file:delete(Tmp),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

write_sync(Fd, Data) ->
    case file:write(Fd, Data) of
        ok -> file:sync(Fd);
        {error, _} = Err -> Err
    end.

%% @doc Rename an already-synced `Tmp' onto `Path' and fsync the
%% directory, so the rename survives a crash.
-spec commit(file:name_all(), file:name_all()) -> ok | {error, term()}.
commit(Tmp, Path) ->
    case file:rename(Tmp, Path) of
        ok -> fsync_dir(filename:dirname(Path));
        {error, _} = Err -> Err
    end.

%% @doc fsync a directory (its entries, not the files it holds).
-spec fsync_dir(file:name_all()) -> ok | {error, term()}.
fsync_dir(Dir) ->
    fsync_dir_nif(unicode:characters_to_binary(filename:absname(Dir))).

fsync_dir_nif(_Dir) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]}).

%% @doc sha256 (lowercase hex) and size of a file, read in 1 MiB chunks.
-spec sha256_file(file:name_all()) ->
    {ok, binary(), non_neg_integer()} | {error, term()}.
sha256_file(Path) ->
    case file:open(Path, [read, binary, raw]) of
        {ok, Fd} ->
            try hash_loop(Fd, crypto:hash_init(sha256), 0)
            after _ = file:close(Fd)
            end;
        {error, _} = Err ->
            Err
    end.

hash_loop(Fd, Ctx, Size) ->
    case file:read(Fd, ?CHUNK) of
        {ok, Bin} -> hash_loop(Fd, crypto:hash_update(Ctx, Bin), Size + byte_size(Bin));
        eof -> {ok, hex(crypto:hash_final(Ctx)), Size};
        {error, _} = Err -> Err
    end.

%% @doc Lowercase hex of a binary.
-spec hex(binary()) -> binary().
hex(Bin) ->
    binary:encode_hex(Bin, lowercase).

tmp_path(Path) ->
    iolist_to_binary([unicode:characters_to_binary(Path), <<".tmp">>]).

init() ->
    SoName = case code:priv_dir(?APPNAME) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true -> filename:join(["..", priv, ?LIBNAME]);
                _ -> filename:join([priv, ?LIBNAME])
            end;
        Dir ->
            filename:join(Dir, ?LIBNAME)
    end,
    erlang:load_nif(SoName, 0).
