%% @doc Local-disk object storage backend.
%%
%% Stores each object as a file under a base directory; the binary key is
%% hex-encoded to a filesystem-safe name. This is the default backend and needs
%% no external services. Configure with `#{dir => Path}'.
%% @end
-module(barrel_objectdb_fs).
-behaviour(barrel_objectdb_backend).

-export([init/1, put/3, get/2, delete/2, list/2]).

%% @private
init(#{dir := Dir}) ->
    DirL = to_list(Dir),
    case filelib:ensure_path(DirL) of
        ok -> {ok, #{dir => DirL}};
        {error, _} = Err -> Err
    end;
init(_Config) ->
    {error, {missing_config, dir}}.

%% @private
put(#{dir := Dir}, Key, Value) when is_binary(Key), is_binary(Value) ->
    file:write_file(path_for(Dir, Key), Value).

%% @private
get(#{dir := Dir}, Key) when is_binary(Key) ->
    case file:read_file(path_for(Dir, Key)) of
        {ok, Bin} -> {ok, Bin};
        {error, enoent} -> {error, not_found};
        {error, _} = Err -> Err
    end.

%% @private
delete(#{dir := Dir}, Key) when is_binary(Key) ->
    case file:delete(path_for(Dir, Key)) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, _} = Err -> Err
    end.

%% @private
list(#{dir := Dir}, Prefix) when is_binary(Prefix) ->
    case file:list_dir(Dir) of
        {ok, Names} ->
            Keys = [Key
                    || Name <- Names,
                       {ok, Key} <- [decode_name(Name)],
                       has_prefix(Prefix, Key)],
            {ok, Keys};
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Internal
%%====================================================================

path_for(Dir, Key) ->
    filename:join(Dir, binary_to_list(binary:encode_hex(Key))).

decode_name(Name) ->
    try {ok, binary:decode_hex(list_to_binary(Name))}
    catch _:_ -> error
    end.

has_prefix(Prefix, Key) ->
    PS = byte_size(Prefix),
    case Key of
        <<Prefix:PS/binary, _/binary>> -> true;
        _ -> false
    end.

to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(L) when is_list(L) -> L.
