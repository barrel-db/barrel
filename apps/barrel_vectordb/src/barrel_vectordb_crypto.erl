%% @doc Encryption plumbing for vectordb stores: builds the RocksDB
%% EncryptedEnv from a resolved key and enforces a fail-closed open
%% matrix through a cleartext CRYPTO key-check marker inside the store
%% directory.
%%
%% The store never resolves keys itself: the barrel facade resolves one
%% key per logical database (the docdb keyspace) and passes it as
%% `crypto => #{key := <<_:256>>}'; standalone users pass a key the same
%% way. The returned env handle must stay referenced for the whole store
%% lifetime (the NIF frees the env when the handle is garbage
%% collected).
%% @end
-module(barrel_vectordb_crypto).

-export([init/2]).

-type ctx() :: none | #{key := binary(), env := rocksdb:env_handle()}.
-export_type([ctx/0]).

-spec init(none | #{key := binary(), _ => _}, string() | binary()) ->
    {ok, ctx()} | {error, term()}.
init(none, DbPath) ->
    case filelib:is_regular(marker_path(DbPath)) of
        true -> {error, db_is_encrypted};
        false -> {ok, none}
    end;
init(#{key := Key}, DbPath) when is_binary(Key), byte_size(Key) =:= 32 ->
    check_marker(DbPath, Key);
init(Other, _DbPath) ->
    {error, {bad_crypto_config, Other}}.

%%====================================================================
%% Internal
%%====================================================================

check_marker(DbPath, Key) ->
    Marker = marker_path(DbPath),
    case file:read_file(Marker) of
        {ok, Token} ->
            case barrel_crypto:key_check_verify(Key, Token) of
                true -> new_env(Key);
                false -> {error, wrong_encryption_key}
            end;
        {error, enoent} ->
            %% CURRENT marks an existing RocksDB: refuse to encrypt a
            %% store that already has plaintext files
            case filelib:is_regular(filename:join(DbPath, "CURRENT")) of
                true ->
                    {error, cannot_encrypt_existing_db};
                false ->
                    case write_marker(Marker, Key) of
                        ok ->
                            new_env(Key);
                        {error, Reason} ->
                            {error, {crypto_marker_write_failed, Reason}}
                    end
            end;
        {error, Reason} ->
            {error, {crypto_marker_read_failed, Reason}}
    end.

new_env(Key) ->
    case rocksdb:new_env({encrypted, Key}) of
        {ok, Env} -> {ok, #{key => Key, env => Env}};
        {error, Reason} -> {error, {encryption_env_failed, Reason}}
    end.

write_marker(Marker, Key) ->
    Token = barrel_crypto:key_check_new(Key),
    ok = filelib:ensure_dir(Marker),
    Tmp = marker_tmp(Marker),
    case file:write_file(Tmp, Token) of
        ok -> file:rename(Tmp, Marker);
        {error, _} = Err -> Err
    end.

marker_path(DbPath) ->
    filename:join(DbPath, "CRYPTO").

marker_tmp(Marker) when is_binary(Marker) ->
    <<Marker/binary, ".tmp">>;
marker_tmp(Marker) when is_list(Marker) ->
    Marker ++ ".tmp".
