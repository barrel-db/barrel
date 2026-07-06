%% @doc Built-in key provider: per-database keys from one master secret in
%% the environment.
%%
%% Master secret sources, first hit wins: the `BARREL_ENCRYPTION_KEY'
%% environment variable, then the `{barrel_crypto, encryption_key}'
%% application config. Accepted forms: 32 raw bytes (used as key material
%% directly), 64 hex characters (decoded first), anything else is a
%% passphrase. In every form the per-database key is
%% `HKDF-SHA256(master, salt, <<"barrel_db:", Keyspace>>)': one secret
%% yields a distinct key per database, and a branch derives its parent's
%% key because it shares the parent's keyspace.
%% @end
-module(barrel_keyprovider_env).
-behaviour(barrel_keyprovider).

-export([key_for_db/1]).

-spec key_for_db(binary()) -> {ok, binary()} | {error, term()}.
key_for_db(Keyspace) when is_binary(Keyspace) ->
    case master_key() of
        {ok, Ikm} ->
            {ok, barrel_crypto:derive_key(Ikm, <<"barrel_db:", Keyspace/binary>>)};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Internal
%%====================================================================

master_key() ->
    case os:getenv("BARREL_ENCRYPTION_KEY") of
        false -> config_key();
        "" -> config_key();
        Str -> normalize(unicode:characters_to_binary(Str))
    end.

config_key() ->
    case application:get_env(barrel_crypto, encryption_key) of
        {ok, Key} when is_binary(Key), byte_size(Key) > 0 ->
            normalize(Key);
        {ok, Key} when is_list(Key), Key =/= [] ->
            normalize(unicode:characters_to_binary(Key));
        _ ->
            {error, no_encryption_key}
    end.

normalize(<<Key:32/binary>>) ->
    {ok, Key};
normalize(Bin) when byte_size(Bin) =:= 64 ->
    %% 64 chars of hex decode to the raw key material; anything else of
    %% that length is a passphrase.
    try
        {ok, binary:decode_hex(Bin)}
    catch
        error:badarg -> {ok, Bin}
    end;
normalize(Bin) when is_binary(Bin), byte_size(Bin) > 0 ->
    {ok, Bin};
normalize(_) ->
    {error, no_encryption_key}.
