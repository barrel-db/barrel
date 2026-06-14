%% @doc Key provider behaviour for Barrel encryption.
%%
%% A key provider supplies the 256-bit per-database key. The host application
%% (embedded use) or `barrel_server' (networked use) selects a provider through
%% the `barrel_crypto' application env `key_provider'. The built-in default reads
%% `BARREL_ENCRYPTION_KEY' (a 32-byte raw key, 64-char hex, or a passphrase run
%% through HKDF) or the `encryption_key' application config. A KMS- or
%% auth-token-derived provider plugs in by implementing this behaviour.
%% @end
-module(barrel_crypto_keyprovider).

-callback key_for_db(Db :: term()) -> {ok, binary()} | {error, term()}.

-export([key_for_db/1]).

%% @doc Resolve the per-database key using the configured provider, falling back
%% to the built-in environment/config provider.
-spec key_for_db(term()) -> {ok, binary()} | {error, term()}.
key_for_db(Db) ->
    case application:get_env(barrel_crypto, key_provider) of
        {ok, Mod} when is_atom(Mod), Mod =/= ?MODULE ->
            Mod:key_for_db(Db);
        _ ->
            default_key(Db)
    end.

%%====================================================================
%% Internal
%%====================================================================

default_key(_Db) ->
    case os:getenv("BARREL_ENCRYPTION_KEY") of
        false ->
            case application:get_env(barrel_crypto, encryption_key) of
                {ok, Key} -> normalize_key(Key);
                undefined -> {error, no_key}
            end;
        "" ->
            {error, no_key};
        KeyStr ->
            normalize_key(list_to_binary(KeyStr))
    end.

normalize_key(Key) when is_list(Key) ->
    normalize_key(list_to_binary(Key));
normalize_key(Key) when is_binary(Key), byte_size(Key) =:= 32 ->
    {ok, Key};
normalize_key(Key) when is_binary(Key) ->
    case is_hex64(Key) of
        true ->
            try {ok, binary:decode_hex(Key)}
            catch _:_ -> {ok, barrel_crypto:derive_key(Key)}
            end;
        false ->
            {ok, barrel_crypto:derive_key(Key)}
    end.

is_hex64(Bin) when byte_size(Bin) =:= 64 ->
    lists:all(
        fun(C) ->
            (C >= $0 andalso C =< $9)
                orelse (C >= $a andalso C =< $f)
                orelse (C >= $A andalso C =< $F)
        end,
        binary_to_list(Bin)
    );
is_hex64(_) ->
    false.
