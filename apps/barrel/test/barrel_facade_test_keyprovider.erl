%% @doc Test-only key provider for facade suites. The master material
%% lives in app env so cases can swap it to simulate a wrong key.
-module(barrel_facade_test_keyprovider).
-behaviour(barrel_keyprovider).

-export([key_for_db/1]).

key_for_db(Keyspace) ->
    case application:get_env(barrel, test_encryption_master) of
        {ok, Master} when is_binary(Master) ->
            {ok, barrel_crypto:derive_key(Master, Keyspace)};
        _ ->
            {error, no_test_master}
    end.
