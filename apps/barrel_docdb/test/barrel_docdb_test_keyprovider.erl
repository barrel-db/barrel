%% @doc Test-only key provider. The master material lives in app env so
%% suites can swap it between opens to simulate a wrong key.
-module(barrel_docdb_test_keyprovider).
-behaviour(barrel_keyprovider).

-export([key_for_db/1]).

key_for_db(Keyspace) ->
    case application:get_env(barrel_docdb, test_encryption_master) of
        {ok, Master} when is_binary(Master) ->
            {ok, barrel_crypto:derive_key(Master, Keyspace)};
        _ ->
            {error, no_test_master}
    end.
