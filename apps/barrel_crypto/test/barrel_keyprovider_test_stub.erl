%% @doc Test-only key provider exercising every dispatcher return shape.
-module(barrel_keyprovider_test_stub).
-behaviour(barrel_keyprovider).

-export([key_for_db/1]).

key_for_db(<<"boom">>) -> {error, boom};
key_for_db(<<"short">>) -> {ok, <<1, 2, 3>>};
key_for_db(<<"bad_return">>) -> nope;
key_for_db(<<"crash">>) -> error(kaboom);
key_for_db(Keyspace) -> {ok, barrel_crypto:derive_key(Keyspace, <<"stub">>)}.
