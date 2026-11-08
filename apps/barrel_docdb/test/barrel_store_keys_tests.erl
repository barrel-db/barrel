-module(barrel_store_keys_tests).
-include_lib("eunit/include/eunit.hrl").

parse_key_test() ->
    EntityKey = barrel_store_keys:doc_entity(<<"mydb">>, <<"doc123">>),
    ?assertEqual({doc_entity, <<"mydb">>, <<"doc123">>},
                 barrel_store_keys:parse_key(EntityKey)),
    BodyKey = barrel_store_keys:doc_body_rev(<<"mydb">>, <<"doc123">>, <<"1-abc">>),
    ?assertEqual({doc_body_rev, <<"mydb">>, <<"doc123">>, <<"1-abc">>},
                 barrel_store_keys:parse_key(BodyKey)),
    ?assertEqual(other, barrel_store_keys:parse_key(<<"unknown">>)).
