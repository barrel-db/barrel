-module(barrel_rep_transport_http_tests).

-include_lib("eunit/include/eunit.hrl").

normalize_test() ->
    ?assertMatch(#{url := <<"http://host:8080/db/mydb">>},
                 barrel_rep_transport_http:endpoint(
                     <<"HTTP://HOST:8080/db/mydb/">>)),
    ?assertMatch(#{url := <<"https://host/db/x">>},
                 barrel_rep_transport_http:endpoint("https://host/db/x")),
    %% map form keeps its options, URL normalized
    ?assertMatch(#{url := <<"http://h/db/x">>, pool := p},
                 barrel_rep_transport_http:endpoint(
                     #{url => <<"http://h/db/x/">>, pool => p})).

reject_test() ->
    Bad = [<<"ftp://h/db/x">>,
           <<"http://h/nope">>,
           <<"http://h/db/">>,
           <<"http://h/db/x?y=1">>,
           <<"not a url">>],
    lists:foreach(
        fun(Url) ->
            ?assertError({invalid_sync_url, _},
                         barrel_rep_transport_http:endpoint(Url))
        end,
        Bad).

rep_id_term_test() ->
    Ep = barrel_rep_transport_http:endpoint(
        #{url => <<"http://h:1/db/x">>,
          auth => #{token => <<"secret">>}}),
    ?assertEqual(<<"http://h:1/db/x">>,
                 barrel_rep_transport_http:rep_id_term(Ep)).
