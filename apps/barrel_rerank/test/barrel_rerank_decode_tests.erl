%%% @doc Unit tests for the sidecar response decoder. This is the Erlang-side
%%% protocol logic that does not need the python reranker to exercise.
-module(barrel_rerank_decode_tests).

-include_lib("eunit/include/eunit.hrl").

results_decoded_as_index_score_pairs_test() ->
    R = #{<<"ok">> => true,
          <<"results">> => [
              #{<<"index">> => 0, <<"score">> => 0.9},
              #{<<"index">> => 2, <<"score">> => 0.1}
          ]},
    ?assertEqual({ok, [{0, 0.9}, {2, 0.1}]}, barrel_rerank:decode_response(R)).

model_info_decoded_test() ->
    R = #{<<"ok">> => true, <<"model">> => <<"m">>, <<"type">> => <<"cross">>},
    ?assertEqual({ok, #{model => <<"m">>, type => <<"cross">>}},
                 barrel_rerank:decode_response(R)).

bare_ok_passthrough_test() ->
    R = #{<<"ok">> => true, <<"note">> => <<"x">>},
    ?assertEqual({ok, R}, barrel_rerank:decode_response(R)).

python_error_test() ->
    R = #{<<"ok">> => false, <<"error">> => <<"boom">>},
    ?assertEqual({error, {python_error, <<"boom">>}},
                 barrel_rerank:decode_response(R)).

invalid_response_test() ->
    ?assertEqual({error, invalid_response}, barrel_rerank:decode_response(#{})),
    ?assertEqual({error, invalid_response}, barrel_rerank:decode_response(garbage)).
