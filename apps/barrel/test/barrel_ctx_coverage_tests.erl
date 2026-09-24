-module(barrel_ctx_coverage_tests).

-include_lib("eunit/include/eunit.hrl").

-define(OFF, #{offline => true, available => true}).
-define(ON, #{offline => false, available => true}).

remote_offline_skipped_test() ->
    M = #{mode => remote, context => <<"ctx_r">>},
    ?assertEqual(#{context => <<"ctx_r">>, status => skipped_offline,
                   error => #{reason => no_local_copy}},
                 barrel_ctx_coverage:member(M, ?OFF)).

remote_online_pending_test() ->
    M = #{mode => remote, context => <<"ctx_r">>},
    #{status := pending, membership := live} =
        barrel_ctx_coverage:member(M, ?ON).

snapshot_complete_generation_test() ->
    M = #{mode => snapshot, context => <<"ctx_s">>, generation => 17,
          local_db => <<"wsnap_s_17">>},
    #{status := ok, membership := complete_generation,
      version := #{kind := generation, generation := 17}} =
        barrel_ctx_coverage:member(M, ?OFF).

snapshot_missing_copy_test() ->
    M = #{mode => snapshot, context => <<"ctx_s">>, generation => 17,
          local_db => <<"wsnap_s_17">>},
    #{status := error, error := #{reason := local_copy_missing}} =
        barrel_ctx_coverage:member(M, #{offline => true, available => false}).

retrieved_set_note_test() ->
    M = #{mode => retrieved_set, context => <<"ctx_d">>,
          derived => #{<<"docs">> => 40,
                       <<"observed">> => #{<<"instance_id">> => <<"0f">>}}},
    #{status := ok, membership := retrieved_set,
      version := #{kind := retrieved_set,
                   observed := #{<<"instance_id">> := <<"0f">>}},
      note := Note} = barrel_ctx_coverage:member(M, ?OFF),
    ?assertNotEqual(nomatch, binary:match(Note, <<"40 saved documents">>)).

local_live_test() ->
    #{status := ok, membership := live} =
        barrel_ctx_coverage:member(#{mode => local, context => <<"c">>},
                                   ?OFF).

predicate_complete_test() ->
    Pred = [{path, [<<"app">>], <<"kernel">>}],
    M = #{mode => snapshot, context => <<"c">>, generation => 1,
          predicate => Pred},
    #{membership := complete_predicate} =
        barrel_ctx_coverage:member(M, maps:put(where, Pred ++ [{x, 1}], ?OFF)).

predicate_overlap_test() ->
    Pred = [{path, [<<"app">>], <<"kernel">>}],
    M = #{mode => snapshot, context => <<"c">>, generation => 1,
          predicate => Pred},
    #{membership := predicate_overlap} =
        barrel_ctx_coverage:member(M, maps:put(where, [{x, 1}], ?OFF)),
    #{membership := predicate_overlap} =
        barrel_ctx_coverage:member(M, ?OFF).

implies_test() ->
    ?assert(barrel_ctx_coverage:implies([a, b], [a])),
    ?assert(barrel_ctx_coverage:implies([a], [])),
    ?assertNot(barrel_ctx_coverage:implies([b], [a])),
    ?assertNot(barrel_ctx_coverage:implies(unknown, [])).

%% the 3.9 example: a snapshot, a slice, a remote-only context offline
summarize_offline_example_test() ->
    Members = [#{mode => snapshot, context => <<"ctx_7h1p">>,
                 generation => 17},
               #{mode => retrieved_set, context => <<"ctx_2d9x">>,
                 derived => #{<<"docs">> => 40}},
               #{mode => remote, context => <<"ctx_4q2m">>}],
    Sources = [barrel_ctx_coverage:member(M, ?OFF) || M <- Members],
    ?assertEqual(
       #{execution => partial,
         coverage => #{requested => 3, answered => 2, failed => 0,
                       skipped => 1, pending => 0,
                       missing => [<<"ctx_4q2m">>],
                       scope_origin => explicit}},
       barrel_ctx_coverage:summarize(Sources, explicit)).

summarize_complete_test() ->
    Sources = [barrel_ctx_coverage:member(#{mode => local,
                                            context => <<"c">>}, ?ON)],
    #{execution := complete} = barrel_ctx_coverage:summarize(Sources,
                                                            explicit).
