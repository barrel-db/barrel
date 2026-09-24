%% Every error code reads as a sentence and a next step.
-module(barrel_ctx_error_tests).

-include_lib("eunit/include/eunit.hrl").

every_code_has_text_test() ->
    lists:foreach(
        fun({Code, Status}) ->
            #{error := Code, message := Msg, hint := Hint, details := D} =
                barrel_ctx_error:to_map({Code, sample(Code)}),
            ?assert(Status >= 400),
            ?assert(is_map(D)),
            [begin
                 ?assert(byte_size(T) > 0),
                 ?assertEqual(nomatch, string:find(T, <<"\x{2014}"/utf8>>))
             end || T <- [Msg, Hint]]
        end, barrel_ctx_error:codes()).

legacy_terms_normalize_test() ->
    ?assertMatch({limit_required, #{max_limit := 1000}},
                 barrel_ctx_error:normalize(
                   {unsupported_federated_query, limit_required})),
    ?assertMatch({unsupported_federated_query,
                  #{reason := offset, accepted_shapes := [_, _, _]}},
                 barrel_ctx_error:normalize(
                   {unsupported_federated_query, offset})),
    ?assertMatch({invalid_argument, #{field := merge, allowed := [_ | _]}},
                 barrel_ctx_error:normalize({bad_request, merge})),
    ?assertMatch({internal, #{detail := _}},
                 barrel_ctx_error:normalize({something, odd, 1})).

remote_reason_reads_like_local_test() ->
    Local = barrel_ctx_error:source_error(
              error, #{reason => embedder_not_configured}),
    Remote = barrel_ctx_error:source_error(
               error, #{reason => <<"embedder_not_configured">>,
                        origin => remote}),
    ?assertEqual(maps:get(hint, Local), maps:get(hint, Remote)).

sample(invalid_argument) -> #{field => merge, allowed => [a]};
sample(invalid_query) -> #{bql_error => <<"x">>};
sample(limit_required) -> #{max_limit => 1000};
sample(limit_too_large) -> #{max_limit => 1000};
sample(too_many_contexts) -> #{max_contexts => 8, requested => 9};
sample(duplicate_context) -> #{context => <<"c">>};
sample(unsupported_federated_query) -> #{reason => offset};
sample(merge_not_allowed) -> #{merge => score};
sample(merge_not_supported) -> #{merge => rrf};
sample(scores_not_comparable) -> #{reason => fingerprint_mismatch,
                                   context => <<"c">>};
sample(unknown_context) -> #{context => <<"c">>, suggestions => []};
sample(ambiguous_context) -> #{name => <<"n">>, candidates => []};
sample(unknown_working_set) -> #{working_set => <<"w">>};
sample(not_attached) -> #{context => <<"c">>};
sample(already_attached) -> #{context => <<"c">>};
sample(already_exists) -> #{};
sample(invalid_card) -> #{reason => name};
sample(no_location) -> #{mode => local};
sample(over_budget) -> #{budget => bytes, needed => 2, available => 1};
sample(offline) -> #{operation => materialize};
sample(invalid_snapshot) -> #{reason => manifest_unreadable};
sample(source_unavailable) -> #{reason => timeout};
sample(forbidden) -> #{};
sample(internal) -> #{detail => <<"x">>}.

%% A timeout reports its budget when known, and never claims 0 ms.
timeout_text_test() ->
    ?assertMatch(#{message := <<"no answer within 300 ms">>},
                 barrel_ctx_error:source_error(
                   timeout, #{reason => deadline, after_ms => 300})),
    ?assertMatch(#{message := <<"no answer before the timeout">>},
                 barrel_ctx_error:source_error(timeout, #{reason => timeout})).
