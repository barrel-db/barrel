-module(barrel_embed_fingerprint_tests).

-include_lib("eunit/include/eunit.hrl").

policy() ->
    {ok, P} = barrel_embedding_policy:validate(
                #{fields => [<<"path">>, <<"text">>]}),
    P.

info(Model) ->
    #{configured => true, dimension => 768,
      providers => [#{module => barrel_embed_ollama, name => ollama,
                      model => Model}]}.

fp(Info, Dim, Dist, Policy) ->
    maps:get(fingerprint,
             barrel_embed_fingerprint:identity(Info, Dim, Dist, Policy),
             undefined).

identity_fields_test() ->
    Id = barrel_embed_fingerprint:identity(info(<<"m:latest">>), 768, cosine,
                                           policy()),
    ?assertMatch(#{provider := ollama, model := <<"m:latest">>,
                   dimensions := 768, distance := cosine,
                   preprocessing := #{fields := [[<<"path">>], [<<"text">>]],
                                      join := <<"\n">>},
                   fingerprint := <<"sha256:", _:64/binary>>}, Id),
    ?assertNot(maps:is_key(revision, Id)).

same_inputs_same_fingerprint_test() ->
    ?assertEqual(fp(info(<<"m">>), 768, cosine, policy()),
                 fp(info(<<"m">>), 768, cosine, policy())).

every_input_changes_fingerprint_test() ->
    Base = fp(info(<<"m">>), 768, cosine, policy()),
    {ok, Other} = barrel_embedding_policy:validate(#{fields => [<<"text">>]}),
    {ok, Join} = barrel_embedding_policy:validate(
                   #{fields => [<<"path">>, <<"text">>], join => <<" ">>}),
    Rev = #{configured => true,
            providers => [#{name => ollama, model => <<"m">>,
                            revision => <<"r1">>}]},
    Variants = [fp(info(<<"m2">>), 768, cosine, policy()),
                fp(info(<<"m">>), 384, cosine, policy()),
                fp(info(<<"m">>), 768, euclidean, policy()),
                fp(info(<<"m">>), 768, cosine, Other),
                fp(info(<<"m">>), 768, cosine, Join),
                fp(info(<<"m">>), 768, cosine, none),
                fp(Rev, 768, cosine, policy())],
    ?assertEqual(length(Variants) + 1,
                 length(lists:usort([Base | Variants]))).

policy_mode_ignored_test() ->
    %% write mode and metadata do not change the vectors
    {ok, Sync} = barrel_embedding_policy:validate(
                   #{fields => [<<"path">>, <<"text">>], mode => sync,
                     metadata_fields => [<<"a">>]}),
    ?assertEqual(fp(info(<<"m">>), 768, cosine, policy()),
                 fp(info(<<"m">>), 768, cosine, Sync)).

unknown_model_no_fingerprint_test() ->
    ?assertEqual(undefined, fp(info(undefined), 768, cosine, policy())),
    ?assertEqual(undefined,
                 fp(#{configured => false}, 768, cosine, policy())),
    Id = barrel_embed_fingerprint:identity(#{configured => false}, 768,
                                           cosine, policy()),
    ?assertNot(maps:is_key(provider, Id)).

fallback_chain_in_fingerprint_test() ->
    Chain = #{configured => true,
              providers => [#{name => ollama, model => <<"m">>},
                            #{name => local, model => <<"other">>}]},
    ?assertNotEqual(fp(info(<<"m">>), 768, cosine, policy()),
                    fp(Chain, 768, cosine, policy())),
    %% an unknown fallback model makes the whole chain unidentified
    Unknown = #{configured => true,
                providers => [#{name => ollama, model => <<"m">>},
                              #{name => custom, model => undefined}]},
    ?assertEqual(undefined, fp(Unknown, 768, cosine, policy())).

canonical_json_test() ->
    ?assertEqual(<<"{\"a\":1,\"b\":[\"x\",null,true],\"c\":{\"d\":\"e\"}}">>,
                 barrel_embed_fingerprint:canonical_json(
                   #{c => #{<<"d">> => e}, b => [<<"x">>, undefined, true],
                     a => 1})),
    ?assertEqual(barrel_embed_fingerprint:fingerprint(#{a => 1, b => 2}),
                 barrel_embed_fingerprint:fingerprint(#{b => 2, a => 1})).
