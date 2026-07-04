%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_embedding_policy (pure module).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_embedding_policy_tests).

-include_lib("eunit/include/eunit.hrl").

-define(POLICY(Fields), element(2, barrel_embedding_policy:validate(#{fields => Fields}))).

%%====================================================================
%% validate/1
%%====================================================================

validate_defaults_test() ->
    {ok, P} = barrel_embedding_policy:validate(#{fields => [<<"title">>]}),
    ?assertEqual([[<<"title">>]], maps:get(fields, P)),
    ?assertEqual(<<"\n">>, maps:get(join, P)),
    ?assertEqual(async, maps:get(mode, P)),
    ?assertEqual(async, barrel_embedding_policy:mode(P)).

validate_full_test() ->
    {ok, P} = barrel_embedding_policy:validate(#{
        fields => [[<<"a">>, <<"b">>], <<"c">>],
        join => <<" ">>,
        mode => sync,
        dimensions => 384,
        embedder => {local, #{}},
        metadata_fields => [<<"kind">>]
    }),
    ?assertEqual([[<<"a">>, <<"b">>], [<<"c">>]], maps:get(fields, P)),
    ?assertEqual(sync, maps:get(mode, P)),
    ?assertEqual(384, maps:get(dimensions, P)),
    ?assertEqual([<<"kind">>], maps:get(metadata_fields, P)).

validate_fields_optional_test() ->
    %% No fields: bring-your-own-embeddings mode, never auto-embeds
    {ok, P} = barrel_embedding_policy:validate(#{}),
    ?assertEqual([], maps:get(fields, P)),
    ?assertNot(barrel_embedding_policy:matches(P, #{<<"title">> => <<"x">>})),
    ?assertEqual(<<>>, barrel_embedding_policy:text(P, #{<<"title">> => <<"x">>})),
    {ok, P2} = barrel_embedding_policy:validate(#{fields => []}),
    ?assertEqual([], maps:get(fields, P2)).

validate_errors_test_() ->
    [
     ?_assertMatch({error, {invalid_field, _}},
                   barrel_embedding_policy:validate(#{fields => [42]})),
     ?_assertMatch({error, {invalid_option, mode, _}},
                   barrel_embedding_policy:validate(
                       #{fields => [<<"t">>], mode => whenever})),
     ?_assertMatch({error, {invalid_option, dimensions, _}},
                   barrel_embedding_policy:validate(
                       #{fields => [<<"t">>], dimensions => 0})),
     ?_assertMatch({error, {unknown_options, [feilds]}},
                   barrel_embedding_policy:validate(#{feilds => [<<"t">>]})),
     ?_assertMatch({error, {invalid_policy, _}},
                   barrel_embedding_policy:validate(not_a_map))
    ].

%%====================================================================
%% matches/2 and text/2
%%====================================================================

matches_test() ->
    P = ?POLICY([<<"title">>, [<<"body">>, <<"text">>]]),
    ?assert(barrel_embedding_policy:matches(P, #{<<"title">> => <<"hi">>})),
    ?assert(barrel_embedding_policy:matches(
        P, #{<<"body">> => #{<<"text">> => <<"deep">>}})),
    %% Missing, empty, and non-binary values do not match
    ?assertNot(barrel_embedding_policy:matches(P, #{<<"other">> => <<"x">>})),
    ?assertNot(barrel_embedding_policy:matches(P, #{<<"title">> => <<>>})),
    ?assertNot(barrel_embedding_policy:matches(P, #{<<"title">> => 42})).

text_join_test() ->
    P = ?POLICY([<<"title">>, <<"body">>]),
    Doc = #{<<"title">> => <<"Hello">>, <<"body">> => <<"World">>},
    ?assertEqual(<<"Hello\nWorld">>, barrel_embedding_policy:text(P, Doc)).

text_skips_missing_test() ->
    P = ?POLICY([<<"title">>, <<"missing">>, <<"body">>]),
    Doc = #{<<"title">> => <<"A">>, <<"body">> => <<"B">>, <<"n">> => 7},
    ?assertEqual(<<"A\nB">>, barrel_embedding_policy:text(P, Doc)),
    ?assertEqual(<<>>, barrel_embedding_policy:text(P, #{<<"n">> => 7})).

text_nested_and_custom_join_test() ->
    {ok, P} = barrel_embedding_policy:validate(#{
        fields => [[<<"a">>, <<"b">>], <<"c">>],
        join => <<" | ">>
    }),
    Doc = #{<<"a">> => #{<<"b">> => <<"nested">>}, <<"c">> => <<"top">>},
    ?assertEqual(<<"nested | top">>, barrel_embedding_policy:text(P, Doc)),
    %% Path through a non-map is skipped
    ?assertEqual(<<"top">>,
                 barrel_embedding_policy:text(P, #{<<"a">> => 1,
                                                   <<"c">> => <<"top">>})).

%%====================================================================
%% metadata/2
%%====================================================================

metadata_default_test() ->
    P = ?POLICY([<<"title">>]),
    Doc = #{<<"id">> => <<"x">>, <<"_rev">> => <<"1-a">>,
            <<"_deleted">> => false,
            <<"title">> => <<"T">>, <<"kind">> => <<"note">>},
    ?assertEqual(#{<<"title">> => <<"T">>, <<"kind">> => <<"note">>},
                 barrel_embedding_policy:metadata(P, Doc)).

metadata_projection_test() ->
    {ok, P} = barrel_embedding_policy:validate(#{
        fields => [<<"title">>],
        metadata_fields => [<<"kind">>]
    }),
    Doc = #{<<"id">> => <<"x">>, <<"title">> => <<"T">>,
            <<"kind">> => <<"note">>, <<"noise">> => 1},
    ?assertEqual(#{<<"kind">> => <<"note">>},
                 barrel_embedding_policy:metadata(P, Doc)).
