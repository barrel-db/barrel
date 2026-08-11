%%%-------------------------------------------------------------------
%%% @doc EUnit tests for regex -> trigram query.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ngram_regex_tests).

-include_lib("eunit/include/eunit.hrl").

-define(M, barrel_ngram_regex).

gram(A, B, C) -> (A bsl 16) bor (B bsl 8) bor C.

grams(Bin) ->
    N = byte_size(Bin),
    case N >= 3 of
        true -> ordsets:from_list(
                  [begin <<_:I/binary, A, B, C, _/binary>> = Bin, gram(A, B, C) end
                   || I <- lists:seq(0, N - 3)]);
        false -> ordsets:new()
    end.

and_of(Bin) -> {'and', [{gram, G} || G <- ordsets:to_list(grams(Bin))]}.

%%====================================================================
%% literal_runs/1
%%====================================================================

literal_runs_bare_literal_test() ->
    ?assertEqual([#{bytes => <<"abc">>, prefix_max => 0, suffix_max => 0}],
                 ?M:literal_runs(?M:parse(<<"abc">>))).

%% Prefix/SuffixMax sum the width of EVERYTHING on that side of the
%% chain, not just the gap to the next literal -- the window has to cover
%% the whole match, and another literal run is itself part of what must
%% be present and checked. So "foo"'s suffix includes "." (conservatively
%% 1-4 bytes, see width_bound/1's own moduledoc: upper bound 4) AND
%% "bar" (3 bytes) = 7; symmetrically "bar"'s prefix is "foo" (3) + "."
%% (4) = 7.
literal_runs_chain_prefix_suffix_test() ->
    Runs = ?M:literal_runs(?M:parse(<<"foo.bar">>)),
    ?assertEqual([#{bytes => <<"foo">>, prefix_max => 0, suffix_max => 7},
                  #{bytes => <<"bar">>, prefix_max => 7, suffix_max => 0}],
                 Runs).

%% Two single-char classes ({2} exactly) ahead of "foobar": each class is
%% conservatively 1-4 bytes, so PrefixMax is 8 (2*4), not 2, even though
%% real ASCII digits would only ever consume 2 bytes.
literal_runs_bounded_prefix_test() ->
    [Run] = ?M:literal_runs(?M:parse(<<"[0-9]{2}foobar">>)),
    ?assertEqual(<<"foobar">>, maps:get(bytes, Run)),
    ?assertEqual(8, maps:get(prefix_max, Run)),
    ?assertEqual(0, maps:get(suffix_max, Run)).

%% An unbounded quantifier ahead of the literal makes that side unbounded.
literal_runs_unbounded_prefix_test() ->
    [Run] = ?M:literal_runs(?M:parse(<<".*foo">>)),
    ?assertEqual(unbounded, maps:get(prefix_max, Run)),
    ?assertEqual(0, maps:get(suffix_max, Run)).

%% A top-level alternation has no single definite anchor -- a real match
%% could come from either branch.
literal_runs_top_level_alternation_is_ineligible_test() ->
    ?assertEqual(ineligible, ?M:literal_runs(?M:parse(<<"foo|bar">>))).

%% An alternation nested as a sibling inside a chain: the whole chain is
%% ineligible (deliberately conservative -- see literal_runs/1's doc),
%% even though "foo" and "qux" are themselves unambiguous literals.
literal_runs_nested_alternation_is_ineligible_test() ->
    ?assertEqual(ineligible, ?M:literal_runs(?M:parse(<<"foo(bar|baz)qux">>))).

%% A genuine multi-node chain (build_cat unwraps a single-node chain to
%% the bare node itself, so this needs >= 2 non-literal atoms to actually
%% produce a {cat, _}) with no literal runs at all -- eligible (it IS a
%% chain), just nothing to anchor on.
literal_runs_no_literal_in_chain_is_empty_test() ->
    ?assertEqual([], ?M:literal_runs(?M:parse(<<".+.*">>))).

%%====================================================================
%% Leading inline modifiers ( (?i) (?s) (?m), whole-pattern only )
%%====================================================================

leading_flags(Bin) ->
    {ok, _Node, _Query, #{leading_flags := Flags}} = ?M:analyze(Bin),
    Flags.

no_leading_modifier_is_empty_flags_test() ->
    ?assertEqual([], leading_flags(<<"connect_timeout">>)).

leading_i_reports_caseless_test() ->
    ?assertEqual([caseless], leading_flags(<<"(?i)connect_timeout">>)).

leading_s_reports_dotall_test() ->
    ?assertEqual([dotall], leading_flags(<<"(?s)connect_timeout">>)).

leading_m_reports_multiline_test() ->
    ?assertEqual([multiline], leading_flags(<<"(?m)connect_timeout">>)).

leading_ism_reports_all_three_test() ->
    ?assertEqual(lists:sort([caseless, dotall, multiline]),
                 lists:sort(leading_flags(<<"(?ism)connect_timeout">>))).

%% The modifier group itself is stripped from the body the parser sees --
%% it does not leak into the AST/trigram query as literal text.
leading_i_is_stripped_from_body_test() ->
    ?assertEqual(and_of(<<"connect_timeout">>), ?M:trigram_query(<<"(?i)connect_timeout">>)).

%%====================================================================
%% Analysis of representative patterns
%%====================================================================

literal_run_test() ->
    ?assertEqual(and_of(<<"abcdef">>), ?M:trigram_query(<<"abcdef">>)).

trailing_class_test() ->
    %% connect_\w+  ->  the trigrams of "connect_" (\w+ contributes nothing)
    ?assertEqual(and_of(<<"connect_">>), ?M:trigram_query(<<"connect_\\w+">>)).

alternation_test() ->
    Q = ?M:trigram_query(<<"foo|bar|baz">>),
    ?assertMatch({'or', _}, Q),
    {'or', Branches} = Q,
    ?assertEqual(lists:sort([{gram, gram($f, $o, $o)}, {gram, gram($b, $a, $r)},
                             {gram, gram($b, $a, $z)}]),
                 lists:sort(Branches)).

dot_is_all_test() ->
    ?assertEqual(all, ?M:trigram_query(<<"a.c">>)),
    ?assertEqual(all, ?M:trigram_query(<<"...">>)).

star_is_all_test() ->
    ?assertEqual(all, ?M:trigram_query(<<"x*">>)),
    ?assertEqual(all, ?M:trigram_query(<<"(abc)*">>)).

plus_keeps_child_test() ->
    %% (abc)+ must contain "abc"
    ?assertEqual({gram, gram($a, $b, $c)}, ?M:trigram_query(<<"(abc)+">>)).

concat_across_dot_test() ->
    %% "abc.def" -> abc AND def (the boundary trigrams are lost, still sound)
    ?assertEqual({'and', lists:sort([{gram, gram($a, $b, $c)}, {gram, gram($d, $e, $f)}])},
                 ?M:trigram_query(<<"abc.def">>)).

escaped_metachar_is_literal_test() ->
    %% \. is a literal dot; "a\.b\.c" has literal dots
    ?assertEqual(and_of(<<"a.b.c">>), ?M:trigram_query(<<"a\\.b\\.c">>)).

anchors_are_all_over_short_test() ->
    ?assertEqual(all, ?M:trigram_query(<<"^ab$">>)),
    ?assertEqual(and_of(<<"abcd">>), ?M:trigram_query(<<"^abcd$">>)).

empty_regex_test() ->
    ?assertEqual(all, ?M:trigram_query(<<>>)).

%%====================================================================
%% Soundness property: any text re:run matches must satisfy the query
%%====================================================================

soundness_property_test_() ->
    {timeout, 60, fun soundness_property/0}.

soundness_property() ->
    lists:foreach(
        fun(Seed) ->
            rand:seed(exsss, {Seed, Seed * 3 + 1, Seed * 7 + 5}),
            Regex = gen_regex(3),
            case re:compile(Regex) of
                {ok, RE} ->
                    Query = ?M:trigram_query(Regex),
                    lists:foreach(
                        fun(_) ->
                            Text = random_text(2 + rand:uniform(12)),
                            case re:run(Text, RE, [{capture, none}]) of
                                match ->
                                    ?assert(satisfies(Query, grams(Text)));
                                nomatch ->
                                    ok
                            end
                        end, lists:seq(1, 30));
                {error, _} ->
                    ok
            end
        end, lists:seq(1, 300)).

satisfies(all, _) -> true;
satisfies(none, _) -> false;
satisfies({gram, G}, S) -> ordsets:is_element(G, S);
satisfies({'and', Qs}, S) -> lists:all(fun(Q) -> satisfies(Q, S) end, Qs);
satisfies({'or', Qs}, S) -> lists:any(fun(Q) -> satisfies(Q, S) end, Qs).

%%====================================================================
%% Random regex + text generators (safe subset over alphabet a-d)
%%====================================================================

gen_regex(0) ->
    gen_atom();
gen_regex(D) ->
    case rand:uniform(6) of
        1 -> gen_atom();
        2 -> <<(gen_regex(D - 1))/binary, (gen_regex(D - 1))/binary>>;   %% concat
        3 -> <<(gen_regex(D - 1))/binary, $|, (gen_regex(D - 1))/binary>>; %% alt
        4 -> <<$(, (gen_regex(D - 1))/binary, $), (quant())>>;           %% group+quant
        5 -> <<(gen_atom())/binary, (quant())>>;
        6 -> <<(gen_regex(D - 1))/binary, (gen_regex(D - 1))/binary>>
    end.

gen_atom() ->
    case rand:uniform(5) of
        1 -> <<($a + rand:uniform(4) - 1)>>;
        2 -> <<$.>>;
        3 -> list_to_binary([$a + rand:uniform(4) - 1 || _ <- lists:seq(1, 2)]);
        4 -> list_to_binary([$a + rand:uniform(4) - 1 || _ <- lists:seq(1, 3)]);
        5 -> list_to_binary([$a + rand:uniform(4) - 1 || _ <- lists:seq(1, 4)])
    end.

quant() ->
    case rand:uniform(3) of
        1 -> $*;
        2 -> $+;
        3 -> $?
    end.

random_text(N) ->
    list_to_binary([$a + rand:uniform(4) - 1 || _ <- lists:seq(1, N)]).
