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
