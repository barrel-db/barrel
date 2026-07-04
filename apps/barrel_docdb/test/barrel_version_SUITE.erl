%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_version: codec round-trips, token
%%% ordering (lexicographic == causal), the total order, and the
%%% commutative winner rule.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_version_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("hlc/include/hlc.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).

-export([codec_roundtrip/1,
         token_roundtrip/1,
         token_order_is_causal_order/1,
         compare_total_order/1,
         winner_rule_is_commutative/1,
         construction_and_accessors/1]).

all() ->
    [codec_roundtrip,
     token_roundtrip,
     token_order_is_causal_order,
     compare_total_order,
     winner_rule_is_commutative,
     construction_and_accessors].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Dir = "/tmp/barrel_version_test_"
        ++ integer_to_list(erlang:system_time(millisecond)),
    application:set_env(barrel_docdb, data_dir, Dir),
    [{dir, Dir} | Config].

end_per_suite(Config) ->
    os:cmd("rm -rf " ++ ?config(dir, Config)),
    ok.

%%====================================================================
%% Test cases
%%====================================================================

codec_roundtrip(_Config) ->
    rand:seed(exsss, {101, 102, 103}),
    lists:foreach(
        fun(_) ->
            V = random_version(),
            ?assertEqual(V, barrel_version:decode(barrel_version:encode(V)))
        end,
        lists:seq(1, 200)).

token_roundtrip(_Config) ->
    rand:seed(exsss, {201, 202, 203}),
    lists:foreach(
        fun(_) ->
            V = random_version(),
            Token = barrel_version:to_token(V),
            ?assertEqual(V, barrel_version:from_token(Token)),
            %% token shape: fixed-width hex, @, node
            [Hex, _Node] = binary:split(Token, <<"@">>),
            ?assertEqual(24, byte_size(Hex))
        end,
        lists:seq(1, 200)),
    ?assertError(badarg, barrel_version:from_token(<<"not a token">>)),
    ?assertError(badarg, barrel_version:from_token(<<"deadbeef@node">>)).

%% Lexicographic order of tokens equals causal (HLC-first) order.
token_order_is_causal_order(_Config) ->
    rand:seed(exsss, {301, 302, 303}),
    lists:foreach(
        fun(_) ->
            A = random_version(),
            B = random_version(),
            TokenOrder = compare_terms(barrel_version:to_token(A),
                                       barrel_version:to_token(B)),
            ?assertEqual(barrel_version:compare(A, B), TokenOrder)
        end,
        lists:seq(1, 500)).

compare_total_order(_Config) ->
    rand:seed(exsss, {401, 402, 403}),
    lists:foreach(
        fun(_) ->
            A = random_version(),
            B = random_version(),
            C = random_version(),
            %% antisymmetry
            ?assertEqual(invert(barrel_version:compare(A, B)),
                         barrel_version:compare(B, A)),
            %% eq only for identical versions
            case barrel_version:compare(A, B) of
                eq -> ?assertEqual(A, B);
                _ -> ok
            end,
            %% transitivity via sorting three elements
            Sorted = lists:sort(
                fun(X, Y) -> barrel_version:compare(X, Y) =/= gt end,
                [A, B, C]),
            [S1, S2, S3] = Sorted,
            ?assertNotEqual(gt, barrel_version:compare(S1, S2)),
            ?assertNotEqual(gt, barrel_version:compare(S2, S3)),
            ?assertNotEqual(gt, barrel_version:compare(S1, S3))
        end,
        lists:seq(1, 300)).

%% The winner of a sibling set is invariant under permutation: every
%% replica folding max over the same set picks the same version.
winner_rule_is_commutative(_Config) ->
    rand:seed(exsss, {501, 502, 503}),
    lists:foreach(
        fun(_) ->
            N = 2 + rand:uniform(6),
            Set = [random_version() || _ <- lists:seq(1, N)],
            [First | Rest] = Set,
            Winner = lists:foldl(fun barrel_version:max/2, First, Rest),
            lists:foreach(
                fun(_) ->
                    [F2 | R2] = shuffle(Set),
                    ?assertEqual(Winner,
                                 lists:foldl(fun barrel_version:max/2, F2, R2))
                end,
                lists:seq(1, 5))
        end,
        lists:seq(1, 100)).

construction_and_accessors(_Config) ->
    Author = <<"f1e061a70714abcd">>,
    Hlc = barrel_hlc:new_hlc(),
    V = barrel_version:new(Hlc, Author),
    ?assertEqual(Hlc, barrel_version:hlc(V)),
    ?assertEqual(Author, barrel_version:author(V)),
    %% same author, later HLC: strictly increasing
    V2 = barrel_version:new(barrel_hlc:new_hlc(), Author),
    ?assertEqual(lt, barrel_version:compare(V, V2)).

%%====================================================================
%% Helpers
%%====================================================================

random_version() ->
    Hlc = #timestamp{wall_time = rand:uniform(1 bsl 40),
                     logical = rand:uniform(1000) - 1},
    Node = lists:nth(rand:uniform(4),
                     [<<"node-a">>, <<"node-b">>, <<"host-1">>, <<"host-22">>]),
    {Hlc, Node}.

compare_terms(A, B) when A < B -> lt;
compare_terms(A, B) when A > B -> gt;
compare_terms(_, _) -> eq.

invert(lt) -> gt;
invert(gt) -> lt;
invert(eq) -> eq.

shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), X} || X <- List])].
