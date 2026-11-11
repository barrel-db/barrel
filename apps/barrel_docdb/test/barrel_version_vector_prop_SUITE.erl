%%% @doc Random histories over three databases: local writes with and
%%% without a vector, deletes, replication in any direction and conflict
%%% resolutions. After every step, every live version of every document
%%% reports the vector its writer supplied, and so does every past version
%%% still readable. After a full sync all databases agree.
%%%
%%% Seeds are fixed, so a failure replays exactly; the seed and the steps
%%% are in the failure.
-module(barrel_version_vector_prop_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([random_histories/1]).

-define(SEEDS, lists:seq(1, 40)).
-define(STEPS, 40).
-define(DOCS, [<<"d1">>, <<"d2">>]).

all() -> [random_histories].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

random_histories(_Config) ->
    lists:foreach(fun run_seed/1, ?SEEDS).

run_seed(Seed) ->
    _ = rand:seed(exsss, {Seed, Seed * 7, Seed * 13}),
    Dbs = [<<"vvprop_", (integer_to_binary(Seed))/binary, "_", N/binary>>
           || N <- [<<"a">>, <<"b">>, <<"c">>]],
    [begin _ = barrel_docdb:delete_db(D), {ok, _} = barrel_docdb:create_db(D, #{}) end
     || D <- Dbs],
    try
        Expected = steps(?STEPS, Dbs, #{}, []),
        full_sync(Dbs),
        check_all(Dbs, Expected, {Seed, final}),
        converged(Dbs, {Seed, final})
    after
        [_ = barrel_docdb:delete_db(D) || D <- Dbs]
    end.

%% Expected: #{Token => Vector | none}, what each version's writer gave.
steps(0, _Dbs, Expected, _Log) ->
    Expected;
steps(N, Dbs, Expected0, Log0) ->
    Op = op(Dbs),
    Log = [Op | Log0],
    Expected = apply_op(Op, Expected0),
    check_all(Dbs, Expected, {seed_state(), lists:reverse(Log)}),
    steps(N - 1, Dbs, Expected, Log).

seed_state() -> rand:export_seed().

op(Dbs) ->
    Db = pick(Dbs),
    Doc = pick(?DOCS),
    case rand:uniform(10) of
        N when N =< 4 -> {put, Db, Doc, maybe_vector()};
        5 -> {delete, Db, Doc};
        N when N =< 8 -> {replicate, Db, pick(Dbs -- [Db])};
        9 -> {choose, Db, Doc};
        10 -> {merge, Db, Doc, maybe_vector()}
    end.

apply_op({put, Db, Id, Vec}, Exp) ->
    Doc0 = #{<<"id">> => Id, <<"n">> => rand:uniform(1000)},
    Doc1 = case current(Db, Id) of
        {ok, Cur} -> Doc0#{<<"_rev">> => Cur};
        none -> Doc0
    end,
    Doc = case Vec of none -> Doc1; _ -> Doc1#{<<"_embedding">> => Vec} end,
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(Db, Doc),
    Exp#{Rev => Vec};
apply_op({delete, Db, Id}, Exp) ->
    case barrel_docdb:get_doc(Db, Id) of
        {ok, #{<<"_rev">> := Rev}} ->
            {ok, #{<<"rev">> := Del}} = barrel_docdb:delete_doc(Db, Id, #{rev => Rev}),
            Exp#{Del => none};
        {error, not_found} ->
            Exp
    end;
apply_op({replicate, Src, Tgt}, Exp) ->
    {ok, _} = barrel_rep:replicate(Src, Tgt, #{attachments => false}),
    Exp;
apply_op({choose, Db, Id}, Exp) ->
    case conflict(Db, Id) of
        {Current, Live} ->
            Pick = pick(Live),
            {ok, #{rev := Rev}} =
                barrel_docdb:resolve_conflict(Db, Id, Current, {choose, Pick}),
            Exp#{Rev => maps:get(Pick, Exp, none)};
        none ->
            Exp
    end;
apply_op({merge, Db, Id, Vec}, Exp) ->
    case conflict(Db, Id) of
        {Current, _Live} ->
            Body0 = #{<<"merged">> => rand:uniform(1000)},
            Body = case Vec of none -> Body0; _ -> Body0#{<<"_embedding">> => Vec} end,
            {ok, #{rev := Rev}} =
                barrel_docdb:resolve_conflict(Db, Id, Current, {merge, Body}),
            Exp#{Rev => Vec};
        none ->
            Exp
    end.

%% A document in conflict: its current token and its live version tokens.
conflict(Db, Id) ->
    case barrel_docdb:get_doc_state(Db, Id) of
        {ok, #{state := State}} ->
            Live = barrel_live:live(barrel_live:decode_state(State)),
            Tokens = [barrel_version:to_token(T) || T <- maps:keys(Live)],
            case Tokens of
                [_, _ | _] ->
                    #{winner := W} = barrel_live:view(barrel_live:decode_state(State)),
                    {barrel_version:to_token(W), Tokens};
                _ ->
                    none
            end;
        {error, not_found} ->
            none
    end.

current(Db, Id) ->
    case barrel_docdb:get_doc_for_replication(Db, Id) of
        {ok, #{version := Rev}} -> {ok, Rev};
        {error, not_found} -> none
    end.

%% Every live version's vector (state sync view), the current one (read
%% view) and every readable past version match what their writer gave.
check_all(Dbs, Exp, Ctx) ->
    [check_doc(Db, Id, Exp, Ctx) || Db <- Dbs, Id <- ?DOCS],
    ok.

check_doc(Db, Id, Exp, Ctx) ->
    case barrel_docdb:get_doc_state(Db, Id) of
        {ok, #{state := State, vectors := Vectors}} ->
            Live = barrel_live:live(barrel_live:decode_state(State)),
            Want = maps:from_list(
                     [{Tok, V} || {T, #{deleted := false}} <- maps:to_list(Live),
                                  Tok <- [barrel_version:to_token(T)],
                                  V <- [maps:get(Tok, Exp, none)], V =/= none]),
            same({live, Db, Id, Ctx}, Want, Vectors),
            check_current(Db, Id, Exp, Ctx),
            check_past(Db, Id, Exp, Ctx);
        {error, not_found} ->
            ok
    end.

check_current(Db, Id, Exp, Ctx) ->
    case barrel_docdb:get_doc(Db, Id, #{include_embedding => true}) of
        {ok, #{<<"_rev">> := Rev} = Doc} ->
            Got = case maps:get(<<"_embedding">>, Doc, undefined) of
                #{<<"vector">> := V, <<"source">> := <<"client">>} -> V;
                undefined -> none
            end,
            same({current, Db, Id, Rev, Ctx}, maps:get(Rev, Exp, none), Got);
        {error, not_found} ->
            ok
    end.

check_past(Db, Id, Exp, Ctx) ->
    {ok, Versions} = barrel_docdb:get_doc_versions(Db, Id),
    [case barrel_docdb:get_version_body(Db, Id, Tok, #{include_embedding => true}) of
         {ok, Body} ->
             Got = case maps:get(<<"_embedding">>, Body, undefined) of
                 #{<<"vector">> := V, <<"source">> := <<"client">>} -> V;
                 undefined -> none
             end,
             same({past, Db, Id, Tok, Ctx}, maps:get(Tok, Exp, none), Got);
         {error, not_found} ->
             ok
     end || #{version := Tok, deleted := false} <- Versions],
    ok.

converged([A | Rest], Ctx) ->
    [begin
         [same({converged, A, B, Id, Ctx}, state_of(A, Id), state_of(B, Id))
          || Id <- ?DOCS]
     end || B <- Rest],
    ok.

state_of(Db, Id) ->
    case barrel_docdb:get_doc_state(Db, Id) of
        {ok, #{digest := D, vectors := V}} -> {D, V};
        {error, not_found} -> none
    end.

full_sync([A, B, C] = Dbs) ->
    [{ok, _} = barrel_rep:replicate(S, T, #{attachments => false})
     || {S, T} <- [{A, B}, {B, C}, {C, A}, {A, B}, {B, C}]],
    _ = Dbs,
    ok.

same(_What, X, X) -> ok;
same(What, Want, Got) -> ct:fail({mismatch, What, {want, Want}, {got, Got}}).

maybe_vector() ->
    case rand:uniform(3) of
        1 -> none;
        _ -> [rand:uniform(64) / 4, rand:uniform(64) / 4]
    end.

pick(L) -> lists:nth(rand:uniform(length(L)), L).
